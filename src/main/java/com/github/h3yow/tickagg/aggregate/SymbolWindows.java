package com.github.h3yow.tickagg.aggregate;

import com.github.h3yow.tickagg.config.AggregatorProperties;
import com.github.h3yow.tickagg.model.IngestionResult;
import com.github.h3yow.tickagg.model.OhlcBar;
import com.github.h3yow.tickagg.model.RecomputeOutcome;
import com.github.h3yow.tickagg.model.RejectionReason;
import com.github.h3yow.tickagg.model.RetainedRejection;
import com.github.h3yow.tickagg.model.SymbolStats;
import com.github.h3yow.tickagg.model.TickEvent;


import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Per-symbol aggregation state. All public methods synchronize on this
 * instance, so a symbol's state machine is single-threaded and readers
 * never observe a half-updated window.
 *
 * Decision rules, evaluated in this exact order (first match wins, so the
 * outcome of any event is deterministic and explainable):
 * <ol>
 *   <li>eventId already counted in an unsealed window &rarr; DUPLICATE
 *       (idempotent, no state change). Event ids of windows that have been
 *       sealed (published as confirmed) are reclaimed as a batch; a
 *       redelivery afterwards is caught by rule 2 instead, so an event can
 *       never be counted a second time regardless of retention capacities.</li>
 *   <li>sequence &le; sequenceFloor (max sequence folded from sealed
 *       windows) &rarr; SEQUENCE_REGRESSION;</li>
 *   <li>eventTs &lt; watermark - allowedLateness &rarr;
 *       LATE_BEYOND_ALLOWED_LATENESS. The rejection is retained in a
 *       separate bounded store (its eviction never touches windows or
 *       dedup) so it can be inspected and later recomputed;</li>
 *   <li>otherwise the event joins the tumbling window of its event time and
 *       the watermark advances to max(eventTs).</li>
 * </ol>
 *
 * A window is <b>sealed</b> the first time it becomes externally confirmed;
 * the seal is a permanent latch, so relaxing allowed lateness or
 * recomputing retained events never rewrites a published result.
 */
final class SymbolWindows {

    private final String symbol;
    private final AggregatorProperties props;
    private final TreeMap<Long, WindowState> windows = new TreeMap<>();
    private final RejectedEventRetention retention;

    /** Event ids currently counted in unsealed windows, indexed for dedup. */
    private final Map<String, Long> idToWindow = new java.util.HashMap<>();
    /** Reverse index for bulk reclaim when a window seals. */
    private final Map<Long, Set<String>> idsByWindow = new java.util.HashMap<>();

    private long watermarkMs = Long.MIN_VALUE;
    private long sequenceFloor = -1L;
    /**
     * Lateness actually in force for this symbol. Starts from the configured
     * default and may only be widened at runtime (never narrowed, since a
     * narrower horizon would contradict events already accepted).
     */
    private long effectiveLatenessMs;
    /** Window starts already folded into sequenceFloor; cursor only moves forward. */
    private final NavigableSet<Long> floorFoldedWindows = new TreeSet<>();

    private long acceptedCount;
    private long duplicateCount;
    private long rejectedLateCount;
    private long rejectedSequenceRegressionCount;
    private long incorporatedCount;

    SymbolWindows(String symbol, AggregatorProperties props) {
        this.symbol = symbol;
        this.props = props;
        this.effectiveLatenessMs = props.getAllowedLatenessMs();
        this.retention = new RejectedEventRetention(props.getMaxRetainedRejectionsPerSymbol());
    }

    /** Widen this symbol's lateness; narrowing requests leave the value untouched. */
    synchronized void relaxAllowedLateness(long newAllowedLatenessMs) {
        if (newAllowedLatenessMs > effectiveLatenessMs) {
            effectiveLatenessMs = newAllowedLatenessMs;
        }
    }

    synchronized IngestionResult ingest(TickEvent event) {
        long allowedLateness = effectiveLatenessMs;

        // Rule 1: idempotent dedup against events counted in unsealed windows.
        // Entries are never evicted while their window is unsealed, so a small
        // dedup capacity cannot turn a redelivery into a freshly counted event.
        if (idToWindow.containsKey(event.eventId())) {
            duplicateCount++;
            return IngestionResult.duplicate(
                    "eventId=" + event.eventId() + " already counted in an unsealed window for symbol=" + symbol);
        }

        // Rule 2: sequence regression relative to sealed (final) windows.
        // This also deterministically catches redeliveries of events whose
        // window was sealed and whose ids were therefore reclaimed.
        if (event.sequence() <= sequenceFloor) {
            rejectedSequenceRegressionCount++;
            return IngestionResult.rejected(RejectionReason.SEQUENCE_REGRESSION,
                    "symbol=" + symbol + " sequence=" + event.sequence()
                            + " <= sequenceFloor=" + sequenceFloor
                            + " (max sequence of sealed windows)");
        }

        // Rule 3: lateness relative to the watermark.
        long latenessFloor = watermarkMs == Long.MIN_VALUE
                ? Long.MIN_VALUE
                : watermarkMs - allowedLateness;
        if (event.timestampEpochMs() < latenessFloor) {
            rejectedLateCount++;
            String detail = "symbol=" + symbol + " eventTs=" + event.timestampEpochMs()
                    + " < watermark=" + watermarkMs
                    + " - allowedLatenessMs=" + allowedLateness;
            retention.retain(new RetainedRejection(
                    event.eventId(), symbol, event.sequence(), event.timestampEpochMs(),
                    event.price(), event.volume(),
                    RejectionReason.LATE_BEYOND_ALLOWED_LATENESS, detail,
                    watermarkMs, latenessFloor, allowedLateness,
                    System.currentTimeMillis(), false));
            return IngestionResult.rejected(RejectionReason.LATE_BEYOND_ALLOWED_LATENESS, detail);
        }

        // Rule 4: route into the tumbling window derived from event time.
        long windowStart = floorToWindow(event.timestampEpochMs(), props.getWindowSizeMs());
        WindowState state = windows.computeIfAbsent(
                windowStart, ws -> new WindowState(symbol, ws, props.getWindowSizeMs()));
        state.add(event);

        // Commit bookkeeping only after successful aggregation, so a rejection
        // never mutates any state.
        rememberEventId(event.eventId(), windowStart);
        acceptedCount++;
        if (event.timestampEpochMs() > watermarkMs) {
            watermarkMs = event.timestampEpochMs();
        }
        sealConfirmedWindows();
        promoteSequenceFloor();
        evictIfNeeded();

        return IngestionResult.accepted(
                "symbol=" + symbol + " windowStart=" + windowStart
                        + " watermark=" + watermarkMs
                        + " sequenceFloor=" + sequenceFloor
                        + " windowSealed=" + state.isSealed());
    }

    /**
     * Replay retained late-rejected events under the current (relaxed)
     * lateness and fold them into the windows they would have joined if they
     * had arrived within the horizon. Runs atomically under this symbol's
     * lock, so concurrent ingestion/queries never see a half-applied pass.
     *
     * <p>Properties:
     * <ul>
     *   <li><b>as-if / order independent</b>: folding uses the same
     *       commutative {@link WindowState#add} as live ingestion, so the
     *       result equals on-time arrival regardless of replay order;</li>
     *   <li><b>published results are immutable</b>: sealed windows are
     *       skipped;</li>
     *   <li><b>exactly once</b>: a folded id enters the dedup index and the
     *       retained record is flagged; repeating the pass changes nothing;</li>
     *   <li>watermark is not moved: retained events are older than the
     *       watermark observed at their rejection.</li>
     * </ul>
     */
    synchronized RecomputeOutcome recompute() {
        long allowedLateness = effectiveLatenessMs;
        List<RetainedRejection> candidates = retention.all();
        List<RetainedRejection> incorporated = new ArrayList<>();
        int skippedIncorporated = 0;
        int skippedSealed = 0;
        int skippedDuplicate = 0;

        for (RetainedRejection r : candidates) {
            long windowStart = floorToWindow(r.eventTimestampEpochMs(), props.getWindowSizeMs());
            WindowState state = windows.get(windowStart);
            if (state != null && state.isSealed()) {
                // A published result must never be rewritten.
                skippedSealed++;
                continue;
            }
            if (r.incorporated()) {
                // This record was folded by an earlier pass: idempotent replay.
                skippedIncorporated++;
                continue;
            }
            if (idToWindow.containsKey(r.eventId())) {
                // The event is already counted (e.g. it later arrived live
                // within a widened horizon): it must never be double counted.
                skippedDuplicate++;
                continue;
            }

            if (state == null) {
                // The window had never been seen before; create it exactly as
                // it would have existed had the event arrived on time. The
                // seal sweep below then decides (under the current lateness)
                // whether it is already final or still open to later events.
                state = new WindowState(symbol, windowStart, props.getWindowSizeMs());
                windows.put(windowStart, state);
            }
            state.add(new TickEvent(r.eventId(), r.symbol(), r.sequence(),
                    r.eventTimestampEpochMs(), r.price(), r.volume()));
            rememberEventId(r.eventId(), windowStart);
            retention.markIncorporated(r.eventId());
            incorporatedCount++;
            incorporated.add(r);
        }

        sealConfirmedWindows();
        promoteSequenceFloor();
        evictIfNeeded();

        return new RecomputeOutcome(symbol, allowedLateness, candidates.size(),
                incorporated.size(), skippedIncorporated, skippedSealed, skippedDuplicate,
                List.copyOf(incorporated));
    }

    synchronized List<OhlcBar> query(long fromEpochMs, long toEpochMs, boolean includeUnconfirmed) {
        List<OhlcBar> out = new ArrayList<>();
        for (Map.Entry<Long, WindowState> e : windows.entrySet()) {
            long start = e.getKey();
            long end = start + props.getWindowSizeMs();
            if (end <= fromEpochMs || start >= toEpochMs) {
                continue;
            }
            // Once sealed, a window stays confirmed even after lateness is
            // relaxed: queries never contradict a previously published flag.
            boolean confirmed = e.getValue().isSealed() || isWindowConfirmed(start);
            if (!confirmed && !includeUnconfirmed) {
                continue;
            }
            // Snapshot taken under the lock: no intermediate state escapes.
            out.add(e.getValue().snapshot(confirmed));
        }
        return out;
    }

    synchronized List<RetainedRejection> queryRetained(long fromEpochMs, long toEpochMs) {
        return retention.query(fromEpochMs, toEpochMs);
    }

    /** Immutable, point-in-time bookkeeping snapshot; built under the lock. */
    synchronized SymbolStats stats() {
        int sealed = 0;
        for (WindowState s : windows.values()) {
            if (s.isSealed()) {
                sealed++;
            }
        }
        return new SymbolStats(symbol, watermarkMs, sequenceFloor, effectiveLatenessMs,
                windows.size(), sealed, idToWindow.size(), retention.size(),
                acceptedCount, duplicateCount, rejectedLateCount,
                rejectedSequenceRegressionCount, retention.evictedCount(), incorporatedCount);
    }

    private boolean isWindowConfirmed(long windowStart) {
        if (watermarkMs == Long.MIN_VALUE) {
            return false;
        }
        return watermarkMs - effectiveLatenessMs >= windowStart + props.getWindowSizeMs();
    }

    /** Latch every window that is confirmed under the current lateness. */
    private void sealConfirmedWindows() {
        for (Map.Entry<Long, WindowState> e : windows.entrySet()) {
            long start = e.getKey();
            WindowState state = e.getValue();
            if (state.isSealed()) {
                continue;
            }
            if (!isWindowConfirmed(start)) {
                break; // windows are ordered; later ones cannot be confirmed
            }
            state.seal();
            // Bulk-reclaim the window's dedup ids: rule 2 protects them now.
            Set<String> ids = idsByWindow.remove(start);
            if (ids != null) {
                for (String id : ids) {
                    idToWindow.remove(id);
                }
            }
        }
    }

    private void promoteSequenceFloor() {
        for (Map.Entry<Long, WindowState> e : windows.entrySet()) {
            long start = e.getKey();
            if (floorFoldedWindows.contains(start)) {
                continue;
            }
            if (!e.getValue().isSealed()) {
                break; // windows are ordered; later ones cannot be sealed either
            }
            sequenceFloor = Math.max(sequenceFloor, e.getValue().maxSequence());
            floorFoldedWindows.add(start);
        }
    }

    private void rememberEventId(String eventId, long windowStart) {
        // Entries for unsealed windows are retained until the window seals;
        // they are never FIFO-evicted, because that could turn a redelivery
        // into a double count. Long-term memory is still bounded: every
        // unsealed window seals as the watermark advances and reclaims its
        // ids wholesale.
        idToWindow.put(eventId, windowStart);
        idsByWindow.computeIfAbsent(windowStart, ws -> new HashSet<>()).add(eventId);
    }

    private void evictIfNeeded() {
        int max = props.getMaxRetainedWindowsPerSymbol();
        while (windows.size() > max) {
            Map.Entry<Long, WindowState> eldest = windows.firstEntry();
            // Only immutable sealed windows may be dropped.
            if (!eldest.getValue().isSealed()) {
                break;
            }
            windows.pollFirstEntry();
            // The window can never be visited again, so its floor cursor and
            // (already reclaimed at seal time) dedup entries go as well.
            floorFoldedWindows.remove(eldest.getKey());
        }
    }

    static long floorToWindow(long timestampEpochMs, long windowSizeMs) {
        return Math.floorDiv(timestampEpochMs, windowSizeMs) * windowSizeMs;
    }
}
