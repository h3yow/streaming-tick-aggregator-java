package com.github.h3yow.tickagg.aggregate;

import com.github.h3yow.tickagg.config.AggregatorProperties;
import com.github.h3yow.tickagg.model.IngestionResult;
import com.github.h3yow.tickagg.model.IngestionStatus;
import com.github.h3yow.tickagg.model.OhlcBar;
import com.github.h3yow.tickagg.model.RejectedTickRecord;
import com.github.h3yow.tickagg.model.RejectionReason;
import com.github.h3yow.tickagg.model.ReprocessResult;
import com.github.h3yow.tickagg.model.SymbolStats;
import com.github.h3yow.tickagg.model.TickEvent;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.EnumMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Per-symbol aggregation state. All public methods synchronize on this
 * instance, so a symbol's state machine is single-threaded and readers
 * never observe a half-updated window.
 *
 * Decision rules, evaluated in this exact order (first match wins, so the
 * outcome of any event is deterministic and explainable):
 * <ol>
 *   <li>eventId already accepted &rarr; DUPLICATE (idempotent, no state change);</li>
 *   <li>sequence &le; sequenceFloor (max sequence of any already-confirmed
 *       window) &rarr; SEQUENCE_REGRESSION;</li>
 *   <li>eventTs &lt; watermark - allowedLateness &rarr; LATE_BEYOND_ALLOWED_LATENESS;</li>
 *   <li>target window already confirmed &rarr; WINDOW_CLOSED (confirmed
 *       results are immutable, even after the lateness config is widened);</li>
 *   <li>otherwise the event joins the tumbling window of its event time and
 *       the watermark advances to max(eventTs).</li>
 * </ol>
 * Confirmation is <b>sticky</b>: {@code confirmedHorizonMs} only advances
 * (it is the max of {@code watermark - allowedLateness} ever observed), so
 * widening the lateness config at runtime can never un-confirm a window that
 * was already served as final. A window is confirmed once
 * {@code windowEnd <= confirmedHorizonMs}; confirmed windows are immutable.
 *
 * Memory is bounded by evicting the oldest confirmed windows and the oldest
 * dedup entries <b>whose window is already confirmed</b>. A dedup entry
 * pinned to an unconfirmed window is never evicted: that is what guarantees
 * exactly-once semantics even when the configured dedup cap is exceeded by
 * delivery volume, because a redelivery whose entry was evicted would
 * otherwise be re-counted into a still-mutable window. Redeliveries of
 * evicted entries land on confirmed windows and are rejected by rule 4
 * without touching any state.
 */
final class SymbolWindows {

    private final String symbol;
    private final AggregatorProperties props;
    private final TreeMap<Long, WindowState> windows = new TreeMap<>();
    /** eventId -> windowStart it was counted into; insertion-ordered. */
    private final LinkedHashMap<String, Long> eventIds = new LinkedHashMap<>();
    /**
     * Bounded FIFO retention of aggregator-level rejections, kept for audit
     * and later reprocessing. Eviction here never touches window state, so
     * confirmed results are unaffected no matter how small the cap is.
     */
    private final Deque<RejectedTickRecord> retainedRejections = new ArrayDeque<>();

    private long watermarkMs = Long.MIN_VALUE;
    private long sequenceFloor = -1L;
    /** Sticky confirmation horizon: max(watermark - allowedLateness) ever reached. */
    private long confirmedHorizonMs = Long.MIN_VALUE;
    /** Windows with start <= this cursor are already folded into sequenceFloor. */
    private long floorFoldedUpToStart = Long.MIN_VALUE;

    // Counters, only mutated under this instance's monitor.
    private long acceptedTotal;
    private long duplicateTotal;
    private final EnumMap<RejectionReason, Long> rejectionsByReason = new EnumMap<>(RejectionReason.class);
    private long windowsEvictedTotal;
    private long dedupEvictedTotal;
    private long rejectionsEvictedTotal;
    private long reprocessRunsTotal;
    private long reprocessAcceptedTotal;

    SymbolWindows(String symbol, AggregatorProperties props) {
        this.symbol = symbol;
        this.props = props;
    }

    synchronized IngestionResult ingest(TickEvent event) {
        // Rule 1: idempotent dedup. A redelivery must not change any state.
        if (eventIds.containsKey(event.eventId())) {
            duplicateTotal++;
            return IngestionResult.duplicate(
                    "eventId=" + event.eventId() + " already accepted for symbol=" + symbol);
        }

        // Rule 2: sequence regression relative to confirmed (final) windows.
        // Only confirmed windows count, so the rule is stable under reordering
        // of events that are still inside the allowed-lateness horizon.
        if (event.sequence() <= sequenceFloor) {
            return rejectAndRetain(event, RejectionReason.SEQUENCE_REGRESSION,
                    "symbol=" + symbol + " sequence=" + event.sequence()
                            + " <= sequenceFloor=" + sequenceFloor
                            + " (max sequence of confirmed windows)");
        }

        // Rule 3: lateness relative to the watermark.
        long latenessFloor = watermarkMs == Long.MIN_VALUE
                ? Long.MIN_VALUE
                : watermarkMs - props.getAllowedLatenessMs();
        if (event.timestampEpochMs() < latenessFloor) {
            return rejectAndRetain(event, RejectionReason.LATE_BEYOND_ALLOWED_LATENESS,
                    "symbol=" + symbol + " eventTs=" + event.timestampEpochMs()
                            + " < watermark=" + watermarkMs
                            + " - allowedLatenessMs=" + props.getAllowedLatenessMs());
        }

        // Rule 4: the target window is already confirmed (final). This can
        // only happen after the lateness config was widened past a horizon
        // that had previously confirmed the window. Confirmed results are
        // immutable, so the event is rejected rather than folded in.
        long windowStart = floorToWindow(event.timestampEpochMs(), props.getWindowSizeMs());
        if (isWindowConfirmed(windowStart)) {
            return rejectAndRetain(event, RejectionReason.WINDOW_CLOSED,
                    "symbol=" + symbol + " windowStart=" + windowStart
                            + " already confirmed (confirmedHorizon=" + confirmedHorizonMs + ")"
                            + "; confirmed results are immutable");
        }

        // Rule 5: route into the tumbling window derived from event time.
        WindowState state = windows.computeIfAbsent(
                windowStart, ws -> new WindowState(symbol, ws, props.getWindowSizeMs()));
        state.add(event);

        // Commit bookkeeping only after successful aggregation, so a rejection
        // never mutates any state. The watermark and confirmation horizon
        // advance before dedup bookkeeping, so this event's own contribution
        // is already reflected when eviction decisions are made.
        if (event.timestampEpochMs() > watermarkMs) {
            watermarkMs = event.timestampEpochMs();
        }
        advanceConfirmedHorizon();
        promoteSequenceFloor();
        rememberEventId(event.eventId(), windowStart);
        evictIfNeeded();

        acceptedTotal++;
        return IngestionResult.accepted(
                "symbol=" + symbol + " windowStart=" + windowStart
                        + " watermark=" + watermarkMs
                        + " sequenceFloor=" + sequenceFloor
                        + " windowConfirmed=" + isWindowConfirmed(windowStart));
    }

    synchronized List<OhlcBar> query(long fromEpochMs, long toEpochMs, boolean includeUnconfirmed) {
        List<OhlcBar> out = new ArrayList<>();
        for (Map.Entry<Long, WindowState> e : windows.entrySet()) {
            long start = e.getKey();
            long end = start + props.getWindowSizeMs();
            if (end <= fromEpochMs || start >= toEpochMs) {
                continue;
            }
            boolean confirmed = isWindowConfirmed(start);
            if (!confirmed && !includeUnconfirmed) {
                continue;
            }
            // Snapshot taken under the lock: no intermediate state escapes.
            out.add(e.getValue().snapshot(confirmed));
        }
        return out;
    }

    private boolean isWindowConfirmed(long windowStart) {
        return windowStart + props.getWindowSizeMs() <= confirmedHorizonMs;
    }

    /**
     * Rejects the event and appends it to the bounded retention deque with
     * the full decision context. Retention is a side store: adding or
     * evicting records never touches windows, dedup or the watermark, so
     * confirmed results and live ingestion are unaffected by the cap.
     */
    private IngestionResult rejectAndRetain(TickEvent event, RejectionReason reason, String detail) {
        rejectionsByReason.merge(reason, 1L, Long::sum);
        retain(new RejectedTickRecord(event, reason, detail,
                watermarkMs, sequenceFloor, props.getAllowedLatenessMs(),
                System.currentTimeMillis()));
        return IngestionResult.rejected(reason, detail);
    }

    private void retain(RejectedTickRecord record) {
        retainedRejections.addLast(record);
        int max = props.getMaxRetainedRejectionsPerSymbol();
        while (retainedRejections.size() > max) {
            retainedRejections.pollFirst(); // FIFO: drop the oldest record
            rejectionsEvictedTotal++;
        }
    }

    /**
     * Returns retained rejections whose event time falls in
     * {@code [fromEpochMs, toEpochMs)}. Snapshot copied under the lock, so
     * the result is consistent and never reflects a half-applied update.
     */
    synchronized List<RejectedTickRecord> queryRejections(long fromEpochMs, long toEpochMs) {
        List<RejectedTickRecord> out = new ArrayList<>();
        for (RejectedTickRecord r : retainedRejections) {
            long ts = r.event().timestampEpochMs();
            if (ts >= fromEpochMs && ts < toEpochMs) {
                out.add(r);
            }
        }
        return out;
    }

    /**
     * Reprocesses the rejections retained so far through the exact same
     * decision path as live ingestion. Intended use: the operator widens
     * {@code allowedLatenessMs}, then triggers this pass so previously
     * rejected late events get another chance under the new horizon.
     *
     * Guarantees:
     * <ul>
     *   <li>Events are replayed one at a time and the lock is released
     *       between events, so live ingestion and queries keep working and
     *       never observe a half-applied event;</li>
     *   <li>each event goes through dedup, so an event can never be counted
     *       twice and repeating this pass is a no-op for events already
     *       accepted (idempotent);</li>
     *   <li>events whose window is already confirmed are rejected
     *       (WINDOW_CLOSED) and re-retained with the fresh decision context:
     *       confirmed results are never rewritten;</li>
     *   <li>the result is independent of the order in which retained events
     *       are replayed, because window aggregation is order-independent
     *       and the watermark/horizon only move monotonically.</li>
     * </ul>
     * Only the records present when the pass starts are scanned (rejected
     * ones re-retain themselves at the tail), so the pass always terminates.
     */
    ReprocessResult reprocess() {
        int pending;
        synchronized (this) {
            pending = retainedRejections.size();
        }
        int scanned = 0, accepted = 0, alreadyCounted = 0, stillRejected = 0;
        Map<RejectionReason, Long> remaining = new EnumMap<>(RejectionReason.class);
        for (int i = 0; i < pending; i++) {
            RejectedTickRecord next;
            synchronized (this) {
                next = retainedRejections.pollFirst();
            }
            if (next == null) {
                break; // a concurrent pass drained the queue; nothing left to do
            }
            scanned++;
            IngestionResult r = ingest(next.event());
            if (r.status() == IngestionStatus.ACCEPTED) {
                accepted++;
            } else if (r.status() == IngestionStatus.DUPLICATE) {
                alreadyCounted++; // counted already (e.g. redelivered live): drop from retention
            } else {
                // ingest() already re-retained the event with the fresh context.
                stillRejected++;
                remaining.merge(r.rejectionReason(), 1L, Long::sum);
            }
        }
        synchronized (this) {
            reprocessRunsTotal++;
            reprocessAcceptedTotal += accepted;
        }
        return new ReprocessResult(symbol, props.getAllowedLatenessMs(),
                scanned, accepted, alreadyCounted, stillRejected, remaining);
    }

    /** Sticky: the horizon never retreats, even if the lateness config widens. */
    private void advanceConfirmedHorizon() {
        if (watermarkMs == Long.MIN_VALUE) {
            return;
        }
        long candidate = watermarkMs - props.getAllowedLatenessMs();
        if (candidate > confirmedHorizonMs) {
            confirmedHorizonMs = candidate;
        }
    }

    private void promoteSequenceFloor() {
        for (Map.Entry<Long, WindowState> e : windows.entrySet()) {
            long start = e.getKey();
            if (start <= floorFoldedUpToStart) {
                continue;
            }
            if (!isWindowConfirmed(start)) {
                break; // windows are ordered; later ones cannot be confirmed either
            }
            sequenceFloor = Math.max(sequenceFloor, e.getValue().maxSequence());
            floorFoldedUpToStart = start;
        }
    }

    private void rememberEventId(String eventId, long windowStart) {
        eventIds.put(eventId, windowStart);
        evictDedupIfNeeded();
    }

    /**
     * Evicts eldest dedup entries, but only while the eldest entry's window
     * is already confirmed: a redelivery of such an event is rejected by
     * rule 4 even without the entry, so eviction cannot cause double
     * counting. Entries pinned to unconfirmed windows are kept even if the
     * configured cap is temporarily exceeded; they become evictable as the
     * confirmation horizon advances, so long-run occupancy stays bounded.
     */
    private void evictDedupIfNeeded() {
        int max = props.getMaxDedupEntriesPerSymbol();
        Iterator<Map.Entry<String, Long>> it = eventIds.entrySet().iterator();
        while (eventIds.size() > max && it.hasNext()) {
            Map.Entry<String, Long> eldest = it.next();
            if (!isWindowConfirmed(eldest.getValue())) {
                break;
            }
            it.remove();
            dedupEvictedTotal++;
        }
    }

    private void evictIfNeeded() {
        int max = props.getMaxRetainedWindowsPerSymbol();
        while (windows.size() > max) {
            Map.Entry<Long, WindowState> eldest = windows.firstEntry();
            // Only immutable confirmed windows may be dropped.
            if (!isWindowConfirmed(eldest.getKey())) {
                break;
            }
            windows.pollFirstEntry();
            windowsEvictedTotal++;
        }
    }

    static long floorToWindow(long timestampEpochMs, long windowSizeMs) {
        return Math.floorDiv(timestampEpochMs, windowSizeMs) * windowSizeMs;
    }

    /**
     * Assembles a consistent point-in-time snapshot of this symbol's runtime
     * state. Runs under the monitor, so the counters and sizes are mutually
     * consistent; the lock is held only for the duration of the copy.
     */
    synchronized SymbolStats stats() {
        int confirmed = 0;
        for (Long start : windows.keySet()) {
            if (isWindowConfirmed(start)) {
                confirmed++;
            }
        }
        return new SymbolStats(symbol, watermarkMs, sequenceFloor, confirmedHorizonMs,
                props.getAllowedLatenessMs(),
                windows.size(), confirmed, eventIds.size(), retainedRejections.size(),
                acceptedTotal, duplicateTotal,
                new EnumMap<>(rejectionsByReason),
                windowsEvictedTotal, dedupEvictedTotal, rejectionsEvictedTotal,
                reprocessRunsTotal, reprocessAcceptedTotal);
    }
}
