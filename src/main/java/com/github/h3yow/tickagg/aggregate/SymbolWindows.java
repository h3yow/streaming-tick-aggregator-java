package com.github.h3yow.tickagg.aggregate;

import com.github.h3yow.tickagg.config.AggregatorProperties;
import com.github.h3yow.tickagg.model.IngestionResult;
import com.github.h3yow.tickagg.model.OhlcBar;
import com.github.h3yow.tickagg.model.RejectionReason;
import com.github.h3yow.tickagg.model.TickEvent;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
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
 *   <li>eventId already accepted &rarr; DUPLICATE (idempotent, no state change);</li>
 *   <li>sequence &le; sequenceFloor (max sequence of any already-confirmed
 *       window) &rarr; SEQUENCE_REGRESSION;</li>
 *   <li>eventTs &lt; watermark - allowedLateness &rarr; LATE_BEYOND_ALLOWED_LATENESS;</li>
 *   <li>otherwise the event joins the tumbling window of its event time and
 *       the watermark advances to max(eventTs).</li>
 * </ol>
 * A window is <b>confirmed</b> (immutable, safe to serve as final) once
 * watermark - allowedLateness &gt;= windowEnd. Memory is bounded by evicting
 * the oldest confirmed windows and the oldest dedup entries.
 */
final class SymbolWindows {

    private final String symbol;
    private final AggregatorProperties props;
    private final TreeMap<Long, WindowState> windows = new TreeMap<>();
    private final LinkedHashSet<String> eventIds = new LinkedHashSet<>();

    private long watermarkMs = Long.MIN_VALUE;
    private long sequenceFloor = -1L;
    /** Window starts already folded into sequenceFloor; cursor only moves forward. */
    private final NavigableSet<Long> floorFoldedWindows = new TreeSet<>();

    SymbolWindows(String symbol, AggregatorProperties props) {
        this.symbol = symbol;
        this.props = props;
    }

    synchronized IngestionResult ingest(TickEvent event) {
        // Rule 1: idempotent dedup. A redelivery must not change any state.
        if (eventIds.contains(event.eventId())) {
            return IngestionResult.duplicate(
                    "eventId=" + event.eventId() + " already accepted for symbol=" + symbol);
        }

        // Rule 2: sequence regression relative to confirmed (final) windows.
        // Only confirmed windows count, so the rule is stable under reordering
        // of events that are still inside the allowed-lateness horizon.
        if (event.sequence() <= sequenceFloor) {
            return IngestionResult.rejected(RejectionReason.SEQUENCE_REGRESSION,
                    "symbol=" + symbol + " sequence=" + event.sequence()
                            + " <= sequenceFloor=" + sequenceFloor
                            + " (max sequence of confirmed windows)");
        }

        // Rule 3: lateness relative to the watermark.
        long latenessFloor = watermarkMs == Long.MIN_VALUE
                ? Long.MIN_VALUE
                : watermarkMs - props.getAllowedLatenessMs();
        if (event.timestampEpochMs() < latenessFloor) {
            return IngestionResult.rejected(RejectionReason.LATE_BEYOND_ALLOWED_LATENESS,
                    "symbol=" + symbol + " eventTs=" + event.timestampEpochMs()
                            + " < watermark=" + watermarkMs
                            + " - allowedLatenessMs=" + props.getAllowedLatenessMs());
        }

        // Rule 4: route into the tumbling window derived from event time.
        long windowStart = floorToWindow(event.timestampEpochMs(), props.getWindowSizeMs());
        WindowState state = windows.computeIfAbsent(
                windowStart, ws -> new WindowState(symbol, ws, props.getWindowSizeMs()));
        state.add(event);

        // Commit bookkeeping only after successful aggregation, so a rejection
        // never mutates any state.
        rememberEventId(event.eventId());
        if (event.timestampEpochMs() > watermarkMs) {
            watermarkMs = event.timestampEpochMs();
        }
        promoteSequenceFloor();
        evictIfNeeded();

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
        if (watermarkMs == Long.MIN_VALUE) {
            return false;
        }
        return watermarkMs - props.getAllowedLatenessMs() >= windowStart + props.getWindowSizeMs();
    }

    private void promoteSequenceFloor() {
        for (Map.Entry<Long, WindowState> e : windows.entrySet()) {
            long start = e.getKey();
            if (floorFoldedWindows.contains(start)) {
                continue;
            }
            if (!isWindowConfirmed(start)) {
                break; // windows are ordered; later ones cannot be confirmed either
            }
            sequenceFloor = Math.max(sequenceFloor, e.getValue().maxSequence());
            floorFoldedWindows.add(start);
        }
    }

    private void rememberEventId(String eventId) {
        eventIds.add(eventId);
        int max = props.getMaxDedupEntriesPerSymbol();
        while (eventIds.size() > max) {
            Iterator<String> it = eventIds.iterator();
            it.next();
            it.remove();
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
        }
    }

    static long floorToWindow(long timestampEpochMs, long windowSizeMs) {
        return Math.floorDiv(timestampEpochMs, windowSizeMs) * windowSizeMs;
    }
}
