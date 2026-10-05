package com.github.h3yow.tickagg;

import com.github.h3yow.tickagg.aggregate.WindowAggregator;
import com.github.h3yow.tickagg.config.AggregatorProperties;
import com.github.h3yow.tickagg.model.IngestionResult;
import com.github.h3yow.tickagg.model.IngestionStatus;
import com.github.h3yow.tickagg.model.OhlcBar;
import com.github.h3yow.tickagg.model.RejectionReason;
import com.github.h3yow.tickagg.model.SymbolStats;
import com.github.h3yow.tickagg.model.TickEvent;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Capacity safety: with every cap squeezed and delivery volume far beyond
 * the caps, a redelivered event must still be counted at most once, and
 * capacity eviction must never rewrite confirmed results.
 *
 * Window = 60s, allowedLateness = 30s. B is a window boundary.
 */
class CapacitySafetyTest {

    private static final Logger log = LoggerFactory.getLogger(CapacitySafetyTest.class);
    private static final long B = 1_700_000_040_000L;
    private static final String SYM = "CAPUSD";

    private static TickEvent tick(String id, long seq, long tsOffsetFromB) {
        return new TickEvent(id, SYM, seq, B + tsOffsetFromB, 100.0 + seq, 1.0);
    }

    private static IngestionResult ingestAndLog(WindowAggregator agg, String tag, TickEvent e) {
        IngestionResult r = agg.ingest(e);
        log.info("tag={} input={} -> status={} reason={} detail={}",
                tag, e, r.status(), r.rejectionReason(), r.detail());
        return r;
    }

    @Test
    void dedupEvictionNeverCausesDoubleCount() {
        AggregatorProperties props = new AggregatorProperties();
        props.setMaxDedupEntriesPerSymbol(4);
        props.setMaxRetainedWindowsPerSymbol(4);
        WindowAggregator agg = new WindowAggregator(props);

        // 20 distinct events, one per window, watermark always advancing.
        for (int i = 0; i < 20; i++) {
            IngestionResult r = ingestAndLog(agg, "fill", tick("e" + i, i + 1L, i * 60_000L));
            assertEquals(IngestionStatus.ACCEPTED, r.status());
        }
        SymbolStats afterFill = agg.stats(SYM).get(0);
        log.info("stats-after-fill={}", afterFill);
        assertEquals(20, afterFill.acceptedTotal());
        assertEquals(4, afterFill.dedupEntries(), "dedup occupancy is capped");
        assertEquals(16, afterFill.dedupEvictedTotal(), "oldest confirmed-window entries evicted");
        assertTrue(afterFill.retainedWindows() <= 4, "window retention is capped");

        List<OhlcBar> before = agg.query(SYM, 0, Long.MAX_VALUE, true);

        // Redelivery of an event whose dedup entry was evicted: rejected by
        // the stateful rules, counted zero additional times.
        IngestionResult old = ingestAndLog(agg, "redeliver-evicted", tick("e0", 1L, 0L));
        assertEquals(IngestionStatus.REJECTED, old.status());
        assertEquals(RejectionReason.SEQUENCE_REGRESSION, old.rejectionReason());

        IngestionResult evictedMid = ingestAndLog(agg, "redeliver-evicted-mid", tick("e15", 16L, 15 * 60_000L));
        assertEquals(IngestionStatus.REJECTED, evictedMid.status());

        // Redelivery of an event still in the dedup set: plain duplicate.
        IngestionResult recent = ingestAndLog(agg, "redeliver-recent", tick("e19", 20L, 19 * 60_000L));
        assertEquals(IngestionStatus.DUPLICATE, recent.status());

        List<OhlcBar> after = agg.query(SYM, 0, Long.MAX_VALUE, true);
        log.info("before-redeliveries={} after={}", before, after);
        assertEquals(before, after, "redeliveries must not change any window");

        SymbolStats atEnd = agg.stats(SYM).get(0);
        log.info("stats-at-end={}", atEnd);
        assertEquals(20, atEnd.acceptedTotal(), "no event counted twice");
        assertEquals(1, atEnd.duplicateTotal());
        assertEquals(2L, atEnd.rejectionsByReason().get(RejectionReason.SEQUENCE_REGRESSION));

        // The newest window still matches a reference run with huge caps.
        WindowAggregator ref = new WindowAggregator(new AggregatorProperties());
        for (int i = 0; i < 20; i++) {
            ref.ingest(tick("e" + i, i + 1L, i * 60_000L));
        }
        List<OhlcBar> refTail = ref.query(SYM, B + 16 * 60_000L, B + 20 * 60_000L, true);
        List<OhlcBar> actualTail = agg.query(SYM, B + 16 * 60_000L, B + 20 * 60_000L, true);
        log.info("ref-tail={} actual-tail={}", refTail, actualTail);
        assertEquals(refTail, actualTail, "retained windows must match the uncapped reference");
    }

    @Test
    void pinnedDedupEntriesSurviveCapUntilWindowConfirms() {
        AggregatorProperties props = new AggregatorProperties();
        props.setMaxDedupEntriesPerSymbol(2);
        WindowAggregator agg = new WindowAggregator(props);

        // Five distinct events in the same still-unconfirmed window: the
        // dedup set exceeds its cap, but every entry is pinned to an
        // unconfirmed window and must survive, or a redelivery would be
        // counted twice.
        for (int i = 1; i <= 5; i++) {
            assertEquals(IngestionStatus.ACCEPTED,
                    ingestAndLog(agg, "burst", tick("b" + i, i, i * 1_000L)).status());
        }
        SymbolStats burst = agg.stats(SYM).get(0);
        log.info("stats-after-burst={}", burst);
        assertEquals(5, burst.dedupEntries(), "pinned entries may exceed the cap transiently");
        assertEquals(0, burst.dedupEvictedTotal(), "no pinned entry may be evicted");

        // Redelivery storm against a full dedup set: still exactly-once.
        for (int i = 1; i <= 5; i++) {
            IngestionResult r = ingestAndLog(agg, "redeliver", tick("b" + i, i, i * 1_000L));
            assertEquals(IngestionStatus.DUPLICATE, r.status(),
                    "capacity pressure must not turn a redelivery into a new event");
        }
        OhlcBar w0 = agg.query(SYM, B, B + 60_000, true).get(0);
        log.info("w0-after-redelivery-storm={}", w0);
        assertEquals(5, w0.tickCount(), "each event counted exactly once");

        // Confirm W0 by advancing the watermark; the next accepted event
        // triggers eviction of the now-unpinned entries down to the cap.
        ingestAndLog(agg, "watermark", tick("w1", 6, 100_000));
        SymbolStats confirmed = agg.stats(SYM).get(0);
        log.info("stats-after-confirm={}", confirmed);
        assertTrue(confirmed.dedupEntries() <= 2, "entries converge back under the cap");
        assertTrue(confirmed.dedupEvictedTotal() > 0);

        // Now a redelivery of b1 is rejected by the stateful rules (its
        // dedup entry is gone), and the confirmed bar is untouched.
        IngestionResult r = ingestAndLog(agg, "redeliver-after-evict", tick("b1", 1, 1_000));
        assertEquals(IngestionStatus.REJECTED, r.status());
        OhlcBar w0Final = agg.query(SYM, B, B + 60_000, false).get(0);
        log.info("w0-confirmed-final={}", w0Final);
        assertTrue(w0Final.confirmed());
        assertEquals(5, w0Final.tickCount(), "confirmed result unchanged by eviction+redelivery");
    }

    @Test
    void statsSnapshotReflectsRuntimeState() {
        AggregatorProperties props = new AggregatorProperties();
        WindowAggregator agg = new WindowAggregator(props);

        ingestAndLog(agg, "accept-1", tick("e1", 1, 1_000));
        ingestAndLog(agg, "accept-2", tick("e2", 2, 2_000));
        ingestAndLog(agg, "duplicate", tick("e1", 1, 1_000));
        ingestAndLog(agg, "watermark", tick("w1", 3, 100_000));
        ingestAndLog(agg, "late", tick("late1", 4, 50_000));

        SymbolStats s = agg.stats(SYM).get(0);
        log.info("stats={}", s);
        assertEquals(SYM, s.symbol());
        assertEquals(B + 100_000, s.watermarkMs());
        assertEquals(B + 70_000, s.confirmedHorizonMs(), "watermark - allowedLateness");
        assertEquals(30_000L, s.allowedLatenessMs());
        assertEquals(2, s.sequenceFloor(), "max sequence of the confirmed W0");
        assertEquals(2, s.retainedWindows(), "W0 and the watermark window");
        assertEquals(1, s.confirmedWindows());
        assertEquals(3, s.dedupEntries(), "e1, e2, w1");
        assertEquals(1, s.retainedRejections());
        assertEquals(3, s.acceptedTotal());
        assertEquals(1, s.duplicateTotal());
        assertEquals(1L, s.rejectionsByReason().get(RejectionReason.LATE_BEYOND_ALLOWED_LATENESS));
        assertEquals(0, s.windowsEvictedTotal());
        assertEquals(0, s.dedupEvictedTotal());
        assertEquals(0, s.rejectionsEvictedTotal());
        assertEquals(0, s.reprocessRunsTotal());

        // A reprocess pass is reflected in the counters (the late event
        // targets the confirmed W0, so it stays rejected even when the
        // lateness budget is widened).
        props.setAllowedLatenessMs(90_000);
        agg.reprocess(SYM);
        SymbolStats after = agg.stats(SYM).get(0);
        log.info("stats-after-reprocess={}", after);
        assertEquals(1, after.reprocessRunsTotal());
        assertEquals(0, after.reprocessAcceptedTotal());
        assertEquals(1, after.retainedRejections(), "still-rejected event stays retained");
        assertEquals(1L, after.rejectionsByReason().get(RejectionReason.WINDOW_CLOSED));

        // Unknown symbol yields no stats; all-symbols view contains this one.
        assertTrue(agg.stats("NOSUCH").isEmpty());
        assertEquals(1, agg.stats(null).size());
    }
}
