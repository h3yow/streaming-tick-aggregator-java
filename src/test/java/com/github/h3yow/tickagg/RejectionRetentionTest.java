package com.github.h3yow.tickagg;

import com.github.h3yow.tickagg.aggregate.WindowAggregator;
import com.github.h3yow.tickagg.config.AggregatorProperties;
import com.github.h3yow.tickagg.ingest.TickIngestionService;
import com.github.h3yow.tickagg.model.IngestionResult;
import com.github.h3yow.tickagg.model.IngestionStatus;
import com.github.h3yow.tickagg.model.OhlcBar;
import com.github.h3yow.tickagg.model.RejectedTickRecord;
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
 * Rejection retention: aggregator-level rejections are kept with their
 * decision context, queryable by symbol and event-time range, bounded by a
 * FIFO cap whose eviction never touches confirmed aggregation results.
 *
 * Window = 60s, allowedLateness = 30s. B is a window boundary.
 */
class RejectionRetentionTest {

    private static final Logger log = LoggerFactory.getLogger(RejectionRetentionTest.class);
    private static final long B = 1_700_000_040_000L;
    private static final String SYM = "RETUSD";

    private static TickEvent tick(String id, long seq, long tsOffsetFromB) {
        return new TickEvent(id, SYM, seq, B + tsOffsetFromB, 100.0, 1.0);
    }

    private static IngestionResult ingestAndLog(WindowAggregator agg, String tag, TickEvent e) {
        IngestionResult r = agg.ingest(e);
        log.info("tag={} input={} -> status={} reason={} detail={}",
                tag, e, r.status(), r.rejectionReason(), r.detail());
        return r;
    }

    private static void logRetained(String tag, List<RejectedTickRecord> records) {
        for (RejectedTickRecord r : records) {
            log.info("tag={} retained event={} reason={} watermark={} sequenceFloor={} allowedLatenessMs={} rejectedAt={} detail={}",
                    tag, r.event().eventId(), r.reason(), r.watermarkMs(), r.sequenceFloor(),
                    r.allowedLatenessMs(), r.rejectedAtEpochMs(), r.detail());
        }
    }

    @Test
    void lateRejectionsAreRetainedWithFullDecisionContext() {
        WindowAggregator agg = new WindowAggregator(new AggregatorProperties());
        // Watermark to B+100s: lateness floor is B+70s.
        ingestAndLog(agg, "watermark", tick("w1", 1, 100_000));
        TickEvent late1 = tick("late1", 2, 50_000);
        TickEvent late2 = tick("late2", 3, 10_000);
        assertEquals(IngestionStatus.REJECTED, ingestAndLog(agg, "late-1", late1).status());
        assertEquals(IngestionStatus.REJECTED, ingestAndLog(agg, "late-2", late2).status());

        List<RejectedTickRecord> all = agg.queryRejections(SYM, 0, Long.MAX_VALUE);
        logRetained("all", all);
        assertEquals(2, all.size());

        RejectedTickRecord r1 = all.get(0);
        assertEquals(late1, r1.event(), "original event must be preserved verbatim");
        assertEquals(RejectionReason.LATE_BEYOND_ALLOWED_LATENESS, r1.reason());
        assertEquals(B + 100_000, r1.watermarkMs(), "decision context: watermark at rejection time");
        assertEquals(-1L, r1.sequenceFloor());
        assertEquals(30_000L, r1.allowedLatenessMs());
        assertTrue(r1.rejectedAtEpochMs() > 0, "wall-clock of the decision must be recorded");
        assertTrue(r1.detail().contains("watermark"), "detail must carry the decision grounds");

        // Event-time range filtering: [from, to) on the event timestamp.
        List<RejectedTickRecord> slice = agg.queryRejections(SYM, B + 40_000, B + 60_000);
        logRetained("slice[40s,60s)", slice);
        assertEquals(1, slice.size());
        assertEquals("late1", slice.get(0).event().eventId());
        assertTrue(agg.queryRejections(SYM, 0, B + 5_000).isEmpty(), "range before all events");
        assertTrue(agg.queryRejections("NOSUCH", 0, Long.MAX_VALUE).isEmpty(), "unknown symbol");

        // The retained rejections must not have leaked into any window.
        List<OhlcBar> bars = agg.query(SYM, 0, Long.MAX_VALUE, true);
        log.info("bars-after-rejections={}", bars);
        assertEquals(1, bars.size());
        assertEquals(1, bars.get(0).tickCount(), "only the watermark event may be counted");
    }

    @Test
    void validationRejectionsAreNotRetained() {
        WindowAggregator agg = new WindowAggregator(new AggregatorProperties());
        TickIngestionService service = new TickIngestionService(agg);
        ingestAndLog(agg, "watermark", tick("w1", 1, 100_000));

        TickEvent malformed = new TickEvent("bad-1", SYM, 2L, B + 50_000, null, 1.0);
        IngestionResult r = service.ingest(malformed);
        log.info("tag=malformed input={} -> status={} reason={} detail={}",
                malformed, r.status(), r.rejectionReason(), r.detail());
        assertEquals(IngestionStatus.REJECTED, r.status());
        assertEquals(RejectionReason.MISSING_FIELD, r.rejectionReason());

        List<RejectedTickRecord> retained = agg.queryRejections(SYM, 0, Long.MAX_VALUE);
        logRetained("after-malformed", retained);
        assertTrue(retained.isEmpty(),
                "structurally invalid events can never become valid: not retained");
    }

    @Test
    void retentionCapEvictsOldestAndNeverTouchesConfirmedResults() {
        AggregatorProperties props = new AggregatorProperties();
        props.setMaxRetainedRejectionsPerSymbol(2);
        WindowAggregator agg = new WindowAggregator(props);

        // Two accepted ticks in W0, then push the watermark so W0 confirms.
        ingestAndLog(agg, "w0-tick-1", tick("a1", 1, 1_000));
        ingestAndLog(agg, "w0-tick-2", tick("a2", 2, 2_000));
        ingestAndLog(agg, "watermark", tick("w1", 3, 100_000));
        List<OhlcBar> confirmedBefore = agg.query(SYM, 0, Long.MAX_VALUE, false);
        log.info("confirmed-before-eviction-churn={}", confirmedBefore);
        assertEquals(1, confirmedBefore.size());
        assertTrue(confirmedBefore.get(0).confirmed(), "W0 must be confirmed by watermark B+100s");

        // Five late rejections against a retention cap of 2.
        for (int i = 0; i < 5; i++) {
            TickEvent late = tick("late-" + i, 10 + i, 5_000L * (i + 1));
            assertEquals(IngestionStatus.REJECTED, ingestAndLog(agg, "late-" + i, late).status());
        }

        List<RejectedTickRecord> retained = agg.queryRejections(SYM, 0, Long.MAX_VALUE);
        logRetained("capped", retained);
        assertEquals(2, retained.size(), "retention must be bounded by the cap");
        assertEquals("late-3", retained.get(0).event().eventId(), "FIFO: oldest evicted first");
        assertEquals("late-4", retained.get(1).event().eventId());

        SymbolStats stats = agg.stats(SYM).get(0);
        log.info("stats-after-eviction={}", stats);
        assertEquals(2, stats.retainedRejections());
        assertEquals(3, stats.rejectionsEvictedTotal());
        assertEquals(5L, stats.rejectionsByReason().get(RejectionReason.LATE_BEYOND_ALLOWED_LATENESS));

        // Eviction churn must not have changed any confirmed result.
        List<OhlcBar> confirmedAfter = agg.query(SYM, 0, Long.MAX_VALUE, false);
        log.info("confirmed-after-eviction-churn={}", confirmedAfter);
        assertEquals(confirmedBefore, confirmedAfter,
                "retention eviction must never alter confirmed windows");
    }
}
