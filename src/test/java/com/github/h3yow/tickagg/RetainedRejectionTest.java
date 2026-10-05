package com.github.h3yow.tickagg;

import com.github.h3yow.tickagg.aggregate.WindowAggregator;
import com.github.h3yow.tickagg.config.AggregatorProperties;
import com.github.h3yow.tickagg.model.IngestionResult;
import com.github.h3yow.tickagg.model.IngestionStatus;
import com.github.h3yow.tickagg.model.OhlcBar;
import com.github.h3yow.tickagg.model.RejectionReason;
import com.github.h3yow.tickagg.model.RetainedRejection;
import com.github.h3yow.tickagg.model.SymbolStats;
import com.github.h3yow.tickagg.model.TickEvent;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Retention of late-beyond-lateness rejections: queryable by symbol and
 * event-time range, carrying the decision scene; bounded FIFO eviction that
 * affects only the retention itself (never confirmed windows).
 */
class RetainedRejectionTest {

    private static final Logger log = LoggerFactory.getLogger(RetainedRejectionTest.class);

    private static final long B = 1_700_000_040_000L; // exact 60s boundary
    private static final String SYM = "XRPUSDT";

    private static TickEvent tick(String id, long seq, long offsetMs, double price, double volume) {
        return new TickEvent(id, SYM, seq, B + offsetMs, price, volume);
    }

    private static WindowAggregator ingestLatePair(WindowAggregator agg) {
        // First tick sets the watermark at B+1s (floor B-29s).
        IngestionResult a = agg.ingest(tick("a", 1, 1_000, 100.0, 1.0));
        // A tick at B+50s is within the 30s horizon (floor B-29s): accepted,
        // watermark -> B+50s, floor -> B+20s.
        IngestionResult b = agg.ingest(tick("b", 2, 50_000, 101.0, 1.0));
        log.info("setup a={} b={}", a, b);
        // Now two ticks beyond the horizon: both rejected and retained.
        for (TickEvent late : List.of(
                tick("late1", 3, 5_000, 900.0, 9.0),
                tick("late2", 4, 10_000, 800.0, 8.0))) {
            IngestionResult r = agg.ingest(late);
            log.info("case=reject input={} -> status={} reason={} detail={}",
                    late, r.status(), r.rejectionReason(), r.detail());
            assertEquals(IngestionStatus.REJECTED, r.status());
            assertEquals(RejectionReason.LATE_BEYOND_ALLOWED_LATENESS, r.rejectionReason());
        }
        return agg;
    }

    @Test
    void retainedRejectionsAreQueryableWithDecisionScene() {
        WindowAggregator agg = ingestLatePair(new WindowAggregator(new AggregatorProperties()));

        List<RetainedRejection> all = agg.queryRetained(SYM, B, B + 60_000);
        log.info("retained(window range)={}", all);
        assertEquals(2, all.size());
        RetainedRejection first = all.get(0);
        assertEquals("late1", first.eventId());
        assertEquals(B + 5_000, first.eventTimestampEpochMs());
        assertEquals(900.0, first.price());
        assertEquals(9.0, first.volume());
        assertEquals(RejectionReason.LATE_BEYOND_ALLOWED_LATENESS, first.reason());
        // Scene of the decision is preserved.
        assertEquals(B + 50_000, first.watermarkAtDecisionMs());
        assertEquals(B + 20_000, first.latenessFloorAtDecisionMs());
        assertEquals(30_000, first.allowedLatenessAtDecisionMs());
        assertTrue(first.rejectedAtWallClockMs() > 0);
        assertNotNull(first.decisionDetail());
        assertFalse(first.incorporated(), "freshly retained events must not be marked incorporated");

        // Range filter is half-open [from,to) on event time.
        List<RetainedRejection> narrow = agg.queryRetained(SYM, B + 6_000, B + 60_000);
        log.info("retained(narrow [B+6s,B+60s))={}", narrow);
        assertEquals(1, narrow.size());
        assertEquals("late2", narrow.get(0).eventId());
        assertTrue(agg.queryRetained("UNKNOWN", B, B + 60_000).isEmpty());

        SymbolStats stats = agg.stats(SYM);
        log.info("stats={}", stats);
        assertEquals(2, stats.retainedRejections());
        assertEquals(2, stats.rejectedLateCount());
    }

    @Test
    void retentionIsFifoBoundedAndEvictionTouchesNothingButRetention() {
        AggregatorProperties props = new AggregatorProperties();
        props.setMaxRetainedRejectionsPerSymbol(3);
        WindowAggregator agg = new WindowAggregator(props);

        // Watermark far ahead so every following tick is late beyond horizon.
        IngestionResult wm = agg.ingest(tick("wm", 1, 200_000, 100.0, 1.0));
        log.info("watermark setup -> {}", wm);
        for (int i = 0; i < 6; i++) {
            TickEvent late = tick("L" + i, 100 + i, 1_000 + i * 1_000, 10.0 + i, 1.0);
            IngestionResult r = agg.ingest(late);
            log.info("case=eviction-cycle i={} input={} -> status={} retainedNow={}",
                    i, late, r.status(), agg.stats(SYM).retainedRejections());
            assertEquals(IngestionStatus.REJECTED, r.status());
        }

        SymbolStats stats = agg.stats(SYM);
        log.info("stats after overflow={}", stats);
        assertEquals(3, stats.retainedRejections(), "retention holds at its cap");
        assertEquals(3, stats.retainedRejectionEvictedCount());

        List<RetainedRejection> survivors = agg.queryRetained(SYM, B, B + 60_000);
        log.info("survivors={}", survivors);
        assertEquals(List.of("L3", "L4", "L5"), survivors.stream().map(RetainedRejection::eventId).toList());

        // Eviction only affects the retention store: the watermark tick's
        // window result is intact and there are no windows for rejected ticks.
        OhlcBar live = agg.query(SYM, B + 180_000, B + 240_000, true).get(0);
        log.info("live window untouched by retention eviction: {}", live);
        assertEquals(1, live.tickCount());
        assertEquals(100.0, live.open());
        assertTrue(agg.query(SYM, B, B + 60_000, true).isEmpty(),
                "evicted retained events must never have touched aggregation");
    }

    @Test
    void repeatedLateRedeliveryDoesNotDuplicateRetentionRecord() {
        WindowAggregator agg = ingestLatePair(new WindowAggregator(new AggregatorProperties()));
        TickEvent again = tick("late1", 3, 5_000, 900.0, 9.0);
        IngestionResult r1 = agg.ingest(again);
        IngestionResult r2 = agg.ingest(again);
        log.info("redelivered late twice -> {} | {}", r1.status(), r2.status());
        assertEquals(IngestionStatus.REJECTED, r1.status());
        assertEquals(IngestionStatus.REJECTED, r2.status());
        List<RetainedRejection> all = agg.queryRetained(SYM, B, B + 60_000);
        assertEquals(2, all.size(), "same eventId retained only once");
        assertEquals(1, all.stream().filter(r -> r.eventId().equals("late1")).count());
    }
}
