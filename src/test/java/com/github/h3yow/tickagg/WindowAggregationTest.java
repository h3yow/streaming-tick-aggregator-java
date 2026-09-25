package com.github.h3yow.tickagg;

import com.github.h3yow.tickagg.aggregate.WindowAggregator;
import com.github.h3yow.tickagg.config.AggregatorProperties;
import com.github.h3yow.tickagg.model.IngestionResult;
import com.github.h3yow.tickagg.model.IngestionStatus;
import com.github.h3yow.tickagg.model.OhlcBar;
import com.github.h3yow.tickagg.model.RejectionReason;
import com.github.h3yow.tickagg.model.TickEvent;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Aggregation correctness: out-of-order routing, order-independence,
 * lateness rejection and window-confirmation boundaries.
 *
 * Window = 60s, allowedLateness = 30s. Base B is a window boundary, so
 * window W0 = [B, B+60s) confirms once the watermark reaches B+90s.
 */
class WindowAggregationTest {

    private static final Logger log = LoggerFactory.getLogger(WindowAggregationTest.class);

    private static final long B = 1_700_000_040_000L; // exact 60s boundary
    private static final String SYM = "ETHUSDT";

    private static WindowAggregator newAggregator() {
        return new WindowAggregator(new AggregatorProperties());
    }

    private static TickEvent tick(String id, long seq, long tsOffsetFromB, double price, double volume) {
        return new TickEvent(id, SYM, seq, B + tsOffsetFromB, price, volume);
    }

    /** The canonical batch: five ticks inside W0, all within the lateness horizon. */
    private static List<TickEvent> batch() {
        return List.of(
                tick("t1", 1, 1_000, 100.0, 1.0),
                tick("t2", 2, 5_000, 105.0, 2.0),
                tick("t3", 3, 3_000, 95.0, 3.0),
                tick("t4", 4, 10_000, 102.0, 4.0),
                tick("t5", 5, 2_000, 99.0, 5.0));
    }

    private static OhlcBar expectedBar() {
        // open = earliest ts (t1 @1s), close = latest ts (t4 @10s),
        // high = 105 (t2), low = 95 (t3), volume = 15, count = 5.
        return new OhlcBar(SYM, B, B + 60_000, 100.0, 105.0, 95.0, 102.0, 15.0, 5, false);
    }

    private static void ingestAll(WindowAggregator agg, List<TickEvent> events, String tag) {
        for (TickEvent e : events) {
            IngestionResult r = agg.ingest(e);
            log.info("tag={} input={} -> status={} reason={} detail={}",
                    tag, e, r.status(), r.rejectionReason(), r.detail());
            assertEquals(IngestionStatus.ACCEPTED, r.status(),
                    "expected ACCEPTED for " + e + " but got " + r);
        }
    }

    @Test
    void outOfOrderTicksWithinLatenessLandInCorrectWindow() {
        WindowAggregator agg = newAggregator();
        List<TickEvent> shuffled = new ArrayList<>(batch());
        Collections.shuffle(shuffled, new Random(42L));
        ingestAll(agg, shuffled, "out-of-order");

        List<OhlcBar> bars = agg.query(SYM, B, B + 60_000, true);
        log.info("queried bars={}", bars);
        assertEquals(1, bars.size());
        assertEquals(expectedBar(), bars.get(0));
    }

    @Test
    void sameBatchConvergesUnderAnyArrivalOrder() {
        OhlcBar reference = null;
        // Several deterministic permutations, each against a fresh aggregator.
        for (long seed = 0; seed < 8; seed++) {
            WindowAggregator agg = newAggregator();
            List<TickEvent> perm = new ArrayList<>(batch());
            Collections.shuffle(perm, new Random(seed));
            ingestAll(agg, perm, "perm-" + seed);
            OhlcBar bar = agg.query(SYM, B, B + 60_000, true).get(0);
            log.info("seed={} convergedBar={}", seed, bar);
            if (reference == null) {
                reference = bar;
            } else {
                assertEquals(reference, bar, "arrival order changed the aggregate");
            }
        }
        assertEquals(expectedBar(), reference);
    }

    @Test
    void duplicateRedeliveryDoesNotChangeAggregate() {
        WindowAggregator agg = newAggregator();
        ingestAll(agg, batch(), "first-pass");
        OhlcBar before = agg.query(SYM, B, B + 60_000, true).get(0);

        for (TickEvent e : batch()) {
            IngestionResult r = agg.ingest(e);
            log.info("redelivery input={} -> status={} detail={}", e, r.status(), r.detail());
            assertEquals(IngestionStatus.DUPLICATE, r.status());
        }
        OhlcBar after = agg.query(SYM, B, B + 60_000, true).get(0);
        log.info("before={} after={}", before, after);
        assertEquals(before, after);
    }

    @Test
    void lateTickBeyondAllowedLatenessIsRejectedNotSilentlyDropped() {
        WindowAggregator agg = newAggregator();
        ingestAll(agg, batch(), "warmup");
        // Push the watermark to B+100s: lateness floor becomes B+70s.
        ingestAll(agg, List.of(tick("w1", 100, 100_000, 110.0, 1.0)), "watermark");

        TickEvent late = tick("late1", 101, 50_000, 999.0, 999.0); // B+50s < B+70s floor
        IngestionResult r = agg.ingest(late);
        log.info("late input={} -> status={} reason={} detail={}",
                late, r.status(), r.rejectionReason(), r.detail());
        assertEquals(IngestionStatus.REJECTED, r.status());
        assertEquals(RejectionReason.LATE_BEYOND_ALLOWED_LATENESS, r.rejectionReason());

        // The rejected tick must not leak into any window.
        OhlcBar w0 = agg.query(SYM, B, B + 60_000, true).get(0);
        log.info("w0-after-late-rejection={}", w0);
        assertEquals(5, w0.tickCount());
        assertEquals(15.0, w0.volume());
        assertEquals(105.0, w0.high());
    }

    @Test
    void tickAtLatenessBoundaryIsStillAccepted() {
        WindowAggregator agg = newAggregator();
        ingestAll(agg, List.of(tick("w1", 1, 100_000, 110.0, 1.0)), "watermark");
        // floor = B+100s-30s = B+70s; a tick exactly at the floor is allowed.
        TickEvent boundary = tick("b1", 2, 70_000, 111.0, 2.0);
        IngestionResult r = agg.ingest(boundary);
        log.info("boundary input={} -> status={} detail={}", boundary, r.status(), r.detail());
        assertEquals(IngestionStatus.ACCEPTED, r.status());
    }

    @Test
    void windowConfirmationBoundaryIsDeterministic() {
        WindowAggregator agg = newAggregator();
        ingestAll(agg, batch(), "w0");
        // Watermark B+89.999s: W0 end+lateness = B+90s not yet reached.
        ingestAll(agg, List.of(tick("w1", 100, 89_999, 110.0, 1.0)), "below-confirm");
        List<OhlcBar> unconfirmed = agg.query(SYM, B, B + 60_000, false);
        log.info("before-confirm query(confirmedOnly)={}", unconfirmed);
        assertTrue(unconfirmed.isEmpty(), "W0 must not be confirmed yet");
        assertFalse(agg.query(SYM, B, B + 60_000, true).isEmpty());

        // Watermark B+90s: W0 confirmed, appears in confirmed-only queries.
        ingestAll(agg, List.of(tick("w2", 101, 90_000, 111.0, 1.0)), "at-confirm");
        List<OhlcBar> confirmed = agg.query(SYM, B, B + 60_000, false);
        log.info("after-confirm query(confirmedOnly)={}", confirmed);
        assertEquals(1, confirmed.size());
        OhlcBar bar = confirmed.get(0);
        assertTrue(bar.confirmed());
        assertEquals(new OhlcBar(SYM, B, B + 60_000, 100.0, 105.0, 95.0, 102.0, 15.0, 5, true), bar);
    }
}
