package com.github.h3yow.tickagg;

import com.github.h3yow.tickagg.aggregate.WindowAggregator;
import com.github.h3yow.tickagg.config.AggregatorProperties;
import com.github.h3yow.tickagg.model.IngestionResult;
import com.github.h3yow.tickagg.model.IngestionStatus;
import com.github.h3yow.tickagg.model.OhlcBar;
import com.github.h3yow.tickagg.model.RecomputeOutcome;
import com.github.h3yow.tickagg.model.RetainedRejection;
import com.github.h3yow.tickagg.model.SymbolStats;
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
 * Controlled recomputation after widening allowed lateness: as-if-on-time
 * equivalence, order independence, immutability of sealed windows,
 * idempotent repeat and no double counting.
 *
 * Setup convention: window W_k = [B+k*60s, B+(k+1)*60s). Events are given
 * strictly increasing (ts, sequence), so an on-time reference run and the
 * late-then-recompute run are directly comparable.
 */
class RecomputeTest {

    private static final Logger log = LoggerFactory.getLogger(RecomputeTest.class);

    private static final long B = 1_700_000_040_000L;
    private static final long W = 60_000L;
    private static final String SYM = "ADAUSDT";

    private static TickEvent tick(String id, long seq, long offsetMs, double price, double volume) {
        return new TickEvent(id, SYM, seq, B + offsetMs, price, volume);
    }

    private static void ingest(WindowAggregator agg, TickEvent e, String tag) {
        IngestionResult r = agg.ingest(e);
        log.info("tag={} input={} -> status={} reason={} detail={}",
                tag, e, r.status(), r.rejectionReason(), r.detail());
        assertEquals(IngestionStatus.ACCEPTED, r.status(),
                "expected ACCEPTED for " + e + " but got " + r);
    }

    /** On-time reference: every event accepted immediately, result is the truth. */
    private static OhlcBar referenceWindow(List<TickEvent> onTimeEvents, long windowStart) {
        WindowAggregator ref = new WindowAggregator(new AggregatorProperties());
        for (TickEvent e : onTimeEvents) {
            assertEquals(IngestionStatus.ACCEPTED, ref.ingest(e).status());
        }
        return ref.query(SYM, windowStart, windowStart + W, true).get(0);
    }

    @Test
    void referenceWindowHelperRepresentsOnTimeTruth() {
        // Sanity anchor for the as-if comparisons used in the other cases:
        // the on-time reference run itself aggregates deterministically.
        List<TickEvent> onTime = List.of(
                tick("a", 1, 2_000, 100.0, 1.0),
                tick("b", 2, 40_000, 105.0, 2.0),
                tick("c", 3, 65_000, 110.0, 3.0));
        OhlcBar ref = referenceWindow(onTime, B);
        log.info("on-time reference W0={}", ref);
        // c@B+65s belongs to W1; W0 holds only a and b.
        assertEquals(2, ref.tickCount());
        assertEquals(100.0, ref.open());
        assertEquals(105.0, ref.close());
        assertEquals(105.0, ref.high());
        assertEquals(3.0, ref.volume());
    }

    @Test
    void widenedRecomputeFoldsRetainedEventsAsIfOnTime() {
        // W0 ticks; watermark goes to B+65s (inside W1), floor = B+35s.
        // late@B+10s and later@B+20s are beyond 30s but within 120s.
        List<TickEvent> onTime = new ArrayList<>(List.of(
                tick("a", 1, 2_000, 100.0, 1.0),
                tick("late-x", 5, 10_000, 120.0, 4.0),
                tick("late-y", 6, 20_000, 90.0, 5.0),
                tick("b", 2, 40_000, 105.0, 2.0),
                tick("c", 3, 65_000, 110.0, 3.0)));

        WindowAggregator agg = new WindowAggregator(new AggregatorProperties());
        ingest(agg, tick("a", 1, 2_000, 100.0, 1.0), "live");
        ingest(agg, tick("b", 2, 40_000, 105.0, 2.0), "live");
        ingest(agg, tick("c", 3, 65_000, 110.0, 3.0), "watermark-to-B+65s");
        // floor now B+35s: both late ticks rejected and retained.
        for (TickEvent late : List.of(
                tick("late-x", 5, 10_000, 120.0, 4.0),
                tick("late-y", 6, 20_000, 90.0, 5.0))) {
            IngestionResult r = agg.ingest(late);
            log.info("late input={} -> status={} reason={}", late, r.status(), r.rejectionReason());
            assertEquals(IngestionStatus.REJECTED, r.status());
        }

        OhlcBar before = agg.query(SYM, B, B + W, true).get(0);
        log.info("W0 before recompute={}", before);
        assertEquals(2, before.tickCount());

        // Widen to 120s and recompute. W0 end B+60s; watermark B+65s:
        // 65s-120s < 60s so W0 remains open and the late events fold in.
        agg.relaxAllowedLateness(120_000L, SYM);
        RecomputeOutcome outcome = agg.recompute(SYM);
        log.info("recompute outcome={}", outcome);
        assertEquals(2, outcome.candidates());
        assertEquals(2, outcome.incorporated());
        assertEquals(0, outcome.skippedWindowSealed());
        assertEquals(120_000L, outcome.newAllowedLatenessMs());

        OhlcBar actual = agg.query(SYM, B, B + W, true).get(0);
        OhlcBar expected = referenceWindow(onTime, B);
        log.info("W0 after recompute actual={} expected={}", actual, expected);
        assertEquals(expected, actual, "recomputed window must equal on-time arrival");
        // W0: a@2s=100, late-x@10s=120, late-y@20s=90, b@40s=105.
        // open=100 close=105 high=120 low=90 volume=1+4+5+2=12 count=4.
        assertEquals(4, actual.tickCount());
        assertEquals(12.0, actual.volume());
        assertEquals(120.0, actual.high());
        assertEquals(90.0, actual.low());
        assertFalse(actual.confirmed(), "W0 is open under the widened horizon");

        // Retained records now flagged incorporated; query still shows them.
        List<RetainedRejection> retained = agg.queryRetained(SYM, B, B + W);
        assertTrue(retained.stream().allMatch(RetainedRejection::incorporated));
        assertEquals(2, agg.stats(SYM).incorporatedCount());
    }

    @Test
    void recomputeResultIsIndependentOfReplayOrder() {
        // Three retained late events; run the whole late/recompute cycle with
        // the retained set arriving in several orders and compare W0.
        List<TickEvent> lates = new ArrayList<>(List.of(
                tick("L0", 10, 5_000, 101.0, 1.0),
                tick("L1", 11, 15_000, 102.0, 1.0),
                tick("L2", 12, 25_000, 99.0, 1.0)));

        List<TickEvent> onTime = new ArrayList<>();
        onTime.add(tick("a", 1, 2_000, 100.0, 2.0));
        onTime.addAll(lates);
        onTime.add(tick("b", 2, 40_000, 103.0, 3.0));
        onTime.add(tick("c", 3, 65_000, 104.0, 1.0));
        OhlcBar expected = referenceWindow(onTime, B);

        OhlcBar referenceAfterRecompute = null;
        for (long seed = 0; seed < 6; seed++) {
            WindowAggregator agg = new WindowAggregator(new AggregatorProperties());
            ingest(agg, tick("a", 1, 2_000, 100.0, 2.0), "seed-" + seed);
            ingest(agg, tick("b", 2, 40_000, 103.0, 3.0), "seed-" + seed);
            ingest(agg, tick("c", 3, 65_000, 104.0, 1.0), "seed-" + seed);
            List<TickEvent> shuffled = new ArrayList<>(lates);
            Collections.shuffle(shuffled, new Random(seed * 31 + 7));
            for (TickEvent late : shuffled) {
                IngestionResult r = agg.ingest(late);
                assertEquals(IngestionStatus.REJECTED, r.status());
            }
            agg.relaxAllowedLateness(120_000L, SYM);
            RecomputeOutcome oc = agg.recompute(SYM);
            assertEquals(3, oc.incorporated());
            OhlcBar bar = agg.query(SYM, B, B + W, true).get(0);
            log.info("seed={} retentionOrder={} -> bar={}", seed,
                    shuffled.stream().map(TickEvent::eventId).toList(), bar);
            assertEquals(expected, bar, "recompute order changed the aggregate");
            if (referenceAfterRecompute == null) {
                referenceAfterRecompute = bar;
            } else {
                assertEquals(referenceAfterRecompute, bar);
            }
        }
    }

    @Test
    void sealedWindowIsNeverRewrittenByRecompute() {
        WindowAggregator agg = new WindowAggregator(new AggregatorProperties());
        ingest(agg, tick("a", 1, 2_000, 100.0, 1.0), "w0");
        // Watermark B+95s seals W0 (confirmed once >= B+90s).
        ingest(agg, tick("wm", 2, 95_000, 110.0, 1.0), "seal-w0");
        TickEvent late = tick("late-sealed", 3, 5_000, 999.0, 999.0);
        IngestionResult r = agg.ingest(late);
        log.info("late into sealed window -> status={} reason={}", r.status(), r.rejectionReason());
        assertEquals(IngestionStatus.REJECTED, r.status());

        OhlcBar sealedBefore = agg.query(SYM, B, B + W, false).get(0);
        log.info("sealed W0 before={}", sealedBefore);
        assertTrue(sealedBefore.confirmed());

        agg.relaxAllowedLateness(600_000L, SYM);
        RecomputeOutcome oc = agg.recompute(SYM);
        log.info("recompute over sealed window outcome={}", oc);
        assertEquals(0, oc.incorporated());
        assertEquals(1, oc.skippedWindowSealed());

        OhlcBar sealedAfter = agg.query(SYM, B, B + W, false).get(0);
        log.info("sealed W0 after={}", sealedAfter);
        assertEquals(sealedBefore, sealedAfter, "published result must be immutable");
        assertEquals(1, sealedAfter.tickCount());
    }

    @Test
    void repeatedRecomputeIsIdempotentAndLiveEventsKeepWorking() {
        WindowAggregator agg = new WindowAggregator(new AggregatorProperties());
        ingest(agg, tick("a", 1, 2_000, 100.0, 1.0), "live");
        ingest(agg, tick("b", 2, 40_000, 105.0, 2.0), "live");
        ingest(agg, tick("c", 3, 65_000, 110.0, 3.0), "wm");
        TickEvent late = tick("L", 4, 10_000, 120.0, 4.0);
        assertEquals(IngestionStatus.REJECTED, agg.ingest(late).status());

        agg.relaxAllowedLateness(120_000L, SYM);
        RecomputeOutcome first = agg.recompute(SYM);
        log.info("first recompute={}", first);
        assertEquals(1, first.incorporated());
        OhlcBar barAfterFirst = agg.query(SYM, B, B + W, true).get(0);

        // Trigger the same retained batch again: nothing may change.
        RecomputeOutcome second = agg.recompute(SYM);
        RecomputeOutcome third = agg.recompute(SYM);
        log.info("second recompute={} third recompute={}", second, third);
        assertEquals(0, second.incorporated());
        assertEquals(1, second.skippedAlreadyIncorporated());
        assertEquals(second, third, "repeated recompute must be stable");
        OhlcBar barAfterThird = agg.query(SYM, B, B + W, true).get(0);
        assertEquals(barAfterFirst, barAfterThird);

        // Live ingestion and queries still work during/after recompute: a new
        // late tick that was rejected before widening is now accepted live,
        // and the recomputed tick must not be double counted.
        TickEvent nowWithinHorizon = tick("L2", 5, 12_000, 121.0, 1.0);
        IngestionResult liveLate = agg.ingest(nowWithinHorizon);
        log.info("previously-late-range tick after widening -> {}", liveLate.status());
        assertEquals(IngestionStatus.ACCEPTED, liveLate.status());
        IngestionResult redelivered = agg.ingest(late);
        log.info("recomputed event redelivered live -> {}", redelivered.status());
        assertEquals(IngestionStatus.DUPLICATE, redelivered.status(),
                "a recomputed event lives in the dedup index until its window seals");
        OhlcBar finalBar = agg.query(SYM, B, B + W, true).get(0);
        log.info("final W0={}", finalBar);
        assertEquals(4, finalBar.tickCount(), "a,L,L2,b = 4 ticks, no double count");

        SymbolStats stats = agg.stats(SYM);
        log.info("stats={}", stats);
        assertEquals(1, stats.incorporatedCount());
        assertEquals(1, stats.duplicateCount());
    }
}
