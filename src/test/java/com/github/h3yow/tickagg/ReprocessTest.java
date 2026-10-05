package com.github.h3yow.tickagg;

import com.github.h3yow.tickagg.aggregate.WindowAggregator;
import com.github.h3yow.tickagg.config.AggregatorProperties;
import com.github.h3yow.tickagg.model.IngestionResult;
import com.github.h3yow.tickagg.model.IngestionStatus;
import com.github.h3yow.tickagg.model.OhlcBar;
import com.github.h3yow.tickagg.model.RejectedTickRecord;
import com.github.h3yow.tickagg.model.RejectionReason;
import com.github.h3yow.tickagg.model.ReprocessResult;
import com.github.h3yow.tickagg.model.TickEvent;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Controlled reprocessing: after the operator widens allowedLatenessMs,
 * retained rejections are replayed through the same decision path. The
 * final aggregate must equal the result as if those events had arrived
 * inside the lateness budget from the start; confirmed windows are never
 * rewritten; replay is idempotent and order-independent.
 *
 * Window = 60s. B is a window boundary, so W0=[B,B+60s), W1=[B+60s,B+120s).
 */
class ReprocessTest {

    private static final Logger log = LoggerFactory.getLogger(ReprocessTest.class);
    private static final long B = 1_700_000_040_000L;
    private static final String SYM = "REPROUSD";

    private static TickEvent tick(String id, long seq, long tsOffsetFromB, double price, double volume) {
        return new TickEvent(id, SYM, seq, B + tsOffsetFromB, price, volume);
    }

    private static IngestionResult ingestAndLog(WindowAggregator agg, String tag, TickEvent e) {
        IngestionResult r = agg.ingest(e);
        log.info("tag={} input={} -> status={} reason={} detail={}",
                tag, e, r.status(), r.rejectionReason(), r.detail());
        return r;
    }

    private static ReprocessResult reprocessAndLog(WindowAggregator agg, String tag) {
        List<ReprocessResult> results = agg.reprocess(SYM);
        ReprocessResult r = results.get(0);
        log.info("tag={} reprocess -> scanned={} accepted={} alreadyCounted={} stillRejected={} remaining={}",
                tag, r.scanned(), r.accepted(), r.alreadyCounted(), r.stillRejected(), r.remainingByReason());
        return r;
    }

    /**
     * Reference run: the same events, delivered in an order where nothing is
     * ever late. The reprocessed aggregate must converge to exactly this.
     */
    private static WindowAggregator reference() {
        WindowAggregator ref = new WindowAggregator(new AggregatorProperties());
        ingestAndLog(ref, "ref", tick("c1", 1, 5_000, 100.0, 1.0));
        ingestAndLog(ref, "ref", tick("t1", 2, 65_000, 101.0, 2.0));
        ingestAndLog(ref, "ref", tick("t2", 3, 68_000, 103.0, 4.0));
        ingestAndLog(ref, "ref", tick("w1", 4, 100_000, 102.0, 8.0));
        return ref;
    }

    @Test
    void reprocessAfterWideningMatchesAsIfNeverLate() {
        AggregatorProperties props = new AggregatorProperties();
        WindowAggregator agg = new WindowAggregator(props);

        // c1 lands in W0; w1 pushes the watermark so W0 confirms and the
        // lateness floor becomes B+70s.
        ingestAndLog(agg, "live", tick("c1", 1, 5_000, 100.0, 1.0));
        ingestAndLog(agg, "live", tick("w1", 4, 100_000, 102.0, 8.0));
        List<OhlcBar> w0Before = agg.query(SYM, B, B + 60_000, false);
        log.info("w0-confirmed-before-reprocess={}", w0Before);
        assertEquals(1, w0Before.size());
        assertTrue(w0Before.get(0).confirmed());

        // t1/t2 belong to still-unconfirmed W1 but are beyond the lateness
        // floor; c2 belongs to the already-confirmed W0.
        assertEquals(IngestionStatus.REJECTED,
                ingestAndLog(agg, "late", tick("t1", 2, 65_000, 101.0, 2.0)).status());
        assertEquals(IngestionStatus.REJECTED,
                ingestAndLog(agg, "late", tick("t2", 3, 68_000, 103.0, 4.0)).status());
        assertEquals(IngestionStatus.REJECTED,
                ingestAndLog(agg, "late", tick("c2", 5, 10_000, 999.0, 999.0)).status());
        assertEquals(3, agg.queryRejections(SYM, 0, Long.MAX_VALUE).size());

        // Operator widens the lateness budget, then reprocesses.
        props.setAllowedLatenessMs(90_000);
        ReprocessResult r1 = reprocessAndLog(agg, "pass-1");
        assertEquals(3, r1.scanned());
        assertEquals(2, r1.accepted(), "t1/t2 now inside the widened horizon");
        assertEquals(1, r1.stillRejected(), "c2 targets the confirmed W0");
        assertEquals(1L, r1.remainingByReason().get(RejectionReason.WINDOW_CLOSED));

        WindowAggregator ref = reference();
        List<OhlcBar> w1Actual = agg.query(SYM, B + 60_000, B + 120_000, true);
        List<OhlcBar> w1Expected = ref.query(SYM, B + 60_000, B + 120_000, true);
        log.info("w1-expected={} w1-actual={}", w1Expected, w1Actual);
        assertEquals(w1Expected, w1Actual,
                "reprocessed aggregate must equal the never-late reference");

        List<OhlcBar> w0After = agg.query(SYM, B, B + 60_000, false);
        log.info("w0-confirmed-after-reprocess={}", w0After);
        assertEquals(w0Before, w0After, "confirmed W0 must not be rewritten");
        assertEquals(1, w0After.get(0).tickCount(), "c2 must stay out of confirmed W0");

        // The still-rejected c2 remains retained, now with the WINDOW_CLOSED
        // reason and refreshed decision context.
        List<RejectedTickRecord> retained = agg.queryRejections(SYM, 0, Long.MAX_VALUE);
        assertEquals(1, retained.size());
        assertEquals("c2", retained.get(0).event().eventId());
        assertEquals(RejectionReason.WINDOW_CLOSED, retained.get(0).reason());
        assertEquals(90_000L, retained.get(0).allowedLatenessMs(),
                "context must reflect the decision that produced this record");
        log.info("retained-after-reprocess event={} reason={} watermark={} allowedLatenessMs={}",
                retained.get(0).event().eventId(), retained.get(0).reason(),
                retained.get(0).watermarkMs(), retained.get(0).allowedLatenessMs());

        // Idempotency: a second pass over the same retention changes nothing.
        ReprocessResult r2 = reprocessAndLog(agg, "pass-2");
        assertEquals(1, r2.scanned());
        assertEquals(0, r2.accepted());
        assertEquals(1, r2.stillRejected());
        assertEquals(w1Expected, agg.query(SYM, B + 60_000, B + 120_000, true),
                "repeated reprocessing must be stable");
        assertEquals(w0Before, agg.query(SYM, B, B + 60_000, false));

        // No double counting: events accepted by the pass are now deduped.
        IngestionResult dup = ingestAndLog(agg, "redeliver-t1",
                tick("t1", 2, 65_000, 101.0, 2.0));
        assertEquals(IngestionStatus.DUPLICATE, dup.status());
        assertEquals(3, agg.query(SYM, B + 60_000, B + 120_000, true).get(0).tickCount());
    }

    @Test
    void reprocessConvergesRegardlessOfRetainedOrder() {
        // The same three late events, retained in two different orders.
        AggregatorProperties p1 = new AggregatorProperties();
        AggregatorProperties p2 = new AggregatorProperties();
        WindowAggregator a1 = new WindowAggregator(p1);
        WindowAggregator a2 = new WindowAggregator(p2);
        for (WindowAggregator a : List.of(a1, a2)) {
            ingestAndLog(a, "warmup", tick("w1", 4, 100_000, 102.0, 8.0));
        }
        ingestAndLog(a1, "late-order-1", tick("t1", 2, 65_000, 101.0, 2.0));
        ingestAndLog(a1, "late-order-1", tick("t2", 3, 68_000, 103.0, 4.0));
        ingestAndLog(a1, "late-order-1", tick("t3", 5, 66_000, 104.0, 1.0));
        ingestAndLog(a2, "late-order-2", tick("t3", 5, 66_000, 104.0, 1.0));
        ingestAndLog(a2, "late-order-2", tick("t2", 3, 68_000, 103.0, 4.0));
        ingestAndLog(a2, "late-order-2", tick("t1", 2, 65_000, 101.0, 2.0));

        p1.setAllowedLatenessMs(40_000);
        p2.setAllowedLatenessMs(40_000);
        ReprocessResult r1 = reprocessAndLog(a1, "a1");
        ReprocessResult r2 = reprocessAndLog(a2, "a2");
        assertEquals(3, r1.accepted());
        assertEquals(3, r2.accepted());

        // Reference: the same events with the lateness budget already wide.
        WindowAggregator ref = new WindowAggregator(new AggregatorProperties());
        ingestAndLog(ref, "ref", tick("t1", 2, 65_000, 101.0, 2.0));
        ingestAndLog(ref, "ref", tick("t3", 5, 66_000, 104.0, 1.0));
        ingestAndLog(ref, "ref", tick("t2", 3, 68_000, 103.0, 4.0));
        ingestAndLog(ref, "ref", tick("w1", 4, 100_000, 102.0, 8.0));

        List<OhlcBar> expected = ref.query(SYM, B + 60_000, B + 120_000, true);
        List<OhlcBar> actual1 = a1.query(SYM, B + 60_000, B + 120_000, true);
        List<OhlcBar> actual2 = a2.query(SYM, B + 60_000, B + 120_000, true);
        log.info("expected={} order1={} order2={}", expected, actual1, actual2);
        assertEquals(expected, actual1, "retention order 1 diverged");
        assertEquals(expected, actual2, "retention order 2 diverged");
    }

    @Test
    void reprocessRunsConcurrentlyWithLiveIngestAndQueries() throws Exception {
        AggregatorProperties props = new AggregatorProperties();
        WindowAggregator agg = new WindowAggregator(props);

        // Watermark at B+200s: lateness floor B+170s, confirmation horizon
        // B+170s. W2=[B+120s,B+180s) stays unconfirmed, so retained late
        // events for W2 become reprocessable once lateness is widened.
        ingestAndLog(agg, "watermark", tick("wm", 1, 200_000, 100.0, 1.0));

        List<TickEvent> lateEvents = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            lateEvents.add(tick("L" + i, 1_000 + i, 120_000 + i * 100L, 100.0 + (i % 7), 1.0));
        }
        for (TickEvent e : lateEvents) {
            assertEquals(IngestionStatus.REJECTED, ingestAndLog(agg, "late", e).status());
        }
        assertEquals(100, agg.queryRejections(SYM, 0, Long.MAX_VALUE).size());

        List<TickEvent> liveEvents = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            liveEvents.add(tick("N" + i, 3_000 + i, 201_000 + i * 100L, 200.0 + (i % 5), 2.0));
        }

        // Sequential reference: nothing is ever late in this delivery order.
        WindowAggregator ref = new WindowAggregator(new AggregatorProperties());
        for (TickEvent e : lateEvents) {
            ingestAndLog(ref, "ref", e);
        }
        ingestAndLog(ref, "ref", tick("wm", 1, 200_000, 100.0, 1.0));
        for (TickEvent e : liveEvents) {
            ingestAndLog(ref, "ref", e);
        }

        props.setAllowedLatenessMs(90_000);

        ExecutorService pool = Executors.newFixedThreadPool(3);
        CountDownLatch start = new CountDownLatch(1);
        AtomicBoolean stopReaders = new AtomicBoolean(false);
        AtomicBoolean readerViolation = new AtomicBoolean(false);

        // Reprocessor: repeated passes until the retention is fully absorbed.
        Future<?> reprocessor = pool.submit(() -> {
            try {
                start.await();
                int acceptedTotal = 0;
                for (int pass = 0; pass < 20 && acceptedTotal < 100; pass++) {
                    ReprocessResult r = agg.reprocess(SYM).get(0);
                    acceptedTotal += r.accepted();
                    log.info("concurrent-pass={} accepted={} stillRejected={}",
                            pass, r.accepted(), r.stillRejected());
                }
                assertEquals(100, acceptedTotal, "every retained event absorbed exactly once");
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
        });

        // Live writer: on-time events keep flowing during reprocessing.
        Future<?> writer = pool.submit(() -> {
            try {
                start.await();
                for (TickEvent e : liveEvents) {
                    IngestionResult r = agg.ingest(e);
                    assertEquals(IngestionStatus.ACCEPTED, r.status(),
                            "live ingest must keep working during reprocess: " + r);
                }
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
        });

        // Reader: polls throughout; every observed bar must be consistent.
        Future<?> reader = pool.submit(() -> {
            try {
                start.await();
                while (!stopReaders.get()) {
                    for (OhlcBar b : agg.query(SYM, 0, Long.MAX_VALUE, true)) {
                        if (b.tickCount() <= 0 || b.low() > b.open() || b.open() > b.high()
                                || b.low() > b.close() || b.close() > b.high()) {
                            log.error("reader observed inconsistent bar: {}", b);
                            readerViolation.set(true);
                        }
                    }
                }
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
        });

        start.countDown();
        reprocessor.get(60, TimeUnit.SECONDS);
        writer.get(60, TimeUnit.SECONDS);
        stopReaders.set(true);
        reader.get(60, TimeUnit.SECONDS);
        pool.shutdownNow();

        assertTrue(!readerViolation.get(), "readers observed intermediate state");

        // Final state equals the sequential reference: each event counted once.
        List<OhlcBar> expected = ref.query(SYM, 0, Long.MAX_VALUE, true);
        List<OhlcBar> actual = agg.query(SYM, 0, Long.MAX_VALUE, true);
        log.info("expected={} actual={}", expected, actual);
        assertEquals(expected, actual,
                "concurrent reprocess + live ingest must converge to the sequential reference");
        assertTrue(agg.queryRejections(SYM, 0, Long.MAX_VALUE).isEmpty(),
                "all retained events were absorbed");
    }
}
