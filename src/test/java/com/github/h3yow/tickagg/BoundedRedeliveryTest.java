package com.github.h3yow.tickagg;

import com.github.h3yow.tickagg.aggregate.WindowAggregator;
import com.github.h3yow.tickagg.config.AggregatorProperties;
import com.github.h3yow.tickagg.model.IngestionResult;
import com.github.h3yow.tickagg.model.IngestionStatus;
import com.github.h3yow.tickagg.model.OhlcBar;
import com.github.h3yow.tickagg.model.SymbolStats;
import com.github.h3yow.tickagg.model.TickEvent;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Long-running safety under tiny capacities and overwhelming traffic:
 * a repeated event must be counted exactly once even when retention and
 * window capacities are saturated and evictions happen; eviction of one
 * structure can never resurrect an event into the aggregate.
 */
class BoundedRedeliveryTest {

    private static final Logger log = LoggerFactory.getLogger(BoundedRedeliveryTest.class);

    private static final long B = 1_700_000_040_000L;
    private static final long W = 60_000L;
    private static final String SYM = "SOLUSDT";

    private static TickEvent tick(String id, long seq, long offsetMs, double price, double volume) {
        return new TickEvent(id, SYM, seq, B + offsetMs, price, volume);
    }

    @Test
    void repeatedEventStillCountedOnceAfterRetentionOverflow() {
        AggregatorProperties props = new AggregatorProperties();
        props.setMaxRetainedRejectionsPerSymbol(2);
        WindowAggregator agg = new WindowAggregator(props);

        // Watermark far ahead; every following tick is late beyond horizon.
        assertEquals(IngestionStatus.ACCEPTED, agg.ingest(tick("wm", 1, 300_000, 100.0, 1.0)).status());
        TickEvent recurring = tick("r0", 100, 1_000, 500.0, 7.0);
        IngestionResult first = agg.ingest(recurring);
        log.info("first delivery of late event -> status={} reason={}", first.status(), first.rejectionReason());
        assertEquals(IngestionStatus.REJECTED, first.status());

        // Overflow the retention store twice over; each rejection carries a
        // distinct id, so r0's record is evicted.
        for (int i = 1; i <= 9; i++) {
            IngestionResult r = agg.ingest(tick("x" + i, 100 + i, 2_000 + i * 1_000, 10.0, 1.0));
            assertEquals(IngestionStatus.REJECTED, r.status());
        }
        log.info("retention stats after overflow={}", agg.stats(SYM));

        // Redeliver r0 many times. It must never become ACCEPTED/counted:
        // it is a replay, and no window may absorb it.
        for (int i = 0; i < 5; i++) {
            IngestionResult r = agg.ingest(recurring);
            log.info("redelivery #{} after retention eviction -> status={} reason={}",
                    i, r.status(), r.rejectionReason());
            assertFalse(r.status() == IngestionStatus.ACCEPTED,
                    "eviction of the retention record must not resurrect a rejected event");
        }
        // No late event ever reached any aggregate.
        assertTrue(agg.query(SYM, B, B + 5 * W, true).stream()
                .allMatch(b -> b.windowStartEpochMs() == B + 300_000 && b.tickCount() == 1));
        SymbolStats stats = agg.stats(SYM);
        log.info("final stats={}", stats);
        assertEquals(2, stats.retainedRejections());
    }

    @Test
    void repeatedEventStillCountedOnceAfterWindowEviction() {
        AggregatorProperties props = new AggregatorProperties();
        props.setMaxRetainedWindowsPerSymbol(3);
        WindowAggregator agg = new WindowAggregator(props);

        // Accepted event in an early window.
        TickEvent old = tick("old", 1, 1_000, 100.0, 1.0);
        assertEquals(IngestionStatus.ACCEPTED, agg.ingest(old).status());
        // Advance across many windows; the early window confirms and is evicted.
        for (int w = 1; w <= 8; w++) {
            assertEquals(IngestionStatus.ACCEPTED,
                    agg.ingest(tick("f" + w, 1 + w, w * W + 1_000, 100.0 + w, 1.0)).status());
        }
        SymbolStats mid = agg.stats(SYM);
        log.info("after window eviction stats={}", mid);
        assertTrue(mid.retainedWindows() <= 3);

        // The old event's window and dedup id are long gone: redelivery must
        // not be counted again. sequence=1 <= sequenceFloor -> deterministic
        // rejection; the confirmed aggregate stays exactly as it was served.
        for (int i = 0; i < 3; i++) {
            IngestionResult r = agg.ingest(old);
            log.info("redelivery of evicted-window event #{} -> status={} reason={}",
                    i, r.status(), r.rejectionReason());
            assertFalse(r.status() == IngestionStatus.ACCEPTED);
        }
        // Every retained window counts exactly its own single tick.
        List<OhlcBar> bars = agg.query(SYM, 0, Long.MAX_VALUE, true);
        for (OhlcBar b : bars) {
            assertEquals(1, b.tickCount(), "window gained a phantom tick: " + b);
        }
        log.info("bars after redeliveries={}", bars);
    }

    @Test
    void duplicateStormWithTinyWindowCapacityCountsEachEventOnce() throws Exception {
        AggregatorProperties props = new AggregatorProperties();
        props.setMaxRetainedWindowsPerSymbol(2);
        props.setMaxRetainedRejectionsPerSymbol(1);
        final int n = 300;
        // 300 distinct events spread over 300 windows; repeated concurrently
        // so each event is delivered 3x while evictions churn constantly.
        java.util.List<TickEvent> events = new java.util.ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            events.add(tick("e" + i, i + 1L, i * W + 5_000, 100.0 + (i % 5), 1.0));
        }
        WindowAggregator agg = new WindowAggregator(props);
        int threads = 6;
        ExecutorService pool = Executors.newFixedThreadPool(threads + 1);
        CountDownLatch start = new CountDownLatch(1);
        AtomicBoolean violation = new AtomicBoolean(false);

        // A reader that continuously snapshots: must never observe torn state.
        Future<?> reader = pool.submit(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    for (OhlcBar b : agg.query(SYM, 0, Long.MAX_VALUE, true)) {
                        if (b.tickCount() <= 0 || b.low() > b.high()) {
                            violation.set(true);
                        }
                    }
                } catch (Exception ignored) {
                    // shutting down
                }
            }
        });

        // Count how many times each eventId was ACCEPTED across every
        // delivery thread. Events may be delivered first by any thread, so
        // the property is per-eventId: exactly one ACCEPTED in total.
        java.util.concurrent.ConcurrentHashMap<String, AtomicLong> acceptedById =
                new java.util.concurrent.ConcurrentHashMap<>();
        List<Future<?>> writers = new java.util.ArrayList<>();
        for (int t = 0; t < threads; t++) {
            writers.add(pool.submit(() -> {
                try {
                    start.await();
                    for (TickEvent e : events) {
                        IngestionResult r = agg.ingest(e);
                        if (r.status() == IngestionStatus.ACCEPTED) {
                            acceptedById.computeIfAbsent(e.eventId(), k -> new AtomicLong()).incrementAndGet();
                        }
                    }
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
            }));
        }
        start.countDown();
        for (Future<?> f : writers) {
            f.get(60, TimeUnit.SECONDS);
        }
        reader.cancel(true);
        pool.shutdownNow();

        long totalAccepted = acceptedById.values().stream().mapToLong(AtomicLong::get).sum();
        log.info("unique accepted ids={} total accepts={} deliveredAttempts={}",
                acceptedById.size(), totalAccepted, threads * n);
        assertEquals(n, acceptedById.size(), "every unique event accepted at least once");
        for (var e : acceptedById.entrySet()) {
            assertEquals(1, e.getValue().get(),
                    "event " + e.getKey() + " was accepted " + e.getValue() + " times despite evictions");
        }
        assertFalse(violation.get(), "reader saw torn aggregate state");
        SymbolStats stats = agg.stats(SYM);
        log.info("post-storm stats={}", stats);
        assertTrue(stats.retainedWindows() <= 2);
        // Accepted count is cumulative and must equal the unique event count.
        assertEquals(n, stats.acceptedCount());
    }

    @Test
    void statsSnapshotReflectsCountersAndBoundedOccupancies() {
        AggregatorProperties props = new AggregatorProperties();
        props.setMaxRetainedRejectionsPerSymbol(100);
        WindowAggregator agg = new WindowAggregator(props);

        agg.ingest(tick("a", 1, 1_000, 100.0, 1.0));
        agg.ingest(tick("a", 1, 1_000, 100.0, 1.0)); // duplicate
        agg.ingest(tick("b", 2, 40_000, 105.0, 1.0));
        agg.ingest(tick("c", 3, 95_000, 110.0, 1.0)); // seals W0
        agg.ingest(tick("d", 1, 96_000, 1.0, 1.0));   // sequence regression
        // Late beyond horizon -> retained.
        IngestionResult late = agg.ingest(tick("L", 4, 5_000, 999.0, 1.0));
        assertEquals(IngestionStatus.REJECTED, late.status());

        SymbolStats s = agg.stats(SYM);
        log.info("stats snapshot={}", s);
        assertEquals(SYM, s.symbol());
        assertEquals(B + 95_000, s.watermarkMs());
        assertEquals(30_000, s.allowedLatenessMs());
        assertEquals(3, s.acceptedCount());
        assertEquals(1, s.duplicateCount());
        assertEquals(1, s.rejectedSequenceRegressionCount());
        assertEquals(1, s.rejectedLateCount());
        assertEquals(1, s.retainedRejections());
        assertTrue(s.sealedWindows() >= 1);
        // Dedup only tracks unsealed windows; W0 ids were reclaimed at seal.
        log.info("dedupEntries={} (sealed-window ids reclaimed)", s.dedupEntries());

        // All-symbols listing includes this symbol and stays point-in-time.
        List<SymbolStats> all = agg.statsAll();
        assertEquals(1, all.size());
        assertEquals(s, all.get(0));
    }
}
