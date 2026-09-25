package com.github.h3yow.tickagg;

import com.github.h3yow.tickagg.aggregate.WindowAggregator;
import com.github.h3yow.tickagg.config.AggregatorProperties;
import com.github.h3yow.tickagg.model.IngestionResult;
import com.github.h3yow.tickagg.model.IngestionStatus;
import com.github.h3yow.tickagg.model.OhlcBar;
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
 * Concurrency: parallel writers and readers must observe no lost updates,
 * no double counting and no torn (intermediate) window state.
 */
class ConcurrencyTest {

    private static final Logger log = LoggerFactory.getLogger(ConcurrencyTest.class);
    private static final long B = 1_700_000_040_000L;

    private static List<TickEvent> batch(String symbol, String idPrefix, int n) {
        List<TickEvent> out = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            // All events inside one 60s window; integer-valued volumes keep
            // the expected sums exact.
            out.add(new TickEvent(idPrefix + "-" + i, symbol, i + 1L,
                    B + i * 10L, 100.0 + (i % 9), (i % 7) + 1.0));
        }
        return out;
    }

    private static void assertBarConsistent(OhlcBar b) {
        assertTrue(b.tickCount() > 0, "empty bar leaked: " + b);
        assertTrue(b.low() <= b.open() && b.open() <= b.high(), "open outside [low,high]: " + b);
        assertTrue(b.low() <= b.close() && b.close() <= b.high(), "close outside [low,high]: " + b);
        assertTrue(b.volume() > 0, "non-positive volume: " + b);
        assertEquals(0L, b.windowStartEpochMs() % 60_000L, "unaligned window: " + b);
    }

    @Test
    void parallelWritersAndReadersMatchSequentialReference() throws Exception {
        int symbols = 8, eventsPerSymbol = 1_000;
        WindowAggregator concurrent = new WindowAggregator(new AggregatorProperties());
        AtomicBoolean stopReaders = new AtomicBoolean(false);
        AtomicBoolean readerViolation = new AtomicBoolean(false);

        ExecutorService pool = Executors.newFixedThreadPool(symbols + 2);
        CountDownLatch start = new CountDownLatch(1);

        // Readers poll while writers are mid-flight; every observed bar must
        // be internally consistent (no intermediate state may escape).
        List<Future<?>> readers = new ArrayList<>();
        for (int r = 0; r < 2; r++) {
            readers.add(pool.submit(() -> {
                while (!stopReaders.get()) {
                    for (int s = 0; s < symbols; s++) {
                        for (OhlcBar b : concurrent.query("SYM" + s, B, B + 60_000, true)) {
                            try {
                                assertBarConsistent(b);
                            } catch (AssertionError e) {
                                log.error("reader observed inconsistent bar: {}", b, e);
                                readerViolation.set(true);
                            }
                        }
                    }
                }
            }));
        }

        List<Future<?>> writers = new ArrayList<>();
        for (int s = 0; s < symbols; s++) {
            String symbol = "SYM" + s;
            List<TickEvent> events = batch(symbol, symbol, eventsPerSymbol);
            writers.add(pool.submit(() -> {
                try {
                    start.await();
                    for (TickEvent e : events) {
                        IngestionResult r = concurrent.ingest(e);
                        assertEquals(IngestionStatus.ACCEPTED, r.status(),
                                "unexpected rejection: " + r);
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
        stopReaders.set(true);
        for (Future<?> f : readers) {
            f.get(60, TimeUnit.SECONDS);
        }
        pool.shutdownNow();

        assertTrue(!readerViolation.get(), "readers observed intermediate state");

        // Every symbol's aggregate must equal a single-threaded reference run
        // of the same events: no loss, no duplication under concurrency.
        for (int s = 0; s < symbols; s++) {
            String symbol = "SYM" + s;
            WindowAggregator reference = new WindowAggregator(new AggregatorProperties());
            for (TickEvent e : batch(symbol, symbol, eventsPerSymbol)) {
                reference.ingest(e);
            }
            List<OhlcBar> expected = reference.query(symbol, B, B + 60_000, true);
            List<OhlcBar> actual = concurrent.query(symbol, B, B + 60_000, true);
            log.info("symbol={} expected={} actual={}", symbol, expected, actual);
            assertEquals(expected, actual, "concurrent result diverges from sequential reference");
            assertEquals(eventsPerSymbol, actual.get(0).tickCount());
        }
    }

    @Test
    void concurrentDuplicateStormCountsEachEventOnce() throws Exception {
        int threads = 8;
        List<TickEvent> events = batch("DUP", "d", 500);
        WindowAggregator agg = new WindowAggregator(new AggregatorProperties());

        WindowAggregator reference = new WindowAggregator(new AggregatorProperties());
        for (TickEvent e : events) {
            reference.ingest(e);
        }

        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<long[]>> futures = new ArrayList<>();
        for (int t = 0; t < threads; t++) {
            futures.add(pool.submit(() -> {
                start.await();
                long accepted = 0, duplicate = 0;
                for (TickEvent e : events) {
                    IngestionResult r = agg.ingest(e);
                    if (r.status() == IngestionStatus.ACCEPTED) {
                        accepted++;
                    } else if (r.status() == IngestionStatus.DUPLICATE) {
                        duplicate++;
                    }
                }
                return new long[]{accepted, duplicate};
            }));
        }
        start.countDown();
        long totalAccepted = 0, totalDuplicate = 0;
        for (Future<long[]> f : futures) {
            long[] counts = f.get(60, TimeUnit.SECONDS);
            totalAccepted += counts[0];
            totalDuplicate += counts[1];
        }
        pool.shutdownNow();

        log.info("duplicate-storm accepted={} duplicate={} uniqueEvents={}",
                totalAccepted, totalDuplicate, events.size());
        assertEquals(events.size(), totalAccepted, "each unique event accepted exactly once");
        assertEquals((long) (threads - 1) * events.size(), totalDuplicate);

        List<OhlcBar> actual = agg.query("DUP", B, B + 60_000, true);
        List<OhlcBar> expected = reference.query("DUP", B, B + 60_000, true);
        log.info("expected={} actual={}", expected, actual);
        assertEquals(expected, actual);
    }

    @Test
    void retainedWindowsAreBounded() {
        AggregatorProperties props = new AggregatorProperties();
        props.setMaxRetainedWindowsPerSymbol(8);
        WindowAggregator agg = new WindowAggregator(props);

        // 100 distinct windows, each confirmed by the following event.
        for (int w = 0; w < 100; w++) {
            TickEvent e = new TickEvent("ev-" + w, "BOUND", w + 1L,
                    B + w * 60_000L, 100.0, 1.0);
            IngestionResult r = agg.ingest(e);
            assertEquals(IngestionStatus.ACCEPTED, r.status(), "w=" + w + " -> " + r);
        }
        List<OhlcBar> retained = agg.query("BOUND", 0, Long.MAX_VALUE, true);
        log.info("retainedWindows={} (max=8) retained={}", retained.size(), retained);
        assertTrue(retained.size() <= 8, "window retention must be bounded");
        // Only the newest windows survive; evicted ones were confirmed (immutable).
        // The tail of the retained range may still be unconfirmed: with 30s
        // lateness on 60s windows, up to the newest 2 windows are unconfirmed.
        long lastWatermark = B + 99 * 60_000L;
        for (OhlcBar b : retained) {
            boolean expectConfirmed = b.windowEndEpochMs() <= lastWatermark - 30_000L;
            assertEquals(expectConfirmed, b.confirmed(), "confirmation flag mismatch: " + b);
        }
        assertEquals(B + 92 * 60_000L, retained.get(0).windowStartEpochMs());
        assertEquals(B + 99 * 60_000L, retained.get(retained.size() - 1).windowStartEpochMs());
    }
}
