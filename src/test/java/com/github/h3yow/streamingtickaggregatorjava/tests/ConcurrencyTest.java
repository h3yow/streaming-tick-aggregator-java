package com.github.h3yow.streamingtickaggregatorjava.tests;

import static org.assertj.core.api.Assertions.assertThat;

import com.github.h3yow.streamingtickaggregatorjava.config.AggregatorProperties;
import com.github.h3yow.streamingtickaggregatorjava.model.IngestionVerdict;
import com.github.h3yow.streamingtickaggregatorjava.model.OhlcvSnapshot;
import com.github.h3yow.streamingtickaggregatorjava.model.RejectReason;
import com.github.h3yow.streamingtickaggregatorjava.model.TickEvent;
import com.github.h3yow.streamingtickaggregatorjava.service.AggregationQueryService;
import com.github.h3yow.streamingtickaggregatorjava.service.RejectedTickException;
import com.github.h3yow.streamingtickaggregatorjava.service.TickIngestionService;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 并发：高并发写入不丢失/不重复计数；重复投递幂等；读写并发时查询永远返回一致快照。
 */
class ConcurrencyTest {

    private static final Logger log = LoggerFactory.getLogger(ConcurrencyTest.class);

    private static final long W = 60_000;
    private static final long LATENESS = 10_000;
    private static final long BASE = 600_000;

    @Test
    void concurrentWritesAndDuplicatesAndReadsAreConsistent() throws Exception {
        AggregatorProperties props = new AggregatorProperties();
        props.setWindowSizeMs(W);
        props.setAllowedLatenessMs(LATENESS);
        AggregationQueryService aggregation = new AggregationQueryService(props);
        TickIngestionService ingestion = new TickIngestionService(aggregation, props);

        // 100 个唯一事件，时间 1000ms..1099ms（跨度 100ms < allowedLateness），
        // seq 与 eventTime 严格同序：任意到达顺序都满足 (eventTime,seq) 约束
        int n = 100;
        int threads = 8;
        int duplicateFactor = 3; // 每个事件含首次在内共投递 3 次
        ExecutorService pool = Executors.newFixedThreadPool(threads + 1);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threads * duplicateFactor);
        AtomicInteger accepted = new AtomicInteger();
        AtomicInteger duplicate = new AtomicInteger();
        ConcurrentLinkedQueue<Throwable> errors = new ConcurrentLinkedQueue<>();

        for (int round = 0; round < duplicateFactor; round++) {
            final int r = round;
            for (int t = 0; t < threads; t++) {
                final int tid = t;
                pool.submit(() -> {
                    try {
                        start.await();
                        for (int i = tid; i < n; i += threads) {
                            long eventTime = BASE + 1_000 + i;
                            TickEvent e = new TickEvent("evt-" + i, "BTC", (long) i + 1,
                                    eventTime, 100d + i, 1L);
                            IngestionVerdict v = ingestion.ingest(e);
                            if (v.duplicate()) {
                                duplicate.incrementAndGet();
                            } else if (v.accepted()) {
                                accepted.incrementAndGet();
                            }
                            if (r == 0) {
                                log.info("[CONCURRENT-INPUT] eventId={} seq={} eventTime={} -> accepted={} dup={}",
                                        e.eventId(), e.sequence(), eventTime - BASE, v.accepted(), v.duplicate());
                            }
                        }
                    } catch (Throwable ex) {
                        errors.add(ex);
                    } finally {
                        done.countDown();
                    }
                });
            }
        }

        // 并发读线程：反复查询 OPEN 快照，断言任何时刻 tickCount/volume 自洽且无中间态撕裂
        AtomicInteger readChecks = new AtomicInteger();
        pool.submit(() -> {
            try {
                while (done.getCount() > 0) {
                    List<OhlcvSnapshot> snaps = aggregation.query("BTC", null, null, true);
                    for (OhlcvSnapshot s : snaps) {
                        assertThat(s.volume()).as("volume 必须等于非负 tick 计数之和").isBetween(0L, (long) n);
                        assertThat(s.tickCount()).isBetween(0L, (long) n);
                        assertThat(s.high()).as("high 不得小于 low（无中间态撕裂）").isGreaterThanOrEqualTo(s.low());
                        readChecks.incrementAndGet();
                    }
                }
            } catch (Throwable t) {
                errors.add(t);
            }
        });

        start.countDown();
        assertThat(done.await(60, TimeUnit.SECONDS)).isTrue();
        pool.shutdown();
        pool.awaitTermination(5, TimeUnit.SECONDS);

        log.info("[CONCURRENT-RESULT] 首次接受={} 重复投递={} 并发查询次数={} 异常={}",
                accepted.get(), duplicate.get(), readChecks.get(), errors.size());
        assertThat(errors).isEmpty();
        assertThat(accepted.get()).isEqualTo(n);
        assertThat(duplicate.get()).isEqualTo(n * (duplicateFactor - 1));
        assertThat(readChecks.get()).isPositive();

        // 关闭窗口后验证最终聚合：100 个事件、volume=100，high/low/open/close 与顺序无关
        ingestion.ingest(new TickEvent("closer", "BTC", (long) n + 1, BASE + 70_000, 1d, 1L));
        List<OhlcvSnapshot> closed = aggregation.query("BTC", BASE, BASE + W, false);
        assertThat(closed).hasSize(1);
        OhlcvSnapshot w0 = closed.get(0);
        log.info("[CONCURRENT-FINAL] {}", w0);
        assertThat(w0.status()).isEqualTo("CLOSED");
        assertThat(w0.tickCount()).isEqualTo(n);
        assertThat(w0.volume()).isEqualTo(n);
        // 最早 evt-0(@1000ms,seq1,price100) 为 open；最晚 evt-99(@1099ms,seq100,price199) 为 close
        assertThat(w0.open()).isEqualTo(100d);
        assertThat(w0.close()).isEqualTo(199d);
        assertThat(w0.high()).isEqualTo(199d);
        assertThat(w0.low()).isEqualTo(100d);
    }

    @Test
    void malformedEventsUnderConcurrencyDoNotCorruptState() throws Exception {
        AggregatorProperties props = new AggregatorProperties();
        props.setWindowSizeMs(W);
        props.setAllowedLatenessMs(LATENESS);
        AggregationQueryService aggregation = new AggregationQueryService(props);
        TickIngestionService ingestion = new TickIngestionService(aggregation, props);

        int threads = 8;
        int perThread = 50;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        AtomicInteger rejects = new AtomicInteger();
        for (int t = 0; t < threads; t++) {
            final int tid = t;
            pool.submit(() -> {
                try {
                    start.await();
                    for (int i = 0; i < perThread; i++) {
                        long seq = (long) tid * perThread + i + 1;
                        // 事件时间与全局 seq 同序，跨度仅几百毫秒；一半合法一半非法（负价）
                        TickEvent e = new TickEvent("m-" + tid + "-" + i, "BTC", seq,
                                BASE + 1_000 + seq, i % 2 == 0 ? 100d : -1d, 1L);
                        try {
                            ingestion.ingest(e);
                        } catch (RejectedTickException ex) {
                            assertThat(ex.getReason()).isEqualTo(RejectReason.INVALID_PRICE);
                            rejects.incrementAndGet();
                        }
                    }
                } catch (InterruptedException ignored) {
                    Thread.currentThread().interrupt();
                }
            });
        }
        start.countDown();
        pool.shutdown();
        assertThat(pool.awaitTermination(30, TimeUnit.SECONDS)).isTrue();
        assertThat(rejects.get()).isEqualTo(threads * perThread / 2);
        // 关闭窗口：只有合法事件计入
        ingestion.ingest(new TickEvent("closer2", "BTC", 999_999L, BASE + 70_000, 1d, 1L));
        OhlcvSnapshot w0 = aggregation.query("BTC", BASE, BASE + W, false).get(0);
        log.info("[MALFORMED-FINAL] 拒绝={} 窗口={}", rejects.get(), w0);
        assertThat(w0.tickCount()).isEqualTo(threads * perThread / 2);
        assertThat(w0.volume()).isEqualTo((long) threads * perThread / 2);
    }
}
