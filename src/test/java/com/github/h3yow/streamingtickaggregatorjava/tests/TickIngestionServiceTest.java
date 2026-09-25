package com.github.h3yow.streamingtickaggregatorjava.tests;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.github.h3yow.streamingtickaggregatorjava.config.AggregatorProperties;
import com.github.h3yow.streamingtickaggregatorjava.model.IngestionVerdict;
import com.github.h3yow.streamingtickaggregatorjava.model.OhlcvSnapshot;
import com.github.h3yow.streamingtickaggregatorjava.model.RejectReason;
import com.github.h3yow.streamingtickaggregatorjava.model.TickEvent;
import com.github.h3yow.streamingtickaggregatorjava.service.AggregationQueryService;
import com.github.h3yow.streamingtickaggregatorjava.service.RejectedTickException;
import com.github.h3yow.streamingtickaggregatorjava.service.TickIngestionService;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 服务层：校验拒绝原因、幂等去重、乱序聚合收敛、迟到归类。
 */
class TickIngestionServiceTest {

    private static final Logger log = LoggerFactory.getLogger(TickIngestionServiceTest.class);

    private static final long W = 60_000;
    private static final long LATENESS = 10_000;
    private static final long BASE = 600_000;

    private TickIngestionService ingestion;
    private AggregationQueryService aggregation;

    @BeforeEach
    void setUp() {
        AggregatorProperties props = new AggregatorProperties();
        props.setWindowSizeMs(W);
        props.setAllowedLatenessMs(LATENESS);
        aggregation = new AggregationQueryService(props);
        ingestion = new TickIngestionService(aggregation, props);
    }

    private TickEvent tick(String id, long seq, long t, double price, long volume) {
        return new TickEvent(id, "BTC", seq, BASE + t, price, volume);
    }

    private IngestionVerdict ok(TickEvent e) {
        IngestionVerdict v = ingestion.ingest(e);
        assertThat(v.accepted()).as("事件 %s 应被接受: %s", e.eventId(), v).isTrue();
        return v;
    }

    private RejectReason expectReject(TickEvent e) {
        log.info("[INPUT-REJECT] eventId={} symbol={} seq={} eventTime={} price={} volume={}",
                e.eventId(), e.symbol(), e.sequence(), e.eventTime(), e.price(), e.volume());
        RejectedTickException ex = (RejectedTickException) catchReject(e);
        log.info("[JUDGE] eventId={} -> reason={} message={}", e.eventId(), ex.getReason(), ex.getMessage());
        return ex.getReason();
    }

    private RuntimeException catchReject(TickEvent e) {
        try {
            ingestion.ingest(e);
            throw new AssertionError("事件 " + e.eventId() + " 应被拒绝");
        } catch (RejectedTickException re) {
            return re;
        }
    }

    @Test
    @DisplayName("字段缺失/非法：每种问题返回可区分的拒绝原因")
    void rejectsInvalidInputsWithDistinctReasons() {
        assertThat(expectReject(new TickEvent(null, "BTC", 1L, BASE + 1000L, 100d, 1L)))
                .isEqualTo(RejectReason.MISSING_FIELD);
        assertThat(expectReject(new TickEvent("e2", " ", 1L, BASE + 1000L, 100d, 1L)))
                .isEqualTo(RejectReason.MISSING_FIELD);
        assertThat(expectReject(new TickEvent("e3", "BTC", null, BASE + 1000L, 100d, 1L)))
                .isEqualTo(RejectReason.MISSING_FIELD);
        assertThat(expectReject(new TickEvent("e4", "BTC", 1L, null, 100d, 1L)))
                .isEqualTo(RejectReason.MISSING_FIELD);
        assertThat(expectReject(new TickEvent("e5", "BTC", 1L, 0L, 100d, 1L)))
                .isEqualTo(RejectReason.INVALID_TIMESTAMP);
        assertThat(expectReject(new TickEvent("e6", "BTC", 1L, System.currentTimeMillis() + 60_000L, 100d, 1L)))
                .isEqualTo(RejectReason.INVALID_TIMESTAMP);
        assertThat(expectReject(new TickEvent("e7", "BTC", -1L, BASE + 1000L, 100d, 1L)))
                .isEqualTo(RejectReason.INVALID_SEQUENCE);
        assertThat(expectReject(new TickEvent("e8", "BTC", 1L, BASE + 1000L, 0d, 1L)))
                .isEqualTo(RejectReason.INVALID_PRICE);
        assertThat(expectReject(new TickEvent("e9", "BTC", 1L, BASE + 1000L, Double.NaN, 1L)))
                .isEqualTo(RejectReason.INVALID_PRICE);
        assertThat(expectReject(new TickEvent("e10", "BTC", 1L, BASE + 1000L, 100d, -5L)))
                .isEqualTo(RejectReason.INVALID_VOLUME);
    }

    @Test
    @DisplayName("乱序到达：同一批事件任意顺序收敛到同一 OHLCV")
    void outOfOrderConvergesToSameResult() {
        runOneOrder(List.of("a", "b", "c", "d"));
    }

    @Test
    @DisplayName("乱序到达：完全逆序投递结果一致")
    void reverseOrderConvergesToSameResult() {
        runOneOrder(List.of("d", "c", "b", "a"));
    }

    @Test
    @DisplayName("乱序到达：交错顺序投递结果一致")
    void shuffledOrderConvergesToSameResult() {
        runOneOrder(List.of("c", "a", "d", "b"));
    }

    private void runOneOrder(List<String> order) {
        // 同一窗口 [0,60s) 内 4 个事件，时间跨度 5s < allowedLateness(10s)：
        // 按有界延迟语义，即使最晚事件最先到达（watermark=0），其余事件仍在允许乱序范围内，
        // 故任意投递顺序全部接受且窗口在批次结束时仍为 OPEN。
        java.util.Map<String, TickEvent> events = new java.util.LinkedHashMap<>();
        events.put("a", tick("a", 1, 1_000, 100, 2));
        events.put("b", tick("b", 2, 2_500, 120, 3));
        events.put("c", tick("c", 3, 4_000, 90, 1));
        events.put("d", tick("d", 4, 6_000, 110, 4));
        for (String id : order) {
            TickEvent e = events.get(id);
            log.info("[INPUT] 顺序={} eventId={} seq={} eventTimeOffset={} price={} volume={}",
                    order, e.eventId(), e.sequence(), e.eventTime() - BASE, e.price(), e.volume());
            IngestionVerdict v = ok(e);
            log.info("[JUDGE] eventId={} -> windowStart={} status={} watermark={}",
                    e.eventId(), v.windowStart(), v.windowStatus(), aggregation.watermark("BTC"));
        }
        // 批次结束 watermark = 6s-10s = -4s < 0，窗口仍开；includeOpen 返回一致快照
        List<OhlcvSnapshot> openSnap = aggregation.query("BTC", null, null, true);
        openSnap.forEach(snap -> log.info("[RESULT-OPEN] {}", snap));
        assertThat(openSnap).hasSize(1);
        OhlcvSnapshot w0open = openSnap.get(0);
        assertThat(w0open.status()).isEqualTo("OPEN");
        assertThat(w0open.open()).isEqualTo(100d);
        assertThat(w0open.high()).isEqualTo(120d);
        assertThat(w0open.low()).isEqualTo(90d);
        assertThat(w0open.close()).isEqualTo(110d);
        assertThat(w0open.volume()).isEqualTo(10L);
        assertThat(w0open.tickCount()).isEqualTo(4);

        // 推进 watermark 使窗口关闭（70s 事件 -> wm=60s），已确认结果与投递顺序无关
        ok(tick("close", 5, 70_000, 100, 1));
        List<OhlcvSnapshot> closed = aggregation.query("BTC", BASE, BASE + W, false);
        closed.forEach(snap -> log.info("[RESULT-CLOSED] {}", snap));
        assertThat(closed).hasSize(1);
        OhlcvSnapshot w0 = closed.get(0);
        assertThat(w0.status()).isEqualTo("CLOSED");
        assertThat(w0.windowStart()).isEqualTo(BASE);
        assertThat(w0.open()).isEqualTo(100d);
        assertThat(w0.high()).isEqualTo(120d);
        assertThat(w0.low()).isEqualTo(90d);
        assertThat(w0.close()).isEqualTo(110d);
        assertThat(w0.volume()).isEqualTo(10L);
        assertThat(w0.tickCount()).isEqualTo(4);
    }

    @Test
    @DisplayName("重复投递：接受事件与拒绝事件重放均不改变首次判定")
    void duplicatesAreIdempotent() {
        TickEvent a = tick("dup-ok", 1, 1_000, 100, 2);
        IngestionVerdict first = ok(a);
        IngestionVerdict again = ingestion.ingest(a);
        log.info("[JUDGE] 重复接受事件: first={} again={}", first, again);
        assertThat(again.duplicate()).isTrue();
        assertThat(again.accepted()).isTrue();
        assertThat(again.windowStart()).isEqualTo(first.windowStart());

        // 先建立 watermark，使后续事件成为超时迟到
        ok(tick("adv", 9, 90_000, 100, 1));
        TickEvent late = tick("dup-late", 10, 5_000, 100, 1);
        RejectReason firstReject = expectReject(late);
        assertThat(firstReject).isEqualTo(RejectReason.WINDOW_CLOSED_LATE);
        // 重放：仍以重复判定返回，且原因一致、不进入迟到分类第二次
        IngestionVerdict replay = ingestion.ingest(late);
        log.info("[JUDGE] 重复迟到事件重放: {}", replay);
        assertThat(replay.duplicate()).isTrue();
        assertThat(replay.accepted()).isFalse();
        assertThat(replay.reason()).isEqualTo(RejectReason.WINDOW_CLOSED_LATE);
        assertThat(aggregation.lateRecords("BTC")).hasSize(1);
    }

    @Test
    @DisplayName("超时迟到：超过 allowedLateness 的事件被拒绝并单独归类")
    void lateEventsAreRejectedAndClassified() {
        // x1@10s 先到（wm=0）；x3@15s 作为乱序事件后到（wm=0，窗口未关），应接受
        ok(tick("x1", 1, 10_000, 100, 1));
        TickEvent onTime = tick("x3", 2, 15_000, 102, 1);
        log.info("[INPUT] 延迟边界内的乱序事件 eventTimeOffset=15000（watermark=0，窗口未关，应接受）");
        ok(onTime);
        // 依次推进：x4@35s -> wm=25s；x2@50s -> wm=40s；x6@75s -> wm=65s，窗口[0,60s) 关闭
        ok(tick("x4", 3, 35_000, 101, 2));
        ok(tick("x2", 4, 50_000, 99, 3));
        ok(tick("x6", 5, 75_000, 98, 4));
        // 事件 @20s 所属窗口 end=60s <= watermark=65s，超时迟到，拒绝并归类
        TickEvent late = tick("x5", 6, 20_000, 500, 100);
        log.info("[INPUT] 超时迟到事件 eventTimeOffset=20000，窗口 end=60000 <= watermark=65000，应拒绝归类");
        RejectReason reason = expectReject(late);
        assertThat(reason).isEqualTo(RejectReason.WINDOW_CLOSED_LATE);
        List<IngestionVerdict> lateList = aggregation.lateRecords("BTC");
        lateList.forEach(v -> log.info("[LATE-RECORD] {}", v));
        assertThat(lateList).hasSize(1);
        assertThat(lateList.get(0).eventId()).isEqualTo("x5");
        // 已确认窗口不被迟到事件污染：x1/x3/x4/x2 -> volume=1+1+2+3=7，high=102
        OhlcvSnapshot w0 = aggregation.query("BTC", BASE, BASE + W, false).get(0);
        log.info("[RESULT] 已确认窗口 {}", w0);
        assertThat(w0.status()).isEqualTo("CLOSED");
        assertThat(w0.high()).isEqualTo(102d);
        assertThat(w0.volume()).isEqualTo(7L);
        assertThat(w0.tickCount()).isEqualTo(4);
    }

    @Test
    @DisplayName("序列回退：按 (eventTime,sequence) 排序后 seq 必须严格递增，与到达顺序无关")
    void sequenceRegressionIsRejected() {
        ok(tick("s1", 10, 5_000, 100, 1));
        ok(tick("s2", 11, 40_000, 101, 1));
        // 回退1：时间更早(2s)但 seq=12 >= 后继 5s 上 seq=10，(eventTime,seq) 次序矛盾
        TickEvent bad1 = tick("s3", 12, 2_000, 99, 1);
        log.info("[INPUT] seq=12 eventTimeOffset=2000 但后继 5000 已有 seq=10 -> 序列回退");
        assertThat(expectReject(bad1)).isEqualTo(RejectReason.SEQUENCE_REGRESSION);
        // 回退2：时间更晚(6s)但 seq=9 <= 前驱 5s 上 seq=10
        TickEvent bad2 = tick("s4", 9, 6_000, 98, 1);
        log.info("[INPUT] seq=9 eventTimeOffset=6000 但前驱 5000 已有 seq=10 -> 序列回退");
        assertThat(expectReject(bad2)).isEqualTo(RejectReason.SEQUENCE_REGRESSION);
        // 回退3：同一时间点 seq 重复（5s 再来 seq=10）
        TickEvent bad3 = tick("s5", 10, 5_000, 97, 1);
        log.info("[INPUT] seq=10 eventTimeOffset=5000 与已接受事件同时间同 seq -> 序列回退");
        assertThat(expectReject(bad3)).isEqualTo(RejectReason.SEQUENCE_REGRESSION);
        // 合法乱序（ETH）：seq2@40s 先到，seq1@5s 后到，(eventTime,seq) 次序一致
        TickEvent q2 = new TickEvent("q2", "ETH", 2L, BASE + 40_000, 101d, 1L);
        TickEvent q1 = new TickEvent("q1", "ETH", 1L, BASE + 5_000, 100d, 1L);
        ok(q2);
        ok(q1);
        // 合法乱序（SOL）：中间事件 20s/seq20 在 5s/10 与 40s/30 之间，满足双向约束
        ok(new TickEvent("m1", "SOL", 10L, BASE + 5_000, 100d, 1L));
        ok(new TickEvent("m3", "SOL", 30L, BASE + 40_000, 103d, 1L));
        ok(new TickEvent("m2", "SOL", 20L, BASE + 20_000, 102d, 1L));
    }

    @Test
    @DisplayName("窗口不可变：关闭后查询恒为同一已确认结果")
    void closedWindowsAreImmutable() {
        ok(tick("i1", 1, 1_000, 100, 1));
        ok(tick("i2", 2, 90_000, 110, 1));
        OhlcvSnapshot first = aggregation.query("BTC", BASE, BASE + W, false).get(0);
        OhlcvSnapshot second = aggregation.query("BTC", BASE, BASE + W, false).get(0);
        assertThat(second).isEqualTo(first);
    }
}
