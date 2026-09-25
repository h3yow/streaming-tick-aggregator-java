package com.github.h3yow.streamingtickaggregatorjava.service;

import com.github.h3yow.streamingtickaggregatorjava.config.AggregatorProperties;
import com.github.h3yow.streamingtickaggregatorjava.model.IngestionVerdict;
import com.github.h3yow.streamingtickaggregatorjava.model.RejectReason;
import com.github.h3yow.streamingtickaggregatorjava.model.TickEvent;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * 行情事件接入：字段校验 → 幂等去重 → 序列/窗口判定（委托 {@link AggregationQueryService}）。
 *
 * <p>并发模型：按 eventId 哈希取段锁，保证同一事件的首次投递与重复投递串行且结果一致；
 * 不同 eventId 可并行。幂等表的回收以"仍保留的最老已关闭窗口"为界，界外记录才删除，
 * 因此只要对应的已确认结果仍可查询，重复投递就一定返回与首次一致的判定。
 */
@Service
public class TickIngestionService {

    private static final Logger log = LoggerFactory.getLogger(TickIngestionService.class);
    private static final int STRIPE_COUNT = 64;
    private static final long PRUNE_INTERVAL = 256;

    private final AggregationQueryService aggregationService;
    private final AggregatorProperties props;

    private final Object[] stripes;
    /** eventId -> 首次投递的判定（含完整拒绝原因）。 */
    private final ConcurrentHashMap<String, DedupEntry> dedup = new ConcurrentHashMap<>();
    private final AtomicLong ingestedSincePrune = new AtomicLong();

    public TickIngestionService(AggregationQueryService aggregationService, AggregatorProperties props) {
        this.aggregationService = aggregationService;
        this.props = props;
        this.stripes = new Object[STRIPE_COUNT];
        for (int i = 0; i < STRIPE_COUNT; i++) {
            stripes[i] = new Object();
        }
    }

    public IngestionVerdict ingest(TickEvent event) {
        if (event == null) {
            throw new RejectedTickException(RejectReason.MISSING_FIELD, "请求体为空");
        }
        // 校验不依赖任何可变状态，先做；同一非法请求重复提交永远得到同一拒绝原因。
        validate(event);

        Object stripe = stripes[Math.floorMod(event.eventId().hashCode(), STRIPE_COUNT)];
        IngestionVerdict verdict;
        boolean duplicate;
        synchronized (stripe) {
            DedupEntry existing = dedup.get(event.eventId());
            if (existing != null) {
                verdict = existing.verdict();
                duplicate = true;
                log.info("DUPLICATE    symbol={} eventId={} seq={} eventTime={} 首次判定: accepted={} reason={}",
                        event.symbol(), event.eventId(), event.sequence(), event.eventTime(),
                        verdict.accepted(), verdict.reason());
            } else {
                verdict = aggregationService.tryIngest(event);
                dedup.put(event.eventId(), new DedupEntry(verdict, verdict.windowStart()));
                duplicate = false;
                if (!verdict.accepted()) {
                    log.info("REJECT       symbol={} eventId={} reason={} message={}",
                            event.symbol(), event.eventId(), verdict.reason(), verdict.message());
                }
                maybePrune();
            }
        }
        if (duplicate) {
            return withDuplicate(verdict);
        }
        if (!verdict.accepted()) {
            throw new RejectedTickException(verdict.reason(), verdict.message());
        }
        return verdict;
    }

    private static IngestionVerdict withDuplicate(IngestionVerdict v) {
        return new IngestionVerdict(v.eventId(), v.accepted(), true, v.reason(),
                v.windowStart(), v.windowStatus(),
                (v.message() == null ? "" : v.message()) + " [duplicate: 与首次投递判定一致]");
    }

    private void validate(TickEvent e) {
        if (isBlank(e.eventId())) {
            reject(RejectReason.MISSING_FIELD, "eventId 缺失");
        }
        if (isBlank(e.symbol())) {
            reject(RejectReason.MISSING_FIELD, "symbol 缺失");
        }
        if (e.sequence() == null) {
            reject(RejectReason.MISSING_FIELD, "sequence 缺失");
        } else if (e.sequence() < 0) {
            reject(RejectReason.INVALID_SEQUENCE, "sequence 不能为负: " + e.sequence());
        }
        if (e.eventTime() == null) {
            reject(RejectReason.MISSING_FIELD, "eventTime 缺失");
        } else if (e.eventTime() <= 0 || e.eventTime() > System.currentTimeMillis()) {
            reject(RejectReason.INVALID_TIMESTAMP,
                    "eventTime 非法: " + e.eventTime() + "（须为正数且不晚于当前时间）");
        }
        if (e.price() == null) {
            reject(RejectReason.MISSING_FIELD, "price 缺失");
        } else if (!Double.isFinite(e.price()) || e.price() <= 0) {
            reject(RejectReason.INVALID_PRICE, "price 非法: " + e.price() + "（须为有限正数）");
        }
        if (e.volume() == null) {
            reject(RejectReason.MISSING_FIELD, "volume 缺失");
        } else if (e.volume() < 0) {
            reject(RejectReason.INVALID_VOLUME, "volume 不能为负: " + e.volume());
        }
    }

    private static void reject(RejectReason reason, String message) {
        log.info("REJECT-INPUT reason={} message={}", reason, message);
        throw new RejectedTickException(reason, message);
    }

    private static boolean isBlank(String s) {
        return s == null || s.isBlank();
    }

    /**
     * 周期性回收幂等表：仅删除 windowStart 早于"全局仍保留最老已关闭窗口"的记录。
     * 该界内任何事件重复投递仍能命中首次判定；界外结果本就不再可查，按 WINDOW_CLOSED_LATE 归类。
     */
    private void maybePrune() {
        if (ingestedSincePrune.incrementAndGet() < PRUNE_INTERVAL) {
            return;
        }
        ingestedSincePrune.set(0);
        long horizon = aggregationService.globalRetentionHorizon();
        if (horizon == Long.MIN_VALUE) {
            return;
        }
        int removed = 0;
        for (Iterator<Map.Entry<String, DedupEntry>> it = dedup.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry<String, DedupEntry> en = it.next();
            Long ws = en.getValue().windowStart();
            if (ws != null && ws < horizon) {
                it.remove();
                removed++;
            }
        }
        if (removed > 0) {
            log.debug("dedup pruned {} entries before horizon {}", removed, horizon);
        }
    }

    /** 幂等记录：首次判定与其所属窗口起点（用于界内存回收）。 */
    private record DedupEntry(IngestionVerdict verdict, Long windowStart) {
    }
}
