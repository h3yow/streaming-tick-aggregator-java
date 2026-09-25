package com.github.h3yow.streamingtickaggregatorjava.service;

import com.github.h3yow.streamingtickaggregatorjava.config.AggregatorProperties;
import com.github.h3yow.streamingtickaggregatorjava.model.IngestionVerdict;
import com.github.h3yow.streamingtickaggregatorjava.model.OhlcvSnapshot;
import com.github.h3yow.streamingtickaggregatorjava.model.RejectReason;
import com.github.h3yow.streamingtickaggregatorjava.model.TickEvent;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * OHLCV 聚合与查询：进程内按标的维护窗口状态与 watermark。
 *
 * <p>边界规则（按标的独立维护）：
 * <ul>
 *   <li>窗口：[floor(eventTime / windowSizeMs), +windowSizeMs)，左闭右开。</li>
 *   <li>watermark = 已观测最大事件时间 - allowedLatenessMs（无事件时为 Long.MIN_VALUE）。</li>
 *   <li>窗口结束时刻 &le; watermark 时窗口关闭（CLOSED），结果不可变，查询永远返回一致快照。</li>
 *   <li>事件时间所在窗口结束时刻 &le; watermark 即判定为超时迟到，拒绝并归类，绝不计入任何窗口。</li>
 *   <li>序列规则：同标的内，已接受事件按 (eventTime, sequence) 排序后 sequence 必须严格递增；
 *       新事件的 seq 须严格大于前驱时间点最大 seq、严格小于后继时间点最小 seq。
 *       该检查与到达顺序无关，保证乱序接受但拒绝真正的序列回退。</li>
 *   <li>内存界：已关闭窗口、open 窗口索引、迟到记录均按配置上限裁剪；
 *       幂等去重表由接入服务在 watermark 推进后按窗口边界裁剪。</li>
 * </ul>
 */
@Service
public class AggregationQueryService {

    private static final Logger log = LoggerFactory.getLogger(AggregationQueryService.class);

    private final AggregatorProperties props;
    private final ConcurrentHashMap<String, SymbolState> symbols = new ConcurrentHashMap<>();

    public AggregationQueryService(AggregatorProperties props) {
        this.props = props;
    }

    private SymbolState state(String symbol) {
        return symbols.computeIfAbsent(symbol, s -> new SymbolState(s, props));
    }

    /** 当前标的 watermark（epoch millis），无事件时为 Long.MIN_VALUE。 */
    public long watermark(String symbol) {
        return state(symbol).watermark();
    }

    /**
     * 全局最早的"仍保留的已关闭窗口"起点；任何 windowStart 小于它的去重记录均可安全回收。
     * 不存在任何已关闭窗口时返回 Long.MIN_VALUE（不回收）。
     */
    public long globalRetentionHorizon() {
        long horizon = Long.MIN_VALUE;
        for (SymbolState st : symbols.values()) {
            synchronized (st.lock) {
                if (!st.closedWindows.isEmpty()) {
                    long first = st.closedWindows.firstKey();
                    if (horizon == Long.MIN_VALUE || first < horizon) {
                        horizon = first;
                    }
                } else {
                    // 该标的存在但尚无窗口关闭，不能回收其记录
                    return Long.MIN_VALUE;
                }
            }
        }
        return horizon;
    }

    /**
     * 尝试在标的锁内归并事件。调用方须先完成字段校验与幂等去重。
     *
     * @return 接受时返回 CLOSED/OPEN 判定；拒绝时携带原因
     */
    public IngestionVerdict tryIngest(TickEvent event) {
        SymbolState st = state(event.symbol());
        synchronized (st.lock) {
            long windowSize = props.getWindowSizeMs();
            long windowStart = floorWindow(event.eventTime(), windowSize);
            long windowEnd = windowStart + windowSize;
            long wm = st.watermark();
            if (windowEnd <= wm) {
                IngestionVerdict v = late(event, windowStart,
                        "eventTime=" + event.eventTime() + " 落入窗口 [" + windowStart + "," + windowEnd
                                + ") 已在 watermark=" + wm + " 之前关闭（allowedLatenessMs="
                                + props.getAllowedLatenessMs() + "）");
                st.recordLate(v);
                log.info("REJECT-LATE  symbol={} eventId={} seq={} eventTime={} windowStart={} watermark={} -> {}",
                        event.symbol(), event.eventId(), event.sequence(), event.eventTime(),
                        windowStart, wm, v.message());
                return v;
            }
            // floorKey 含同一时间点：同时间点 seq 必须严格递增
            Long floorTime = st.seqIndex.floorKey(event.eventTime());
            if (floorTime != null && event.sequence() <= st.seqIndex.get(floorTime).maxSeq) {
                log.info("REJECT-SEQ   symbol={} eventId={} seq={} eventTime={} prevMaxSeq={}@{}",
                        event.symbol(), event.eventId(), event.sequence(), event.eventTime(),
                        st.seqIndex.get(floorTime).maxSeq, floorTime);
                return new IngestionVerdict(event.eventId(), false, false,
                        RejectReason.SEQUENCE_REGRESSION, windowStart, null,
                        "sequence=" + event.sequence() + " 不大于 eventTime<=" + event.eventTime()
                                + " 已接受事件最大 sequence=" + st.seqIndex.get(floorTime).maxSeq
                                + "（@" + floorTime + "），判定序列回退");
            }
            // higherKey：只取严格晚于当前时间的时间点
            Long ceilTime = st.seqIndex.higherKey(event.eventTime());
            if (ceilTime != null && event.sequence() >= st.seqIndex.get(ceilTime).minSeq) {
                log.info("REJECT-SEQ   symbol={} eventId={} seq={} eventTime={} nextMinSeq={}@{}",
                        event.symbol(), event.eventId(), event.sequence(), event.eventTime(),
                        st.seqIndex.get(ceilTime).minSeq, ceilTime);
                return new IngestionVerdict(event.eventId(), false, false,
                        RejectReason.SEQUENCE_REGRESSION, windowStart, null,
                        "sequence=" + event.sequence() + " 不小于 eventTime>=" + event.eventTime()
                                + " 已接受事件最小 sequence=" + st.seqIndex.get(ceilTime).minSeq
                                + "（@" + ceilTime + "），判定序列回退");
            }
            WindowState win = st.openWindows.computeIfAbsent(windowStart,
                    k -> new WindowState(event.symbol(), k, k + windowSize));
            win.add(event);
            st.upsertSeqIndex(event.eventTime(), event.sequence());
            if (event.eventTime() > st.maxEventTime) {
                st.maxEventTime = event.eventTime();
            }
            st.advanceWatermark();
            String windowStatus = windowEnd <= st.watermark() ? "CLOSED" : "OPEN";
            IngestionVerdict v = new IngestionVerdict(event.eventId(), true, false, null,
                    windowStart, windowStatus,
                    "accepted into window [" + windowStart + "," + windowEnd
                            + "), watermark=" + st.watermark());
            log.info("ACCEPT       symbol={} eventId={} seq={} eventTime={} price={} volume={} windowStart={} -> {}",
                    event.symbol(), event.eventId(), event.sequence(), event.eventTime(),
                    event.price(), event.volume(), windowStart, v.message());
            return v;
        }
    }

    private IngestionVerdict late(TickEvent event, long windowStart, String message) {
        return new IngestionVerdict(event.eventId(), false, false,
                RejectReason.WINDOW_CLOSED_LATE, windowStart, "LATE", message);
    }

    public List<OhlcvSnapshot> query(String symbol, Long fromMs, Long toMs, boolean includeOpen) {
        SymbolState st = state(symbol);
        synchronized (st.lock) {
            List<OhlcvSnapshot> out = new ArrayList<>();
            for (OhlcvSnapshot s : st.closedWindows.values()) {
                if (inRange(s.windowStart(), fromMs, toMs)) {
                    out.add(s);
                }
            }
            if (includeOpen) {
                for (WindowState w : st.openWindows.values()) {
                    if (inRange(w.windowStart, fromMs, toMs)) {
                        out.add(w.snapshot());
                    }
                }
            }
            return out;
        }
    }

    public List<IngestionVerdict> lateRecords(String symbol) {
        SymbolState st = state(symbol);
        synchronized (st.lock) {
            return List.copyOf(st.lateRecords);
        }
    }

    private static boolean inRange(long windowStart, Long fromMs, Long toMs) {
        return (fromMs == null || windowStart >= fromMs) && (toMs == null || windowStart < toMs);
    }

    private static long floorWindow(long eventTime, long windowSize) {
        return Math.floorDiv(eventTime, windowSize) * windowSize;
    }

    /** 单个标的的全部可变状态；一切读写均在 {@code lock} 上串行，对外只发布不可变快照。 */
    private static final class SymbolState {
        private final String symbol;
        private final AggregatorProperties props;
        private final Object lock = new Object();
        private long maxEventTime = Long.MIN_VALUE;
        private long watermark = Long.MIN_VALUE;
        /**
         * key=事件时间，value=该时间点上已接受事件的最小/最大 sequence。
         * 序列检查要求按 (eventTime, sequence) 排序后严格递增，因此对新事件需同时比较前驱与后继。
         */
        private final TreeMap<Long, SeqBounds> seqIndex = new TreeMap<>();
        private final TreeMap<Long, WindowState> openWindows = new TreeMap<>();
        private final TreeMap<Long, OhlcvSnapshot> closedWindows = new TreeMap<>();
        private final ArrayDeque<IngestionVerdict> lateRecords = new ArrayDeque<>();

        private SymbolState(String symbol, AggregatorProperties props) {
            this.symbol = symbol;
            this.props = props;
        }

        private long watermark() {
            return watermark;
        }

        private void upsertSeqIndex(long eventTime, long sequence) {
            SeqBounds b = seqIndex.get(eventTime);
            if (b == null) {
                seqIndex.put(eventTime, new SeqBounds(sequence, sequence));
            } else {
                seqIndex.put(eventTime,
                        new SeqBounds(Math.min(b.minSeq, sequence), Math.max(b.maxSeq, sequence)));
            }
        }

        /**
         * 推进 watermark 并关闭 end &le; watermark 的窗口。
         * 幂等：重复调用不改变任何已确认结果。
         */
        private void advanceWatermark() {
            long newWm = maxEventTime - props.getAllowedLatenessMs();
            if (newWm <= watermark) {
                return;
            }
            watermark = newWm;
            // 窗口 [start, start+W) 的关闭条件是 end <= watermark，即 start <= watermark - W
            long maxStartToClose = watermark - props.getWindowSizeMs();
            NavigableMap<Long, WindowState> closing = openWindows.headMap(maxStartToClose, true);
            for (Map.Entry<Long, WindowState> e : closing.entrySet()) {
                OhlcvSnapshot snap = e.getValue().freeze();
                closedWindows.put(e.getKey(), snap);
            }
            closing.clear();
            // 已关闭窗口不可能再被写入，但其相邻/允许迟到范围内的序列检查仍可能需要索引；
            // 仅回收 eventTime <= watermark-W 的时间点（这些时间点所属窗口必已关闭且不会再接受事件）
            seqIndex.headMap(watermark - props.getWindowSizeMs(), true).clear();
            pruneClosedWindows();
        }

        private void pruneClosedWindows() {
            int max = props.getRetainedClosedWindows();
            while (closedWindows.size() > max) {
                closedWindows.pollFirstEntry();
            }
        }

        private void recordLate(IngestionVerdict v) {
            lateRecords.addLast(v);
            while (lateRecords.size() > props.getMaxLateRecordsPerSymbol()) {
                lateRecords.pollFirst();
            }
        }
    }

    /** 同一事件时间点上已接受序列号的上下界。 */
    private record SeqBounds(long minSeq, long maxSeq) {
    }

    /** 事件在窗口内的确定性先后次序：先按事件时间，同时间按序列号。 */
    private record TickKey(long eventTime, long sequence) implements Comparable<TickKey> {
        @Override
        public int compareTo(TickKey o) {
            int c = Long.compare(eventTime, o.eventTime);
            return c != 0 ? c : Long.compare(sequence, o.sequence);
        }
    }

    /** 一个窗口的可变累加器，仅在标的锁内访问。 */
    private static final class WindowState {
        private final String symbol;
        private final long windowStart;
        private final long windowEnd;
        private double open;
        private double high;
        private double low;
        private double close;
        private long volume;
        private long tickCount;
        private TickKey openKey;
        private TickKey closeKey;
        private boolean initialized;
        private boolean closed;

        private WindowState(String symbol, long windowStart, long windowEnd) {
            this.symbol = symbol;
            this.windowStart = windowStart;
            this.windowEnd = windowEnd;
        }

        private void add(TickEvent e) {
            double p = e.price();
            TickKey k = new TickKey(e.eventTime(), e.sequence());
            if (!initialized) {
                open = high = low = close = p;
                openKey = closeKey = k;
                initialized = true;
            } else {
                if (p > high) {
                    high = p;
                }
                if (p < low) {
                    low = p;
                }
                if (k.compareTo(openKey) < 0) {
                    open = p;
                    openKey = k;
                }
                if (k.compareTo(closeKey) > 0) {
                    close = p;
                    closeKey = k;
                }
            }
            volume += e.volume();
            tickCount++;
        }

        private OhlcvSnapshot freeze() {
            closed = true;
            return snapshot();
        }

        private boolean closed() {
            return closed;
        }

        private OhlcvSnapshot snapshot() {
            return new OhlcvSnapshot(symbol, windowStart, windowEnd,
                    initialized ? open : null,
                    initialized ? high : null,
                    initialized ? low : null,
                    initialized ? close : null,
                    volume, tickCount, closed ? "CLOSED" : "OPEN");
        }
    }
}
