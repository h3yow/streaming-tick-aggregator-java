package com.github.h3yow.streamingtickaggregatorjava.model;

/**
 * 一个时间窗口的 OHLCV 聚合快照。
 * status=CLOSED 表示窗口已关闭、结果已确认不可变；OPEN 仅在 includeOpen=true 时返回一致快照。
 */
public record OhlcvSnapshot(
        String symbol,
        long windowStart,
        long windowEnd,
        Double open,
        Double high,
        Double low,
        Double close,
        long volume,
        long tickCount,
        String status
) {
}
