package com.github.h3yow.streamingtickaggregatorjava.model;

/**
 * 行情事件接入请求（单条）。
 *
 * @param eventId  事件幂等键（同一事件重复投递必须相同）
 * @param symbol   标的代码
 * @param sequence 标的内单调递增序列号
 * @param eventTime 事件时间（epoch millis）
 * @param price    成交价
 * @param volume   成交量
 */
public record TickEvent(
        String eventId,
        String symbol,
        Long sequence,
        Long eventTime,
        Double price,
        Long volume
) {
}
