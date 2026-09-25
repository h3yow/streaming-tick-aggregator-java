package com.github.h3yow.streamingtickaggregatorjava.model;

/**
 * 单条事件接入判定结果。重复投递返回与首次完全一致的判定，duplicate=true。
 */
public record IngestionVerdict(
        String eventId,
        boolean accepted,
        boolean duplicate,
        RejectReason reason,
        Long windowStart,
        String windowStatus,
        String message
) {
}
