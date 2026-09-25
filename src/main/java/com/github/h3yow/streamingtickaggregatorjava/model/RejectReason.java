package com.github.h3yow.streamingtickaggregatorjava.model;

/**
 * 事件被拒绝的可区分原因。
 */
public enum RejectReason {

    MISSING_FIELD("必填字段缺失"),
    INVALID_TIMESTAMP("事件时间戳非法"),
    INVALID_PRICE("价格非法"),
    INVALID_VOLUME("成交量非法"),
    INVALID_SEQUENCE("序列号非法"),
    SEQUENCE_REGRESSION("序列回退"),
    WINDOW_CLOSED_LATE("事件晚于允许延迟，窗口已关闭");

    private final String description;

    RejectReason(String description) {
        this.description = description;
    }

    public String getDescription() {
        return description;
    }
}
