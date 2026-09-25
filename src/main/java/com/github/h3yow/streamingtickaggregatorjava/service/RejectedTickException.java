package com.github.h3yow.streamingtickaggregatorjava.service;

import com.github.h3yow.streamingtickaggregatorjava.model.RejectReason;

/**
 * 接入请求被拒绝（字段问题 / 序列回退 / 窗口关闭）。
 * HTTP 层映射为 422，并携带可区分 reason。
 */
public class RejectedTickException extends RuntimeException {

    private final RejectReason reason;

    public RejectedTickException(RejectReason reason, String message) {
        super(message);
        this.reason = reason;
    }

    public RejectReason getReason() {
        return reason;
    }
}
