package com.github.h3yow.tickagg.model;

import java.util.Map;

/**
 * Outcome of one reprocessing pass over a symbol's retained rejections.
 */
public record ReprocessResult(
        String symbol,
        long allowedLatenessMs,
        int scanned,
        int accepted,
        int alreadyCounted,
        int stillRejected,
        Map<RejectionReason, Long> remainingByReason) {
}
