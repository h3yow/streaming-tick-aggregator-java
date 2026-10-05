package com.github.h3yow.tickagg.model;

import java.util.List;

/**
 * Result of one controlled recomputation pass over retained rejections.
 */
public record RecomputeOutcome(
        String symbol,
        long newAllowedLatenessMs,
        int candidates,
        int incorporated,
        int skippedAlreadyIncorporated,
        int skippedWindowSealed,
        int skippedDuplicate,
        List<RetainedRejection> incorporatedEvents) {
}
