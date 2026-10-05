package com.github.h3yow.tickagg.model;

import java.util.Map;

/**
 * Point-in-time, internally consistent snapshot of one symbol's runtime
 * state. Assembled under the symbol lock, so readers never observe a
 * half-updated counter set.
 */
public record SymbolStats(
        String symbol,
        long watermarkMs,
        long sequenceFloor,
        long confirmedHorizonMs,
        long allowedLatenessMs,
        int retainedWindows,
        int confirmedWindows,
        int dedupEntries,
        int retainedRejections,
        long acceptedTotal,
        long duplicateTotal,
        Map<RejectionReason, Long> rejectionsByReason,
        long windowsEvictedTotal,
        long dedupEvictedTotal,
        long rejectionsEvictedTotal,
        long reprocessRunsTotal,
        long reprocessAcceptedTotal) {
}
