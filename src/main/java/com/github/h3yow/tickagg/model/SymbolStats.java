package com.github.h3yow.tickagg.model;

/**
 * Consistent, immutable snapshot of one symbol's bookkeeping at a point in time.
 */
public record SymbolStats(
        String symbol,
        long watermarkMs,
        long sequenceFloor,
        long allowedLatenessMs,
        int retainedWindows,
        int sealedWindows,
        int dedupEntries,
        int retainedRejections,
        long acceptedCount,
        long duplicateCount,
        long rejectedLateCount,
        long rejectedSequenceRegressionCount,
        long retainedRejectionEvictedCount,
        long incorporatedCount) {
}
