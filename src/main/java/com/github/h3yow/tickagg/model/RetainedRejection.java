package com.github.h3yow.tickagg.model;

/**
 * A retained late-beyond-lateness rejection, kept aside (independently of
 * confirmed windows) for later inspection and controlled recomputation.
 */
public record RetainedRejection(
        String eventId,
        String symbol,
        long sequence,
        long eventTimestampEpochMs,
        double price,
        double volume,
        RejectionReason reason,
        String decisionDetail,
        long watermarkAtDecisionMs,
        long latenessFloorAtDecisionMs,
        long allowedLatenessAtDecisionMs,
        long rejectedAtWallClockMs,
        boolean incorporated) {
}
