package com.github.h3yow.tickagg.model;

/**
 * A rejected event retained for audit and later reprocessing, together with
 * the decision context captured at rejection time. Immutable snapshot: a
 * retained record never changes the aggregate it was rejected from.
 */
public record RejectedTickRecord(
        TickEvent event,
        RejectionReason reason,
        String detail,
        long watermarkMs,
        long sequenceFloor,
        long allowedLatenessMs,
        long rejectedAtEpochMs) {
}
