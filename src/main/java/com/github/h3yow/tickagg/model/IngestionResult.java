package com.github.h3yow.tickagg.model;

public record IngestionResult(
        IngestionStatus status,
        RejectionReason rejectionReason,
        String detail) {

    public static IngestionResult accepted(String detail) {
        return new IngestionResult(IngestionStatus.ACCEPTED, null, detail);
    }

    public static IngestionResult duplicate(String detail) {
        return new IngestionResult(IngestionStatus.DUPLICATE, null, detail);
    }

    public static IngestionResult rejected(RejectionReason reason, String detail) {
        return new IngestionResult(IngestionStatus.REJECTED, reason, detail);
    }
}
