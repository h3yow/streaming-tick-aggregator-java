package com.github.h3yow.tickagg.model;

public enum RejectionReason {
    MISSING_FIELD,
    INVALID_VALUE,
    INVALID_TIMESTAMP,
    SEQUENCE_REGRESSION,
    LATE_BEYOND_ALLOWED_LATENESS,
    WINDOW_CLOSED
}
