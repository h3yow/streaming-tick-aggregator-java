package com.github.h3yow.tickagg.model;

public record TickEvent(
        String eventId,
        String symbol,
        Long sequence,
        Long timestampEpochMs,
        Double price,
        Double volume) {
}
