package com.github.h3yow.tickagg.model;

public record OhlcBar(
        String symbol,
        long windowStartEpochMs,
        long windowEndEpochMs,
        double open,
        double high,
        double low,
        double close,
        double volume,
        long tickCount,
        boolean confirmed) {
}
