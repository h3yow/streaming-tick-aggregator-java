package com.github.h3yow.tickagg.aggregate;

import com.github.h3yow.tickagg.model.OhlcBar;
import com.github.h3yow.tickagg.model.TickEvent;

/**
 * Mutable accumulator for a single tumbling window of one symbol.
 *
 * Open is the price of the tick with the smallest event timestamp;
 * close is the price of the tick with the largest event timestamp.
 * Ties are broken by sequence so that the result is independent of
 * arrival order. Not thread-safe: callers must synchronize externally.
 */
final class WindowState {

    private final String symbol;
    private final long windowStartEpochMs;
    private final long windowSizeMs;

    private double open;
    private double high;
    private double low;
    private double close;
    private double volume;
    private long tickCount;

    private long earliestTs = Long.MAX_VALUE;
    private long earliestSeq = Long.MAX_VALUE;
    private long latestTs = Long.MIN_VALUE;
    private long latestSeq = Long.MIN_VALUE;
    private long maxSequence = Long.MIN_VALUE;

    /**
     * Latch set the first time the window becomes externally confirmed.
     * A sealed window is immutable forever: relaxing allowed lateness and
     * recomputing retained events can never rewrite an already-published
     * result.
     */
    private boolean sealed;

    WindowState(String symbol, long windowStartEpochMs, long windowSizeMs) {
        this.symbol = symbol;
        this.windowStartEpochMs = windowStartEpochMs;
        this.windowSizeMs = windowSizeMs;
    }

    void add(TickEvent event) {
        long ts = event.timestampEpochMs();
        long seq = event.sequence();
        double price = event.price();

        if (seq > maxSequence) {
            maxSequence = seq;
        }

        if (tickCount == 0) {
            open = close = high = low = price;
            earliestTs = latestTs = ts;
            earliestSeq = latestSeq = seq;
        } else {
            if (price > high) {
                high = price;
            }
            if (price < low) {
                low = price;
            }
            // Order-independent open: smallest (ts, seq) wins.
            if (ts < earliestTs || (ts == earliestTs && seq < earliestSeq)) {
                earliestTs = ts;
                earliestSeq = seq;
                open = price;
            }
            // Order-independent close: largest (ts, seq) wins.
            if (ts > latestTs || (ts == latestTs && seq > latestSeq)) {
                latestTs = ts;
                latestSeq = seq;
                close = price;
            }
        }
        volume += event.volume();
        tickCount++;
    }

    long maxSequence() {
        return maxSequence;
    }

    boolean isSealed() {
        return sealed;
    }

    /** Idempotent latch: first confirmation seals the window forever. */
    void seal() {
        this.sealed = true;
    }

    OhlcBar snapshot(boolean confirmed) {
        return new OhlcBar(symbol, windowStartEpochMs, windowStartEpochMs + windowSizeMs,
                open, high, low, close, volume, tickCount, confirmed);
    }
}
