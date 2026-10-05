package com.github.h3yow.tickagg.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "tick.aggregator")
public class AggregatorProperties {

    private long windowSizeMs = 60_000;
    private long allowedLatenessMs = 30_000;
    private int maxRetainedWindowsPerSymbol = 1024;
    /**
     * @deprecated FIFO-evicting dedup entries is unsafe: an evicted id could
     * be redelivered and, inside its still-open window, be counted a second
     * time. Dedup entries are therefore retained until their window seals
     * (and reclaimed wholesale then), so this knob no longer enforces an
     * eviction. It is kept only for configuration compatibility. Long-term
     * memory stays bounded because every open window seals as the watermark
     * advances; observe the live footprint via the stats endpoint.
     */
    @Deprecated
    private int maxDedupEntriesPerSymbol = 65_536;
    private int maxRetainedRejectionsPerSymbol = 4096;

    public long getWindowSizeMs() {
        return windowSizeMs;
    }

    public void setWindowSizeMs(long windowSizeMs) {
        this.windowSizeMs = windowSizeMs;
    }

    public long getAllowedLatenessMs() {
        return allowedLatenessMs;
    }

    public void setAllowedLatenessMs(long allowedLatenessMs) {
        this.allowedLatenessMs = allowedLatenessMs;
    }

    public int getMaxRetainedWindowsPerSymbol() {
        return maxRetainedWindowsPerSymbol;
    }

    public void setMaxRetainedWindowsPerSymbol(int maxRetainedWindowsPerSymbol) {
        this.maxRetainedWindowsPerSymbol = maxRetainedWindowsPerSymbol;
    }

    public int getMaxDedupEntriesPerSymbol() {
        return maxDedupEntriesPerSymbol;
    }

    public void setMaxDedupEntriesPerSymbol(int maxDedupEntriesPerSymbol) {
        this.maxDedupEntriesPerSymbol = maxDedupEntriesPerSymbol;
    }

    public int getMaxRetainedRejectionsPerSymbol() {
        return maxRetainedRejectionsPerSymbol;
    }

    public void setMaxRetainedRejectionsPerSymbol(int maxRetainedRejectionsPerSymbol) {
        this.maxRetainedRejectionsPerSymbol = maxRetainedRejectionsPerSymbol;
    }
}
