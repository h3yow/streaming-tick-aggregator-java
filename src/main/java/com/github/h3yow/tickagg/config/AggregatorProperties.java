package com.github.h3yow.tickagg.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "tick.aggregator")
public class AggregatorProperties {

    private long windowSizeMs = 60_000;
    private long allowedLatenessMs = 30_000;
    private int maxRetainedWindowsPerSymbol = 1024;
    private int maxDedupEntriesPerSymbol = 65_536;

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
}
