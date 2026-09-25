package com.github.h3yow.streamingtickaggregatorjava.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 聚合器可配置项（前缀 aggregator）。
 */
@ConfigurationProperties(prefix = "aggregator")
public class AggregatorProperties {

    /** 窗口长度（毫秒）。 */
    private long windowSizeMs = 60_000;

    /** 允许乱序延迟（毫秒）：watermark = maxObservedEventTime - allowedLatenessMs。 */
    private long allowedLatenessMs = 10_000;

    /** 每个标的保留的已关闭窗口数量上限（界内存）。 */
    private int retainedClosedWindows = 1000;

    /** 每个标的保留的迟到事件分类记录上限。 */
    private int maxLateRecordsPerSymbol = 1000;

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

    public int getRetainedClosedWindows() {
        return retainedClosedWindows;
    }

    public void setRetainedClosedWindows(int retainedClosedWindows) {
        this.retainedClosedWindows = retainedClosedWindows;
    }

    public int getMaxLateRecordsPerSymbol() {
        return maxLateRecordsPerSymbol;
    }

    public void setMaxLateRecordsPerSymbol(int maxLateRecordsPerSymbol) {
        this.maxLateRecordsPerSymbol = maxLateRecordsPerSymbol;
    }
}
