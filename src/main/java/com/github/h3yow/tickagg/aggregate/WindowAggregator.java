package com.github.h3yow.tickagg.aggregate;

import com.github.h3yow.tickagg.config.AggregatorProperties;
import com.github.h3yow.tickagg.model.IngestionResult;
import com.github.h3yow.tickagg.model.OhlcBar;
import com.github.h3yow.tickagg.model.RejectedTickRecord;
import com.github.h3yow.tickagg.model.ReprocessResult;
import com.github.h3yow.tickagg.model.SymbolStats;
import com.github.h3yow.tickagg.model.TickEvent;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Routes events to per-symbol state. Each symbol's state is confined to a
 * single SymbolWindows instance whose monitor serializes writes and reads,
 * so queries always observe a consistent snapshot and concurrent writers
 * cannot lose or double-count events.
 */
@Component
public class WindowAggregator {

    private final AggregatorProperties properties;
    private final ConcurrentHashMap<String, SymbolWindows> bySymbol = new ConcurrentHashMap<>();

    public WindowAggregator(AggregatorProperties properties) {
        this.properties = properties;
    }

    public IngestionResult ingest(TickEvent event) {
        return bySymbol
                .computeIfAbsent(event.symbol(), s -> new SymbolWindows(s, properties))
                .ingest(event);
    }

    public List<OhlcBar> query(String symbol, long fromEpochMs, long toEpochMs, boolean includeUnconfirmed) {
        SymbolWindows state = bySymbol.get(symbol);
        if (state == null) {
            return List.of();
        }
        return state.query(fromEpochMs, toEpochMs, includeUnconfirmed);
    }

    /** Retained rejections for one symbol, filtered by event-time range. */
    public List<RejectedTickRecord> queryRejections(String symbol, long fromEpochMs, long toEpochMs) {
        SymbolWindows state = bySymbol.get(symbol);
        if (state == null) {
            return List.of();
        }
        return state.queryRejections(fromEpochMs, toEpochMs);
    }

    /**
     * Reprocesses retained rejections. When {@code symbol} is null every
     * known symbol is reprocessed; each symbol is passed independently, so
     * one symbol's pass never blocks another symbol's ingestion or queries.
     */
    public List<ReprocessResult> reprocess(String symbol) {
        List<ReprocessResult> out = new ArrayList<>();
        if (symbol != null) {
            SymbolWindows state = bySymbol.get(symbol);
            if (state != null) {
                out.add(state.reprocess());
            }
            return out;
        }
        for (SymbolWindows state : bySymbol.values()) {
            out.add(state.reprocess());
        }
        return out;
    }

    /** Consistent per-symbol runtime snapshot; null symbol means all symbols. */
    public List<SymbolStats> stats(String symbol) {
        List<SymbolStats> out = new ArrayList<>();
        if (symbol != null) {
            SymbolWindows state = bySymbol.get(symbol);
            if (state != null) {
                out.add(state.stats());
            }
            return out;
        }
        for (SymbolWindows state : bySymbol.values()) {
            out.add(state.stats());
        }
        return out;
    }
}
