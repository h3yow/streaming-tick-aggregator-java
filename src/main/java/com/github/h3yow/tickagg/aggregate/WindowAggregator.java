package com.github.h3yow.tickagg.aggregate;

import com.github.h3yow.tickagg.config.AggregatorProperties;
import com.github.h3yow.tickagg.model.IngestionResult;
import com.github.h3yow.tickagg.model.OhlcBar;
import com.github.h3yow.tickagg.model.TickEvent;
import org.springframework.stereotype.Component;

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
}
