package com.github.h3yow.tickagg.aggregate;

import com.github.h3yow.tickagg.config.AggregatorProperties;
import com.github.h3yow.tickagg.model.IngestionResult;
import com.github.h3yow.tickagg.model.OhlcBar;
import com.github.h3yow.tickagg.model.RecomputeOutcome;
import com.github.h3yow.tickagg.model.RetainedRejection;
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
        return stateFor(event.symbol()).ingest(event);
    }

    public List<OhlcBar> query(String symbol, long fromEpochMs, long toEpochMs, boolean includeUnconfirmed) {
        SymbolWindows state = bySymbol.get(symbol);
        if (state == null) {
            return List.of();
        }
        return state.query(fromEpochMs, toEpochMs, includeUnconfirmed);
    }

    /** Retained late-rejected events for a symbol within [from, to) event time. */
    public List<RetainedRejection> queryRetained(String symbol, long fromEpochMs, long toEpochMs) {
        SymbolWindows state = bySymbol.get(symbol);
        if (state == null) {
            return List.of();
        }
        return state.queryRetained(fromEpochMs, toEpochMs);
    }

    /**
     * Replay a symbol's retained rejections under the current lateness.
     * Returns an empty outcome for a symbol that has never been seen.
     */
    public RecomputeOutcome recompute(String symbol) {
        SymbolWindows state = bySymbol.get(symbol);
        if (state == null) {
            return new RecomputeOutcome(symbol, properties.getAllowedLatenessMs(),
                    0, 0, 0, 0, 0, List.of());
        }
        return state.recompute();
    }

    /** Point-in-time bookkeeping snapshot for one symbol, or null if unknown. */
    public SymbolStats stats(String symbol) {
        SymbolWindows state = bySymbol.get(symbol);
        return state == null ? null : state.stats();
    }

    /** Snapshot for every symbol that currently holds state. */
    public List<SymbolStats> statsAll() {
        List<SymbolStats> out = new ArrayList<>(bySymbol.size());
        for (SymbolWindows state : bySymbol.values()) {
            out.add(state.stats());
        }
        return out;
    }

    /**
     * Relax (only widen) the allowed lateness at runtime. If a symbol is
     * given it alone is affected; otherwise the new default applies and is
     * inherited by every existing symbol as well. A request to narrow is
     * applied only as far as the current value (lateness never decreases,
     * because shrinking it would contradict previously accepted events).
     */
    public void relaxAllowedLateness(Long newAllowedLatenessMs, String symbol) {
        if (newAllowedLatenessMs == null || newAllowedLatenessMs < 0) {
            throw new IllegalArgumentException("allowedLatenessMs must be a non-negative number");
        }
        if (symbol != null) {
            stateFor(symbol).relaxAllowedLateness(newAllowedLatenessMs);
            return;
        }
        if (newAllowedLatenessMs > properties.getAllowedLatenessMs()) {
            properties.setAllowedLatenessMs(newAllowedLatenessMs);
        }
        for (SymbolWindows state : bySymbol.values()) {
            state.relaxAllowedLateness(newAllowedLatenessMs);
        }
    }

    private SymbolWindows stateFor(String symbol) {
        return bySymbol.computeIfAbsent(symbol, s -> new SymbolWindows(s, properties));
    }
}
