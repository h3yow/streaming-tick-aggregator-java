package com.github.h3yow.tickagg.api;

import com.github.h3yow.tickagg.aggregate.WindowAggregator;
import com.github.h3yow.tickagg.ingest.TickIngestionService;
import com.github.h3yow.tickagg.model.IngestionResult;
import com.github.h3yow.tickagg.model.OhlcBar;
import com.github.h3yow.tickagg.model.RecomputeOutcome;
import com.github.h3yow.tickagg.model.RetainedRejection;
import com.github.h3yow.tickagg.model.SymbolStats;
import com.github.h3yow.tickagg.model.TickEvent;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/api/ticks")
public class TickController {

    private final TickIngestionService ingestionService;
    private final WindowAggregator aggregator;

    public TickController(TickIngestionService ingestionService, WindowAggregator aggregator) {
        this.ingestionService = ingestionService;
        this.aggregator = aggregator;
    }

    @PostMapping
    public IngestionResult ingest(@RequestBody TickEvent event) {
        return ingestionService.ingest(event);
    }

    @GetMapping("/aggregates")
    public List<OhlcBar> aggregates(@RequestParam String symbol,
                                    @RequestParam long from,
                                    @RequestParam long to,
                                    @RequestParam(defaultValue = "false") boolean includeUnconfirmed) {
        return aggregator.query(symbol, from, to, includeUnconfirmed);
    }

    /** Retained late-rejected events, queryable by symbol and event-time range. */
    @GetMapping("/retained")
    public List<RetainedRejection> retained(@RequestParam String symbol,
                                            @RequestParam long from,
                                            @RequestParam long to) {
        return aggregator.queryRetained(symbol, from, to);
    }

    /** One symbol's point-in-time bookkeeping snapshot; 404 for unknown symbols. */
    @GetMapping("/stats")
    public ResponseEntity<SymbolStats> stats(@RequestParam String symbol) {
        SymbolStats stats = aggregator.stats(symbol);
        return stats == null ? ResponseEntity.notFound().build() : ResponseEntity.ok(stats);
    }

    /** Point-in-time bookkeeping snapshots for every symbol that holds state. */
    @GetMapping("/stats/all")
    public List<SymbolStats> statsAll() {
        return aggregator.statsAll();
    }

    /**
     * Widen the allowed lateness (runtime, never narrows). Pass a symbol to
     * affect only that symbol; otherwise the new default applies globally.
     */
    @PostMapping("/admin/lateness")
    public ResponseEntity<String> relaxLateness(@RequestParam long allowedLatenessMs,
                                                @RequestParam(required = false) String symbol) {
        try {
            aggregator.relaxAllowedLateness(allowedLatenessMs, symbol);
        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest().body(e.getMessage());
        }
        return ResponseEntity.ok("allowedLatenessMs widened to " + allowedLatenessMs
                + (symbol == null ? " (all symbols)" : " for symbol=" + symbol));
    }

    /**
     * Replay one symbol's retained late-rejected events under the current
     * lateness and fold them into the still-open windows.
     */
    @PostMapping("/admin/recompute")
    public RecomputeOutcome recompute(@RequestParam String symbol) {
        return aggregator.recompute(symbol);
    }
}
