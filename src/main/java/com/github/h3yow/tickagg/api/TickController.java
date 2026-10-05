package com.github.h3yow.tickagg.api;

import com.github.h3yow.tickagg.aggregate.WindowAggregator;
import com.github.h3yow.tickagg.config.AggregatorProperties;
import com.github.h3yow.tickagg.ingest.TickIngestionService;
import com.github.h3yow.tickagg.model.IngestionResult;
import com.github.h3yow.tickagg.model.OhlcBar;
import com.github.h3yow.tickagg.model.RejectedTickRecord;
import com.github.h3yow.tickagg.model.ReprocessResult;
import com.github.h3yow.tickagg.model.SymbolStats;
import com.github.h3yow.tickagg.model.TickEvent;
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
    private final AggregatorProperties properties;

    public TickController(TickIngestionService ingestionService,
                          WindowAggregator aggregator,
                          AggregatorProperties properties) {
        this.ingestionService = ingestionService;
        this.aggregator = aggregator;
        this.properties = properties;
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

    /**
     * Retained rejections for a symbol, filtered by event-time range
     * {@code [from, to)}. Each record carries the original event, the
     * rejection reason and the decision context (watermark, sequenceFloor,
     * allowedLateness) captured at rejection time.
     */
    @GetMapping("/rejections")
    public List<RejectedTickRecord> rejections(@RequestParam String symbol,
                                               @RequestParam long from,
                                               @RequestParam long to) {
        return aggregator.queryRejections(symbol, from, to);
    }

    /**
     * Triggers a reprocessing pass over retained rejections. If
     * {@code allowedLatenessMs} is given, the lateness budget is updated
     * first (service-wide), so previously too-late events get another
     * chance under the new horizon. Already-confirmed windows are never
     * rewritten; the pass is idempotent and order-independent.
     */
    @PostMapping("/reprocess")
    public List<ReprocessResult> reprocess(@RequestParam(required = false) String symbol,
                                           @RequestParam(required = false) Long allowedLatenessMs) {
        if (allowedLatenessMs != null) {
            properties.setAllowedLatenessMs(allowedLatenessMs);
        }
        return aggregator.reprocess(symbol);
    }

    /** Per-symbol runtime snapshot: watermark, retention sizes, counters. */
    @GetMapping("/stats")
    public List<SymbolStats> stats(@RequestParam(required = false) String symbol) {
        return aggregator.stats(symbol);
    }
}
