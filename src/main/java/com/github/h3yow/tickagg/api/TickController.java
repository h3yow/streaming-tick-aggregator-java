package com.github.h3yow.tickagg.api;

import com.github.h3yow.tickagg.aggregate.WindowAggregator;
import com.github.h3yow.tickagg.ingest.TickIngestionService;
import com.github.h3yow.tickagg.model.IngestionResult;
import com.github.h3yow.tickagg.model.OhlcBar;
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
}
