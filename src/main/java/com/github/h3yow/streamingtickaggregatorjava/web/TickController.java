package com.github.h3yow.streamingtickaggregatorjava.web;

import com.github.h3yow.streamingtickaggregatorjava.model.IngestionVerdict;
import com.github.h3yow.streamingtickaggregatorjava.model.TickEvent;
import com.github.h3yow.streamingtickaggregatorjava.service.TickIngestionService;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * 行情事件接入端点。
 */
@RestController
@RequestMapping("/api/ticks")
public class TickController {

    private final TickIngestionService ingestionService;

    public TickController(TickIngestionService ingestionService) {
        this.ingestionService = ingestionService;
    }

    @PostMapping
    public IngestionVerdict ingest(@RequestBody TickEvent event) {
        return ingestionService.ingest(event);
    }
}
