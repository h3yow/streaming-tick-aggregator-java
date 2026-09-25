package com.github.h3yow.streamingtickaggregatorjava.web;

import com.github.h3yow.streamingtickaggregatorjava.model.IngestionVerdict;
import com.github.h3yow.streamingtickaggregatorjava.model.OhlcvSnapshot;
import com.github.h3yow.streamingtickaggregatorjava.service.AggregationQueryService;
import java.util.List;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * OHLCV 查询与迟到事件归类查询端点。
 */
@RestController
@RequestMapping("/api")
public class OhlcvController {

    private final AggregationQueryService queryService;

    public OhlcvController(AggregationQueryService queryService) {
        this.queryService = queryService;
    }

    @GetMapping("/ohlcv")
    public List<OhlcvSnapshot> query(
            @RequestParam String symbol,
            @RequestParam(required = false) Long from,
            @RequestParam(required = false) Long to,
            @RequestParam(defaultValue = "false") boolean includeOpen) {
        return queryService.query(symbol, from, to, includeOpen);
    }

    @GetMapping("/late")
    public List<IngestionVerdict> lateRecords(@RequestParam String symbol) {
        return queryService.lateRecords(symbol);
    }
}
