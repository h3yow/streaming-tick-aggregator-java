package com.github.h3yow.tickagg;

import com.github.h3yow.tickagg.model.IngestionResult;
import com.github.h3yow.tickagg.model.IngestionStatus;
import com.github.h3yow.tickagg.model.OhlcBar;
import com.github.h3yow.tickagg.model.RejectionReason;
import com.github.h3yow.streamingtickaggregatorjava.Application;
import com.github.h3yow.tickagg.model.TickEvent;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end check of the HTTP ingestion protocol and aggregate query API.
 */
@SpringBootTest(classes = Application.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class TickApiIntegrationTest {

    private static final Logger log = LoggerFactory.getLogger(TickApiIntegrationTest.class);
    private static final long B = 1_700_000_040_000L;

    @Autowired
    private TestRestTemplate rest;

    @Test
    void ingestThenQueryRoundTrip() {
        TickEvent e1 = new TickEvent("api-1", "APIUSD", 1L, B + 1_000, 100.0, 2.0);
        TickEvent e2 = new TickEvent("api-2", "APIUSD", 2L, B + 2_000, 104.0, 3.0);

        IngestionResult r1 = rest.postForObject("/api/ticks", e1, IngestionResult.class);
        log.info("POST {} -> {}", e1, r1);
        assertEquals(IngestionStatus.ACCEPTED, r1.status());

        IngestionResult r2 = rest.postForObject("/api/ticks", e2, IngestionResult.class);
        log.info("POST {} -> {}", e2, r2);
        assertEquals(IngestionStatus.ACCEPTED, r2.status());

        IngestionResult dup = rest.postForObject("/api/ticks", e1, IngestionResult.class);
        log.info("POST(redelivery) {} -> {}", e1, dup);
        assertEquals(IngestionStatus.DUPLICATE, dup.status());

        TickEvent bad = new TickEvent("api-3", "APIUSD", 3L, -1L, 1.0, 1.0);
        IngestionResult rejected = rest.postForObject("/api/ticks", bad, IngestionResult.class);
        log.info("POST(invalid-ts) {} -> {}", bad, rejected);
        assertEquals(IngestionStatus.REJECTED, rejected.status());
        assertEquals(RejectionReason.INVALID_TIMESTAMP, rejected.rejectionReason());

        OhlcBar[] bars = rest.getForObject(
                "/api/ticks/aggregates?symbol=APIUSD&from=" + B + "&to=" + (B + 60_000)
                        + "&includeUnconfirmed=true",
                OhlcBar[].class);
        log.info("GET aggregates -> {}", (Object) bars);
        assertEquals(1, bars.length);
        assertEquals(100.0, bars[0].open());
        assertEquals(104.0, bars[0].close());
        assertEquals(104.0, bars[0].high());
        assertEquals(5.0, bars[0].volume());
        assertEquals(2, bars[0].tickCount());

        OhlcBar[] confirmedOnly = rest.getForObject(
                "/api/ticks/aggregates?symbol=APIUSD&from=" + B + "&to=" + (B + 60_000),
                OhlcBar[].class);
        log.info("GET aggregates(confirmed-only) -> {}", (Object) confirmedOnly);
        assertEquals(0, confirmedOnly.length, "window not yet confirmed by watermark");
        assertTrue(bars[0].tickCount() > 0);
    }
}
