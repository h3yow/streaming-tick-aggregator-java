package com.github.h3yow.tickagg;

import com.github.h3yow.tickagg.model.IngestionResult;
import com.github.h3yow.tickagg.model.IngestionStatus;
import com.github.h3yow.tickagg.model.OhlcBar;
import com.github.h3yow.tickagg.model.RejectedTickRecord;
import com.github.h3yow.tickagg.model.RejectionReason;
import com.github.h3yow.tickagg.model.ReprocessResult;
import com.github.h3yow.tickagg.model.SymbolStats;
import com.github.h3yow.tickagg.model.TickEvent;
import com.github.h3yow.streamingtickaggregatorjava.Application;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end check of the operations API: rejection retention query,
 * lateness widening + reprocessing, and runtime stats over HTTP.
 *
 * Window = 60s, default allowedLateness = 30s. B is a window boundary;
 * ok1/late1/wmk all land in W1 = [B+60s, B+120s).
 */
@SpringBootTest(classes = Application.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class OpsApiIntegrationTest {

    private static final Logger log = LoggerFactory.getLogger(OpsApiIntegrationTest.class);
    private static final long B = 1_700_000_040_000L;
    private static final String SYM = "OPSUSD";

    @Autowired
    private TestRestTemplate rest;

    @Test
    void retentionReprocessAndStatsRoundTrip() {
        try {
            TickEvent ok1 = new TickEvent("ops-1", SYM, 1L, B + 65_000, 100.0, 1.0);
            TickEvent wmk = new TickEvent("ops-2", SYM, 2L, B + 100_000, 110.0, 1.0);
            TickEvent late1 = new TickEvent("ops-3", SYM, 3L, B + 62_000, 105.0, 2.0);

            log.info("POST {} -> {}", ok1, rest.postForObject("/api/ticks", ok1, IngestionResult.class));
            log.info("POST {} -> {}", wmk, rest.postForObject("/api/ticks", wmk, IngestionResult.class));
            IngestionResult rejected = rest.postForObject("/api/ticks", late1, IngestionResult.class);
            log.info("POST {} -> {}", late1, rejected);
            assertEquals(IngestionStatus.REJECTED, rejected.status());
            assertEquals(RejectionReason.LATE_BEYOND_ALLOWED_LATENESS, rejected.rejectionReason());

            // 1) The rejection is retained with its decision context.
            RejectedTickRecord[] retained = rest.getForObject(
                    "/api/ticks/rejections?symbol=" + SYM + "&from=0&to=" + Long.MAX_VALUE,
                    RejectedTickRecord[].class);
            log.info("GET rejections -> {}", (Object) retained);
            assertEquals(1, retained.length);
            assertEquals(late1, retained[0].event());
            assertEquals(RejectionReason.LATE_BEYOND_ALLOWED_LATENESS, retained[0].reason());
            assertEquals(B + 100_000, retained[0].watermarkMs());
            assertEquals(30_000L, retained[0].allowedLatenessMs());

            // 2) Stats reflect the runtime state without disturbing writes.
            SymbolStats[] stats = rest.getForObject(
                    "/api/ticks/stats?symbol=" + SYM, SymbolStats[].class);
            log.info("GET stats -> {}", (Object) stats);
            assertEquals(1, stats.length);
            assertEquals(2, stats[0].acceptedTotal());
            assertEquals(1, stats[0].retainedRejections());
            assertEquals(B + 100_000, stats[0].watermarkMs());

            // 3) Widen the lateness budget and reprocess over HTTP.
            ReprocessResult[] pass1 = rest.postForObject(
                    "/api/ticks/reprocess?symbol=" + SYM + "&allowedLatenessMs=60000",
                    null, ReprocessResult[].class);
            log.info("POST reprocess(60s) -> {}", (Object) pass1);
            assertEquals(1, pass1.length);
            assertEquals(1, pass1[0].scanned());
            assertEquals(1, pass1[0].accepted());
            assertEquals(0, pass1[0].stillRejected());

            // The reprocessed event is now aggregated exactly as if it had
            // arrived inside the budget: open is the earliest tick (late1).
            OhlcBar[] bars = rest.getForObject(
                    "/api/ticks/aggregates?symbol=" + SYM + "&from=" + (B + 60_000)
                            + "&to=" + (B + 120_000) + "&includeUnconfirmed=true",
                    OhlcBar[].class);
            log.info("GET aggregates(W1) -> {}", (Object) bars);
            assertEquals(1, bars.length);
            assertEquals(3, bars[0].tickCount());
            assertEquals(105.0, bars[0].open(), "late1 @B+62s is the earliest tick");
            assertEquals(110.0, bars[0].close(), "wmk @B+100s is the latest tick");
            assertEquals(4.0, bars[0].volume());

            // 4) Repeating the pass is a stable no-op.
            ReprocessResult[] pass2 = rest.postForObject(
                    "/api/ticks/reprocess?symbol=" + SYM, null, ReprocessResult[].class);
            log.info("POST reprocess(again) -> {}", (Object) pass2);
            assertEquals(0, pass2[0].scanned(), "nothing left in retention");
            assertEquals(0, pass2[0].accepted());
            OhlcBar[] barsAfter = rest.getForObject(
                    "/api/ticks/aggregates?symbol=" + SYM + "&from=" + (B + 60_000)
                            + "&to=" + (B + 120_000) + "&includeUnconfirmed=true",
                    OhlcBar[].class);
            assertEquals(3, barsAfter[0].tickCount(), "repeated reprocess changes nothing");

            SymbolStats[] all = rest.getForObject("/api/ticks/stats", SymbolStats[].class);
            log.info("GET stats(all) -> {}", (Object) all);
            assertTrue(all.length >= 1);
        } finally {
            // Restore the service-wide default so other tests are unaffected.
            ReprocessResult[] reset = rest.postForObject(
                    "/api/ticks/reprocess?allowedLatenessMs=30000", null, ReprocessResult[].class);
            log.info("POST reprocess(restore 30s) -> {}", (Object) reset);
        }
    }
}
