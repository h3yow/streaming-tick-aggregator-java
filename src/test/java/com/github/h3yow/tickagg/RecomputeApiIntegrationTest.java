package com.github.h3yow.tickagg;

import com.github.h3yow.streamingtickaggregatorjava.Application;
import com.github.h3yow.tickagg.model.IngestionResult;
import com.github.h3yow.tickagg.model.IngestionStatus;
import com.github.h3yow.tickagg.model.OhlcBar;
import com.github.h3yow.tickagg.model.RecomputeOutcome;
import com.github.h3yow.tickagg.model.RetainedRejection;
import com.github.h3yow.tickagg.model.SymbolStats;
import com.github.h3yow.tickagg.model.TickEvent;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end flow over HTTP: late rejection is retained, lateness is
 * widened at runtime, recompute folds the retained event as if it had
 * arrived on time, and stats/retained endpoints stay consistent.
 */
@SpringBootTest(classes = Application.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class RecomputeApiIntegrationTest {

    private static final Logger log = LoggerFactory.getLogger(RecomputeApiIntegrationTest.class);
    private static final long B = 1_700_000_040_000L;
    private static final String SYM = "RECOMPUTEUSD";

    @Autowired
    private TestRestTemplate rest;

    @Test
    void retainWidenRecomputeRoundTrip() {
        TickEvent a = new TickEvent("rc-a", SYM, 1L, B + 2_000, 100.0, 1.0);
        TickEvent b = new TickEvent("rc-b", SYM, 2L, B + 40_000, 105.0, 2.0);
        TickEvent wm = new TickEvent("rc-wm", SYM, 3L, B + 65_000, 110.0, 3.0);
        TickEvent late = new TickEvent("rc-late", SYM, 4L, B + 10_000, 120.0, 4.0);

        for (TickEvent e : new TickEvent[]{a, b, wm}) {
            IngestionResult r = rest.postForObject("/api/ticks", e, IngestionResult.class);
            log.info("POST {} -> {}", e, r);
            assertEquals(IngestionStatus.ACCEPTED, r.status());
        }
        IngestionResult rejected = rest.postForObject("/api/ticks", late, IngestionResult.class);
        log.info("POST(late) {} -> {}", late, rejected);
        assertEquals(IngestionStatus.REJECTED, rejected.status());

        RetainedRejection[] retained = rest.getForObject(
                "/api/ticks/retained?symbol=" + SYM + "&from=" + B + "&to=" + (B + 60_000),
                RetainedRejection[].class);
        log.info("GET retained -> {}", (Object) retained);
        assertEquals(1, retained.length);
        assertFalse(retained[0].incorporated());
        assertEquals(B + 65_000, retained[0].watermarkAtDecisionMs());

        // W0 has only a,b before recompute.
        OhlcBar[] before = rest.getForObject(
                "/api/ticks/aggregates?symbol=" + SYM + "&from=" + B + "&to=" + (B + 60_000)
                        + "&includeUnconfirmed=true", OhlcBar[].class);
        assertEquals(2, before[0].tickCount());

        rest.postForEntity("/api/ticks/admin/lateness?allowedLatenessMs=120000&symbol=" + SYM,
                null, String.class);
        RecomputeOutcome outcome = rest.postForObject(
                "/api/ticks/admin/recompute?symbol=" + SYM, null, RecomputeOutcome.class);
        log.info("POST recompute -> {}", outcome);
        assertEquals(1, outcome.incorporated());

        OhlcBar[] after = rest.getForObject(
                "/api/ticks/aggregates?symbol=" + SYM + "&from=" + B + "&to=" + (B + 60_000)
                        + "&includeUnconfirmed=true", OhlcBar[].class);
        log.info("GET aggregates after recompute -> {}", (Object) after);
        // W0: a@2s, late@10s, b@40s (wm@65s belongs to W1): 3 ticks, volume 7.
        assertEquals(3, after[0].tickCount());
        assertEquals(120.0, after[0].high());
        assertEquals(7.0, after[0].volume());
        assertFalse(after[0].confirmed());

        // Repeating recompute is a stable no-op.
        RecomputeOutcome again = rest.postForObject(
                "/api/ticks/admin/recompute?symbol=" + SYM, null, RecomputeOutcome.class);
        log.info("POST recompute again -> {}", again);
        assertEquals(0, again.incorporated());
        assertEquals(1, again.skippedAlreadyIncorporated());

        SymbolStats stats = rest.getForObject("/api/ticks/stats?symbol=" + SYM, SymbolStats.class);
        log.info("GET stats -> {}", stats);
        assertEquals(1, stats.incorporatedCount());
        assertEquals(1, stats.retainedRejections());
        assertEquals(1, stats.rejectedLateCount());
        assertEquals(120_000, stats.allowedLatenessMs());
        assertTrue(stats.duplicateCount() >= 0);

        SymbolStats[] all = rest.getForObject("/api/ticks/stats/all", SymbolStats[].class);
        assertTrue(all.length >= 1);
    }
}
