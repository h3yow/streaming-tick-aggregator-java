package com.github.h3yow.tickagg;

import com.github.h3yow.tickagg.aggregate.WindowAggregator;
import com.github.h3yow.tickagg.config.AggregatorProperties;
import com.github.h3yow.tickagg.ingest.TickIngestionService;
import com.github.h3yow.tickagg.model.IngestionResult;
import com.github.h3yow.tickagg.model.IngestionStatus;
import com.github.h3yow.tickagg.model.RejectionReason;
import com.github.h3yow.tickagg.model.TickEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class TickIngestionServiceTest {

    private static final Logger log = LoggerFactory.getLogger(TickIngestionServiceTest.class);
    private static final long T0 = 1_700_000_000_000L;

    private TickIngestionService service;

    @BeforeEach
    void setUp() {
        AggregatorProperties props = new AggregatorProperties();
        service = new TickIngestionService(new WindowAggregator(props));
    }

    private IngestionResult ingestAndLog(String caseName, TickEvent e) {
        IngestionResult r = service.ingest(e);
        log.info("case={} input={} -> status={} reason={} detail={}",
                caseName, e, r.status(), r.rejectionReason(), r.detail());
        return r;
    }

    private static TickEvent valid(String id, long seq, long ts) {
        return new TickEvent(id, "BTCUSDT", seq, ts, 100.0, 1.0);
    }

    @Test
    void acceptsWellFormedEvent() {
        IngestionResult r = ingestAndLog("well-formed", valid("e1", 1L, T0));
        assertEquals(IngestionStatus.ACCEPTED, r.status());
        assertNull(r.rejectionReason());
    }

    @Test
    void rejectsMissingFieldsWithDistinguishableReason() {
        assertEquals(RejectionReason.MISSING_FIELD,
                ingestAndLog("null-eventId", new TickEvent(null, "S", 1L, T0, 1.0, 1.0)).rejectionReason());
        assertEquals(RejectionReason.MISSING_FIELD,
                ingestAndLog("blank-symbol", new TickEvent("e2", " ", 1L, T0, 1.0, 1.0)).rejectionReason());
        assertEquals(RejectionReason.MISSING_FIELD,
                ingestAndLog("null-sequence", new TickEvent("e3", "S", null, T0, 1.0, 1.0)).rejectionReason());
        assertEquals(RejectionReason.MISSING_FIELD,
                ingestAndLog("null-timestamp", new TickEvent("e4", "S", 1L, null, 1.0, 1.0)).rejectionReason());
        assertEquals(RejectionReason.MISSING_FIELD,
                ingestAndLog("null-price", new TickEvent("e5", "S", 1L, T0, null, 1.0)).rejectionReason());
        assertEquals(RejectionReason.MISSING_FIELD,
                ingestAndLog("null-volume", new TickEvent("e6", "S", 1L, T0, 1.0, null)).rejectionReason());
    }

    @Test
    void rejectsInvalidTimestamp() {
        assertEquals(RejectionReason.INVALID_TIMESTAMP,
                ingestAndLog("zero-ts", valid("e1", 1L, 0L)).rejectionReason());
        assertEquals(RejectionReason.INVALID_TIMESTAMP,
                ingestAndLog("negative-ts", valid("e2", 1L, -5L)).rejectionReason());
        assertEquals(RejectionReason.INVALID_TIMESTAMP,
                ingestAndLog("far-future-ts", valid("e3", 1L, 4_102_444_800_001L)).rejectionReason());
    }

    @Test
    void rejectsInvalidPriceAndVolume() {
        assertEquals(RejectionReason.INVALID_VALUE,
                ingestAndLog("nan-price", new TickEvent("e1", "S", 1L, T0, Double.NaN, 1.0)).rejectionReason());
        assertEquals(RejectionReason.INVALID_VALUE,
                ingestAndLog("negative-price", new TickEvent("e2", "S", 1L, T0, -1.0, 1.0)).rejectionReason());
        assertEquals(RejectionReason.INVALID_VALUE,
                ingestAndLog("zero-volume", new TickEvent("e3", "S", 1L, T0, 1.0, 0.0)).rejectionReason());
    }

    @Test
    void duplicateDeliveryIsIdempotent() {
        assertEquals(IngestionStatus.ACCEPTED, ingestAndLog("first", valid("e1", 1L, T0)).status());
        IngestionResult dup = ingestAndLog("duplicate-same-eventId", valid("e1", 1L, T0));
        assertEquals(IngestionStatus.DUPLICATE, dup.status());
        // A duplicate must not advance the sequence expectation: seq=2 still accepted afterwards.
        assertEquals(IngestionStatus.ACCEPTED, ingestAndLog("next-after-dup", valid("e2", 2L, T0 + 1)).status());
    }

    @Test
    void rejectsSequenceRegressionBelowConfirmedFloor() {
        // Window of T0 is [T0-20000, T0+40000); with 30s lateness it confirms
        // once the watermark reaches T0+70000.
        assertEquals(IngestionStatus.ACCEPTED, ingestAndLog("seq-5-in-window", valid("e1", 5L, T0)).status());
        assertEquals(IngestionStatus.ACCEPTED,
                ingestAndLog("advance-watermark", valid("e2", 6L, T0 + 70_000L)).status());
        // Now the window holding seq=5 is confirmed, so sequenceFloor=5.
        IngestionResult r = ingestAndLog("seq-5-regression", valid("e3", 5L, T0 + 70_001L));
        assertEquals(IngestionStatus.REJECTED, r.status());
        assertEquals(RejectionReason.SEQUENCE_REGRESSION, r.rejectionReason());
        IngestionResult lower = ingestAndLog("seq-4-regression", valid("e4", 4L, T0 + 70_002L));
        assertEquals(RejectionReason.SEQUENCE_REGRESSION, lower.rejectionReason());
        // A sequence above the floor is still fine.
        assertEquals(IngestionStatus.ACCEPTED,
                ingestAndLog("seq-7-ok", valid("e5", 7L, T0 + 70_003L)).status());
    }

    @Test
    void outOfOrderSequenceInsideLatenessHorizonIsAccepted() {
        // No window is confirmed yet, so the sequence floor is unset: reordering
        // inside the allowed-lateness horizon must not be mistaken for regression.
        assertEquals(IngestionStatus.ACCEPTED, ingestAndLog("seq-10", valid("e1", 10L, T0)).status());
        assertEquals(IngestionStatus.ACCEPTED, ingestAndLog("seq-3-reordered", valid("e2", 3L, T0 + 1)).status());
    }
}
