package com.github.h3yow.tickagg.ingest;

import com.github.h3yow.tickagg.aggregate.WindowAggregator;
import com.github.h3yow.tickagg.model.IngestionResult;
import com.github.h3yow.tickagg.model.RejectionReason;
import com.github.h3yow.tickagg.model.TickEvent;
import org.springframework.stereotype.Service;

/**
 * Validates raw events before they reach the aggregator. Validation is pure
 * (no shared state), so it is safe under any concurrency; all stateful rules
 * (dedup, sequence, lateness) live in the per-symbol aggregate state.
 */
@Service
public class TickIngestionService {

    /** 2100-01-01T00:00:00Z: upper sanity bound for epoch-millis timestamps. */
    static final long MAX_PLAUSIBLE_EPOCH_MS = 4_102_444_800_000L;

    private final WindowAggregator aggregator;

    public TickIngestionService(WindowAggregator aggregator) {
        this.aggregator = aggregator;
    }

    public IngestionResult ingest(TickEvent event) {
        IngestionResult invalid = validate(event);
        if (invalid != null) {
            return invalid;
        }
        return aggregator.ingest(event);
    }

    private IngestionResult validate(TickEvent event) {
        if (event == null) {
            return IngestionResult.rejected(RejectionReason.MISSING_FIELD, "event body is null");
        }
        if (event.eventId() == null || event.eventId().isBlank()) {
            return IngestionResult.rejected(RejectionReason.MISSING_FIELD, "eventId is missing");
        }
        if (event.symbol() == null || event.symbol().isBlank()) {
            return IngestionResult.rejected(RejectionReason.MISSING_FIELD, "symbol is missing");
        }
        if (event.sequence() == null) {
            return IngestionResult.rejected(RejectionReason.MISSING_FIELD, "sequence is missing");
        }
        if (event.timestampEpochMs() == null) {
            return IngestionResult.rejected(RejectionReason.MISSING_FIELD, "timestampEpochMs is missing");
        }
        if (event.price() == null) {
            return IngestionResult.rejected(RejectionReason.MISSING_FIELD, "price is missing");
        }
        if (event.volume() == null) {
            return IngestionResult.rejected(RejectionReason.MISSING_FIELD, "volume is missing");
        }
        if (event.timestampEpochMs() <= 0 || event.timestampEpochMs() > MAX_PLAUSIBLE_EPOCH_MS) {
            return IngestionResult.rejected(RejectionReason.INVALID_TIMESTAMP,
                    "timestampEpochMs=" + event.timestampEpochMs()
                            + " outside plausible range (0, " + MAX_PLAUSIBLE_EPOCH_MS + "]");
        }
        if (!Double.isFinite(event.price()) || event.price() <= 0) {
            return IngestionResult.rejected(RejectionReason.INVALID_VALUE,
                    "price=" + event.price() + " must be a positive finite number");
        }
        if (!Double.isFinite(event.volume()) || event.volume() <= 0) {
            return IngestionResult.rejected(RejectionReason.INVALID_VALUE,
                    "volume=" + event.volume() + " must be a positive finite number");
        }
        return null;
    }
}
