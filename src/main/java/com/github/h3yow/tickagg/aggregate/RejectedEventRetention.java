package com.github.h3yow.tickagg.aggregate;

import com.github.h3yow.tickagg.model.RetainedRejection;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Bounded FIFO store of retained late-rejected events for one symbol.
 *
 * <p>Eviction here affects only this store: dropping an entry never mutates
 * confirmed windows, dedup state or any aggregate, and a query can therefore
 * never observe a contradiction with a confirmed result. The same eventId is
 * retained at most once (a redelivery of an already-rejected event does not
 * create a duplicate record).
 *
 * <p>Not thread-safe; callers (the owning per-symbol state machine) synchronize.
 */
final class RejectedEventRetention {

    private final int maxEntries;
    private final Deque<String> arrivalOrder = new ArrayDeque<>();
    private final Map<String, RetainedRejection> byEventId = new HashMap<>();
    private long evictedCount;

    RejectedEventRetention(int maxEntries) {
        this.maxEntries = Math.max(0, maxEntries);
    }

    /**
     * Retain a rejection. If the same eventId is already present the call is a
     * no-op (a redelivered rejection must not duplicate the record). Oldest
     * entries are evicted until the capacity holds; a zero capacity evicts
     * immediately.
     */
    void retain(RetainedRejection rejection) {
        if (byEventId.containsKey(rejection.eventId())) {
            return;
        }
        arrivalOrder.addLast(rejection.eventId());
        byEventId.put(rejection.eventId(), rejection);
        while (byEventId.size() > maxEntries) {
            String eldest = arrivalOrder.pollFirst();
            if (eldest == null) {
                break;
            }
            byEventId.remove(eldest);
            evictedCount++;
        }
    }

    /**
     * Immutable snapshot of entries whose event time lies in [from, to),
     * in arrival (FIFO) order.
     */
    List<RetainedRejection> query(long fromEpochMs, long toEpochMs) {
        List<RetainedRejection> out = new ArrayList<>();
        for (String id : arrivalOrder) {
            RetainedRejection r = byEventId.get(id);
            if (r == null) {
                continue;
            }
            if (r.eventTimestampEpochMs() >= fromEpochMs && r.eventTimestampEpochMs() < toEpochMs) {
                out.add(r);
            }
        }
        return List.copyOf(out);
    }

    /** All retained entries in arrival (FIFO) order; recomputation candidates. */
    List<RetainedRejection> all() {
        List<RetainedRejection> out = new ArrayList<>(arrivalOrder.size());
        for (String id : arrivalOrder) {
            RetainedRejection r = byEventId.get(id);
            if (r != null) {
                out.add(r);
            }
        }
        return List.copyOf(out);
    }

    boolean containsEventId(String eventId) {
        return byEventId.containsKey(eventId);
    }

    /** Flip the incorporation flag. A no-op if the entry was already evicted. */
    void markIncorporated(String eventId) {
        RetainedRejection r = byEventId.get(eventId);
        if (r != null && !r.incorporated()) {
            byEventId.put(eventId, new RetainedRejection(
                    r.eventId(), r.symbol(), r.sequence(), r.eventTimestampEpochMs(),
                    r.price(), r.volume(), r.reason(), r.decisionDetail(),
                    r.watermarkAtDecisionMs(), r.latenessFloorAtDecisionMs(),
                    r.allowedLatenessAtDecisionMs(), r.rejectedAtWallClockMs(), true));
        }
    }

    int size() {
        return byEventId.size();
    }

    long evictedCount() {
        return evictedCount;
    }
}
