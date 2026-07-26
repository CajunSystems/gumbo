package com.cajunsystems.gumbo.examples;

import com.cajunsystems.gumbo.api.SharedLog;
import com.cajunsystems.gumbo.core.AppendRequest;
import com.cajunsystems.gumbo.core.AppendResult;
import com.cajunsystems.gumbo.core.LogEntry;
import com.cajunsystems.gumbo.core.LogPosition;
import com.cajunsystems.gumbo.core.LogTag;
import com.cajunsystems.gumbo.persistence.InMemoryPersistenceAdapter;
import com.cajunsystems.gumbo.service.SharedLogConfig;
import com.cajunsystems.gumbo.service.SharedLogService;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Runnable version of the Quick Start section in the README.
 *
 * <p>Covers the four core operations:
 * <ol>
 *   <li>Append entries to a tag.</li>
 *   <li>Read entries back (point-in-time and streaming).</li>
 *   <li>Subscribe to live entries on a virtual thread.</li>
 *   <li>Multi-tag entries visible from multiple views simultaneously.</li>
 * </ol>
 *
 * <p>Run with: {@code mvn test -Dtest=QuickStartExample}
 */
class QuickStartExample {

    private static SharedLogService openInMemory() throws Exception {
        return SharedLogService.open(SharedLogConfig.builder()
                .persistenceAdapter(new InMemoryPersistenceAdapter())
                .build());
    }

    // -------------------------------------------------------------------------

    @Test
    void appendAndRead() throws Exception {
        try (SharedLogService log = openInMemory()) {
            LogTag orders = LogTag.of("orders");

            AppendResult r1 = log.append(AppendRequest.to(orders, "order-placed".getBytes())).join();
            AppendResult r2 = log.append(AppendRequest.to(orders, "order-confirmed".getBytes())).join();

            System.out.printf("r1: seqnum=%d streamVersion=%d%n", r1.seqnum(), r1.streamVersion());
            System.out.printf("r2: seqnum=%d streamVersion=%d%n", r2.seqnum(), r2.streamVersion());

            // versions are per-tag counters (0, 1, 2, …)
            assertThat(r1.streamVersion()).isEqualTo(0);
            assertThat(r2.streamVersion()).isEqualTo(1);

            List<LogEntry> entries = log.readAll(orders).join();
            assertThat(entries).hasSize(2);
            assertThat(new String(entries.get(0).data())).isEqualTo("order-placed");
            assertThat(new String(entries.get(1).data())).isEqualTo("order-confirmed");
        }
    }

    // -------------------------------------------------------------------------

    @Test
    void subscribeReceivesNewEntries() throws Exception {
        try (SharedLogService log = openInMemory()) {
            LogTag orders = LogTag.of("orders");

            CountDownLatch received = new CountDownLatch(1);

            // subscribeTail: only receives entries appended after this call
            SharedLog.Subscription sub = log.subscribeTail(orders,
                    entry -> received.countDown());

            log.append(AppendRequest.to(orders, "order-shipped".getBytes())).join();

            assertThat(received.await(5, TimeUnit.SECONDS))
                    .as("subscriber should receive the new entry within 5 s")
                    .isTrue();
            sub.close();
        }
    }

    // -------------------------------------------------------------------------

    @Test
    void readFromPosition() throws Exception {
        try (SharedLogService log = openInMemory()) {
            LogTag tag = LogTag.of("events");

            log.append(AppendRequest.to(tag, "A".getBytes())).join();
            log.append(AppendRequest.to(tag, "B".getBytes())).join();
            log.append(AppendRequest.to(tag, "C".getBytes())).join();

            List<LogEntry> all = log.readAll(tag).join();
            assertThat(all).hasSize(3);

            // Read starting at the second entry's position
            long midSeqnum = all.get(1).seqnum();
            List<LogEntry> fromB = log.read(tag, new LogPosition(midSeqnum), 10).join();
            assertThat(fromB).hasSize(2);
            assertThat(new String(fromB.get(0).data())).isEqualTo("B");
        }
    }

    // -------------------------------------------------------------------------

    @Test
    void multiTagEntryVisibleFromBothViews() throws Exception {
        try (SharedLogService log = openInMemory()) {
            LogTag allOrders   = LogTag.of("orders");
            LogTag singleOrder = LogTag.of("orders", "order-42");

            // This entry is indexed under both tags simultaneously
            log.append(AppendRequest.to(
                    Set.of(allOrders, singleOrder),
                    "order-42-fulfilled".getBytes())).join();

            // An unrelated order lands only on the namespace-wide tag
            log.append(AppendRequest.to(allOrders, "order-99-placed".getBytes())).join();

            assertThat(log.readAll(allOrders).join()).hasSize(2);
            assertThat(log.readAll(singleOrder).join()).hasSize(1); // only the first entry
        }
    }

    // -------------------------------------------------------------------------

    @Test
    void logViewScopesReadsAndAppends() throws Exception {
        try (SharedLogService log = openInMemory()) {
            var view = log.getView(LogTag.of("events"));

            // Appending through the view automatically tags the entry
            view.append("event-A".getBytes()).join();
            view.append("event-B".getBytes()).join();
            view.append("event-C".getBytes()).join();

            List<LogEntry> all = view.readAll().join();
            assertThat(all).hasSize(3);
            assertThat(all.stream().map(e -> new String(e.data())).toList())
                    .containsExactly("event-A", "event-B", "event-C");

            // Read from a specific cursor position (inclusive)
            long midSeqnum = all.get(1).seqnum();
            List<LogEntry> fromMid = view.readFrom(new LogPosition(midSeqnum), 10).join();
            assertThat(fromMid).hasSize(2);
            assertThat(new String(fromMid.get(0).data())).isEqualTo("event-B");
        }
    }
}
