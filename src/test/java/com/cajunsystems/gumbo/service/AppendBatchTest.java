package com.cajunsystems.gumbo.service;

import com.cajunsystems.gumbo.core.AppendRequest;
import com.cajunsystems.gumbo.core.AppendResult;
import com.cajunsystems.gumbo.core.LogEntry;
import com.cajunsystems.gumbo.core.LogPosition;
import com.cajunsystems.gumbo.core.LogTag;
import com.cajunsystems.gumbo.persistence.InMemoryPersistenceAdapter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link SharedLogService#appendBatch(List)}.
 *
 * <p>Uses InMemoryPersistenceAdapter + LocalSequencer so there is no FDB
 * dependency. The important invariants are:
 * <ul>
 *   <li>All seqnums claimed in a single {@code nextBatch} call are strictly
 *       increasing and contiguous.</li>
 *   <li>Results are returned in the same order as the input requests.</li>
 *   <li>Every entry is readable via {@code readAll} after the batch commits.</li>
 *   <li>Subscriber notifications fire for every entry in the batch.</li>
 * </ul>
 */
class AppendBatchTest {

    private SharedLogService service;
    private static final LogTag ORDERS    = LogTag.of("orders");
    private static final LogTag INVENTORY = LogTag.of("inventory");

    @BeforeEach
    void setUp() throws IOException {
        SharedLogConfig config = SharedLogConfig.builder()
                .persistenceAdapter(new InMemoryPersistenceAdapter())
                .build();
        service = SharedLogService.open(config);
    }

    @AfterEach
    void tearDown() {
        service.close();
    }

    // -------------------------------------------------------------------------
    // Basic correctness
    // -------------------------------------------------------------------------

    @Test
    void appendBatch_returnsResultsInInputOrder() {
        List<AppendRequest> requests = List.of(
                AppendRequest.to(ORDERS, "e0".getBytes()),
                AppendRequest.to(ORDERS, "e1".getBytes()),
                AppendRequest.to(ORDERS, "e2".getBytes())
        );

        List<AppendResult> results = service.appendBatch(requests).join();

        assertThat(results).hasSize(3);
        assertThat(new String(service.readAll(ORDERS).join().get(0).data())).isEqualTo("e0");
        assertThat(new String(service.readAll(ORDERS).join().get(1).data())).isEqualTo("e1");
        assertThat(new String(service.readAll(ORDERS).join().get(2).data())).isEqualTo("e2");
    }

    @Test
    void appendBatch_seqnumsAreStrictlyMonotonic() {
        int n = 64;
        List<AppendRequest> requests = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            requests.add(AppendRequest.to(ORDERS, ("entry-" + i).getBytes()));
        }

        List<AppendResult> results = service.appendBatch(requests).join();

        for (int i = 0; i < results.size() - 1; i++) {
            assertThat(results.get(i).seqnum())
                    .as("seqnum at index %d must be less than at index %d", i, i + 1)
                    .isLessThan(results.get(i + 1).seqnum());
        }
    }

    @Test
    void appendBatch_seqnumsAreContiguous() {
        List<AppendRequest> requests = List.of(
                AppendRequest.to(ORDERS, "a".getBytes()),
                AppendRequest.to(ORDERS, "b".getBytes()),
                AppendRequest.to(ORDERS, "c".getBytes())
        );

        List<AppendResult> results = service.appendBatch(requests).join();

        long base = results.get(0).seqnum();
        for (int i = 0; i < results.size(); i++) {
            assertThat(results.get(i).seqnum()).isEqualTo(base + i);
        }
    }

    @Test
    void appendBatch_localIdsArePerTagMonotonic() {
        List<AppendRequest> requests = List.of(
                AppendRequest.to(ORDERS,    "o0".getBytes()),
                AppendRequest.to(INVENTORY, "i0".getBytes()),
                AppendRequest.to(ORDERS,    "o1".getBytes())
        );

        List<AppendResult> results = service.appendBatch(requests).join();

        // ORDERS entries should have localId 0, 1
        AppendResult o0 = results.get(0);
        AppendResult o1 = results.get(2);
        assertThat(o0.localId()).isEqualTo(0L);
        assertThat(o1.localId()).isEqualTo(1L);

        // INVENTORY entry should have localId 0
        assertThat(results.get(1).localId()).isEqualTo(0L);
    }

    @Test
    void appendBatch_allEntriesReadableAfterCommit() {
        int n = 10;
        List<AppendRequest> requests = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            requests.add(AppendRequest.to(ORDERS, ("entry-" + i).getBytes()));
        }

        service.appendBatch(requests).join();

        List<LogEntry> all = service.readAll(ORDERS).join();
        assertThat(all).hasSize(n);
    }

    @Test
    void appendBatch_emptyListReturnsEmpty() {
        List<AppendResult> results = service.appendBatch(List.of()).join();
        assertThat(results).isEmpty();
    }

    @Test
    void appendBatch_singleEntryBehavesLikeAppend() {
        AppendRequest req = AppendRequest.to(ORDERS, "solo".getBytes());

        AppendResult single = service.append(req).join();
        // Reset service to start fresh
        service.close();
        try {
            SharedLogConfig config = SharedLogConfig.builder()
                    .persistenceAdapter(new InMemoryPersistenceAdapter())
                    .build();
            service = SharedLogService.open(config);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        List<AppendResult> batch = service.appendBatch(List.of(
                AppendRequest.to(ORDERS, "solo".getBytes())
        )).join();

        assertThat(batch).hasSize(1);
        assertThat(batch.get(0).seqnum()).isEqualTo(single.seqnum()); // both start from 0
        assertThat(batch.get(0).localId()).isEqualTo(single.localId());
    }

    // -------------------------------------------------------------------------
    // Interaction with single append: seqnums remain globally monotonic
    // -------------------------------------------------------------------------

    @Test
    void appendBatch_seqnumsContinueAfterSingleAppend() {
        AppendResult before = service.append(AppendRequest.to(ORDERS, "before".getBytes())).join();

        List<AppendResult> batch = service.appendBatch(List.of(
                AppendRequest.to(ORDERS, "b0".getBytes()),
                AppendRequest.to(ORDERS, "b1".getBytes())
        )).join();

        assertThat(batch.get(0).seqnum()).isGreaterThan(before.seqnum());
        assertThat(batch.get(1).seqnum()).isGreaterThan(batch.get(0).seqnum());
    }

    @Test
    void appendBatch_singleAppendContinuesAfterBatch() {
        List<AppendResult> batch = service.appendBatch(List.of(
                AppendRequest.to(ORDERS, "b0".getBytes()),
                AppendRequest.to(ORDERS, "b1".getBytes())
        )).join();

        AppendResult after = service.append(AppendRequest.to(ORDERS, "after".getBytes())).join();

        assertThat(after.seqnum()).isGreaterThan(batch.get(batch.size() - 1).seqnum());
    }

    // -------------------------------------------------------------------------
    // Subscriber notifications
    // -------------------------------------------------------------------------

    @Test
    void appendBatch_notifiesSubscribersForEveryEntry() throws InterruptedException {
        int n = 5;
        CountDownLatch latch = new CountDownLatch(n);
        List<LogEntry> received = new ArrayList<>();

        service.subscribe(ORDERS, LogPosition.BEGINNING, entry -> {
            synchronized (received) { received.add(entry); }
            latch.countDown();
        });

        List<AppendRequest> requests = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            requests.add(AppendRequest.to(ORDERS, ("msg-" + i).getBytes()));
        }
        service.appendBatch(requests).join();

        boolean done = latch.await(5, TimeUnit.SECONDS);
        assertThat(done).as("All subscribers should have been notified within 5s").isTrue();
        assertThat(received).hasSize(n);
    }

    // -------------------------------------------------------------------------
    // Multi-tag entries in a batch
    // -------------------------------------------------------------------------

    @Test
    void appendBatch_multiTagEntriesVisibleFromBothViews() {
        LogTag orderTag    = LogTag.of("orders");
        LogTag instanceTag = LogTag.of("orders", "order-1");

        service.appendBatch(List.of(
                AppendRequest.to(Set.of(orderTag, instanceTag), "placed".getBytes()),
                AppendRequest.to(orderTag, "other".getBytes())
        )).join();

        List<LogEntry> allOrders   = service.readAll(orderTag).join();
        List<LogEntry> singleOrder = service.readAll(instanceTag).join();

        assertThat(allOrders).hasSize(2);
        assertThat(singleOrder).hasSize(1);
        assertThat(new String(singleOrder.get(0).data())).isEqualTo("placed");
    }
}
