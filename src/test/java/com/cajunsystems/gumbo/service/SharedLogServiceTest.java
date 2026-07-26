package com.cajunsystems.gumbo.service;

import com.cajunsystems.gumbo.api.LogView;
import com.cajunsystems.gumbo.api.SharedLog;
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
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

class SharedLogServiceTest {

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

    @Test
    void appendAssignsMonotonicSeqnums() {
        AppendResult r1 = service.append(AppendRequest.to(ORDERS, "e1".getBytes())).join();
        AppendResult r2 = service.append(AppendRequest.to(ORDERS, "e2".getBytes())).join();

        assertThat(r1.seqnum()).isLessThan(r2.seqnum());
        assertThat(r1.streamVersion()).isEqualTo(0L);
        assertThat(r2.streamVersion()).isEqualTo(1L);
    }

    @Test
    void appendAssignsPerTagStreamVersions() {
        AppendResult o1 = service.append(AppendRequest.to(ORDERS,    "o1".getBytes())).join();
        AppendResult i1 = service.append(AppendRequest.to(INVENTORY, "i1".getBytes())).join();
        AppendResult o2 = service.append(AppendRequest.to(ORDERS,    "o2".getBytes())).join();

        assertThat(o1.streamVersion()).isEqualTo(0L);
        assertThat(i1.streamVersion()).isEqualTo(0L); // fresh local counter for inventory
        assertThat(o2.streamVersion()).isEqualTo(1L);
    }

    @Test
    void readAllReturnsByTagOrder() {
        service.append(AppendRequest.to(ORDERS,    "o1".getBytes())).join();
        service.append(AppendRequest.to(INVENTORY, "i1".getBytes())).join();
        service.append(AppendRequest.to(ORDERS,    "o2".getBytes())).join();

        List<LogEntry> orders = service.readAll(ORDERS).join();
        assertThat(orders).hasSize(2);
        assertThat(orders).allMatch(e -> e.tags().contains(ORDERS));
    }

    @Test
    void readFromLowerBound() {
        service.append(AppendRequest.to(ORDERS, "e0".getBytes())).join();
        service.append(AppendRequest.to(ORDERS, "e1".getBytes())).join();
        AppendResult e2 = service.append(AppendRequest.to(ORDERS, "e2".getBytes())).join();

        List<LogEntry> result = service.read(ORDERS, new LogPosition(e2.seqnum()), 100).join();
        assertThat(result).hasSize(1);
        assertThat(result.get(0).seqnum()).isEqualTo(e2.seqnum());
    }

    @Test
    void logViewReadAll() {
        service.append(AppendRequest.to(ORDERS, "o1".getBytes())).join();
        service.append(AppendRequest.to(ORDERS, "o2".getBytes())).join();

        LogView view = service.getView(ORDERS);
        List<LogEntry> entries = view.readAll().join();
        assertThat(entries).hasSize(2);
    }

    @Test
    void logViewAppend() {
        LogView view = service.getView(ORDERS);
        view.append("via-view".getBytes()).join();

        assertThat(view.readAll().join()).hasSize(1);
    }

    @Test
    void subscribeReceivesNewEntries() throws InterruptedException {
        List<LogEntry> received = new ArrayList<>();
        CountDownLatch latch = new CountDownLatch(2);

        SharedLog.Subscription sub = service.subscribe(ORDERS, LogPosition.BEGINNING, e -> {
            received.add(e);
            latch.countDown();
        });

        service.append(AppendRequest.to(ORDERS, "e1".getBytes())).join();
        service.append(AppendRequest.to(ORDERS, "e2".getBytes())).join();

        assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(received).hasSize(2);
        sub.close();
    }

    @Test
    void subscribeFromMidPoint() throws InterruptedException {
        service.append(AppendRequest.to(ORDERS, "before".getBytes())).join();
        AppendResult second = service.append(AppendRequest.to(ORDERS, "start-here".getBytes())).join();

        List<LogEntry> received = new ArrayList<>();
        CountDownLatch latch = new CountDownLatch(2);

        SharedLog.Subscription sub = service.subscribe(
                ORDERS, new LogPosition(second.seqnum()), e -> {
                    received.add(e);
                    latch.countDown();
                });

        service.append(AppendRequest.to(ORDERS, "after1".getBytes())).join();

        assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
        // The subscription starts from second's seqnum → should include 'start-here' + 'after1'
        assertThat(received).hasSize(2);
        sub.close();
    }

    @Test
    void subscriptionCanBeClosed() throws InterruptedException {
        List<LogEntry> received = new ArrayList<>();

        SharedLog.Subscription sub = service.subscribe(ORDERS, LogPosition.BEGINNING, received::add);
        service.append(AppendRequest.to(ORDERS, "e1".getBytes())).join();

        await().atMost(3, TimeUnit.SECONDS).until(() -> !received.isEmpty());

        sub.close();
        assertThat(sub.isActive()).isFalse();

        int countBefore = received.size();
        service.append(AppendRequest.to(ORDERS, "e2".getBytes())).join();
        Thread.sleep(200); // give delivery time if subscription were still live
        assertThat(received.size()).isEqualTo(countBefore); // no new entries delivered
    }

    @Test
    void trimReducesVisibleEntries() {
        service.append(AppendRequest.to(ORDERS, "e0".getBytes())).join();
        service.append(AppendRequest.to(ORDERS, "e1".getBytes())).join();
        AppendResult e2 = service.append(AppendRequest.to(ORDERS, "e2".getBytes())).join();

        service.trim(e2.seqnum()).join();

        List<LogEntry> remaining = service.readAll(ORDERS).join();
        assertThat(remaining).hasSize(1);
        assertThat(remaining.get(0).seqnum()).isEqualTo(e2.seqnum());
    }

    @Test
    void logViewReadNextAfter() {
        service.append(AppendRequest.to(ORDERS, "e0".getBytes())).join();
        AppendResult e1 = service.append(AppendRequest.to(ORDERS, "e1".getBytes())).join();
        service.append(AppendRequest.to(ORDERS, "e2".getBytes())).join();

        DefaultLogView view = (DefaultLogView) service.getView(ORDERS);
        Optional<LogEntry> next = view.readNextAfter(e1.seqnum()).join();
        assertThat(next).isPresent();
        assertThat(next.get().seqnum()).isEqualTo(e1.seqnum());
    }

    @Test
    void logViewCheckTail() {
        service.append(AppendRequest.to(ORDERS, "e0".getBytes())).join();
        AppendResult last = service.append(AppendRequest.to(ORDERS, "e1".getBytes())).join();

        DefaultLogView view = (DefaultLogView) service.getView(ORDERS);
        Optional<LogEntry> tail = view.checkTail().join();
        assertThat(tail).isPresent();
        assertThat(tail.get().seqnum()).isEqualTo(last.seqnum());
    }

    @Test
    void latestSeqnumMonotonic() {
        assertThat(service.getLatestSeqnum()).isEqualTo(-1L);
        service.append(AppendRequest.to(ORDERS, "x".getBytes())).join();
        long s1 = service.getLatestSeqnum();
        service.append(AppendRequest.to(ORDERS, "y".getBytes())).join();
        long s2 = service.getLatestSeqnum();
        assertThat(s1).isGreaterThanOrEqualTo(0L);
        assertThat(s2).isGreaterThan(s1);
    }

    @Test
    void logViewGetLatestSeqnum_returnsCorrectTagSeqnum() {
        LogView ordersView    = service.getView(ORDERS);
        LogView inventoryView = service.getView(INVENTORY);

        assertThat(ordersView.getLatestSeqnum()).isEqualTo(-1L);

        service.append(AppendRequest.to(ORDERS,    "o1".getBytes())).join();
        service.append(AppendRequest.to(INVENTORY, "i1".getBytes())).join();
        service.append(AppendRequest.to(ORDERS,    "o2".getBytes())).join();

        long ordersLatest    = ordersView.getLatestSeqnum();
        long inventoryLatest = inventoryView.getLatestSeqnum();

        assertThat(ordersLatest).isGreaterThan(inventoryLatest);   // orders got a later seqnum
        assertThat(inventoryLatest).isGreaterThanOrEqualTo(0L);
        assertThat(ordersView.getLatestSeqnum()).isEqualTo(ordersLatest); // stable
    }

    @Test
    void kvApi_setAndGetValue() throws Exception {
        LogView view = service.getView(ORDERS);

        view.setValue("checkpoint", "100".getBytes()).join();
        byte[] result = view.getValue("checkpoint").join();

        assertThat(result).isEqualTo("100".getBytes());
        assertThat(view.getValue("nonexistent").join()).isNull();
    }

    @Test
    void kvApi_deleteValue() throws Exception {
        LogView view = service.getView(ORDERS);

        view.setValue("ck", "val".getBytes()).join();
        view.deleteValue("ck").join();

        assertThat(view.getValue("ck").join()).isNull();
    }

    @Test
    void kvApi_isolatedPerTag() throws Exception {
        LogView ordersView    = service.getView(ORDERS);
        LogView inventoryView = service.getView(INVENTORY);

        ordersView.setValue("ck", "orders-val".getBytes()).join();
        inventoryView.setValue("ck", "inventory-val".getBytes()).join();

        assertThat(ordersView.getValue("ck").join()).isEqualTo("orders-val".getBytes());
        assertThat(inventoryView.getValue("ck").join()).isEqualTo("inventory-val".getBytes());
    }

    @Test
    void subscribeTail_receivesOnlyFutureEntries() throws Exception {
        LogView view = service.getView(ORDERS);

        // Pre-populate — subscribeTail should NOT deliver these
        service.append(AppendRequest.to(ORDERS, "past-1".getBytes())).join();
        service.append(AppendRequest.to(ORDERS, "past-2".getBytes())).join();

        List<String> received = new CopyOnWriteArrayList<>();
        SharedLog.Subscription sub = view.subscribeTail(
                e -> received.add(new String(e.dataUnsafe()))
        );

        try {
            // Append future entries after subscribeTail()
            service.append(AppendRequest.to(ORDERS, "future-1".getBytes())).join();
            service.append(AppendRequest.to(ORDERS, "future-2".getBytes())).join();

            await().atMost(3, TimeUnit.SECONDS)
                   .until(() -> received.size() == 2);

            assertThat(received).containsExactly("future-1", "future-2");
            assertThat(received).doesNotContain("past-1", "past-2");
        } finally {
            sub.close();
        }
    }
}
