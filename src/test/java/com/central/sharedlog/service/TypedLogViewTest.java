package com.central.sharedlog.service;

import com.central.sharedlog.api.SharedLog;
import com.central.sharedlog.api.TypedLogView;
import com.central.sharedlog.core.LogPosition;
import com.central.sharedlog.core.LogTag;
import com.central.sharedlog.persistence.InMemoryPersistenceAdapter;
import com.central.sharedlog.serialization.KryoLogSerializer;
import com.central.sharedlog.serialization.LogSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

class TypedLogViewTest {

    // -------------------------------------------------------------------------
    // Domain type
    // -------------------------------------------------------------------------

    static class OrderEvent {
        String orderId;
        String status;

        OrderEvent() {} // no-arg for Kryo

        OrderEvent(String orderId, String status) {
            this.orderId = orderId;
            this.status  = status;
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof OrderEvent e)) return false;
            return Objects.equals(orderId, e.orderId) && Objects.equals(status, e.status);
        }

        @Override public int hashCode() { return Objects.hash(orderId, status); }

        @Override public String toString() { return orderId + "/" + status; }
    }

    // -------------------------------------------------------------------------
    // Fixtures
    // -------------------------------------------------------------------------

    private SharedLogService service;
    private TypedLogView<OrderEvent> view;

    private static final LogTag ORDERS = LogTag.of("orders");

    @BeforeEach
    void setUp() throws Exception {
        service = SharedLogService.open(SharedLogConfig.builder()
                .persistenceAdapter(new InMemoryPersistenceAdapter())
                .build());
        LogSerializer<OrderEvent> s = new KryoLogSerializer<>(OrderEvent.class);
        view = service.getTypedView(ORDERS, s);
    }

    @AfterEach
    void tearDown() {
        service.close();
    }

    // -------------------------------------------------------------------------
    // Tests
    // -------------------------------------------------------------------------

    @Test
    void appendAndReadAllRoundtrip() throws Exception {
        view.append(new OrderEvent("ord-1", "placed")).join();
        view.append(new OrderEvent("ord-2", "confirmed")).join();
        view.append(new OrderEvent("ord-3", "shipped")).join();

        List<OrderEvent> events = view.readAll().join();

        assertThat(events).containsExactly(
                new OrderEvent("ord-1", "placed"),
                new OrderEvent("ord-2", "confirmed"),
                new OrderEvent("ord-3", "shipped"));
    }

    @Test
    void readFromPosition() throws Exception {
        view.append(new OrderEvent("ord-1", "placed")).join();
        view.append(new OrderEvent("ord-2", "placed")).join();
        view.append(new OrderEvent("ord-3", "placed")).join();

        // Find the seqnum of the second event
        var allRaw = view.rawView().readAll().join();
        long midSeqnum = allRaw.get(1).seqnum();

        List<OrderEvent> fromMid = view.readFrom(new LogPosition(midSeqnum), 10).join();
        assertThat(fromMid).hasSize(2);
        assertThat(fromMid.get(0)).isEqualTo(new OrderEvent("ord-2", "placed"));
    }

    @Test
    void readAfterSeqnum() throws Exception {
        view.append(new OrderEvent("ord-1", "placed")).join();
        view.append(new OrderEvent("ord-2", "placed")).join();
        view.append(new OrderEvent("ord-3", "placed")).join();

        long firstSeqnum = view.rawView().readAll().join().get(0).seqnum();

        List<OrderEvent> afterFirst = view.readAfter(firstSeqnum).join();
        assertThat(afterFirst).hasSize(2);
        assertThat(afterFirst.get(0)).isEqualTo(new OrderEvent("ord-2", "placed"));
    }

    @Test
    void subscribeTailReceivesTypedObjects() throws Exception {
        CountDownLatch latch = new CountDownLatch(2);
        List<OrderEvent> received = new CopyOnWriteArrayList<>();

        SharedLog.Subscription sub = view.subscribeTail(event -> {
            received.add(event);
            latch.countDown();
        });

        view.append(new OrderEvent("ord-1", "placed")).join();
        view.append(new OrderEvent("ord-2", "confirmed")).join();

        assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(received).contains(
                new OrderEvent("ord-1", "placed"),
                new OrderEvent("ord-2", "confirmed"));

        sub.close();
    }

    @Test
    void subscribeFromBeginningDelivershBacklog() throws Exception {
        view.append(new OrderEvent("ord-1", "placed")).join();
        view.append(new OrderEvent("ord-2", "placed")).join();

        CountDownLatch latch = new CountDownLatch(2);
        List<OrderEvent> received = new CopyOnWriteArrayList<>();

        SharedLog.Subscription sub = view.subscribe(LogPosition.BEGINNING, event -> {
            received.add(event);
            latch.countDown();
        });

        assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(received).containsExactlyInAnyOrder(
                new OrderEvent("ord-1", "placed"),
                new OrderEvent("ord-2", "placed"));

        sub.close();
    }

    @Test
    void rawViewBridgeAllowsRawAccess() throws Exception {
        view.append(new OrderEvent("ord-1", "placed")).join();

        // rawView() gives back the untyped byte-level view
        var rawEntries = view.rawView().readAll().join();
        assertThat(rawEntries).hasSize(1);

        // Manually deserializing should match the original
        var s = new KryoLogSerializer<>(OrderEvent.class);
        OrderEvent recovered = s.deserialize(rawEntries.get(0).data());
        assertThat(recovered).isEqualTo(new OrderEvent("ord-1", "placed"));
    }

    @Test
    void multipleTypesSameService() throws Exception {
        // Two typed views over different tags, same service
        record Metric(String name, double value) {}

        LogSerializer<Metric> ms = new KryoLogSerializer<>(Metric.class);
        TypedLogView<Metric> metrics = service.getTypedView(LogTag.of("metrics"), ms);

        view.append(new OrderEvent("ord-1", "placed")).join();
        metrics.append(new Metric("latency_ms", 42.5)).join();

        assertThat(view.readAll().join()).containsExactly(new OrderEvent("ord-1", "placed"));
        assertThat(metrics.readAll().join()).containsExactly(new Metric("latency_ms", 42.5));
    }
}
