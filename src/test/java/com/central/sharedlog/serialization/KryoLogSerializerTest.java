package com.central.sharedlog.serialization;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;

class KryoLogSerializerTest {

    // -------------------------------------------------------------------------
    // Test domain types
    // -------------------------------------------------------------------------

    /** Java record — Kryo 5 auto-detects and uses RecordSerializer. */
    record Point(int x, int y) {}

    /** Plain POJO with no-arg constructor (required by Kryo default). */
    static class Order {
        String id;
        String item;
        int    qty;

        Order() {} // Kryo needs this

        Order(String id, String item, int qty) {
            this.id   = id;
            this.item = item;
            this.qty  = qty;
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof Order other)) return false;
            return qty == other.qty && Objects.equals(id, other.id) && Objects.equals(item, other.item);
        }

        @Override public int hashCode() { return Objects.hash(id, item, qty); }
    }

    // -------------------------------------------------------------------------
    // Roundtrip tests
    // -------------------------------------------------------------------------

    @Test
    void roundtripRecord() {
        KryoLogSerializer<Point> s = new KryoLogSerializer<>(Point.class);
        Point original = new Point(3, 7);

        byte[] bytes = s.serialize(original);
        Point recovered = s.deserialize(bytes);

        assertThat(recovered).isEqualTo(original);
    }

    @Test
    void roundtripPojo() {
        KryoLogSerializer<Order> s = new KryoLogSerializer<>(Order.class);
        Order original = new Order("ord-1", "widget", 5);

        byte[] bytes = s.serialize(original);
        Order recovered = s.deserialize(bytes);

        assertThat(recovered).isEqualTo(original);
    }

    @Test
    void serializedBytesAreCompact() {
        // Kryo binary should be smaller than naive UTF-8 JSON
        KryoLogSerializer<Order> s = new KryoLogSerializer<>(Order.class);
        byte[] bytes = s.serialize(new Order("ord-42", "gizmo", 100));

        String roughJson = "{\"id\":\"ord-42\",\"item\":\"gizmo\",\"qty\":100}";
        assertThat(bytes.length).isLessThan(roughJson.length());
    }

    @Test
    void registeredClassesProduceSmallerOutput() {
        Order order = new Order("ord-1", "widget", 5);

        KryoLogSerializer<Order> unregistered = new KryoLogSerializer<>(Order.class);
        KryoLogSerializer<Order> registered   = new KryoLogSerializer<>(Order.class,
                kryo -> kryo.register(Order.class, 10));

        byte[] unregBytes = unregistered.serialize(order);
        byte[] regBytes   = registered.serialize(order);

        // Registered mode omits the class name → smaller or equal payload
        assertThat(regBytes.length).isLessThanOrEqualTo(unregBytes.length);
        // Both must deserialize correctly
        assertThat(registered.deserialize(regBytes)).isEqualTo(order);
    }

    // -------------------------------------------------------------------------
    // Thread-safety
    // -------------------------------------------------------------------------

    @Test
    void concurrentSerializationIsThreadSafe() throws Exception {
        KryoLogSerializer<Point> s = new KryoLogSerializer<>(Point.class);
        int threads = 20;
        int ops     = 100;

        try (ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor()) {
            List<CompletableFuture<Void>> futures = new ArrayList<>(threads);
            for (int t = 0; t < threads; t++) {
                final int base = t * ops;
                futures.add(CompletableFuture.runAsync(() -> {
                    for (int i = base; i < base + ops; i++) {
                        Point p     = new Point(i, i * 2);
                        byte[] b    = s.serialize(p);
                        Point back  = s.deserialize(b);
                        if (!back.equals(p)) {
                            throw new AssertionError("Roundtrip failed for " + p);
                        }
                    }
                }, pool));
            }
            CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new)).join();
        }
    }
}
