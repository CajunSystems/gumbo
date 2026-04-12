package com.central.sharedlog.examples;

import com.central.sharedlog.api.Executor;
import com.central.sharedlog.api.ExecutorContext;
import com.central.sharedlog.core.AppendRequest;
import com.central.sharedlog.core.LogEntry;
import com.central.sharedlog.core.LogTag;
import com.central.sharedlog.persistence.InMemoryPersistenceAdapter;
import com.central.sharedlog.service.ExecutorEngine;
import com.central.sharedlog.service.SharedLogConfig;
import com.central.sharedlog.service.SharedLogService;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end example of the stateless executor pattern, inspired by Boki's
 * serverless function model.
 *
 * <h2>How it works</h2>
 * <ol>
 *   <li>Three "place-order" commands are appended to the {@code commands} tag
 *       <em>before</em> the executor starts.</li>
 *   <li>On startup, {@link ExecutorEngine} replays the full backlog: the executor's
 *       {@code apply()} is called once per entry to rebuild state from scratch.</li>
 *   <li>The executor's {@code execute()} emits a "fulfilled:N" summary to the
 *       {@code fulfilled} tag.</li>
 *   <li>A fourth command is appended while the executor is running. The engine
 *       delivers it incrementally — no full rebuild required.</li>
 * </ol>
 *
 * <p>Because all state is derived from the log, stopping and restarting the engine
 * would reproduce identical results without any additional checkpointing.
 *
 * <p>Run with: {@code mvn test -Dtest=OrderFulfilmentExample}
 */
class OrderFulfilmentExample {

    private static final LogTag COMMANDS  = LogTag.of("commands");
    private static final LogTag FULFILLED = LogTag.of("fulfilled");

    // -------------------------------------------------------------------------
    // Executor definition
    // -------------------------------------------------------------------------

    /**
     * Reads commands from the {@code commands} tag and, whenever the order count
     * changes, emits a {@code "fulfilled:N"} summary entry to the {@code fulfilled} tag.
     *
     * <p>State type is {@link Integer}: the total number of orders seen so far.
     */
    static class OrderFulfilmentExecutor implements Executor<Integer> {

        /** Tracks how many orders were reported in the last execute() cycle. */
        private final AtomicInteger lastEmitted = new AtomicInteger(0);

        @Override public LogTag getInputTag() { return COMMANDS; }
        @Override public String getName()     { return "OrderFulfilment"; }

        /** Initial state: zero orders seen. */
        @Override
        public Integer initialState() { return 0; }

        /** Each entry on the commands tag represents one placed order. */
        @Override
        public Integer apply(Integer orderCount, LogEntry entry) {
            return orderCount + 1;
        }

        /**
         * Emits a fulfilment summary whenever there are new orders to report.
         * The {@code lastEmitted} guard prevents re-emitting on subsequent cycles
         * where no new commands have arrived.
         */
        @Override
        public List<AppendRequest> execute(Integer totalOrders, ExecutorContext ctx) {
            int prev = lastEmitted.get();
            if (totalOrders <= prev) return List.of(); // nothing new
            lastEmitted.set(totalOrders);
            String payload = "fulfilled:" + totalOrders;
            return List.of(AppendRequest.to(FULFILLED, payload.getBytes()));
        }
    }

    // -------------------------------------------------------------------------
    // Test
    // -------------------------------------------------------------------------

    @Test
    void replaysThenProcessesIncrementally() throws Exception {
        SharedLogService service = SharedLogService.open(SharedLogConfig.builder()
                .persistenceAdapter(new InMemoryPersistenceAdapter())
                .build());
        ExecutorEngine engine = new ExecutorEngine(service);

        // Pre-populate the log so the executor must replay a backlog on startup
        service.append(AppendRequest.to(COMMANDS, "place-order-1".getBytes())).join();
        service.append(AppendRequest.to(COMMANDS, "place-order-2".getBytes())).join();
        service.append(AppendRequest.to(COMMANDS, "place-order-3".getBytes())).join();

        engine.register(new OrderFulfilmentExecutor());
        engine.start();

        // After replaying three pre-existing entries the executor should emit "fulfilled:3"
        awaitPayload(service, FULFILLED, "fulfilled:3");

        // Append a fourth command and expect an incremental update: "fulfilled:4"
        service.append(AppendRequest.to(COMMANDS, "place-order-4".getBytes())).join();
        awaitPayload(service, FULFILLED, "fulfilled:4");

        List<LogEntry> results = service.readAll(FULFILLED).join();
        System.out.println("Fulfilled log (" + results.size() + " entries):");
        for (LogEntry e : results) {
            System.out.printf("  [seqnum=%d] %s%n", e.seqnum(), new String(e.data()));
        }

        assertThat(results.stream().map(e -> new String(e.data())).toList())
                .contains("fulfilled:3", "fulfilled:4");

        engine.stop(3_000);
        service.close();
    }

    // -------------------------------------------------------------------------

    private static void awaitPayload(SharedLogService service, LogTag tag, String expected)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + 5_000;
        while (System.currentTimeMillis() < deadline) {
            boolean found = service.readAll(tag).join().stream()
                    .anyMatch(e -> new String(e.data()).equals(expected));
            if (found) return;
            Thread.sleep(50);
        }
        // Trigger an informative assertion failure
        assertThat(service.readAll(tag).join().stream()
                .map(e -> new String(e.data())).toList())
                .contains(expected);
    }
}
