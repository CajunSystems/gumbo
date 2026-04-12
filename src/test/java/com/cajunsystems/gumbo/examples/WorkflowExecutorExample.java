package com.cajunsystems.gumbo.examples;

import com.cajunsystems.gumbo.api.Executor;
import com.cajunsystems.gumbo.api.ExecutorContext;
import com.cajunsystems.gumbo.core.AppendRequest;
import com.cajunsystems.gumbo.core.LogEntry;
import com.cajunsystems.gumbo.core.LogTag;
import com.cajunsystems.gumbo.persistence.InMemoryPersistenceAdapter;
import com.cajunsystems.gumbo.service.ExecutorEngine;
import com.cajunsystems.gumbo.service.SharedLogConfig;
import com.cajunsystems.gumbo.service.SharedLogService;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end example of a multi-stage workflow pipeline built from two chained
 * stateless executors.
 *
 * <h2>Pipeline</h2>
 * <pre>
 *   workflow.commands  ──▶  WorkflowSubmitExecutor  ──▶  workflow.events ("processing:<id>")
 *                                                              │
 *   workflow.events    ──▶  WorkflowProcessExecutor  ──▶  workflow.events ("complete:<id>")
 * </pre>
 *
 * <ol>
 *   <li>{@code "submit:<id>"} entries arrive on {@code workflow.commands}.</li>
 *   <li>{@link WorkflowSubmitExecutor} reads them and emits {@code "processing:<id>"}
 *       to {@code workflow.events}.</li>
 *   <li>{@link WorkflowProcessExecutor} reads {@code workflow.events}, picks up
 *       any {@code "processing:"} entries, and emits {@code "complete:<id>"} to the
 *       same tag.</li>
 * </ol>
 *
 * <p>Both executors share a single {@link ExecutorEngine} and run concurrently on
 * separate virtual threads. The idempotency guard ({@code emitted} set) prevents
 * re-emitting the same event if the executor is replayed from the log.
 *
 * <p>Run with: {@code mvn test -Dtest=WorkflowExecutorExample}
 */
class WorkflowExecutorExample {

    private static final LogTag COMMANDS = LogTag.of("workflow.commands");
    private static final LogTag EVENTS   = LogTag.of("workflow.events");

    // -------------------------------------------------------------------------
    // Stage 1: submit → processing
    // -------------------------------------------------------------------------

    /**
     * Reads {@code workflow.commands}, collects submitted item IDs, and emits
     * {@code "processing:<id>"} to {@code workflow.events} for each new item.
     *
     * <p>State type is {@code List<String>}: the ordered list of IDs seen so far.
     */
    static class WorkflowSubmitExecutor implements Executor<List<String>> {

        /** Guards against re-emitting on subsequent cycles or after log replay. */
        private final Set<String> emitted = ConcurrentHashMap.newKeySet();

        @Override public LogTag getInputTag() { return COMMANDS; }
        @Override public String getName()     { return "WorkflowSubmit"; }

        @Override
        public List<String> initialState() { return List.of(); }

        @Override
        public List<String> apply(List<String> ids, LogEntry entry) {
            String msg = new String(entry.data());
            if (msg.startsWith("submit:")) {
                String id = msg.substring("submit:".length());
                List<String> next = new ArrayList<>(ids);
                next.add(id);
                return next;
            }
            return ids;
        }

        @Override
        public List<AppendRequest> execute(List<String> ids, ExecutorContext ctx) {
            return ids.stream()
                    .filter(emitted::add)   // only emit each id once
                    .map(id -> AppendRequest.to(EVENTS, ("processing:" + id).getBytes()))
                    .toList();
        }
    }

    // -------------------------------------------------------------------------
    // Stage 2: processing → complete
    // -------------------------------------------------------------------------

    /**
     * Reads {@code workflow.events}, collects IDs in the {@code "processing"} state,
     * and emits {@code "complete:<id>"} for each one.
     *
     * <p>State type is {@code List<String>}: the ordered list of IDs currently in
     * the processing stage.
     */
    static class WorkflowProcessExecutor implements Executor<List<String>> {

        private final Set<String> emitted = ConcurrentHashMap.newKeySet();

        @Override public LogTag getInputTag() { return EVENTS; }
        @Override public String getName()     { return "WorkflowProcess"; }

        @Override
        public List<String> initialState() { return List.of(); }

        @Override
        public List<String> apply(List<String> processing, LogEntry entry) {
            String msg = new String(entry.data());
            if (msg.startsWith("processing:")) {
                String id = msg.substring("processing:".length());
                List<String> next = new ArrayList<>(processing);
                next.add(id);
                return next;
            }
            return processing;
        }

        @Override
        public List<AppendRequest> execute(List<String> processing, ExecutorContext ctx) {
            return processing.stream()
                    .filter(emitted::add)
                    .map(id -> AppendRequest.to(EVENTS, ("complete:" + id).getBytes()))
                    .toList();
        }
    }

    // -------------------------------------------------------------------------
    // Test
    // -------------------------------------------------------------------------

    @Test
    void advancesItemsThroughWorkflowStages() throws Exception {
        SharedLogService service = SharedLogService.open(SharedLogConfig.builder()
                .persistenceAdapter(new InMemoryPersistenceAdapter())
                .build());
        ExecutorEngine engine = new ExecutorEngine(service);

        // Pre-populate two commands before the engine starts so both executors
        // must replay the backlog on startup.
        service.append(AppendRequest.to(COMMANDS, "submit:order-1".getBytes())).join();
        service.append(AppendRequest.to(COMMANDS, "submit:order-2".getBytes())).join();

        engine.register(new WorkflowSubmitExecutor());
        engine.register(new WorkflowProcessExecutor());
        engine.start();

        // Both pre-existing orders should complete via the chained pipeline.
        awaitPayload(service, "complete:order-1");
        awaitPayload(service, "complete:order-2");

        // Submit a third order while the engine is running to exercise incremental
        // (non-replay) processing.
        service.append(AppendRequest.to(COMMANDS, "submit:order-3".getBytes())).join();
        awaitPayload(service, "complete:order-3");

        List<String> eventPayloads = service.readAll(EVENTS).join().stream()
                .map(e -> new String(e.data()))
                .toList();

        System.out.println("Workflow events (" + eventPayloads.size() + " entries):");
        eventPayloads.forEach(p -> System.out.println("  " + p));

        assertThat(eventPayloads)
                .contains(
                        "processing:order-1", "complete:order-1",
                        "processing:order-2", "complete:order-2",
                        "processing:order-3", "complete:order-3");

        engine.stop(3_000);
        service.close();
    }

    // -------------------------------------------------------------------------

    private static void awaitPayload(SharedLogService service, String expected)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + 5_000;
        while (System.currentTimeMillis() < deadline) {
            boolean found = service.readAll(EVENTS).join().stream()
                    .anyMatch(e -> new String(e.data()).equals(expected));
            if (found) return;
            Thread.sleep(50);
        }
        assertThat(service.readAll(EVENTS).join().stream()
                .map(e -> new String(e.data())).toList())
                .contains(expected);
    }
}
