package com.cajunsystems.gumbo.service;

import com.cajunsystems.gumbo.api.Executor;
import com.cajunsystems.gumbo.api.ExecutorContext;
import com.cajunsystems.gumbo.core.AppendRequest;
import com.cajunsystems.gumbo.core.AppendResult;
import com.cajunsystems.gumbo.core.LogEntry;
import com.cajunsystems.gumbo.core.LogTag;
import com.cajunsystems.gumbo.persistence.InMemoryPersistenceAdapter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

class ExecutorEngineTest {

    private SharedLogService service;
    private ExecutorEngine engine;

    private static final LogTag INPUT_TAG  = LogTag.of("commands");
    private static final LogTag OUTPUT_TAG = LogTag.of("results");

    @BeforeEach
    void setUp() throws IOException {
        SharedLogConfig config = SharedLogConfig.builder()
                .persistenceAdapter(new InMemoryPersistenceAdapter())
                .build();
        service = SharedLogService.open(config);
        engine = new ExecutorEngine(service);
    }

    @AfterEach
    void tearDown() throws InterruptedException {
        engine.stop(3_000);
        service.close();
    }

    // -------------------------------------------------------------------------
    // A simple counting executor: counts the number of commands seen and
    // appends the count to the output tag.
    // -------------------------------------------------------------------------

    @Test
    void executorRebuildsStateFromLog() throws InterruptedException {
        // Pre-populate the log before the executor starts
        service.append(AppendRequest.to(INPUT_TAG, "cmd1".getBytes())).join();
        service.append(AppendRequest.to(INPUT_TAG, "cmd2".getBytes())).join();

        CountDownLatch outputLatch = new CountDownLatch(1);
        List<String> outputs = Collections.synchronizedList(new ArrayList<>());

        engine.register(new CountingExecutor(outputLatch, outputs));
        engine.start();

        // After start, executor should replay the 2 pre-existing commands and emit a count
        assertThat(outputLatch.await(5, TimeUnit.SECONDS))
                .as("executor should emit output within 5s")
                .isTrue();

        assertThat(outputs).isNotEmpty();
        // The executor emits "count=N" for N=2 pre-existing commands
        assertThat(outputs.get(outputs.size() - 1)).isEqualTo("count=2");
    }

    @Test
    void executorProcessesNewEntriesIncrementally() throws InterruptedException {
        CountDownLatch firstLatch  = new CountDownLatch(1);
        CountDownLatch secondLatch = new CountDownLatch(1);
        List<String> outputs = Collections.synchronizedList(new ArrayList<>());

        engine.register(new CountingExecutor(firstLatch, outputs));
        engine.start();

        // Append first command → executor should produce "count=1"
        service.append(AppendRequest.to(INPUT_TAG, "cmd1".getBytes())).join();
        assertThat(firstLatch.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(outputs).anyMatch(s -> s.equals("count=1"));

        // Append second command → executor should produce "count=2"
        service.append(AppendRequest.to(INPUT_TAG, "cmd2".getBytes())).join();
        await(secondLatch, outputs, "count=2");
    }

    @Test
    void idleExecutorDoesNotSpamLog() throws InterruptedException {
        engine.register(new CountingExecutor(new CountDownLatch(1), new ArrayList<>()));
        engine.start();

        // Let it run for 1s without any input
        Thread.sleep(1_000);

        List<LogEntry> results = service.readAll(OUTPUT_TAG).join();
        // No inputs → no outputs
        assertThat(results).isEmpty();
    }

    @Test
    void executorStopsCleanly() throws InterruptedException {
        engine.register(new CountingExecutor(new CountDownLatch(1), new ArrayList<>()));
        engine.start();
        boolean stopped = engine.stop(3_000);
        assertThat(stopped).isTrue();
    }

    // -------------------------------------------------------------------------
    // Executor impl
    // -------------------------------------------------------------------------

    /**
     * Counts the number of entries on the INPUT_TAG, appends "count=N" to
     * the OUTPUT_TAG, and signals the latch on first output.
     */
    private static class CountingExecutor implements Executor<Integer> {

        private final CountDownLatch outputLatch;
        private final List<String> outputs;
        private final AtomicInteger lastEmittedCount = new AtomicInteger(0);

        CountingExecutor(CountDownLatch outputLatch, List<String> outputs) {
            this.outputLatch = outputLatch;
            this.outputs = outputs;
        }

        @Override public LogTag getInputTag() { return INPUT_TAG; }
        @Override public String getName() { return "CountingExecutor"; }

        @Override
        public Integer initialState() { return 0; }

        @Override
        public Integer apply(Integer count, LogEntry entry) { return count + 1; }

        @Override
        public List<AppendRequest> execute(Integer state, ExecutorContext ctx) {
            if (state == 0) return Collections.emptyList(); // nothing to do
            // Only emit when count changes to avoid duplicate outputs
            int prev = lastEmittedCount.get();
            if (state <= prev) return Collections.emptyList();

            lastEmittedCount.set(state);
            String msg = "count=" + state;
            outputs.add(msg);
            outputLatch.countDown();

            return List.of(AppendRequest.to(OUTPUT_TAG, msg.getBytes()));
        }
    }

    // -------------------------------------------------------------------------

    private static void await(CountDownLatch latch, List<String> outputs, String expected)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + 5_000;
        while (System.currentTimeMillis() < deadline) {
            if (outputs.contains(expected)) return;
            Thread.sleep(50);
        }
        assertThat(outputs).contains(expected);
    }
}
