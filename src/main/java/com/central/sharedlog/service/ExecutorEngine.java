package com.central.sharedlog.service;

import com.central.sharedlog.api.Executor;
import com.central.sharedlog.api.ExecutorContext;
import com.central.sharedlog.api.LogView;
import com.central.sharedlog.api.SharedLog;
import com.central.sharedlog.core.AppendRequest;
import com.central.sharedlog.core.LogEntry;
import com.central.sharedlog.core.LogPosition;
import com.central.sharedlog.core.LogTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Runs registered {@link Executor executors} against the shared log,
 * each on its own <strong>virtual thread</strong>.
 *
 * <h2>Execution model</h2>
 * <p>For each registered executor the engine maintains one <em>runner</em> that
 * loops continuously on a virtual thread:
 * <ol>
 *   <li>Wait (blocking) for new entries to arrive on the executor's input tag.</li>
 *   <li>Fold all entries since the last checkpoint into state via
 *       {@link Executor#apply}.</li>
 *   <li>Call {@link Executor#execute} to obtain {@link AppendRequest requests}.</li>
 *   <li>Append those requests to the shared log.</li>
 *   <li>Advance the checkpoint to the latest processed seqnum.</li>
 *   <li>Go back to step 1.</li>
 * </ol>
 *
 * <h2>Statelessness and crash-safety</h2>
 * <p>Executors rebuild all state from the log on every restart.  The engine
 * persists no per-executor state beyond the in-memory checkpoint seqnum, which
 * is re-derived on startup by reading the log from the beginning.  This mirrors
 * Boki's stateless function model exactly.
 *
 * <h2>Virtual threads</h2>
 * <p>Runners park on a {@link LinkedBlockingQueue#poll} call while waiting for
 * new entries, consuming no OS thread.  Because virtual threads are cheap,
 * having many executors has negligible overhead.  Blocking I/O inside
 * {@link Executor#apply} or {@link Executor#execute} is perfectly fine.
 *
 * <h2>Incremental state</h2>
 * <p>After the initial full replay the runner caches the rebuilt state and only
 * applies new (incremental) entries on subsequent cycles.  If the runner detects
 * that the log has been trimmed past its checkpoint it triggers a full rebuild.
 *
 * <h2>Lifecycle</h2>
 * <pre>{@code
 * ExecutorEngine engine = new ExecutorEngine(sharedLogService);
 * engine.register(new MyOrderExecutor());
 * engine.start();
 * // ...
 * engine.stop();
 * }</pre>
 */
public class ExecutorEngine implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(ExecutorEngine.class);

    /** How long a runner waits for new entries before polling the log directly. */
    private static final long POLL_TIMEOUT_MS = 500;

    private final SharedLogService logService;
    private final Map<LogTag, ExecutorRunner<?>> runners = new ConcurrentHashMap<>();
    private volatile boolean running = false;

    public ExecutorEngine(SharedLogService logService) {
        this.logService = logService;
    }

    // -------------------------------------------------------------------------
    // Registration
    // -------------------------------------------------------------------------

    /**
     * Registers an executor. Must be called before {@link #start()}.
     *
     * @throws IllegalStateException if the engine is already running
     */
    public <S> void register(Executor<S> executor) {
        if (running) throw new IllegalStateException("Cannot register after start()");
        runners.put(executor.getInputTag(), new ExecutorRunner<>(executor, logService));
        logger.info("Registered executor '{}' for tag={}", executor.getName(), executor.getInputTag());
    }

    // -------------------------------------------------------------------------
    // Lifecycle
    // -------------------------------------------------------------------------

    /**
     * Starts all registered executor runners on virtual threads.
     * Returns immediately; runners execute asynchronously.
     */
    public void start() {
        if (running) throw new IllegalStateException("Already started");
        running = true;
        for (ExecutorRunner<?> runner : runners.values()) {
            Thread.ofVirtual()
                    .name("executor-" + runner.executor.getName())
                    .start(runner::run);
        }
        logger.info("ExecutorEngine started: {} executors", runners.size());
    }

    /**
     * Signals all runners to stop and waits up to {@code timeoutMs} milliseconds.
     *
     * @param timeoutMs how long to wait for a clean stop
     * @return true if all runners stopped within the timeout
     */
    public boolean stop(long timeoutMs) throws InterruptedException {
        running = false;
        CountDownLatch latch = new CountDownLatch(runners.size());
        runners.values().forEach(r -> r.stop(latch));
        boolean clean = latch.await(timeoutMs, TimeUnit.MILLISECONDS);
        if (!clean) logger.warn("Some executor runners did not stop within {}ms", timeoutMs);
        logger.info("ExecutorEngine stopped");
        return clean;
    }

    @Override
    public void close() {
        try {
            stop(5_000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    // -------------------------------------------------------------------------
    // Runner
    // -------------------------------------------------------------------------

    /**
     * Manages the lifecycle of a single executor: subscribes to the log,
     * rebuilds state, runs the execute cycle, and appends results.
     */
    private static final class ExecutorRunner<S> {

        private final Executor<S> executor;
        private final SharedLogService logService;

        /** Queue of incoming entries delivered by the log subscription. */
        private final LinkedBlockingQueue<LogEntry> inbox = new LinkedBlockingQueue<>();

        /** Seqnum of the last entry included in {@code cachedState}. */
        private final AtomicLong checkpointSeqnum = new AtomicLong(-1L);

        /** Incrementally-built state; {@code null} on first run. */
        private volatile S cachedState = null;

        private volatile boolean stopRequested = false;
        private CountDownLatch stopLatch;
        private SharedLog.Subscription subscription;

        ExecutorRunner(Executor<S> executor, SharedLogService logService) {
            this.executor  = executor;
            this.logService = logService;
        }

        // ----- control -------------------------------------------------------

        void stop(CountDownLatch latch) {
            stopRequested = true;
            stopLatch = latch;
            // Unblock the inbox poll
            inbox.offer(POISON_PILL);
            if (subscription != null) subscription.close();
        }

        // ----- main loop (runs on a virtual thread) --------------------------

        void run() {
            logger.debug("Runner starting for executor '{}'", executor.getName());
            try {
                // Full initial state rebuild
                buildFullState();

                // Subscribe to new entries (future-only; backlog already consumed above)
                long startSeqnum = Math.max(0, checkpointSeqnum.get() + 1);
                subscription = logService.subscribe(
                        executor.getInputTag(),
                        new LogPosition(startSeqnum),
                        entry -> {
                            if (!stopRequested) inbox.offer(entry);
                        });

                // Incremental loop
                while (!stopRequested) {
                    LogEntry entry = inbox.poll(POLL_TIMEOUT_MS, TimeUnit.MILLISECONDS);

                    if (entry == POISON_PILL) break;

                    if (entry != null) {
                        // Drain all buffered entries for this cycle
                        List<LogEntry> batch = new ArrayList<>();
                        batch.add(entry);
                        inbox.drainTo(batch);

                        // Check for poison in batch
                        if (batch.remove(POISON_PILL)) stopRequested = true;

                        applyAndExecute(batch);
                    } else {
                        // Timeout: check for any missed entries (anti-drift safety net)
                        long checkpoint = checkpointSeqnum.get();
                        List<LogEntry> missed = logService
                                .read(executor.getInputTag(),
                                        new LogPosition(checkpoint + 1), Integer.MAX_VALUE)
                                .join();
                        if (!missed.isEmpty()) applyAndExecute(missed);
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.debug("Runner interrupted for executor '{}'", executor.getName());
            } catch (Exception e) {
                logger.error("Runner failed for executor '{}': {}", executor.getName(), e.getMessage(), e);
            } finally {
                if (subscription != null) subscription.close();
                if (stopLatch != null) stopLatch.countDown();
                logger.debug("Runner stopped for executor '{}'", executor.getName());
            }
        }

        // ----- state management ----------------------------------------------

        /** Reads the entire tag log and folds it into cachedState. */
        private void buildFullState() {
            logger.debug("Building full state for executor '{}'", executor.getName());
            List<LogEntry> all = logService
                    .readAll(executor.getInputTag())
                    .join();

            S state = executor.initialState();
            long lastSeqnum = -1L;
            for (LogEntry e : all) {
                state = executor.apply(state, e);
                lastSeqnum = e.seqnum();
            }
            cachedState = state;
            checkpointSeqnum.set(lastSeqnum);

            // Run execute even on initial build — executors may produce entries
            // based solely on the current state (e.g. initialisation side-effects).
            if (!all.isEmpty()) {
                runExecute(lastSeqnum);
            }
            logger.debug("Full state built for executor '{}': {} entries, checkpoint={}",
                    executor.getName(), all.size(), lastSeqnum);
        }

        /** Applies a batch of new entries to cachedState, then runs the execute cycle. */
        private void applyAndExecute(List<LogEntry> newEntries) {
            long lastSeqnum = checkpointSeqnum.get();
            S state = cachedState;

            // Guard: if we somehow fell behind the trim boundary, rebuild from scratch
            long firstInBatch = newEntries.get(0).seqnum();
            if (lastSeqnum >= 0 && firstInBatch > lastSeqnum + 1) {
                logger.warn("Executor '{}' gap detected (checkpoint={}, firstBatch={}); rebuilding",
                        executor.getName(), lastSeqnum, firstInBatch);
                buildFullState();
                return;
            }

            for (LogEntry e : newEntries) {
                if (e.seqnum() <= lastSeqnum) continue; // deduplicate
                state = executor.apply(state, e);
                lastSeqnum = e.seqnum();
            }

            cachedState = state;
            checkpointSeqnum.set(lastSeqnum);
            runExecute(lastSeqnum);
        }

        /** Calls executor.execute and appends the returned requests. */
        private void runExecute(long checkpointAtCycle) {
            ExecutorContextImpl ctx = new ExecutorContextImpl(
                    logService.getView(executor.getInputTag()),
                    new LogPosition(Math.max(0, checkpointAtCycle)),
                    Instant.now(),
                    logService);

            List<AppendRequest> requests;
            try {
                requests = executor.execute(cachedState, ctx);
            } catch (Exception e) {
                logger.error("Executor '{}' execute() threw: {}", executor.getName(), e.getMessage(), e);
                return;
            }

            for (AppendRequest req : requests) {
                if (stopRequested) break;
                try {
                    logService.append(req).join();
                } catch (Exception e) {
                    logger.error("Executor '{}' append failed: {}", executor.getName(), e.getMessage(), e);
                }
            }
        }

        // ----- sentinel for blocking queue wakeup ----------------------------
        private static final LogEntry POISON_PILL =
                new LogEntry(Long.MAX_VALUE, -1, java.util.Set.of(LogTag.of("__poison__")),
                        new byte[0], Instant.EPOCH);
    }

    // -------------------------------------------------------------------------
    // ExecutorContext implementation
    // -------------------------------------------------------------------------

    private static final class ExecutorContextImpl implements ExecutorContext {

        private final LogView view;
        private final LogPosition currentPosition;
        private final Instant cycleTimestamp;
        private final SharedLogService logService;

        ExecutorContextImpl(LogView view, LogPosition currentPosition,
                            Instant cycleTimestamp, SharedLogService logService) {
            this.view            = view;
            this.currentPosition = currentPosition;
            this.cycleTimestamp  = cycleTimestamp;
            this.logService      = logService;
        }

        @Override public LogView getView() { return view; }
        @Override public LogPosition getCurrentPosition() { return currentPosition; }
        @Override public Instant getCycleTimestamp() { return cycleTimestamp; }

        @Override
        public LogView openView(LogTag tag) {
            return logService.getView(tag);
        }
    }
}
