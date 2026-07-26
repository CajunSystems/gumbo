package com.cajunsystems.gumbo.service;

import com.cajunsystems.gumbo.api.LogView;
import com.cajunsystems.gumbo.api.SharedLog;
import com.cajunsystems.gumbo.api.TypedLogView;
import com.cajunsystems.gumbo.serialization.LogSerializer;
import com.cajunsystems.gumbo.core.AppendRequest;
import com.cajunsystems.gumbo.core.AppendResult;
import com.cajunsystems.gumbo.core.LogEntry;
import com.cajunsystems.gumbo.core.LogPosition;
import com.cajunsystems.gumbo.core.LogTag;
import com.cajunsystems.gumbo.persistence.PersistenceAdapter;
import com.cajunsystems.gumbo.sequencer.Sequencer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * The primary {@link SharedLog} implementation.
 *
 * <h2>Concurrency model</h2>
 * <ul>
 *   <li>A single {@link ReentrantLock} serialises appends so that seqnum
 *       assignment and persistence are an atomic unit.</li>
 *   <li>Reads are lock-free; they query the persistence adapter directly.</li>
 *   <li>Each subscription owns one virtual thread that delivers its entries in
 *       seqnum order, so a slow listener blocks neither appends nor other
 *       subscribers — and never runs concurrently with itself.</li>
 * </ul>
 *
 * <h2>Lifecycle</h2>
 * <pre>{@code
 * SharedLogConfig config = SharedLogConfig.builder()
 *     .persistenceAdapter(new FileBasedPersistenceAdapter("/data"))
 *     .build();
 * try (SharedLogService log = SharedLogService.open(config)) {
 *     AppendResult r = log.append(AppendRequest.to(LogTag.of("orders"), payload)).join();
 * }
 * }</pre>
 */
public class SharedLogService implements SharedLog {

    private static final Logger logger = LoggerFactory.getLogger(SharedLogService.class);

    private final PersistenceAdapter adapter;
    private final Sequencer sequencer;
    private final ExecutorService asyncPool;

    /** Serialises seqnum assignment + persistence write. */
    private final ReentrantLock writeLock = new ReentrantLock();

    /** Per-tag local-id counters, initialised from persisted state on open. */
    private final ConcurrentHashMap<LogTag, AtomicLong> versionCounters = new ConcurrentHashMap<>();

    /** Active subscriptions keyed by tag. */
    private final ConcurrentHashMap<LogTag, CopyOnWriteArrayList<SubscriptionImpl>> subscriptions =
            new ConcurrentHashMap<>();

    private volatile boolean closed = false;

    // -------------------------------------------------------------------------
    // Factory
    // -------------------------------------------------------------------------

    public static SharedLogService open(SharedLogConfig config) throws IOException {
        SharedLogService service = new SharedLogService(config);
        service.init();
        return service;
    }

    private SharedLogService(SharedLogConfig config) {
        this.adapter   = config.getPersistenceAdapter();
        this.sequencer = config.getSequencer();
        this.asyncPool = config.getAsyncPool();
    }

    private void init() throws IOException {
        adapter.open();
        // Seed sequencer so it continues from the last persisted seqnum on restart.
        long latest = adapter.getLatestSeqnum();
        if (latest >= 0 && sequencer instanceof com.cajunsystems.gumbo.sequencer.LocalSequencer ls) {
            ls.advanceTo(latest + 1);
        }
        logger.info("SharedLogService started: latestSeqnum={}", latest);
    }

    // -------------------------------------------------------------------------
    // Append
    // -------------------------------------------------------------------------

    @Override
    public CompletableFuture<AppendResult> append(AppendRequest request) {
        return CompletableFuture.supplyAsync(() -> doAppend(request), asyncPool);
    }

    /**
     * Appends multiple entries in a single lock acquisition, claiming all seqnums
     * in one {@link com.cajunsystems.gumbo.sequencer.Sequencer#nextBatch} call.
     *
     * <p>With {@link com.cajunsystems.gumbo.sequencer.FoundationDBSequencer} this
     * reduces sequencer round-trips from N to 1 for an N-entry batch.  Combined
     * with {@link com.cajunsystems.gumbo.persistence.BatchingPersistenceAdapter}
     * wrapping the FDB adapter, an N-entry batch costs exactly 2 FDB transactions
     * (one seqnum claim + one data commit) instead of N + 1.
     */
    @Override
    public CompletableFuture<List<AppendResult>> appendBatch(List<AppendRequest> requests) {
        if (requests.isEmpty()) return CompletableFuture.completedFuture(List.of());
        return CompletableFuture.supplyAsync(() -> doAppendBatch(requests), asyncPool);
    }

    private List<AppendResult> doAppendBatch(List<AppendRequest> requests) {
        ensureNotClosed();
        writeLock.lock();
        try {
            // Claim all seqnums in a single sequencer call (1 FDB RTT for FoundationDBSequencer)
            long[] seqnums = sequencer.nextBatch(requests.size());

            List<LogEntry>     entries = new ArrayList<>(requests.size());
            List<AppendResult> results = new ArrayList<>(requests.size());
            Instant            now     = Instant.now();

            for (int i = 0; i < requests.size(); i++) {
                AppendRequest req      = requests.get(i);
                long          seqnum   = seqnums[i];
                LogTag        primary  = req.tags().iterator().next();
                long          version  = nextVersionFor(primary);
                LogEntry      entry    = new LogEntry(seqnum, version, req.tags(), req.dataUnsafe(), now);
                entries.add(entry);
                results.add(new AppendResult(seqnum, version, primary, now));
            }

            // Single persistence call — 1 FDB transaction for entire batch when using FDB adapter
            adapter.appendBatch(entries);

            // Queue for delivery; each subscription drains its own queue.
            for (LogEntry entry : entries) notifySubscribers(entry);

            return List.copyOf(results);
        } catch (IOException e) {
            throw new LogWriteException("Failed to persist batch of " + requests.size() + " entries", e);
        } finally {
            writeLock.unlock();
        }
    }

    private AppendResult doAppend(AppendRequest request) {
        ensureNotClosed();
        writeLock.lock();
        try {
            long seqnum    = sequencer.next();
            Set<LogTag> tags = request.tags();
            LogTag primaryTag = tags.iterator().next();
            long version   = nextVersionFor(primaryTag);
            Instant now    = Instant.now();

            LogEntry entry = new LogEntry(seqnum, version, tags, request.dataUnsafe(), now);
            adapter.append(entry);

            // Queue for delivery; each subscription drains its own queue.
            notifySubscribers(entry);

            return new AppendResult(seqnum, version, primaryTag, now);
        } catch (IOException e) {
            throw new LogWriteException("Failed to persist log entry", e);
        } finally {
            writeLock.unlock();
        }
    }

    // -------------------------------------------------------------------------
    // Read
    // -------------------------------------------------------------------------

    @Override
    public LogView getView(LogTag tag) {
        return new DefaultLogView(tag, this);
    }

    /**
     * Returns a {@link TypedLogView} that serializes domain objects of type
     * {@code T} using the supplied {@link LogSerializer}.
     *
     * <pre>{@code
     * LogSerializer<OrderEvent> s = new KryoLogSerializer<>(OrderEvent.class);
     * TypedLogView<OrderEvent> view = service.getTypedView(LogTag.of("orders"), s);
     * view.append(new OrderEvent(...)).join();
     * List<OrderEvent> events = view.readAll().join();
     * }</pre>
     *
     * @param tag        the tag to scope the view to
     * @param serializer converts {@code T} ↔ {@code byte[]}
     * @param <T>        the domain type
     */
    public <T> TypedLogView<T> getTypedView(LogTag tag, LogSerializer<T> serializer) {
        return new DefaultTypedLogView<>(getView(tag), serializer);
    }

    @Override
    public CompletableFuture<List<LogEntry>> read(LogTag tag, LogPosition from, int maxEntries) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                List<LogEntry> raw = adapter.readByTag(tag, from.seqnum());
                return raw.size() <= maxEntries ? raw : raw.subList(0, maxEntries);
            } catch (IOException e) {
                throw new LogReadException("Failed to read log entries for tag=" + tag, e);
            }
        }, asyncPool);
    }

    @Override
    public CompletableFuture<List<LogEntry>> readFromVersion(LogTag tag, long fromVersion) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return adapter.readFromVersion(tag, fromVersion);
            } catch (IOException e) {
                throw new LogReadException(
                        "Failed to read log entries for tag=" + tag + " fromVersion=" + fromVersion, e);
            }
        }, asyncPool);
    }

    // -------------------------------------------------------------------------
    // Subscribe
    // -------------------------------------------------------------------------

    @Override
    public Subscription subscribe(LogTag tag, LogPosition from, Consumer<LogEntry> listener) {
        ensureNotClosed();
        SubscriptionImpl sub = new SubscriptionImpl(tag, listener, from.seqnum());

        // Registered before the backlog is read, so anything appended from here on is
        // queued rather than missed. The pump below decides what to skip.
        subscriptions.computeIfAbsent(tag, k -> new CopyOnWriteArrayList<>()).add(sub);

        // One virtual thread per subscription: it reads the backlog, delivers it, then
        // drains the queue for as long as the subscription lives. subscribe() returns
        // without waiting for any of it.
        sub.start(() -> {
            try {
                return adapter.readByTag(tag, from.seqnum());
            } catch (IOException ex) {
                logger.warn("Error reading backlog for tag={}: {}", tag, ex.getMessage());
                return List.of();
            }
        });

        return sub;
    }

    // -------------------------------------------------------------------------
    // Trim
    // -------------------------------------------------------------------------

    @Override
    public CompletableFuture<Void> trim(long upToSeqnum) {
        return CompletableFuture.runAsync(() -> {
            try {
                adapter.trim(upToSeqnum);
            } catch (IOException e) {
                throw new LogWriteException("Trim failed at seqnum=" + upToSeqnum, e);
            }
        }, asyncPool);
    }

    // -------------------------------------------------------------------------
    // Metadata
    // -------------------------------------------------------------------------

    @Override
    public long getLatestSeqnum() {
        return adapter.getLatestSeqnum();
    }

    // -------------------------------------------------------------------------
    // Lifecycle
    // -------------------------------------------------------------------------

    @Override
    public void close() {
        if (closed) return;
        closed = true;
        // Cancel all active subscriptions
        subscriptions.values().forEach(list -> list.forEach(SubscriptionImpl::close));
        subscriptions.clear();
        asyncPool.shutdown();
        try {
            adapter.close();
        } catch (IOException e) {
            logger.warn("Error closing persistence adapter: {}", e.getMessage());
        }
        logger.info("SharedLogService closed");
    }

    // -------------------------------------------------------------------------
    // Package-private helpers used by DefaultLogView
    // -------------------------------------------------------------------------

    PersistenceAdapter adapter() { return adapter; }
    ExecutorService asyncPool() { return asyncPool; }

    Subscription addSubscription(LogTag tag, LogPosition from, Consumer<LogEntry> listener) {
        return subscribe(tag, from, listener);
    }

    // -------------------------------------------------------------------------
    // Internal helpers
    // -------------------------------------------------------------------------

    private long nextVersionFor(LogTag primaryTag) {
        return versionCounters
                .computeIfAbsent(primaryTag, k -> new AtomicLong(adapter.getNextStreamVersion(k)))
                .getAndIncrement();
    }

    /**
     * Hands {@code entry} to every live subscription on its tags.
     *
     * <p>Only enqueues — it never delivers, and never decides whether a subscriber is
     * "ready". Both were the bug: a subscriber still working through its backlog was
     * skipped here, and the backlog read had already happened, so the entry reached it
     * by neither route and was silently lost.
     *
     * <p>Called under the append write lock, so entries enter each queue in seqnum
     * order, and the enqueue never blocks.
     */
    private void notifySubscribers(LogEntry entry) {
        for (LogTag tag : entry.tags()) {
            CopyOnWriteArrayList<SubscriptionImpl> subs = subscriptions.get(tag);
            if (subs == null || subs.isEmpty()) continue;
            for (SubscriptionImpl sub : subs) {
                sub.enqueue(entry);
            }
        }
    }

    private void ensureNotClosed() {
        if (closed) throw new IllegalStateException("SharedLogService is closed");
    }

    // -------------------------------------------------------------------------
    // Subscription implementation
    // -------------------------------------------------------------------------

    /**
     * A subscription and its single delivery thread.
     *
     * <p>Everything the listener ever sees goes through one queue drained by one virtual
     * thread: the backlog first, then live entries. That gives three properties the
     * previous design could not.
     *
     * <p><strong>Nothing is lost at the handover.</strong> Live entries are queued from
     * the moment the subscription is registered, which is before the backlog is read, so
     * an entry appended while the backlog is still being delivered waits its turn instead
     * of being dropped by a not-yet-ready check.
     *
     * <p><strong>Delivery is ordered.</strong> Appends enqueue under the write lock, so
     * the queue is in seqnum order and one draining thread keeps it that way. Spawning a
     * thread per entry left ordering to the scheduler.
     *
     * <p><strong>The listener is never called concurrently with itself</strong>, so it
     * needs no synchronisation of its own. It still runs on a virtual thread and may
     * block freely; a slow listener now delays its own subscription rather than
     * consuming a thread per pending entry.
     */
    private static final class SubscriptionImpl implements Subscription {

        /**
         * Total budget {@link #close()} spends waiting for an in-flight listener call,
         * split between waiting politely and waiting after an interrupt.
         */
        private static final long CLOSE_TIMEOUT_MS = 5_000;

        /**
         * Queued by {@link #close()} to wake a pump blocked on {@code take()}.
         *
         * <p>An interrupt would do it too, but only by landing wherever the pump happens
         * to be — including inside {@code listener.accept}, where it aborts whatever the
         * listener was waiting on. Matched by identity, so no real entry can collide.
         */
        private static final LogEntry POISON = new LogEntry(
                0, 0, Set.of(LogTag.of("gumbo", "close")), new byte[0], Instant.EPOCH);

        private final LogTag tag;
        private final Consumer<LogEntry> listener;
        private final BlockingQueue<LogEntry> queue = new LinkedBlockingQueue<>();

        /** Highest seqnum handed to the listener; guards against redelivering the backlog. */
        private long lastDelivered;

        private volatile boolean active = true;
        private volatile Thread pump;

        SubscriptionImpl(LogTag tag, Consumer<LogEntry> listener, long fromSeqnum) {
            this.tag = tag;
            this.listener = listener;
            this.lastDelivered = fromSeqnum - 1;
        }

        /** Queues an entry for delivery. Non-blocking; safe to call under the write lock. */
        void enqueue(LogEntry entry) {
            if (active) queue.add(entry);
        }

        /** Starts the delivery thread: {@code backlog} is read on it, then the queue is drained. */
        void start(Supplier<List<LogEntry>> backlog) {
            pump = Thread.ofVirtual().name("sharedlog-deliver-" + tag).start(() -> {
                try {
                    for (LogEntry e : backlog.get()) {
                        if (!active) return;
                        deliver(e);
                    }
                    while (active) {
                        LogEntry e;
                        try {
                            e = queue.take();
                        } catch (InterruptedException ie) {
                            Thread.currentThread().interrupt();
                            return;
                        }
                        if (e == POISON) return;
                        // An entry appended between registration and the backlog read is
                        // in both, so the seqnum decides. Without this the fix for losing
                        // entries would trade it for delivering them twice.
                        if (e.seqnum() > lastDelivered) deliver(e);
                    }
                } finally {
                    // However this thread ends, the subscription is over. Without this a
                    // pump killed by an unexpected throwable would leave isActive()
                    // reporting true while entries piled up in a queue nobody drains —
                    // the same silent loss this class exists to remove, one level up.
                    active = false;
                    queue.clear();
                }
            });
        }

        private void deliver(LogEntry entry) {
            lastDelivered = entry.seqnum();
            // Re-checked here, not only at the top of the loop: close() can land while
            // this entry is being dequeued, and a listener must not be called after the
            // caller has torn down whatever it closes over.
            if (!active) return;
            try {
                listener.accept(entry);
            } catch (Throwable t) {
                // Throwable, not Exception. One thread now serves the whole subscription,
                // so an Error escaping a listener would kill delivery for every later
                // entry — where the previous thread-per-entry design lost only its own.
                // A broken listener must not become a broken subscription.
                LoggerFactory.getLogger(SubscriptionImpl.class)
                        .warn("Listener threw for tag={} seqnum={}: {}",
                                tag, entry.seqnum(), t.toString(), t);
            }
        }

        /**
         * Stops delivery and waits for any in-flight listener call to return.
         *
         * <p>No entry is delivered after this is called. When it returns <em>normally</em>
         * the listener has also stopped running, which is the property a caller needs
         * before releasing whatever the listener closes over.
         *
         * <p>It escalates rather than waiting on one timer. The pump is first woken with
         * a queued sentinel, which leaves a listener mid-call alone to finish. If it has
         * not returned within half the {@link #CLOSE_TIMEOUT_MS} budget the thread is
         * interrupted and waited on again, so a listener that honours interruption is
         * reaped before this method returns rather than after it.
         *
         * <p><strong>The guarantee has a limit, and it is the JVM's, not this class's.</strong>
         * A listener that neither returns nor honours interruption cannot be stopped —
         * Java has no way to force it. After the full budget this method logs a warning
         * and returns while that listener is still running, because the alternative is an
         * unclosable subscription that hangs {@link SharedLogService#close()} with it.
         * A caller whose listener can block indefinitely and whose resources must not be
         * released early has to coordinate that with the listener itself.
         *
         * <p>Closing from inside the listener does nothing beyond deactivating: waiting
         * for the pump from the pump would deadlock.
         *
         * <p>An interrupt on the <em>calling</em> thread does not cut the wait short.
         * Shutdown paths frequently run on a thread that is already interrupted, and
         * {@link Thread#join(long)} throws immediately for such a caller — so honouring
         * it here would make this method skip both waits and return while the listener
         * ran on, in precisely the situation where a caller is about to tear down what
         * the listener is using. The interrupt is deferred instead and restored before
         * returning, so the caller still observes it.
         */
        @Override
        public void close() {
            active = false;
            Thread t = pump;
            if (t == null || t == Thread.currentThread()) return;

            boolean interrupted = false;
            try {
                long half = CLOSE_TIMEOUT_MS / 2;
                queue.add(POISON);   // wakes take() without touching a running listener

                interrupted = awaitPump(t, half);
                if (t.isAlive()) {
                    // Mid-listener. Interrupt, then wait again — interrupting and
                    // returning immediately would deny an interruptible listener the one
                    // chance the interrupt exists to give it.
                    t.interrupt();
                    interrupted |= awaitPump(t, CLOSE_TIMEOUT_MS - half);
                    if (t.isAlive()) {
                        LoggerFactory.getLogger(SubscriptionImpl.class)
                                .warn("Listener for tag={} still running {} ms after close and did"
                                        + " not respond to interruption; abandoning the wait."
                                        + " It may still be using resources the caller releases next.",
                                        tag, CLOSE_TIMEOUT_MS);
                    }
                }
                queue.clear();
            } finally {
                if (interrupted) Thread.currentThread().interrupt();
            }
        }

        /**
         * Joins {@code t} for up to {@code ms}, without letting an interrupt of the
         * calling thread end the wait early.
         *
         * <p>Returns whether the caller was interrupted while waiting, so {@link #close()}
         * can restore the flag once it is done rather than dropping it. Whether the
         * thread finished is read from {@link Thread#isAlive()} by the caller — keeping
         * the two answers separate is what stops an interrupt being mistaken for
         * "still running".
         */
        private static boolean awaitPump(Thread t, long ms) {
            boolean interrupted = false;
            long deadline = System.nanoTime() + ms * 1_000_000L;
            long remaining;
            while (t.isAlive() && (remaining = deadline - System.nanoTime()) > 0) {
                try {
                    t.join(Math.max(1L, remaining / 1_000_000L));
                } catch (InterruptedException ie) {
                    interrupted = true;   // deferred; close() restores it before returning
                }
            }
            return interrupted;
        }

        @Override
        public boolean isActive() { return active; }
    }

    // -------------------------------------------------------------------------
    // Exceptions
    // -------------------------------------------------------------------------

    public static class LogWriteException extends RuntimeException {
        public LogWriteException(String msg, Throwable cause) { super(msg, cause); }
    }

    public static class LogReadException extends RuntimeException {
        public LogReadException(String msg, Throwable cause) { super(msg, cause); }
    }
}
