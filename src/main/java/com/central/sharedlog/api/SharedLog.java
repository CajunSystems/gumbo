package com.central.sharedlog.api;

import com.central.sharedlog.core.AppendRequest;
import com.central.sharedlog.core.AppendResult;
import com.central.sharedlog.core.LogEntry;
import com.central.sharedlog.core.LogPosition;
import com.central.sharedlog.core.LogTag;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * The shared log: a totally-ordered, append-only ledger of {@link LogEntry} records.
 *
 * <h2>Design intent (Boki-inspired)</h2>
 * <p>The shared log is the single source of truth. Entries are assigned a
 * globally monotonic {@code seqnum} by the sequencer so every reader observes
 * the same total order. Multiple logical streams coexist in one physical log
 * via {@linkplain LogTag tags}; each tag produces its own filtered
 * {@link LogView}.
 *
 * <p>Stateless {@link Executor executors} re-derive all their state by replaying
 * a {@link LogView} from a known checkpoint, then append new entries, making
 * crash-recovery trivial.
 *
 * <h2>Thread safety</h2>
 * <p>All methods are safe for concurrent use. The async methods return
 * {@link CompletableFuture} values that complete on an internal thread pool;
 * callers may chain further work without blocking application threads.
 */
public interface SharedLog extends AutoCloseable {

    // -------------------------------------------------------------------------
    // Write
    // -------------------------------------------------------------------------

    /**
     * Appends a single entry to the log.
     *
     * @param request the payload and target tags
     * @return a future that resolves to the assigned {@link AppendResult}
     */
    CompletableFuture<AppendResult> append(AppendRequest request);

    // -------------------------------------------------------------------------
    // Read
    // -------------------------------------------------------------------------

    /**
     * Returns a {@link LogView} scoped to {@code tag}. Views are lightweight and
     * can be created freely; they share the underlying storage of this log.
     */
    LogView getView(LogTag tag);

    /**
     * Reads at most {@code maxEntries} entries with the given tag, starting at
     * (and including) {@code from}.
     */
    CompletableFuture<List<LogEntry>> read(LogTag tag, LogPosition from, int maxEntries);

    /**
     * Reads all entries with the given tag from {@link LogPosition#BEGINNING}.
     */
    default CompletableFuture<List<LogEntry>> readAll(LogTag tag) {
        return read(tag, LogPosition.BEGINNING, Integer.MAX_VALUE);
    }

    // -------------------------------------------------------------------------
    // Subscribe
    // -------------------------------------------------------------------------

    /**
     * Registers a listener that will be called (on a virtual thread) for every
     * new entry appended with the given tag, starting from {@code from}.
     *
     * <p>Listeners may perform blocking operations safely because each invocation
     * runs on its own virtual thread.
     *
     * @return a {@link Subscription} that the caller must close to unregister
     */
    Subscription subscribe(LogTag tag, LogPosition from, Consumer<LogEntry> listener);

    /** Subscribes starting at the current tail (future entries only). */
    default Subscription subscribeTail(LogTag tag, Consumer<LogEntry> listener) {
        return subscribe(tag, new LogPosition(getLatestSeqnum() + 1), listener);
    }

    // -------------------------------------------------------------------------
    // Housekeeping
    // -------------------------------------------------------------------------

    /**
     * Hints that entries with {@code seqnum < upToSeqnum} are no longer needed.
     * The underlying adapter may reclaim their storage.
     */
    CompletableFuture<Void> trim(long upToSeqnum);

    /** Returns the highest seqnum that has been committed, or {@code -1} if empty. */
    long getLatestSeqnum();

    @Override
    void close();

    // -------------------------------------------------------------------------
    // Nested type
    // -------------------------------------------------------------------------

    /**
     * An active log subscription. Must be closed to stop receiving notifications
     * and release associated resources.
     */
    interface Subscription extends AutoCloseable {
        @Override
        void close();

        boolean isActive();
    }
}
