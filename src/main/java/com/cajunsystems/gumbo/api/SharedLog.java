package com.cajunsystems.gumbo.api;

import com.cajunsystems.gumbo.core.AppendRequest;
import com.cajunsystems.gumbo.core.AppendResult;
import com.cajunsystems.gumbo.core.LogEntry;
import com.cajunsystems.gumbo.core.LogPosition;
import com.cajunsystems.gumbo.core.LogTag;
import com.cajunsystems.gumbo.core.StreamVersions;

import java.util.ArrayList;
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

    /**
     * Appends multiple entries to the log, claiming all seqnums in a single
     * sequencer operation where supported.  Results are returned in the same
     * order as the input list.
     *
     * <p>For distributed deployments backed by
     * {@link com.cajunsystems.gumbo.sequencer.FoundationDBSequencer}, this
     * reduces sequencer round-trips from N to 1 for an N-entry batch — the same
     * optimisation Boki uses in its metalog to avoid the sequencer becoming a
     * throughput bottleneck.
     *
     * <p>The default implementation chains N individual {@link #append} calls
     * sequentially; production implementations should override for single-operation
     * seqnum claiming.
     *
     * @param requests entries to append; must not be empty
     * @return a future resolving to results in the same order as {@code requests}
     */
    default CompletableFuture<List<AppendResult>> appendBatch(List<AppendRequest> requests) {
        if (requests.isEmpty()) return CompletableFuture.completedFuture(List.of());
        List<AppendResult> results = new ArrayList<>(requests.size());
        CompletableFuture<Void> chain = CompletableFuture.completedFuture(null);
        for (AppendRequest req : requests) {
            chain = chain.thenCompose(ignored -> append(req).thenAccept(results::add));
        }
        return chain.thenApply(ignored -> List.copyOf(results));
    }

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

    /**
     * Reads entries with the given tag from {@code fromVersion} (inclusive) in that
     * tag's <em>own</em> numbering — its {@code streamVersion} — rather than the global
     * seqnum {@link #read} takes.
     *
     * <p>Use this whenever the position came from a previous entry of this tag:
     * a checkpoint, a stored cursor, a resume point. A seqnum-keyed read is only
     * equivalent while the log holds a single tag, and silently returns entries
     * already consumed once it holds more than one.
     */
    CompletableFuture<List<LogEntry>> readFromVersion(LogTag tag, long fromVersion);

    /**
     * Reads entries with the given tag after {@code afterVersion} (exclusive) in that
     * tag's own numbering — for a caller holding the version of the last entry it
     * processed. Pass {@code -1} for the whole stream.
     */
    default CompletableFuture<List<LogEntry>> readAfterVersion(LogTag tag, long afterVersion) {
        return readFromVersion(tag, StreamVersions.afterToInclusive(afterVersion));
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
