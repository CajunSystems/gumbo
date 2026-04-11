package com.central.sharedlog.api;

import com.central.sharedlog.core.AppendResult;
import com.central.sharedlog.core.LogEntry;
import com.central.sharedlog.core.LogPosition;
import com.central.sharedlog.core.LogTag;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * A tag-scoped, read/append view of the shared log.
 *
 * <p>A {@code LogView} filters the global log to entries that carry
 * {@linkplain #getTag() its tag}. Entries are still ordered by their global
 * {@code seqnum}; only visibility is restricted.
 *
 * <p>{@link #append} writes through to the underlying {@link SharedLog} with
 * this view's tag automatically included in the request's tag set.
 *
 * <h2>Usage pattern (executor)</h2>
 * <pre>{@code
 * LogView view = sharedLog.getView(LogTag.of("orders"));
 * List<LogEntry> entries = view.readAll().join();
 * MyState state = executor.rebuildState(entries);
 * List<AppendRequest> writes = executor.execute(state, ctx);
 * writes.forEach(req -> view.append(req.data()).join());
 * }</pre>
 */
public interface LogView {

    /** The tag this view is scoped to. */
    LogTag getTag();

    // -------------------------------------------------------------------------
    // Read
    // -------------------------------------------------------------------------

    /**
     * Reads at most {@code maxEntries} entries visible from this tag, starting
     * at (and including) {@code from}.
     */
    CompletableFuture<List<LogEntry>> readFrom(LogPosition from, int maxEntries);

    /** Reads all entries visible from this tag. */
    default CompletableFuture<List<LogEntry>> readAll() {
        return readFrom(LogPosition.BEGINNING, Integer.MAX_VALUE);
    }

    /**
     * Reads all entries with {@code seqnum > afterSeqnum}, i.e. the "delta"
     * since a known position. Useful for incremental state updates in executors.
     */
    default CompletableFuture<List<LogEntry>> readAfter(long afterSeqnum) {
        return readFrom(new LogPosition(afterSeqnum + 1), Integer.MAX_VALUE);
    }

    /** Returns the latest seqnum visible in this view, or {@code -1} if empty. */
    long getLatestSeqnum();

    /** Returns the current read tip as a {@link LogPosition}. */
    default LogPosition currentPosition() {
        long s = getLatestSeqnum();
        return s < 0 ? LogPosition.BEGINNING : new LogPosition(s);
    }

    // -------------------------------------------------------------------------
    // Write (passes through to the parent SharedLog)
    // -------------------------------------------------------------------------

    /**
     * Appends {@code data} tagged with this view's tag.
     */
    CompletableFuture<AppendResult> append(byte[] data);

    // -------------------------------------------------------------------------
    // Subscribe
    // -------------------------------------------------------------------------

    /**
     * Subscribes to new entries for this tag.  The listener runs on a virtual
     * thread so it may block freely.
     *
     * @param from     start position (inclusive)
     * @param listener called for each new entry
     * @return a subscription handle the caller must close when done
     */
    SharedLog.Subscription subscribe(LogPosition from, Consumer<LogEntry> listener);

    /** Convenience: subscribe from the current tail (future entries only). */
    default SharedLog.Subscription subscribeTail(Consumer<LogEntry> listener) {
        long s = getLatestSeqnum();
        return subscribe(new LogPosition(Math.max(0, s + 1)), listener);
    }
}
