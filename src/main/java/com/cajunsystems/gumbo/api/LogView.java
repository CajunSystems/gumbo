package com.cajunsystems.gumbo.api;

import com.cajunsystems.gumbo.core.AppendResult;
import com.cajunsystems.gumbo.core.LogEntry;
import com.cajunsystems.gumbo.core.LogPosition;
import com.cajunsystems.gumbo.core.LogTag;

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
     *
     * <p>Note that {@code afterSeqnum} is a <em>global</em> seqnum. If the position you
     * are resuming from came from this view's own stream — a checkpointed
     * {@code localId}, the version of the last entry you handled — use
     * {@link #readAfterVersion} instead. The two agree only while the log holds a single
     * tag; with more than one tag this method silently returns entries you have already
     * seen, because the other tags' entries push this tag's seqnums past the cursor.
     */
    default CompletableFuture<List<LogEntry>> readAfter(long afterSeqnum) {
        return readFrom(new LogPosition(afterSeqnum + 1), Integer.MAX_VALUE);
    }

    /**
     * Reads entries from {@code fromVersion} (inclusive) in this tag's <em>own</em>
     * numbering — the {@code localId} carried by every {@link LogEntry} and returned by
     * every append — rather than the global seqnum.
     *
     * <p>This is the read for a per-stream cursor. A view's entries are numbered
     * {@code 0, 1, 2, …} within its own tag no matter how many other tags share the
     * physical log, so a version survives the log gaining new writers; a seqnum does
     * not.
     *
     * <pre>{@code
     * long cursor = Long.parseLong(new String(view.getValue("cursor").join()));
     * for (LogEntry e : view.readAfterVersion(cursor).join()) {
     *     state = apply(state, e);
     *     cursor = e.localId();
     * }
     * view.setValue("cursor", Long.toString(cursor).getBytes()).join();
     * }</pre>
     */
    CompletableFuture<List<LogEntry>> readFromVersion(long fromVersion);

    /**
     * Reads entries after {@code afterVersion} (exclusive) in this tag's own numbering —
     * the delta since the last entry this reader processed. Pass {@code -1} to read the
     * whole stream.
     */
    default CompletableFuture<List<LogEntry>> readAfterVersion(long afterVersion) {
        return readFromVersion(afterVersion < 0 ? 0L : afterVersion + 1);
    }

    /** Returns the latest seqnum visible in this view, or {@code -1} if empty. */
    long getLatestSeqnum();

    /**
     * Returns the latest version in this tag's own numbering (the highest
     * {@code localId} written to it), or {@code -1} if the tag is empty.
     *
     * <p>This is the counterpart to {@link #getLatestSeqnum()} for a per-stream cursor,
     * and the value to pass to {@link #readAfterVersion}.
     */
    long getLatestVersion();

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

    // ── Key-Value ──

    /**
     * Durably stores {@code value} under {@code key} in this view's tag-scoped
     * key-value store. Overwrites any existing value.
     */
    CompletableFuture<Void> setValue(String key, byte[] value);

    /**
     * Returns the stored value for {@code key}, or {@code null} if absent.
     */
    CompletableFuture<byte[]> getValue(String key);

    /**
     * Removes the stored value for {@code key}. No-op if absent.
     */
    CompletableFuture<Void> deleteValue(String key);
}
