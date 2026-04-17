package com.cajunsystems.gumbo.api;

import com.cajunsystems.gumbo.core.AppendResult;
import com.cajunsystems.gumbo.core.LogPosition;
import com.cajunsystems.gumbo.core.LogTag;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * A type-safe, tag-scoped view of the shared log.
 *
 * <p>{@code TypedLogView<T>} wraps a {@link LogView} and uses a
 * {@link com.cajunsystems.gumbo.serialization.LogSerializer LogSerializer&lt;T&gt;}
 * to transparently convert domain objects to {@code byte[]} on writes and back
 * on reads — removing all manual serialization boilerplate from call sites.
 *
 * <h2>Before (raw bytes)</h2>
 * <pre>{@code
 * // Write
 * log.append(AppendRequest.to(orders, OrderEvent.toBytes(event))).join();
 * // Read
 * List<LogEntry> raw = log.readAll(orders).join();
 * List<OrderEvent> events = raw.stream()
 *     .map(e -> OrderEvent.fromBytes(e.data()))
 *     .toList();
 * }</pre>
 *
 * <h2>After (TypedLogView)</h2>
 * <pre>{@code
 * TypedLogView<OrderEvent> view = service.getTypedView(orders, serializer);
 * view.append(event).join();
 * List<OrderEvent> events = view.readAll().join();
 * }</pre>
 *
 * <h2>Obtaining a TypedLogView</h2>
 * <pre>{@code
 * LogSerializer<OrderEvent> serializer = new KryoLogSerializer<>(OrderEvent.class);
 * TypedLogView<OrderEvent> view = service.getTypedView(LogTag.of("orders"), serializer);
 * }</pre>
 *
 * <p>The underlying {@link #rawView()} is always available for interoperability
 * with code that works with raw {@code byte[]} / {@link com.cajunsystems.gumbo.core.LogEntry}.
 *
 * @param <T> the domain type stored in this log view
 */
public interface TypedLogView<T> {

    /** The tag this view is scoped to. */
    LogTag getTag();

    // -------------------------------------------------------------------------
    // Write
    // -------------------------------------------------------------------------

    /**
     * Serializes {@code object} and appends it to the log under this view's tag.
     *
     * @param object the domain object to append; must not be {@code null}
     * @return a future that resolves to the assigned {@link AppendResult}
     */
    CompletableFuture<AppendResult> append(T object);

    // -------------------------------------------------------------------------
    // Read
    // -------------------------------------------------------------------------

    /**
     * Reads all entries for this tag from the beginning, deserializing each
     * payload into a {@code T}.
     */
    default CompletableFuture<List<T>> readAll() {
        return readFrom(LogPosition.BEGINNING, Integer.MAX_VALUE);
    }

    /**
     * Reads at most {@code maxEntries} entries starting at (and including)
     * {@code from}, deserializing each payload.
     */
    CompletableFuture<List<T>> readFrom(LogPosition from, int maxEntries);

    /**
     * Reads all entries with {@code seqnum > afterSeqnum}, i.e. the delta
     * since a known position.  Useful for incremental state updates.
     */
    default CompletableFuture<List<T>> readAfter(long afterSeqnum) {
        return readFrom(new LogPosition(afterSeqnum + 1), Integer.MAX_VALUE);
    }

    // -------------------------------------------------------------------------
    // Subscribe
    // -------------------------------------------------------------------------

    /**
     * Subscribes to entries for this tag.  The listener receives deserialized
     * {@code T} objects and runs on a virtual thread.
     *
     * @param from     start position (inclusive)
     * @param listener called for each new entry
     * @return a subscription handle the caller must close when done
     */
    SharedLog.Subscription subscribe(LogPosition from, Consumer<T> listener);

    /** Subscribes from the current tail (future entries only). */
    SharedLog.Subscription subscribeTail(Consumer<T> listener);

    // ── Metadata ──

    /** Returns the highest seqnum of any entry for this view's tag, or -1 if empty. */
    long getLatestSeqnum();

    /** Returns a {@link LogPosition} positioned at the current latest entry. */
    default LogPosition currentPosition() {
        long s = getLatestSeqnum();
        return s < 0 ? LogPosition.BEGINNING : new LogPosition(s);
    }

    // ── Key-Value ──

    /** Durably stores {@code value} under {@code key} in this view's tag-scoped KV store. */
    CompletableFuture<Void> setValue(String key, byte[] value);

    /** Returns the stored value for {@code key}, or {@code null} if absent. */
    CompletableFuture<byte[]> getValue(String key);

    /** Removes the stored value for {@code key}. No-op if absent. */
    CompletableFuture<Void> deleteValue(String key);

    // -------------------------------------------------------------------------
    // Interoperability
    // -------------------------------------------------------------------------

    /**
     * Returns the underlying raw {@link LogView} for interoperability with
     * code that works at the {@code byte[]} / {@link com.cajunsystems.gumbo.core.LogEntry}
     * level.
     */
    LogView rawView();
}
