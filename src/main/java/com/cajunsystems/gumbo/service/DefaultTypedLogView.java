package com.cajunsystems.gumbo.service;

import com.cajunsystems.gumbo.api.LogView;
import com.cajunsystems.gumbo.api.SharedLog;
import com.cajunsystems.gumbo.api.TypedLogView;
import com.cajunsystems.gumbo.core.AppendResult;
import com.cajunsystems.gumbo.core.LogPosition;
import com.cajunsystems.gumbo.core.LogTag;
import com.cajunsystems.gumbo.serialization.LogSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * Default {@link TypedLogView} implementation.
 *
 * <p>Delegates all I/O to a {@link LogView} and uses a
 * {@link LogSerializer} for transparent serialization/deserialization.
 * Deserialization errors in subscribers are logged and the entry is skipped
 * rather than crashing the subscription.
 */
final class DefaultTypedLogView<T> implements TypedLogView<T> {

    private static final Logger logger = LoggerFactory.getLogger(DefaultTypedLogView.class);

    private final LogView delegate;
    private final LogSerializer<T> serializer;

    DefaultTypedLogView(LogView delegate, LogSerializer<T> serializer) {
        this.delegate   = delegate;
        this.serializer = serializer;
    }

    @Override
    public LogTag getTag() {
        return delegate.getTag();
    }

    // -------------------------------------------------------------------------
    // Write
    // -------------------------------------------------------------------------

    @Override
    public CompletableFuture<AppendResult> append(T object) {
        return delegate.append(serializer.serialize(object));
    }

    // -------------------------------------------------------------------------
    // Read
    // -------------------------------------------------------------------------

    @Override
    public CompletableFuture<List<T>> readFrom(LogPosition from, int maxEntries) {
        return delegate.readFrom(from, maxEntries)
                .thenApply(entries -> entries.stream()
                        .map(e -> serializer.deserialize(e.data()))
                        .toList());
    }

    @Override
    public CompletableFuture<List<T>> readFromVersion(long fromVersion) {
        return delegate.readFromVersion(fromVersion)
                .thenApply(entries -> entries.stream()
                        .map(e -> serializer.deserialize(e.data()))
                        .toList());
    }

    @Override
    public long getLatestVersion() {
        return delegate.getLatestVersion();
    }

    // -------------------------------------------------------------------------
    // Subscribe
    // -------------------------------------------------------------------------

    @Override
    public SharedLog.Subscription subscribe(LogPosition from, Consumer<T> listener) {
        return delegate.subscribe(from, entry -> {
            try {
                listener.accept(serializer.deserialize(entry.data()));
            } catch (Exception ex) {
                logger.warn("Failed to deserialize entry seqnum={} tag={}; skipping",
                        entry.seqnum(), delegate.getTag(), ex);
            }
        });
    }

    @Override
    public SharedLog.Subscription subscribeTail(Consumer<T> listener) {
        return delegate.subscribeTail(entry -> {
            try {
                listener.accept(serializer.deserialize(entry.data()));
            } catch (Exception ex) {
                logger.warn("Failed to deserialize entry seqnum={} tag={}; skipping",
                        entry.seqnum(), delegate.getTag(), ex);
            }
        });
    }

    // ── Metadata ──

    @Override
    public long getLatestSeqnum() {
        return delegate.getLatestSeqnum();
    }

    // ── Key-Value ──

    @Override
    public CompletableFuture<Void> setValue(String key, byte[] value) {
        return delegate.setValue(key, value);
    }

    @Override
    public CompletableFuture<byte[]> getValue(String key) {
        return delegate.getValue(key);
    }

    @Override
    public CompletableFuture<Void> deleteValue(String key) {
        return delegate.deleteValue(key);
    }

    @Override
    public CompletableFuture<Boolean> compareAndSetValue(String key, byte[] expected, byte[] value) {
        return delegate.compareAndSetValue(key, expected, value);
    }

    @Override
    public CompletableFuture<Long> incrementValue(String key, long delta) {
        return delegate.incrementValue(key, delta);
    }

    // -------------------------------------------------------------------------
    // Interoperability
    // -------------------------------------------------------------------------

    @Override
    public LogView rawView() {
        return delegate;
    }
}
