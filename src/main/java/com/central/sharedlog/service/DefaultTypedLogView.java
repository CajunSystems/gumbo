package com.central.sharedlog.service;

import com.central.sharedlog.api.LogView;
import com.central.sharedlog.api.SharedLog;
import com.central.sharedlog.api.TypedLogView;
import com.central.sharedlog.core.AppendResult;
import com.central.sharedlog.core.LogPosition;
import com.central.sharedlog.core.LogTag;
import com.central.sharedlog.serialization.LogSerializer;
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

    // -------------------------------------------------------------------------
    // Interoperability
    // -------------------------------------------------------------------------

    @Override
    public LogView rawView() {
        return delegate;
    }
}
