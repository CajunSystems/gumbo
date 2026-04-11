package com.central.sharedlog.core;

import java.util.Objects;

/**
 * Identifies a logical log stream within the shared log.
 *
 * <p>Inspired by Boki's log-tagging mechanism: a single physical log holds entries
 * for many logical streams, each distinguished by a tag. A tag has a mandatory
 * {@code namespace} (the entity type, e.g. {@code "orders"}) and an optional
 * {@code key} (an entity instance, e.g. {@code "order-42"}). This lets callers
 * subscribe to either the full namespace stream or a single-instance stream.
 *
 * <pre>{@code
 * LogTag allOrders   = LogTag.of("orders");
 * LogTag singleOrder = LogTag.of("orders", "order-42");
 * LogTag inventory   = LogTag.of("inventory");
 * }</pre>
 */
public record LogTag(String namespace, String key) {

    /** Tag whose key scope is the entire namespace (no instance key). */
    public static LogTag of(String namespace) {
        return new LogTag(namespace, "");
    }

    /** Tag scoped to a specific entity instance within a namespace. */
    public static LogTag of(String namespace, String key) {
        Objects.requireNonNull(namespace, "namespace");
        Objects.requireNonNull(key, "key");
        return new LogTag(namespace, key);
    }

    public LogTag {
        Objects.requireNonNull(namespace, "namespace");
        Objects.requireNonNull(key, "key");
        if (namespace.isBlank()) throw new IllegalArgumentException("namespace must not be blank");
    }

    /** Returns true if this tag covers the whole namespace rather than one instance. */
    public boolean isNamespaceWide() {
        return key.isEmpty();
    }

    /** Returns the namespace-wide tag for this tag's namespace. */
    public LogTag toNamespaceTag() {
        return isNamespaceWide() ? this : LogTag.of(namespace);
    }

    @Override
    public String toString() {
        return key.isEmpty() ? namespace : namespace + ":" + key;
    }
}
