package com.cajunsystems.gumbo.core;

import java.util.Arrays;
import java.util.Objects;
import java.util.Set;

/**
 * A request to append a new entry to the shared log.
 *
 * <p>An entry may be associated with multiple tags simultaneously, making it
 * visible from several {@link com.cajunsystems.gumbo.api.LogView} instances.
 * For example, an order event might carry both {@code "orders"} (namespace-wide)
 * and {@code "orders:order-42"} (instance-scoped) tags so that both an
 * aggregate listener and a single-order executor can see it.
 *
 * <pre>{@code
 * AppendRequest req = AppendRequest.to(
 *     Set.of(LogTag.of("orders"), LogTag.of("orders", "order-42")),
 *     payload);
 * }</pre>
 */
public final class AppendRequest {

    private final Set<LogTag> tags;
    private final byte[] data;

    private AppendRequest(Set<LogTag> tags, byte[] data) {
        Objects.requireNonNull(tags, "tags");
        if (tags.isEmpty()) throw new IllegalArgumentException("tags must not be empty");
        Objects.requireNonNull(data, "data");
        this.tags = Set.copyOf(tags);
        this.data = Arrays.copyOf(data, data.length);
    }

    /** Append with a single tag. */
    public static AppendRequest to(LogTag tag, byte[] data) {
        Objects.requireNonNull(tag, "tag");
        return new AppendRequest(Set.of(tag), data);
    }

    /** Append visible from multiple tag views. */
    public static AppendRequest to(Set<LogTag> tags, byte[] data) {
        return new AppendRequest(tags, data);
    }

    public Set<LogTag> tags() { return tags; }

    /** Returns a defensive copy of the payload. */
    public byte[] data() { return Arrays.copyOf(data, data.length); }

    /** Raw payload without defensive copy; callers must not mutate. */
    public byte[] dataUnsafe() { return data; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof AppendRequest r)) return false;
        return tags.equals(r.tags) && Arrays.equals(data, r.data);
    }

    @Override
    public int hashCode() {
        return 31 * tags.hashCode() + Arrays.hashCode(data);
    }

    @Override
    public String toString() {
        return "AppendRequest{tags=" + tags + ", dataLen=" + data.length + '}';
    }
}
