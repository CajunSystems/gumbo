package com.central.sharedlog.core;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;

/**
 * An immutable entry in the shared log.
 *
 * <p>Each entry carries:
 * <ul>
 *   <li>{@code seqnum}  – global, monotonically increasing sequence number assigned by
 *       the sequencer; provides total ordering across the entire log.</li>
 *   <li>{@code localId} – position of this entry within the primary tag's stream;
 *       useful as a per-entity cursor.</li>
 *   <li>{@code tags}    – the set of logical streams this entry belongs to. An entry
 *       may be visible from multiple tag views (e.g. both {@code "orders"} and
 *       {@code "orders:order-42"}).</li>
 *   <li>{@code data}    – opaque payload bytes; interpretation is left to the caller.</li>
 *   <li>{@code timestamp} – wall-clock time at which the entry was appended.</li>
 * </ul>
 *
 * <p>Because {@code data} is a mutable byte array, this class implements defensive
 * copying in its constructor and custom {@code equals}/{@code hashCode}.
 */
public final class LogEntry {

    private final long seqnum;
    private final long localId;
    private final Set<LogTag> tags;
    private final byte[] data;
    private final Instant timestamp;

    public LogEntry(long seqnum, long localId, Set<LogTag> tags, byte[] data, Instant timestamp) {
        if (seqnum < 0) throw new IllegalArgumentException("seqnum must be >= 0");
        if (localId < 0) throw new IllegalArgumentException("localId must be >= 0");
        Objects.requireNonNull(tags, "tags");
        if (tags.isEmpty()) throw new IllegalArgumentException("tags must not be empty");
        Objects.requireNonNull(data, "data");
        Objects.requireNonNull(timestamp, "timestamp");

        this.seqnum = seqnum;
        this.localId = localId;
        this.tags = Collections.unmodifiableSet(Set.copyOf(tags));
        this.data = Arrays.copyOf(data, data.length);
        this.timestamp = timestamp;
    }

    public long seqnum() { return seqnum; }
    public long localId() { return localId; }
    public Set<LogTag> tags() { return tags; }

    /** Returns a defensive copy of the payload. */
    public byte[] data() { return Arrays.copyOf(data, data.length); }

    /** Returns the raw payload without copying; callers must not mutate the returned array. */
    public byte[] dataUnsafe() { return data; }

    public Instant timestamp() { return timestamp; }

    /** Returns the primary tag (first tag in a deterministic iteration order). */
    public LogTag primaryTag() {
        return tags.iterator().next();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof LogEntry e)) return false;
        return seqnum == e.seqnum
                && localId == e.localId
                && tags.equals(e.tags)
                && Arrays.equals(data, e.data)
                && timestamp.equals(e.timestamp);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(seqnum, localId, tags, timestamp);
        result = 31 * result + Arrays.hashCode(data);
        return result;
    }

    @Override
    public String toString() {
        return "LogEntry{seqnum=" + seqnum
                + ", localId=" + localId
                + ", tags=" + tags
                + ", dataLen=" + data.length
                + ", timestamp=" + timestamp + '}';
    }
}
