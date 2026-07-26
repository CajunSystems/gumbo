package com.cajunsystems.gumbo.core;

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
 *   <li>{@code streamVersion} – position of this entry within its primary tag's stream,
 *       numbered densely from zero; the cursor a per-entity consumer holds and the
 *       quantity version-keyed reads are keyed on.</li>
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
    private final long streamVersion;
    private final Set<LogTag> tags;
    private final byte[] data;
    private final Instant timestamp;

    public LogEntry(long seqnum, long streamVersion, Set<LogTag> tags, byte[] data, Instant timestamp) {
        if (seqnum < 0) throw new IllegalArgumentException("seqnum must be >= 0");
        if (streamVersion < 0) throw new IllegalArgumentException("streamVersion must be >= 0");
        Objects.requireNonNull(tags, "tags");
        if (tags.isEmpty()) throw new IllegalArgumentException("tags must not be empty");
        Objects.requireNonNull(data, "data");
        Objects.requireNonNull(timestamp, "timestamp");

        this.seqnum = seqnum;
        this.streamVersion = streamVersion;
        this.tags = Collections.unmodifiableSet(Set.copyOf(tags));
        this.data = Arrays.copyOf(data, data.length);
        this.timestamp = timestamp;
    }

    public long seqnum() { return seqnum; }

    /**
     * This entry's position within its primary tag's stream, counted from zero.
     *
     * <p>Independent of how many other tags share the physical log, which is what makes
     * it usable as a durable cursor where {@link #seqnum()} is not.
     */
    public long streamVersion() { return streamVersion; }

    /**
     * @deprecated renamed to {@link #streamVersion()}. The name was a fossil of Boki's
     *     per-<em>engine</em> {@code localid}, which is a write-path detail superseded
     *     once the sequencer assigns a seqnum — a different quantity from the permanent
     *     per-<em>tag</em> cursor this actually is. Scheduled for removal.
     */
    @Deprecated(forRemoval = true)
    public long localId() { return streamVersion; }

    public Set<LogTag> tags() { return tags; }

    /** Returns a defensive copy of the payload. */
    public byte[] data() { return Arrays.copyOf(data, data.length); }

    /** Returns the raw payload without copying; callers must not mutate the returned array. */
    public byte[] dataUnsafe() { return data; }

    public Instant timestamp() { return timestamp; }

    /**
     * Returns the primary tag — the tag whose stream {@link #streamVersion()} counts.
     *
     * <p><strong>Not stable across JVM runs</strong> for a multi-tag entry: {@code tags}
     * is an immutable {@code Set} whose iteration order Java salts per run, so an entry
     * carrying two tags may report either as primary depending on the process that reads
     * it. Single-tag entries — the normal case — are unaffected.
     */
    public LogTag primaryTag() {
        return tags.iterator().next();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof LogEntry e)) return false;
        return seqnum == e.seqnum
                && streamVersion == e.streamVersion
                && tags.equals(e.tags)
                && Arrays.equals(data, e.data)
                && timestamp.equals(e.timestamp);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(seqnum, streamVersion, tags, timestamp);
        result = 31 * result + Arrays.hashCode(data);
        return result;
    }

    @Override
    public String toString() {
        return "LogEntry{seqnum=" + seqnum
                + ", streamVersion=" + streamVersion
                + ", tags=" + tags
                + ", dataLen=" + data.length
                + ", timestamp=" + timestamp + '}';
    }
}
