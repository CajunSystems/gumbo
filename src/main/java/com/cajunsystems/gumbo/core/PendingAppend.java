package com.cajunsystems.gumbo.core;

import java.time.Instant;
import java.util.Objects;
import java.util.Set;

/**
 * An append whose {@code streamVersion} has not been assigned yet.
 *
 * <p>{@link LogEntry} cannot express this: it requires a version at construction, which
 * is precisely the thing only storage may decide. This is the shape a write takes on its
 * way <em>into</em> an adapter, where {@code LogEntry} is the shape it has coming out.
 *
 * <p>The {@code seqnum} is already assigned — that comes from the sequencer, which is a
 * separate authority from the per-tag version.
 */
public record PendingAppend(
        long seqnum,
        LogTag primaryTag,
        Set<LogTag> tags,
        byte[] data,
        Instant timestamp) {

    public PendingAppend {
        if (seqnum < 0) throw new IllegalArgumentException("seqnum must be >= 0");
        Objects.requireNonNull(primaryTag, "primaryTag");
        Objects.requireNonNull(tags, "tags");
        if (tags.isEmpty()) throw new IllegalArgumentException("tags must not be empty");
        if (!tags.contains(primaryTag)) {
            throw new IllegalArgumentException("primaryTag " + primaryTag + " is not among tags " + tags);
        }
        Objects.requireNonNull(data, "data");
        Objects.requireNonNull(timestamp, "timestamp");
        tags = Set.copyOf(tags);
    }

    /** The entry this becomes once storage has assigned {@code streamVersion}. */
    public LogEntry withVersion(long streamVersion) {
        return new LogEntry(seqnum, streamVersion, tags, data, timestamp);
    }

    /** The raw payload without copying; callers must not mutate it. */
    public byte[] dataUnsafe() {
        return data;
    }
}
