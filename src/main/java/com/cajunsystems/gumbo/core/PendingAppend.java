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

    /**
     * The entry this becomes once storage has assigned one position per tag.
     *
     * <p>Every tag needs its own: an entry appended to a per-instance history tag and a
     * shared work queue occupies a different position in each, and giving both the same
     * number is what made a version-keyed cursor over the queue unusable.
     *
     * @param streamVersions one position per tag in {@link #tags()}
     */
    public LogEntry withVersions(java.util.Map<LogTag, Long> streamVersions) {
        return new LogEntry(seqnum, streamVersions, tags, data, timestamp);
    }

    /**
     * The entry this becomes when only one position is known, applied to every tag.
     *
     * <p>Correct for a single-tag append, which is what almost every caller does. For a
     * multi-tag append it reproduces the pre-per-tag behaviour — one number, no idea which
     * tag it counts — so prefer {@link #withVersions}.
     */
    public LogEntry withVersion(long streamVersion) {
        return new LogEntry(seqnum, streamVersion, tags, data, timestamp);
    }

    /** The raw payload without copying; callers must not mutate it. */
    public byte[] dataUnsafe() {
        return data;
    }
}
