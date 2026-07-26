package com.cajunsystems.gumbo.core;

import java.time.Instant;

/**
 * The result of a successful {@link com.cajunsystems.gumbo.api.SharedLog#append} call.
 *
 * <p>Contains the globally assigned {@code seqnum}, the {@code streamVersion} (this
 * entry's position within its primary tag's stream), the primary tag, and the
 * server-side timestamp.
 *
 * <p>Which of the two positions to keep depends on what you will do with it. Use
 * {@code seqnum} to refer to this entry in the log as a whole; use {@code streamVersion}
 * as a cursor into the tag's own stream, since it does not shift as other tags write to
 * the same log. Version-keyed reads
 * ({@link com.cajunsystems.gumbo.api.LogView#readAfterVersion}) take the latter.
 */
public record AppendResult(
        long seqnum,
        long streamVersion,
        LogTag primaryTag,
        Instant timestamp) {

    /**
     * @deprecated renamed to {@link #streamVersion()}. {@code localId} was Boki's name
     *     for a per-<em>engine</em> write-path id, not the per-<em>tag</em> cursor this
     *     field has always held. Scheduled for removal.
     */
    @Deprecated(forRemoval = true)
    public long localId() { return streamVersion; }

    /** Returns a {@link LogPosition} pointing at the newly appended entry. */
    public LogPosition position() {
        return new LogPosition(seqnum);
    }

    /** Returns a {@link LogPosition} pointing just past the appended entry. */
    public LogPosition nextPosition() {
        return position().next();
    }
}
