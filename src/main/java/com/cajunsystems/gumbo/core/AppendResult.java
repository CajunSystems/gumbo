package com.cajunsystems.gumbo.core;

import java.time.Instant;

/**
 * The result of a successful {@link com.cajunsystems.gumbo.api.SharedLog#append} call.
 *
 * <p>Contains the globally assigned {@code seqnum}, the per-tag {@code localId}
 * (position within the primary tag's stream), the primary tag, and the server-side
 * timestamp.  Callers can use {@code seqnum} as a durable reference to the entry
 * (e.g. to pass to a read-from operation later).
 */
public record AppendResult(
        long seqnum,
        long localId,
        LogTag primaryTag,
        Instant timestamp) {

    /** Returns a {@link LogPosition} pointing at the newly appended entry. */
    public LogPosition position() {
        return new LogPosition(seqnum);
    }

    /** Returns a {@link LogPosition} pointing just past the appended entry. */
    public LogPosition nextPosition() {
        return position().next();
    }
}
