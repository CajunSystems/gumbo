package com.cajunsystems.gumbo.core;

import java.io.IOException;

/**
 * Thrown when a conditional append is rejected because the tag was not at the version the
 * writer expected.
 *
 * <p>This is the fence. A lock service can tell a node it <em>holds</em> a lock, but never
 * that it still holds it at the instant the write lands — a GC pause between those two
 * moments is enough for two nodes to both believe they own a stream. Conditioning the
 * write on the version the writer last saw moves that check to where the write actually
 * happens, so a stale writer is rejected by storage rather than by a coordination layer
 * that may itself be out of date.
 *
 * <p>Receiving this means someone else wrote to the stream first. The usual response is to
 * re-read from {@link #actualVersion()}, rebuild whatever was derived from the stream, and
 * retry — not to retry the same append blindly.
 */
public class VersionConflictException extends IOException {

    private final transient LogTag tag;
    private final long expectedVersion;
    private final long actualVersion;

    public VersionConflictException(LogTag tag, long expectedVersion, long actualVersion) {
        super("Conditional append to " + tag + " expected version " + expectedVersion
                + " but the tag is at " + actualVersion
                + (actualVersion > expectedVersion
                        ? " — another writer appended " + (actualVersion - expectedVersion)
                          + " entr" + (actualVersion - expectedVersion == 1 ? "y" : "ies") + " first"
                        : ""));
        this.tag = tag;
        this.expectedVersion = expectedVersion;
        this.actualVersion = actualVersion;
    }

    /** The stream the append was rejected on. */
    public LogTag tag() {
        return tag;
    }

    /** The version the writer believed the tag was at. */
    public long expectedVersion() {
        return expectedVersion;
    }

    /** The version the tag is actually at — where a retry should read from. */
    public long actualVersion() {
        return actualVersion;
    }
}
