package com.cajunsystems.gumbo.persistence;

import java.io.IOException;
import java.nio.file.Path;

/**
 * Thrown by {@link FileBasedPersistenceAdapter#open()} when the data directory is
 * already held by another adapter — in this JVM or in another process.
 *
 * <p>The file adapter is <strong>single-writer</strong>. Two writers on one directory
 * assign the same per-tag local ids and overwrite each other's {@code index.dat}, so the
 * log ends up holding entries that no reader can see and ids that address two different
 * entries. Neither failure is detectable after the fact, which is why the adapter refuses
 * the second open instead of allowing it.
 */
public class LogAlreadyOpenException extends IOException {

    private final transient Path dataDir;

    public LogAlreadyOpenException(Path dataDir, Throwable cause) {
        super("Log directory " + dataDir + " is already open by another adapter"
                + " (this JVM or another process). Gumbo's file adapter is single-writer:"
                + " a second writer would duplicate local ids and clobber the index."
                + " Close the other adapter first, or use a different directory.", cause);
        this.dataDir = dataDir;
    }

    /** The directory that could not be locked. */
    public Path dataDir() {
        return dataDir;
    }
}
