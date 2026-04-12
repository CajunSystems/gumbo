package com.cajunsystems.gumbo.persistence;

import com.cajunsystems.gumbo.core.LogEntry;
import com.cajunsystems.gumbo.core.LogTag;

import java.io.IOException;
import java.util.List;
import java.util.Collections;

/**
 * Pluggable storage back-end for the shared log.
 *
 * <p>The adapter owns storage and is responsible for durably persisting entries
 * in {@code seqnum} order. All methods are called only after the sequencer has
 * assigned a {@code seqnum}, so the adapter never needs to generate IDs.
 *
 * <h2>Implementation notes</h2>
 * <ul>
 *   <li>The adapter is always accessed from {@link com.cajunsystems.gumbo.service.SharedLogService}
 *       under a write lock, so implementations do <em>not</em> need to be
 *       internally thread-safe for writes.  Reads may happen concurrently and
 *       implementations should be prepared for that.</li>
 *   <li>Implementations that perform blocking I/O are fine; the service layer
 *       wraps calls appropriately.</li>
 * </ul>
 *
 * <h2>Lifecycle</h2>
 * <p>Call {@link #open()} before any other method. Call {@link #close()} when
 * done (idempotent).  Re-opening after close is not required to be supported.
 */
public interface PersistenceAdapter extends AutoCloseable {

    // -------------------------------------------------------------------------
    // Lifecycle
    // -------------------------------------------------------------------------

    /**
     * Opens (or creates) the underlying storage.  Must be called before any
     * read/write operations.  Implementations recover existing state here
     * (e.g. replaying a WAL or loading an index file).
     *
     * @throws IOException if storage cannot be opened
     */
    void open() throws IOException;

    /**
     * Flushes any buffered writes and releases resources.  Idempotent.
     *
     * @throws IOException if a flush error occurs
     */
    @Override
    void close() throws IOException;

    // -------------------------------------------------------------------------
    // Write
    // -------------------------------------------------------------------------

    /**
     * Persists a single entry.  The entry already has its {@code seqnum} and
     * {@code localId} assigned by the caller.
     *
     * @param entry the entry to store
     * @throws IOException if the write fails
     */
    void append(LogEntry entry) throws IOException;

    /**
     * Persists multiple entries as a single batch with <em>one</em> durability
     * flush at the end, rather than one flush per entry.
     *
     * <p>The default implementation calls {@link #append} for each entry
     * (same semantics, no batching benefit).  Storage back-ends that support
     * group-commit should override this to write all entries and call
     * {@code fdatasync} / {@code msync} exactly once.
     *
     * @param entries entries to store, in seqnum order; must not be empty
     * @throws IOException if the write fails
     */
    default void appendBatch(List<LogEntry> entries) throws IOException {
        for (LogEntry entry : entries) {
            append(entry);
        }
    }

    // -------------------------------------------------------------------------
    // Read
    // -------------------------------------------------------------------------

    /**
     * Returns all entries in seqnum order.
     *
     * @throws IOException if a read error occurs
     */
    List<LogEntry> readAll() throws IOException;

    /**
     * Returns all entries with {@code seqnum >= fromSeqnum}, in seqnum order.
     *
     * @throws IOException if a read error occurs
     */
    List<LogEntry> readFrom(long fromSeqnum) throws IOException;

    /**
     * Returns entries visible to {@code tag} (i.e. entries whose tag set
     * contains {@code tag}) with {@code seqnum >= fromSeqnum}, in seqnum order.
     *
     * <p>Implementations are encouraged to maintain a per-tag index for
     * O(log n) lookups rather than a full scan.
     *
     * @throws IOException if a read error occurs
     */
    List<LogEntry> readByTag(LogTag tag, long fromSeqnum) throws IOException;

    // -------------------------------------------------------------------------
    // Housekeeping
    // -------------------------------------------------------------------------

    /**
     * Signals that entries with {@code seqnum < upToSeqnum} may be discarded.
     * Implementations may delay or batch this operation.
     *
     * @throws IOException if the trim operation fails
     */
    void trim(long upToSeqnum) throws IOException;

    // -------------------------------------------------------------------------
    // Metadata
    // -------------------------------------------------------------------------

    /**
     * Returns the highest persisted {@code seqnum}, or {@code -1} if no entries
     * have been stored yet.
     */
    long getLatestSeqnum();

    /**
     * Returns the number of entries visible to {@code tag} that have been
     * persisted (i.e. the next {@code localId} that would be assigned for
     * that tag).
     */
    long getLocalIdCountForTag(LogTag tag);
}
