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

    /**
     * Returns entries visible to {@code tag} with {@code localId >= fromVersion}, in
     * seqnum order — the tag's <em>own</em> stream position, not the global seqnum.
     *
     * <p>This is the read a per-stream cursor needs. {@link #readByTag} takes a global
     * {@code seqnum}, and the two number spaces coincide only when the log holds a
     * single tag: with two tags in one log, {@code readByTag(second, 3)} returns
     * everything the second tag has (its seqnums start above 3), not the entries after
     * its own third. A consumer holding a cursor into one stream — an executor resuming
     * from a checkpoint, a workflow replaying its history — wants this method instead.
     *
     * <p>{@code fromVersion} is inclusive, so {@code readFromVersion(tag, 0)} is
     * equivalent to {@code readByTag(tag, 0)}. Versions below the trim point are gone;
     * this returns what remains rather than failing.
     *
     * <h2>Multi-tag entries</h2>
     * <p>An entry carries <em>one</em> {@code localId}, assigned from its primary tag's
     * counter, so a version identifies a position in the primary tag's stream. For a tag
     * that an entry carries only as a <em>secondary</em> tag, that number belongs to a
     * different stream: it does not count that tag's entries and it need not start at
     * zero. Version-keyed reads are therefore well-defined for a tag whose entries were
     * all written with it as the primary tag — the normal case, and the only one for a
     * per-entity stream — and are not meaningful for a tag used purely as a secondary
     * fan-out tag, such as a shared work queue fed by atomic multi-tag appends.
     *
     * <p>Fixing that requires a version per tag per entry, which means storage-owned
     * per-tag versions rather than one field on the entry. Until then, seqnum-keyed
     * {@link #readByTag} remains the correct read for a fan-out tag.
     *
     * <p>The default implementation reads the tag's whole stream and filters, which is
     * correct but reads storage it discards. Adapters that maintain a per-tag index
     * should override it to resolve the range first and read only the result. All four
     * adapters shipped with Gumbo do.
     *
     * @param tag         the stream to read
     * @param fromVersion inclusive lower bound on {@code localId} within {@code tag}
     * @throws IOException if a read error occurs
     */
    default List<LogEntry> readFromVersion(LogTag tag, long fromVersion) throws IOException {
        if (fromVersion <= 0) return readByTag(tag, 0L);
        return readByTag(tag, 0L).stream()
                .filter(e -> e.localId() >= fromVersion)
                .toList();
    }

    /**
     * Returns entries visible to {@code tag} with {@code localId > afterVersion} — the
     * exclusive form of {@link #readFromVersion}, for a consumer holding the version of
     * the last entry it processed.
     *
     * @param tag          the stream to read
     * @param afterVersion exclusive lower bound on {@code localId} within {@code tag};
     *                     pass {@code -1} for the whole stream
     * @throws IOException if a read error occurs
     */
    default List<LogEntry> readAfterVersion(LogTag tag, long afterVersion) throws IOException {
        return readFromVersion(tag, afterVersion < 0 ? 0L : afterVersion + 1);
    }

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

    /**
     * Returns the highest {@code seqnum} of any entry visible to {@code tag},
     * or {@code -1} if the tag has no entries.
     *
     * <p>Implementations should satisfy this in O(1) or O(log n) using maintained
     * metadata rather than a full log scan. The default implementation falls back
     * to a full scan via {@link #readByTag} and should be overridden.
     */
    default long getLatestSeqnumForTag(LogTag tag) throws IOException {
        List<LogEntry> entries = readByTag(tag, 0L);
        return entries.isEmpty() ? -1L : entries.get(entries.size() - 1).seqnum();
    }

    // ── Key-Value ──

    /**
     * Durably stores {@code value} under {@code key} for {@code tag}.
     * Overwrites any existing value. Value of {@code null} is not permitted; use
     * {@link #deleteTagValue} to remove a key.
     */
    default void setTagValue(LogTag tag, String key, byte[] value) throws IOException {
        throw new UnsupportedOperationException("setTagValue not implemented by " + getClass().getSimpleName());
    }

    /**
     * Returns the stored value for {@code key} under {@code tag}, or {@code null}
     * if the key has never been set (or was deleted).
     */
    default byte[] getTagValue(LogTag tag, String key) throws IOException {
        throw new UnsupportedOperationException("getTagValue not implemented by " + getClass().getSimpleName());
    }

    /**
     * Removes the stored value for {@code key} under {@code tag}. No-op if the
     * key does not exist.
     */
    default void deleteTagValue(LogTag tag, String key) throws IOException {
        throw new UnsupportedOperationException("deleteTagValue not implemented by " + getClass().getSimpleName());
    }
}
