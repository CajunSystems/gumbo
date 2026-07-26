package com.cajunsystems.gumbo.persistence;

import com.cajunsystems.gumbo.core.CounterValues;
import com.cajunsystems.gumbo.core.LogEntry;
import com.cajunsystems.gumbo.core.PendingAppend;
import com.cajunsystems.gumbo.core.VersionConflictException;
import com.cajunsystems.gumbo.core.LogTag;
import com.cajunsystems.gumbo.core.StreamVersions;

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
     * {@code streamVersion} assigned by the caller.
     *
     * @param entry the entry to store
     * @throws IOException if the write fails
     */
    void append(LogEntry entry) throws IOException;

    /** Passed as {@code expectedVersion} to append unconditionally. */
    long ANY_VERSION = -1L;

    /**
     * Persists an entry, assigning its primary tag's {@code streamVersion} <em>here</em>,
     * and returns the entry as stored.
     *
     * <h2>Why the adapter assigns it</h2>
     * <p>Because only storage can. A caller-side counter is seeded once from whatever the
     * tag was at and then diverges from every other writer's copy silently: two processes
     * on one log both hand out {@code 0, 1, 2}, and nothing ever reconciles them. Moving
     * the assignment into the same operation as the write is what makes the version
     * actually describe the stream rather than one process's opinion of it.
     *
     * <h2>Conditional append</h2>
     * <p>With {@code expectedVersion} other than {@link #ANY_VERSION}, the append happens
     * only if the tag is still at that version, and is otherwise rejected with
     * {@link VersionConflictException}. The comparison and the increment must be one
     * atomic operation in the same store — a client-side compare would race the
     * assignment underneath it.
     *
     * <p>The default implementation assigns from {@link #getNextStreamVersion} and writes,
     * which is exactly what callers did before and no weaker — but it <strong>rejects</strong>
     * a non-{@link #ANY_VERSION} {@code expectedVersion} with
     * {@link UnsupportedOperationException} rather than performing a non-atomic check. An
     * adapter that appeared to participate in the fencing protocol while providing none of
     * it would surface as corruption instead of as an error.
     *
     * @param pending         the append, minus the version
     * @param expectedVersion {@link #ANY_VERSION}, or the version the tag must still be at
     * @return the persisted entry, carrying the assigned version
     * @throws VersionConflictException if the tag is not at {@code expectedVersion}
     * @throws UnsupportedOperationException if conditional append is not supported
     * @throws IOException if the write fails
     */
    default LogEntry append(PendingAppend pending, long expectedVersion) throws IOException {
        if (expectedVersion != ANY_VERSION) {
            throw new UnsupportedOperationException(
                    getClass().getSimpleName() + " does not support conditional append;"
                    + " it cannot compare and increment the version atomically");
        }
        LogEntry entry = pending.withVersion(getNextStreamVersion(pending.primaryTag()));
        append(entry);
        return entry;
    }

    /**
     * Batch form of {@link #append(PendingAppend, long)}: assigns each entry's version and
     * persists them with one durability flush, returning them as stored.
     *
     * <p>Versions are assigned in list order, so several appends to one tag within a batch
     * take consecutive versions. Unconditional only — a batch conditioned per entry would
     * need to define what happens to the rest when one is rejected, and no caller needs
     * that yet.
     *
     * @param pendings appends in seqnum order; must not be empty
     * @return the persisted entries, in the same order
     * @throws IOException if the write fails
     */
    default List<LogEntry> appendBatchAssigningVersions(List<PendingAppend> pendings)
            throws IOException {
        List<LogEntry> entries = new java.util.ArrayList<>(pendings.size());
        java.util.Map<LogTag, Long> next = new java.util.HashMap<>();
        for (PendingAppend p : pendings) {
            long v = next.computeIfAbsent(p.primaryTag(), this::getNextStreamVersion);
            next.put(p.primaryTag(), v + 1);
            entries.add(p.withVersion(v));
        }
        appendBatch(entries);
        return entries;
    }

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
     * Returns entries visible to {@code tag} with {@code streamVersion >= fromVersion}, in
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
     * <p>An entry carries <em>one</em> {@code streamVersion}, assigned from its primary tag's
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
     * @param fromVersion inclusive lower bound on {@code streamVersion} within {@code tag}
     * @throws IOException if a read error occurs
     */
    default List<LogEntry> readFromVersion(LogTag tag, long fromVersion) throws IOException {
        if (fromVersion <= 0) return readByTag(tag, 0L);
        return readByTag(tag, 0L).stream()
                .filter(e -> e.streamVersion() >= fromVersion)
                .toList();
    }

    /**
     * Returns entries visible to {@code tag} with {@code streamVersion > afterVersion} — the
     * exclusive form of {@link #readFromVersion}, for a consumer holding the version of
     * the last entry it processed.
     *
     * @param tag          the stream to read
     * @param afterVersion exclusive lower bound on {@code streamVersion} within {@code tag};
     *                     pass {@code -1} for the whole stream
     * @throws IOException if a read error occurs
     */
    default List<LogEntry> readAfterVersion(LogTag tag, long afterVersion) throws IOException {
        return readFromVersion(tag, StreamVersions.afterToInclusive(afterVersion));
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
     * Returns the next {@code streamVersion} that would be assigned for {@code tag} —
     * one past the highest version persisted for it, and {@code 0} for an unused tag.
     *
     * <p>Deliberately has no default: an adapter that silently inherited one would hand
     * out versions that collide with those already on disk. Third-party adapters
     * implementing the old {@code getNextStreamVersion} will fail to compile, which is
     * the intended outcome — the rename is not cosmetic at this seam.
     */
    long getNextStreamVersion(LogTag tag);

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

    // ── Key-Value: conditional mutation ──

    /**
     * Sets {@code key} to {@code value} only if it currently holds {@code expected},
     * comparing by content. Returns whether the swap happened.
     *
     * <p>This is the KV's counterpart to conditional append, and it exists for the same
     * reason: it lets a claim be decided <em>by storage</em>, so correctness stops depending
     * on any coordination layer being right. A lock service can tell a node it holds a lock
     * but never that it <em>still</em> holds it at the instant it writes — a GC pause
     * between those two moments is enough for two nodes to both believe they own a stream.
     * With the comparison where the write lands, the loser is rejected instead.
     *
     * <p>It also makes lease expiry an efficiency problem rather than a correctness one.
     * A claimant compares an {@code expiresAt} it stored in the value, and clock skew can
     * let two nodes both decide a lease is free — but only one of them wins the swap, and
     * (with a conditional append behind it) only one of them can write.
     *
     * <h2>Absence and removal</h2>
     * <ul>
     *   <li>{@code expected == null} means <em>the key must be absent</em>. A stored value
     *       is never {@code null} — {@link #setTagValue} forbids it — so absence has an
     *       unambiguous representation and "claim if unclaimed" needs no separate
     *       protocol.</li>
     *   <li>{@code value == null} <em>removes</em> the key, making a conditional release
     *       the same operation as a conditional claim.</li>
     * </ul>
     *
     * <h2>Atomicity</h2>
     * <p>The comparison and the write must be one atomic operation in the same store.
     * A client-side read-then-write races the other writers it is trying to exclude, which
     * is the whole failure this method exists to remove.
     *
     * <p>There is deliberately <strong>no</strong> working default. An implementation that
     * compared non-atomically would make a log look like it was arbitrating claims while
     * arbitrating nothing, surfacing as two owners rather than as an error — the same
     * reasoning as {@link #append(PendingAppend, long)}. Adapters that can do this override
     * it; the three other conditional methods below are defined in terms of it, so
     * overriding this one supplies all four.
     *
     * @param tag      the tag whose KV namespace holds {@code key}
     * @param key      the key to swap
     * @param expected the value {@code key} must currently hold, or {@code null} to require
     *                 that it is absent
     * @param value    the value to store, or {@code null} to remove the key
     * @return {@code true} if {@code key} held {@code expected} and was written
     * @throws UnsupportedOperationException if this adapter cannot compare and write atomically
     * @throws IOException if the write fails
     */
    default boolean compareAndSetTagValue(LogTag tag, String key, byte[] expected, byte[] value)
            throws IOException {
        throw new UnsupportedOperationException(
                getClass().getSimpleName() + " does not support compareAndSetTagValue;"
                + " it cannot compare and write atomically");
    }

    /**
     * Sets {@code key} to {@code value} only if it is currently absent — the claim
     * operation: exactly one of N contending callers gets {@code true}.
     *
     * @return {@code true} if the key was absent and is now {@code value}
     * @throws IOException if the write fails
     */
    default boolean setTagValueIfAbsent(LogTag tag, String key, byte[] value) throws IOException {
        return compareAndSetTagValue(tag, key, null, value);
    }

    /**
     * Removes {@code key} only if it currently holds {@code expected} — the release
     * operation, which a holder uses to avoid releasing a claim that has since been taken
     * over by someone else.
     *
     * @return {@code true} if the key held {@code expected} and was removed
     * @throws IOException if the write fails
     */
    default boolean deleteTagValueIf(LogTag tag, String key, byte[] expected) throws IOException {
        return compareAndSetTagValue(tag, key, expected, null);
    }

    /**
     * Adds {@code delta} to the counter at {@code key} and returns the new value. An absent
     * key counts as {@code 0}, so a counter needs no initialisation.
     *
     * <p>The value is eight bytes, big-endian — see {@link CounterValues}, which a client
     * uses to decode the same key through {@link #getTagValue}.
     *
     * <p>The default retries a compare-and-set until it wins, which is correct on any
     * adapter that implements {@link #compareAndSetTagValue} and does not require it to
     * offer a native add. Adapters whose store can do the whole read-modify-write in one
     * round trip should override.
     *
     * @return the counter's value after adding {@code delta}
     * @throws IllegalStateException if {@code key} holds something other than a counter
     * @throws IOException if the write fails
     */
    default long incrementTagValue(LogTag tag, String key, long delta) throws IOException {
        while (true) {
            byte[] current = getTagValue(tag, key);
            long next = CounterValues.toLong(current) + delta;
            if (compareAndSetTagValue(tag, key, current, CounterValues.toBytes(next))) {
                return next;
            }
            // Lost the race; re-read and retry. Unbounded, as a CAS loop is — every
            // iteration means some other caller made progress.
        }
    }
}
