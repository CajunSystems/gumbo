package com.cajunsystems.gumbo.persistence;

import com.cajunsystems.gumbo.core.LogEntry;
import com.cajunsystems.gumbo.core.PendingAppend;
import com.cajunsystems.gumbo.core.VersionConflictException;
import com.cajunsystems.gumbo.core.LogTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A {@link PersistenceAdapter} decorator that accumulates writes in an
 * in-memory buffer and flushes to the delegate as a single
 * {@link PersistenceAdapter#appendBatch batch}, reducing the number of
 * {@code fdatasync} calls.
 *
 * <h2>How it works</h2>
 * <p>Each {@link #append} call adds the entry to a pending list.  The list
 * is flushed (via {@link PersistenceAdapter#appendBatch}) to the wrapped
 * adapter when either condition is met:
 * <ol>
 *   <li>The pending list reaches {@code maxBatchSize} entries.</li>
 *   <li>A background virtual thread fires after {@code maxDelayMs} milliseconds.</li>
 * </ol>
 * With {@link FileBasedPersistenceAdapter} as the delegate, N entries in one
 * batch cost <strong>2 {@code fdatasync} calls</strong> instead of
 * <strong>2N</strong>.
 *
 * <h2>Durability tradeoff</h2>
 * <p>Entries appended but not yet flushed live only in memory.  A JVM crash
 * between {@link #append} and the next flush will lose those entries.  Choose
 * {@code maxBatchSize} and {@code maxDelayMs} to balance throughput vs.
 * durability window.
 *
 * <h2>Read consistency</h2>
 * <p>Reads ({@link #readAll}, {@link #readFrom}, {@link #readByTag}) merge the
 * delegate's view with the in-memory pending buffer, so callers always see
 * every appended entry regardless of flush state.
 *
 * <h2>Usage</h2>
 * <pre>{@code
 * PersistenceAdapter base = new FileBasedPersistenceAdapter("/var/data");
 * PersistenceAdapter batching = BatchingPersistenceAdapter.of(base);   // defaults
 * // or with explicit tuning:
 * PersistenceAdapter batching = new BatchingPersistenceAdapter(base, 128, 5);
 *
 * SharedLogConfig config = SharedLogConfig.builder()
 *     .persistenceAdapter(batching)
 *     .build();
 * }</pre>
 */
public class BatchingPersistenceAdapter implements PersistenceAdapter {

    private static final Logger logger = LoggerFactory.getLogger(BatchingPersistenceAdapter.class);

    /** Default batch size: flush after this many pending entries. */
    public static final int DEFAULT_MAX_BATCH_SIZE = 64;

    /** Default maximum delay (ms) before a background flush fires. */
    public static final long DEFAULT_MAX_DELAY_MS = 10;

    private final PersistenceAdapter delegate;
    private final int maxBatchSize;
    private final long maxDelayMs;

    /** Entries appended but not yet flushed to the delegate. */
    private final List<LogEntry> pendingBatch;

    /**
     * Guards {@code pendingBatch} for both write-path additions and flush
     * operations from the background thread.
     */
    private final ReentrantLock flushLock = new ReentrantLock();

    private volatile boolean closed = false;
    private Thread flusherThread;

    // -------------------------------------------------------------------------
    // Factory
    // -------------------------------------------------------------------------

    /**
     * Creates a batching adapter with {@link #DEFAULT_MAX_BATCH_SIZE} and
     * {@link #DEFAULT_MAX_DELAY_MS}.
     */
    public static BatchingPersistenceAdapter of(PersistenceAdapter delegate) {
        return new BatchingPersistenceAdapter(delegate, DEFAULT_MAX_BATCH_SIZE, DEFAULT_MAX_DELAY_MS);
    }

    /**
     * @param delegate     the adapter to flush batches into
     * @param maxBatchSize flush immediately when pending entries reach this count
     * @param maxDelayMs   background flush interval in milliseconds
     */
    public BatchingPersistenceAdapter(PersistenceAdapter delegate, int maxBatchSize, long maxDelayMs) {
        this.delegate     = delegate;
        this.maxBatchSize = maxBatchSize;
        this.maxDelayMs   = maxDelayMs;
        this.pendingBatch = new ArrayList<>(maxBatchSize);
    }

    // -------------------------------------------------------------------------
    // Lifecycle
    // -------------------------------------------------------------------------

    @Override
    public void open() throws IOException {
        delegate.open();
        closed = false;
        flusherThread = Thread.ofVirtual()
                .name("batching-flusher")
                .start(() -> {
                    while (!closed) {
                        try {
                            Thread.sleep(maxDelayMs);
                            if (!closed) flushQuietly();
                        } catch (InterruptedException e) {
                            break;
                        }
                    }
                });
    }

    @Override
    public void close() throws IOException {
        closed = true;
        if (flusherThread != null) {
            flusherThread.interrupt();
        }
        flushNow(); // drain any remaining pending entries
        delegate.close();
    }

    // -------------------------------------------------------------------------
    // Write
    // -------------------------------------------------------------------------

    @Override
    public void append(LogEntry entry) throws IOException {
        flushLock.lock();
        try {
            pendingBatch.add(entry);
            if (pendingBatch.size() >= maxBatchSize) {
                flushUnderLock();
            }
        } finally {
            flushLock.unlock();
        }
    }

    /**
     * Assigns the version at append time rather than at flush time, because the caller
     * needs it in the {@code AppendResult} now — the batching here defers durability, not
     * identity.
     *
     * <p>The tag's next version is the delegate's, advanced past anything still pending
     * for that tag. Held under the flush lock so a concurrent flush cannot land between
     * counting the pending entries and adding this one.
     */
    @Override
    public LogEntry append(PendingAppend pending, long expectedVersion) throws IOException {
        flushLock.lock();
        try {
            LogEntry entry = pending.withVersion(claimVersion(pending.primaryTag(), expectedVersion));
            pendingBatch.add(entry);
            if (pendingBatch.size() >= maxBatchSize) flushUnderLock();
            return entry;
        } finally {
            flushLock.unlock();
        }
    }

    @Override
    public List<LogEntry> appendBatchAssigningVersions(List<PendingAppend> pendings)
            throws IOException {
        flushLock.lock();
        try {
            List<LogEntry> entries = new ArrayList<>(pendings.size());
            for (PendingAppend p : pendings) {
                LogEntry e = p.withVersion(claimVersion(p.primaryTag(), ANY_VERSION));
                entries.add(e);
                pendingBatch.add(e);
            }
            if (pendingBatch.size() >= maxBatchSize) flushUnderLock();
            return entries;
        } finally {
            flushLock.unlock();
        }
    }

    /** Next version for {@code tag}: the delegate's, plus anything pending for it. */
    private long claimVersion(LogTag tag, long expectedVersion) throws VersionConflictException {
        long next = getNextStreamVersionUnderLock(tag);
        if (expectedVersion != ANY_VERSION && expectedVersion != next) {
            throw new VersionConflictException(tag, expectedVersion, next);
        }
        return next;
    }

    private long getNextStreamVersionUnderLock(LogTag tag) {
        long next = delegate.getNextStreamVersion(tag);
        for (LogEntry e : pendingBatch) {
            if (e.tags().contains(tag)) next = Math.max(next, e.streamVersion() + 1);
        }
        return next;
    }

    /**
     * Forces all pending entries to the delegate immediately, regardless of
     * batch size or delay.  Useful for testing and controlled shutdown.
     */
    public void flushNow() throws IOException {
        flushLock.lock();
        try {
            flushUnderLock();
        } finally {
            flushLock.unlock();
        }
    }

    // -------------------------------------------------------------------------
    // Read  (merge delegate state with in-memory pending buffer)
    // -------------------------------------------------------------------------

    @Override
    public List<LogEntry> readAll() throws IOException {
        List<LogEntry> snapshot = pendingSnapshot();
        List<LogEntry> fromDelegate = delegate.readAll();
        return merge(fromDelegate, snapshot, e -> true);
    }

    @Override
    public List<LogEntry> readFrom(long fromSeqnum) throws IOException {
        List<LogEntry> snapshot = pendingSnapshot();
        List<LogEntry> fromDelegate = delegate.readFrom(fromSeqnum);
        return merge(fromDelegate, snapshot, e -> e.seqnum() >= fromSeqnum);
    }

    @Override
    public List<LogEntry> readByTag(LogTag tag, long fromSeqnum) throws IOException {
        List<LogEntry> snapshot = pendingSnapshot();
        List<LogEntry> fromDelegate = delegate.readByTag(tag, fromSeqnum);
        return merge(fromDelegate, snapshot,
                e -> e.seqnum() >= fromSeqnum && e.tags().contains(tag));
    }

    /**
     * Same merge as {@link #readByTag}, keyed on the tag's own version instead of the
     * global seqnum. Delegating rather than inheriting the interface default matters
     * here: the default filters {@link #readByTag}, which would read the delegate's
     * whole stream from storage and then discard most of it.
     */
    @Override
    public List<LogEntry> readFromVersion(LogTag tag, long fromVersion) throws IOException {
        List<LogEntry> snapshot = pendingSnapshot();
        List<LogEntry> fromDelegate = delegate.readFromVersion(tag, fromVersion);
        return merge(fromDelegate, snapshot,
                e -> e.tags().contains(tag) && e.streamVersion() >= fromVersion);
    }

    // -------------------------------------------------------------------------
    // Housekeeping
    // -------------------------------------------------------------------------

    @Override
    public void trim(long upToSeqnum) throws IOException {
        flushNow(); // ensure trimmed entries are persisted before removal
        delegate.trim(upToSeqnum);
    }

    // -------------------------------------------------------------------------
    // Metadata
    // -------------------------------------------------------------------------

    @Override
    public long getLatestSeqnum() {
        List<LogEntry> snapshot = pendingSnapshot();
        long delegateLatest = delegate.getLatestSeqnum();
        if (snapshot.isEmpty()) return delegateLatest;
        // Pending entries always have higher seqnums than flushed ones
        long pendingLatest = snapshot.get(snapshot.size() - 1).seqnum();
        return Math.max(delegateLatest, pendingLatest);
    }

    @Override
    public long getNextStreamVersion(LogTag tag) {
        // The pending buffer has to be counted too. SharedLogService only asks on first
        // use of a tag, when nothing is pending — but a caller reading a tag's version
        // tip (LogView.getLatestVersion) asks at any time, and would otherwise see a
        // count that stops at the last flush.
        long count = delegate.getNextStreamVersion(tag);
        for (LogEntry e : pendingSnapshot()) {
            if (e.tags().contains(tag)) count = Math.max(count, e.streamVersion() + 1);
        }
        return count;
    }

    @Override
    public long getLatestSeqnumForTag(LogTag tag) throws IOException {
        long delegateLatest = delegate.getLatestSeqnumForTag(tag);
        // Check pending buffer for any higher seqnum for this tag
        List<LogEntry> snapshot = pendingSnapshot();
        long pendingLatest = -1L;
        for (LogEntry e : snapshot) {
            if (e.tags().contains(tag) && e.seqnum() > pendingLatest) {
                pendingLatest = e.seqnum();
            }
        }
        return Math.max(delegateLatest, pendingLatest);
    }

    /*
     * The KV is write-through: it is not batched, and never was. Worth stating now that the
     * conditional forms exist, because it decides an ordering a caller can otherwise get
     * wrong. A claim taken here is durable as soon as the delegate says so, while log
     * entries appended after it may still be pending — so "claim, then append" leaves a
     * window where the claim survives a crash and the work it authorised does not. That is
     * the safe direction (the claim can be released or retried; a lost claim with committed
     * work cannot be reconciled), but it is only safe if the caller knows which way it goes.
     *
     * Batching them would not help either: a deferred compare-and-set is a claim whose
     * outcome is unknown at the moment the caller has to act on it.
     */

    @Override
    public void setTagValue(LogTag tag, String key, byte[] value) throws IOException {
        delegate.setTagValue(tag, key, value);
    }

    @Override
    public byte[] getTagValue(LogTag tag, String key) throws IOException {
        return delegate.getTagValue(tag, key);
    }

    @Override
    public void deleteTagValue(LogTag tag, String key) throws IOException {
        delegate.deleteTagValue(tag, key);
    }

    @Override
    public boolean compareAndSetTagValue(LogTag tag, String key, byte[] expected, byte[] value)
            throws IOException {
        return delegate.compareAndSetTagValue(tag, key, expected, value);
    }

    @Override
    public boolean setTagValueIfAbsent(LogTag tag, String key, byte[] value) throws IOException {
        return delegate.setTagValueIfAbsent(tag, key, value);
    }

    @Override
    public boolean deleteTagValueIf(LogTag tag, String key, byte[] expected) throws IOException {
        return delegate.deleteTagValueIf(tag, key, expected);
    }

    /**
     * Forwarded whole rather than looped here: the delegate may satisfy it in one round trip
     * (FoundationDB does), and a retry loop in the decorator would replace that with a
     * sequence of separate transactions.
     */
    @Override
    public long incrementTagValue(LogTag tag, String key, long delta) throws IOException {
        return delegate.incrementTagValue(tag, key, delta);
    }

    // -------------------------------------------------------------------------
    // Internal helpers
    // -------------------------------------------------------------------------

    /** Flushes the pending batch to the delegate; caller must hold flushLock. */
    /**
     * Writes the pending entries to the delegate, and drops them only once that has
     * succeeded.
     *
     * <p>Clearing first loses them outright if the delegate throws: the background flush
     * logs the failure after the data is already gone, and a caller that received an
     * {@code AppendResult} — with a version assigned and consumed — has no way to learn
     * its entry was discarded. Leaving them pending means the next flush retries.
     *
     * <p>Always called with {@link #flushLock} held, so nothing is appended between the
     * write and the removal.
     */
    private void flushUnderLock() throws IOException {
        if (pendingBatch.isEmpty()) return;
        List<LogEntry> batch = List.copyOf(pendingBatch);
        try {
            delegate.appendBatch(batch);
            pendingBatch.subList(0, batch.size()).clear();
        } catch (IOException | RuntimeException ex) {
            dropWhatTheDelegateKept(batch);
            throw ex;
        }
    }

    /**
     * After a failed flush, drops exactly the entries the delegate acknowledges holding.
     *
     * <p>A failed {@code appendBatch} is not necessarily an empty one. The file adapter
     * writes entry by entry, so a failure part-way leaves a persisted prefix; FoundationDB
     * chunks large batches and can commit an earlier chunk before a later one fails.
     * Keeping the whole batch for retry would rewrite that prefix — trading the data loss
     * this replaced for duplicate records, which is no better in an append-only log that
     * things are folded from.
     *
     * <p>Entries are flushed in seqnum order, so the delegate's latest seqnum names the
     * boundary precisely: at or below it is durable, above it is not. Reconciling against
     * that leaves the retry neither losing nor duplicating, without requiring delegates to
     * make {@code appendBatch} all-or-nothing.
     *
     * <p>If the delegate cannot say, nothing is dropped: a duplicate is recoverable by
     * inspection, whereas a discarded entry is not.
     */
    private void dropWhatTheDelegateKept(List<LogEntry> batch) {
        long persistedThrough;
        try {
            persistedThrough = delegate.getLatestSeqnum();
        } catch (RuntimeException probeFailed) {
            logger.warn("Could not determine what the delegate persisted after a failed flush;"
                    + " keeping all {} entries for retry", batch.size(), probeFailed);
            return;
        }
        int persisted = 0;
        while (persisted < batch.size() && batch.get(persisted).seqnum() <= persistedThrough) {
            persisted++;
        }
        if (persisted > 0) {
            logger.warn("Flush failed after the delegate persisted {} of {} entries;"
                    + " retrying only the remainder", persisted, batch.size());
            pendingBatch.subList(0, persisted).clear();
        }
    }

    private void flushQuietly() {
        try {
            flushNow();
        } catch (IOException e) {
            logger.error("Background flush failed: {}", e.getMessage(), e);
        }
    }

    /** Returns a snapshot of the pending batch without holding the lock. */
    private List<LogEntry> pendingSnapshot() {
        flushLock.lock();
        try {
            return List.copyOf(pendingBatch);
        } finally {
            flushLock.unlock();
        }
    }

    /**
     * Appends pending entries whose seqnum >= {@code fromSeqnum} to the
     * delegate result list.
     */
    /**
     * Merges the delegate's view with the still-pending entries that {@code keep} selects.
     *
     * <p>Drops any pending entry the delegate already returned. The snapshot is taken
     * before the delegate is read, so a flush landing between the two puts the same entry
     * in both — and a consumer using these reads to avoid reprocessing what it has
     * already seen would be handed a duplicate by the very call meant to prevent that.
     * Seqnums are globally unique, which makes them the identity to dedupe on.
     *
     * <p>Ordering survives: every pending entry that is not in the delegate's result was
     * appended after everything in it, so appending them keeps the result ascending.
     */
    private static List<LogEntry> merge(List<LogEntry> fromDelegate,
                                        List<LogEntry> pending,
                                        java.util.function.Predicate<LogEntry> keep) {
        if (pending.isEmpty()) return fromDelegate;

        java.util.Set<Long> alreadyFlushed = new java.util.HashSet<>(fromDelegate.size());
        for (LogEntry e : fromDelegate) alreadyFlushed.add(e.seqnum());

        List<LogEntry> fromPending = pending.stream()
                .filter(keep)
                .filter(e -> !alreadyFlushed.contains(e.seqnum()))
                .toList();
        if (fromPending.isEmpty()) return fromDelegate;

        List<LogEntry> merged = new ArrayList<>(fromDelegate.size() + fromPending.size());
        merged.addAll(fromDelegate);
        merged.addAll(fromPending);
        return merged;
    }
}
