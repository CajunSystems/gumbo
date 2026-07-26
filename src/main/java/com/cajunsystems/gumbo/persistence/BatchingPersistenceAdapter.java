package com.cajunsystems.gumbo.persistence;

import com.cajunsystems.gumbo.core.LogEntry;
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
        return merge(fromDelegate, snapshot, 0);
    }

    @Override
    public List<LogEntry> readFrom(long fromSeqnum) throws IOException {
        List<LogEntry> snapshot = pendingSnapshot();
        List<LogEntry> fromDelegate = delegate.readFrom(fromSeqnum);
        return merge(fromDelegate, snapshot, fromSeqnum);
    }

    @Override
    public List<LogEntry> readByTag(LogTag tag, long fromSeqnum) throws IOException {
        List<LogEntry> snapshot = pendingSnapshot();
        List<LogEntry> fromDelegate = delegate.readByTag(tag, fromSeqnum);
        if (snapshot.isEmpty()) return fromDelegate;

        List<LogEntry> fromPending = snapshot.stream()
                .filter(e -> e.seqnum() >= fromSeqnum && e.tags().contains(tag))
                .toList();
        if (fromPending.isEmpty()) return fromDelegate;

        List<LogEntry> merged = new ArrayList<>(fromDelegate.size() + fromPending.size());
        merged.addAll(fromDelegate);
        merged.addAll(fromPending);
        return merged;
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
        if (snapshot.isEmpty()) return fromDelegate;

        List<LogEntry> fromPending = snapshot.stream()
                .filter(e -> e.tags().contains(tag) && e.streamVersion() >= fromVersion)
                .toList();
        if (fromPending.isEmpty()) return fromDelegate;

        List<LogEntry> merged = new ArrayList<>(fromDelegate.size() + fromPending.size());
        merged.addAll(fromDelegate);
        merged.addAll(fromPending);
        return merged;
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

    // -------------------------------------------------------------------------
    // Internal helpers
    // -------------------------------------------------------------------------

    /** Flushes the pending batch to the delegate; caller must hold flushLock. */
    private void flushUnderLock() throws IOException {
        if (pendingBatch.isEmpty()) return;
        List<LogEntry> batch = List.copyOf(pendingBatch);
        pendingBatch.clear();
        delegate.appendBatch(batch);
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
    private static List<LogEntry> merge(List<LogEntry> fromDelegate,
                                        List<LogEntry> pending,
                                        long fromSeqnum) {
        if (pending.isEmpty()) return fromDelegate;
        List<LogEntry> fromPending = pending.stream()
                .filter(e -> e.seqnum() >= fromSeqnum)
                .toList();
        if (fromPending.isEmpty()) return fromDelegate;
        List<LogEntry> merged = new ArrayList<>(fromDelegate.size() + fromPending.size());
        merged.addAll(fromDelegate);
        merged.addAll(fromPending);
        return merged;
    }
}
