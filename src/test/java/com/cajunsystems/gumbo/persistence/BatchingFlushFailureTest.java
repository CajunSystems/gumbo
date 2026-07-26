package com.cajunsystems.gumbo.persistence;

import com.cajunsystems.gumbo.core.LogEntry;
import com.cajunsystems.gumbo.core.LogTag;
import com.cajunsystems.gumbo.core.PendingAppend;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A flush that fails must leave its entries pending, not drop them.
 *
 * <p>Clearing the buffer before the delegate write succeeds loses the data outright: the
 * background flush logs the failure after it is already gone, and a caller holding an
 * {@code AppendResult} — with a version assigned and consumed — has no way to learn its
 * entry never landed.
 */
class BatchingFlushFailureTest {

    private static final LogTag TAG = LogTag.of("orders");

    @TempDir
    Path tempDir;

    /** Fails writes on demand, so the failure is chosen rather than waited for. */
    private static final class FlakyDelegate implements PersistenceAdapter {
        private final PersistenceAdapter inner;
        volatile boolean failWrites = false;

        FlakyDelegate(PersistenceAdapter inner) { this.inner = inner; }

        private void maybeFail() throws IOException {
            if (failWrites) throw new IOException("disk on fire");
        }

        @Override public void open() throws IOException { inner.open(); }
        @Override public void close() throws IOException { inner.close(); }
        @Override public void append(LogEntry e) throws IOException { maybeFail(); inner.append(e); }
        @Override public void appendBatch(List<LogEntry> es) throws IOException {
            maybeFail(); inner.appendBatch(es);
        }
        @Override public List<LogEntry> readAll() throws IOException { return inner.readAll(); }
        @Override public List<LogEntry> readFrom(long s) throws IOException { return inner.readFrom(s); }
        @Override public List<LogEntry> readByTag(LogTag t, long s) throws IOException {
            return inner.readByTag(t, s);
        }
        @Override public List<LogEntry> readFromVersion(LogTag t, long v) throws IOException {
            return inner.readFromVersion(t, v);
        }
        @Override public void trim(long s) throws IOException { inner.trim(s); }
        @Override public long getLatestSeqnum() { return inner.getLatestSeqnum(); }
        @Override public long getNextStreamVersion(LogTag t) { return inner.getNextStreamVersion(t); }
    }

    @Test
    void aFailedFlushKeepsTheEntriesForTheNextAttempt() throws IOException {
        FlakyDelegate delegate = new FlakyDelegate(new FileBasedPersistenceAdapter(tempDir));
        // Batch size 3: the third append triggers a flush from inside append().
        BatchingPersistenceAdapter adapter = new BatchingPersistenceAdapter(delegate, 3, 600_000);
        adapter.open();
        try {
            delegate.failWrites = true;
            adapter.append(entry(0));
            adapter.append(entry(1));
            assertThatThrownBy(() -> adapter.append(entry(2)))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("disk on fire");

            // Nothing reached storage, and nothing was thrown away either.
            delegate.failWrites = false;
            adapter.flushNow();

            assertThat(adapter.readAll().stream().map(LogEntry::seqnum))
                    .as("a failed flush must not discard the buffered entries")
                    .containsExactly(0L, 1L, 2L);
        } finally {
            adapter.close();
        }
    }

    @Test
    void aFailedFlushDoesNotStrandAVersionThatWasHandedOut() throws IOException {
        FlakyDelegate delegate = new FlakyDelegate(new FileBasedPersistenceAdapter(tempDir));
        BatchingPersistenceAdapter adapter = new BatchingPersistenceAdapter(delegate, 2, 600_000);
        adapter.open();
        try {
            delegate.failWrites = true;
            LogEntry first = adapter.append(pending(0), PersistenceAdapter.ANY_VERSION);
            assertThat(first.streamVersion()).isEqualTo(0L);

            assertThatThrownBy(() -> adapter.append(pending(1), PersistenceAdapter.ANY_VERSION))
                    .isInstanceOf(IOException.class);

            delegate.failWrites = false;
            adapter.flushNow();

            // Both versions handed out are backed by entries that actually exist.
            assertThat(adapter.readAll().stream().map(LogEntry::streamVersion))
                    .containsExactly(0L, 1L);
        } finally {
            adapter.close();
        }
    }

    /**
     * A delegate that persists part of a batch and then fails must not have that part
     * rewritten on the retry.
     *
     * <p>Keeping entries for retry fixes losing them, and introduces this: the file
     * adapter writes entry by entry, so a mid-batch failure leaves a persisted prefix, and
     * FoundationDB can commit an earlier chunk before a later one fails. Resubmitting the
     * whole batch would duplicate that prefix — trading data loss for duplicate records,
     * which in an append-only log that state is folded from is no better.
     */
    @Test
    void aPartiallyPersistedBatchIsNotRewrittenOnRetry() throws IOException {
        PartialDelegate delegate = new PartialDelegate(new FileBasedPersistenceAdapter(tempDir));
        BatchingPersistenceAdapter adapter = new BatchingPersistenceAdapter(delegate, 4, 600_000);
        adapter.open();
        try {
            delegate.persistThenFailAfter = 2;   // entries 0 and 1 land, then it throws
            adapter.append(entry(0));
            adapter.append(entry(1));
            adapter.append(entry(2));
            assertThatThrownBy(() -> adapter.append(entry(3)))
                    .isInstanceOf(IOException.class);

            delegate.persistThenFailAfter = -1;  // healthy again
            adapter.flushNow();

            // Asserted on what the delegate was asked to persist, not on what a read
            // returns: both durable adapters key by seqnum, so a rewritten record is
            // deduplicated on the way back out and the waste stays invisible there.
            assertThat(delegate.persisted)
                    .as("the persisted prefix must not be handed to the delegate twice")
                    .doesNotHaveDuplicates()
                    .containsExactly(0L, 1L, 2L, 3L);
            assertThat(adapter.readAll().stream().map(LogEntry::seqnum))
                    .containsExactly(0L, 1L, 2L, 3L);
        } finally {
            adapter.close();
        }
    }

    /** Persists the first {@code persistThenFailAfter} entries of a batch, then throws. */
    private static final class PartialDelegate implements PersistenceAdapter {
        private final PersistenceAdapter inner;
        volatile int persistThenFailAfter = -1;
        /** Every seqnum this delegate actually wrote, in order, duplicates included. */
        final List<Long> persisted = new java.util.ArrayList<>();

        PartialDelegate(PersistenceAdapter inner) { this.inner = inner; }

        @Override public void appendBatch(List<LogEntry> es) throws IOException {
            if (persistThenFailAfter < 0) {
                inner.appendBatch(es);
                es.forEach(e -> persisted.add(e.seqnum()));
                return;
            }
            int n = Math.min(persistThenFailAfter, es.size());
            List<LogEntry> prefix = es.subList(0, n);
            inner.appendBatch(prefix);
            prefix.forEach(e -> persisted.add(e.seqnum()));
            throw new IOException("failed after persisting " + n + " of " + es.size());
        }
        @Override public void append(LogEntry e) throws IOException { appendBatch(List.of(e)); }
        @Override public void open() throws IOException { inner.open(); }
        @Override public void close() throws IOException { inner.close(); }
        @Override public List<LogEntry> readAll() throws IOException { return inner.readAll(); }
        @Override public List<LogEntry> readFrom(long s) throws IOException { return inner.readFrom(s); }
        @Override public List<LogEntry> readByTag(LogTag t, long s) throws IOException {
            return inner.readByTag(t, s);
        }
        @Override public List<LogEntry> readFromVersion(LogTag t, long v) throws IOException {
            return inner.readFromVersion(t, v);
        }
        @Override public void trim(long s) throws IOException { inner.trim(s); }
        @Override public long getLatestSeqnum() { return inner.getLatestSeqnum(); }
        @Override public long getNextStreamVersion(LogTag t) { return inner.getNextStreamVersion(t); }
    }

    /**
     * A batch whose bytes are written but whose fsync fails must stay pending.
     *
     * <p>This is where the reconciliation could turn back into data loss. It drops the
     * prefix the delegate reports holding, and the file adapter used to make entries
     * visible at write time — so a failed sync looked exactly like a successful write.
     * The entries would be dropped from the retry that was their last chance, and a crash
     * after that loses records the caller was already handed versions for.
     *
     * <p>Visibility now follows durability: nothing is published until the sync returns.
     */
    @Test
    void aBatchWhoseSyncFailsStaysPendingForRetry() throws IOException {
        SyncFailingFileAdapter file = new SyncFailingFileAdapter(tempDir);
        BatchingPersistenceAdapter adapter = new BatchingPersistenceAdapter(file, 3, 600_000);
        adapter.open();
        try {
            file.failSync = true;
            adapter.append(entry(0));
            adapter.append(entry(1));
            assertThatThrownBy(() -> adapter.append(entry(2)))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("fsync");

            // The bytes may be on disk, but nothing is durable, so nothing may be
            // reported as landed — and nothing may be dropped from the retry.
            assertThat(file.getLatestSeqnum())
                    .as("an unsynced entry must not be reported as the durable tip")
                    .isEqualTo(-1L);

            file.failSync = false;
            adapter.flushNow();

            assertThat(adapter.readAll().stream().map(LogEntry::seqnum))
                    .as("entries whose sync failed must survive to the next flush")
                    .containsExactly(0L, 1L, 2L);
        } finally {
            adapter.close();
        }
    }

    /** A file adapter whose fsync can be made to fail on demand. */
    private static final class SyncFailingFileAdapter extends FileBasedPersistenceAdapter {
        volatile boolean failSync = false;

        SyncFailingFileAdapter(Path dir) { super(dir); }

        @Override
        void syncChannels() throws IOException {
            if (failSync) throw new IOException("fsync failed");
            super.syncChannels();
        }
    }

    private static LogEntry entry(long n) {
        return new LogEntry(n, n, Set.of(TAG), ("e" + n).getBytes(), Instant.now());
    }

    private static PendingAppend pending(long n) {
        return new PendingAppend(n, TAG, Set.of(TAG), ("e" + n).getBytes(), Instant.now());
    }
}
