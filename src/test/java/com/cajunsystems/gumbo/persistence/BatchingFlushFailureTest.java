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

    private static LogEntry entry(long n) {
        return new LogEntry(n, n, Set.of(TAG), ("e" + n).getBytes(), Instant.now());
    }

    private static PendingAppend pending(long n) {
        return new PendingAppend(n, TAG, Set.of(TAG), ("e" + n).getBytes(), Instant.now());
    }
}
