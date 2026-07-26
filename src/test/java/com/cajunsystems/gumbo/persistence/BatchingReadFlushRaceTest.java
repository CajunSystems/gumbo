package com.cajunsystems.gumbo.persistence;

import com.cajunsystems.gumbo.core.LogEntry;
import com.cajunsystems.gumbo.core.LogTag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A read on {@link BatchingPersistenceAdapter} snapshots the pending buffer and then
 * reads the delegate. A flush landing between those two steps puts the same entry in
 * both, and concatenating them hands the caller a duplicate.
 *
 * <p>The window is real but narrow, so these tests do not race for it: a delegate
 * wrapper flushes the adapter from <em>inside</em> its own read call, which is exactly
 * the interleaving, made deterministic.
 */
class BatchingReadFlushRaceTest {

    private static final LogTag TAG = LogTag.of("orders");

    @TempDir
    Path tempDir;

    /**
     * Wraps the real adapter and runs {@code beforeRead} at the start of every read —
     * the seam where a background flush would land.
     */
    private static final class FlushingDelegate implements PersistenceAdapter {
        private final PersistenceAdapter inner;
        private final AtomicReference<Runnable> beforeRead = new AtomicReference<>(() -> {});

        FlushingDelegate(PersistenceAdapter inner) { this.inner = inner; }

        private void fire() { beforeRead.getAndSet(() -> {}).run(); }

        @Override public void open() throws IOException { inner.open(); }
        @Override public void close() throws IOException { inner.close(); }
        @Override public void append(LogEntry e) throws IOException { inner.append(e); }
        @Override public void appendBatch(List<LogEntry> es) throws IOException { inner.appendBatch(es); }
        @Override public void trim(long s) throws IOException { inner.trim(s); }
        @Override public long getLatestSeqnum() { return inner.getLatestSeqnum(); }
        @Override public long getLocalIdCountForTag(LogTag t) { return inner.getLocalIdCountForTag(t); }

        @Override public List<LogEntry> readAll() throws IOException { fire(); return inner.readAll(); }
        @Override public List<LogEntry> readFrom(long s) throws IOException { fire(); return inner.readFrom(s); }
        @Override public List<LogEntry> readByTag(LogTag t, long s) throws IOException {
            fire(); return inner.readByTag(t, s);
        }
        @Override public List<LogEntry> readFromVersion(LogTag t, long v) throws IOException {
            fire(); return inner.readFromVersion(t, v);
        }
    }

    @Test
    void aFlushDuringAVersionKeyedReadDoesNotDuplicateEntries() throws IOException {
        withFlushDuringRead((adapter, tag) -> adapter.readFromVersion(tag, 0));
    }

    @Test
    void aFlushDuringATagReadDoesNotDuplicateEntries() throws IOException {
        withFlushDuringRead((adapter, tag) -> adapter.readByTag(tag, 0));
    }

    @Test
    void aFlushDuringReadAllDoesNotDuplicateEntries() throws IOException {
        withFlushDuringRead((adapter, tag) -> adapter.readAll());
    }

    @Test
    void aFlushDuringReadFromDoesNotDuplicateEntries() throws IOException {
        withFlushDuringRead((adapter, tag) -> adapter.readFrom(0));
    }

    private interface Read {
        List<LogEntry> apply(BatchingPersistenceAdapter adapter, LogTag tag) throws IOException;
    }

    private void withFlushDuringRead(Read read) throws IOException {
        FlushingDelegate delegate = new FlushingDelegate(new FileBasedPersistenceAdapter(tempDir));
        // A huge batch size and delay: nothing flushes unless this test makes it.
        BatchingPersistenceAdapter adapter =
                new BatchingPersistenceAdapter(delegate, 10_000, 600_000);
        adapter.open();
        try {
            for (int i = 0; i < 3; i++) {
                adapter.append(new LogEntry(i, i, Set.of(TAG), ("e" + i).getBytes(), Instant.now()));
            }
            // All three are pending. Flush them once the read has taken its snapshot but
            // before the delegate is consulted — so the delegate returns them too.
            delegate.beforeRead.set(() -> {
                try {
                    adapter.flushNow();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });

            List<LogEntry> entries = read.apply(adapter, TAG);

            assertThat(entries.stream().map(LogEntry::seqnum))
                    .as("an entry flushed mid-read must appear once, not twice")
                    .containsExactly(0L, 1L, 2L);
        } finally {
            adapter.close();
        }
    }
}
