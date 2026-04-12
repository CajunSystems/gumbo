package com.central.sharedlog.persistence;

import com.central.sharedlog.core.LogEntry;
import com.central.sharedlog.core.LogTag;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

class BatchingPersistenceAdapterTest {

    private static final LogTag TAG = LogTag.of("test");

    // -------------------------------------------------------------------------
    // Tracking wrapper — counts appendBatch() calls reaching the inner adapter
    // -------------------------------------------------------------------------

    static class CountingAdapter extends InMemoryPersistenceAdapter {
        final AtomicInteger batchCalls      = new AtomicInteger(0);
        final AtomicInteger entriesReceived = new AtomicInteger(0);

        @Override
        public void appendBatch(List<LogEntry> entries) throws IOException {
            batchCalls.incrementAndGet();
            entriesReceived.addAndGet(entries.size());
            super.appendBatch(entries); // persist in-memory
        }
    }

    // -------------------------------------------------------------------------
    // Fixtures
    // -------------------------------------------------------------------------

    private CountingAdapter       inner;
    private BatchingPersistenceAdapter adapter;

    @BeforeEach
    void setUp() throws IOException {
        inner   = new CountingAdapter();
        // batchSize=3, delay=10s — flush only on batch-full during most tests
        adapter = new BatchingPersistenceAdapter(inner, 3, 10_000);
        inner.open();
        adapter.open();
    }

    @AfterEach
    void tearDown() throws IOException {
        adapter.close();
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private static LogEntry entry(long seqnum) {
        return new LogEntry(seqnum, 0, Set.of(TAG), ("data-" + seqnum).getBytes(), Instant.now());
    }

    private static LogEntry entry(long seqnum, LogTag tag) {
        return new LogEntry(seqnum, 0, Set.of(tag), ("data-" + seqnum).getBytes(), Instant.now());
    }

    // -------------------------------------------------------------------------
    // Tests
    // -------------------------------------------------------------------------

    @Test
    void pendingEntriesAreNotImmediatelyFlushed() throws IOException {
        adapter.append(entry(0));
        adapter.append(entry(1));

        // Batch not full (max=3) → delegate untouched
        assertThat(inner.batchCalls.get()).isEqualTo(0);
        assertThat(inner.entriesReceived.get()).isEqualTo(0);
    }

    @Test
    void flushesWhenBatchSizeReached() throws IOException {
        adapter.append(entry(0));
        adapter.append(entry(1));
        adapter.append(entry(2)); // ← fills batch

        assertThat(inner.batchCalls.get()).isEqualTo(1);
        assertThat(inner.entriesReceived.get()).isEqualTo(3);
    }

    @Test
    void flushNowPersistsImmediately() throws IOException {
        adapter.append(entry(0));
        adapter.append(entry(1));
        assertThat(inner.batchCalls.get()).isEqualTo(0);

        adapter.flushNow();

        assertThat(inner.batchCalls.get()).isEqualTo(1);
        assertThat(inner.entriesReceived.get()).isEqualTo(2);
    }

    @Test
    void closeFlushesPendingEntries() throws IOException {
        adapter.append(entry(0));
        adapter.append(entry(1));
        assertThat(inner.batchCalls.get()).isEqualTo(0);

        adapter.close(); // must flush before delegating close

        assertThat(inner.batchCalls.get()).isEqualTo(1);
        assertThat(inner.entriesReceived.get()).isEqualTo(2);
    }

    @Test
    void timeBasedFlush() throws InterruptedException, IOException {
        // Use a short delay adapter for this test
        adapter.close();
        adapter = new BatchingPersistenceAdapter(inner, 1000, 50 /* ms */);
        adapter.open();

        adapter.append(entry(0));
        assertThat(inner.batchCalls.get()).isEqualTo(0); // not yet

        Thread.sleep(300); // wait for background flusher

        assertThat(inner.batchCalls.get()).isGreaterThanOrEqualTo(1);
        assertThat(inner.entriesReceived.get()).isEqualTo(1);
    }

    @Test
    void readsIncludePendingEntriesBeforeFlush() throws IOException {
        adapter.append(entry(0));
        adapter.append(entry(1));

        // Delegate has nothing yet, but reads should see pending entries
        List<LogEntry> all = adapter.readAll();
        assertThat(all).hasSize(2);

        List<LogEntry> fromOne = adapter.readFrom(1);
        assertThat(fromOne).hasSize(1);
        assertThat(fromOne.get(0).seqnum()).isEqualTo(1);
    }

    @Test
    void readByTagIncludesPendingEntries() throws IOException {
        // Use a large batch size so entries stay in the pending buffer (not flushed to delegate),
        // avoiding the localId→seqnum collision in InMemoryPersistenceAdapter's tag index.
        adapter.close();
        adapter = new BatchingPersistenceAdapter(inner, 100, 10_000);
        adapter.open();

        LogTag other = LogTag.of("other");
        adapter.append(entry(0, TAG));
        adapter.append(entry(1, other));
        adapter.append(entry(2, TAG));

        List<LogEntry> forTag   = adapter.readByTag(TAG, 0);
        List<LogEntry> forOther = adapter.readByTag(other, 0);

        assertThat(forTag).hasSize(2);
        assertThat(forOther).hasSize(1);
    }

    @Test
    void getLatestSeqnumIncludesPendingEntries() throws IOException {
        assertThat(adapter.getLatestSeqnum()).isEqualTo(-1L);

        adapter.append(entry(0));
        adapter.append(entry(1));

        assertThat(adapter.getLatestSeqnum()).isEqualTo(1L);

        adapter.flushNow();

        assertThat(adapter.getLatestSeqnum()).isEqualTo(1L); // same after flush
    }

    @Test
    void batchSizeOneFlushesEveryEntry() throws IOException {
        adapter.close();
        adapter = new BatchingPersistenceAdapter(inner, 1, 10_000);
        adapter.open();

        adapter.append(entry(0));
        assertThat(inner.batchCalls.get()).isEqualTo(1);

        adapter.append(entry(1));
        assertThat(inner.batchCalls.get()).isEqualTo(2);

        adapter.append(entry(2));
        assertThat(inner.batchCalls.get()).isEqualTo(3);

        assertThat(inner.entriesReceived.get()).isEqualTo(3);
    }

    @Test
    void trimFlushesBeforeDelegating() throws IOException {
        adapter.append(entry(0));
        adapter.append(entry(1));

        // trim should flush first so entries are in delegate before being trimmed
        adapter.trim(0);

        // All entries were flushed to delegate
        assertThat(inner.batchCalls.get()).isEqualTo(1);
    }
}
