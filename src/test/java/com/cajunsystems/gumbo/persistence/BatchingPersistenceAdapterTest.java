package com.cajunsystems.gumbo.persistence;

import com.cajunsystems.gumbo.core.LogEntry;
import com.cajunsystems.gumbo.core.LogTag;
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
        // avoiding the version→seqnum collision in InMemoryPersistenceAdapter's tag index.
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
    void readByTag_fromSeqnum_skipsEntriesBelowBoundary() throws IOException {
        // Use a large batch size so entries stay in the pending buffer (not flushed to delegate)
        adapter.close();
        adapter = new BatchingPersistenceAdapter(inner, 100, 10_000);
        adapter.open();

        // Non-contiguous seqnums — simulates actor replay from a mid-log checkpoint
        adapter.append(entry(0, TAG));
        adapter.append(entry(5, TAG));
        adapter.append(entry(10, TAG));

        // All entries are still pending (unflushed) — verify boundary filtering works
        List<LogEntry> result = adapter.readByTag(TAG, 5);

        assertThat(result).hasSize(2);
        assertThat(result).extracting(LogEntry::seqnum).containsExactlyInAnyOrder(5L, 10L);
        // Entry at seqnum 0 must NOT appear
        assertThat(result).noneMatch(e -> e.seqnum() == 0L);
    }

    @Test
    void readByTag_fromSeqnum_includesPendingEntriesAboveBoundary() throws IOException {
        // Use a large batch size so some entries stay in the pending buffer
        adapter.close();
        adapter = new BatchingPersistenceAdapter(inner, 100, 10_000);
        adapter.open();

        // Append entries at seqnums 0 and 5, then flush them to the delegate
        adapter.append(entry(0, TAG));
        adapter.append(entry(5, TAG));
        adapter.flushNow();

        // Append seqnum 10 — stays pending (not yet flushed)
        adapter.append(entry(10, TAG));

        // readByTag from 5 should include seqnum 5 (from delegate) and 10 (pending)
        List<LogEntry> result = adapter.readByTag(TAG, 5);

        assertThat(result).hasSize(2);
        assertThat(result).extracting(LogEntry::seqnum).containsExactlyInAnyOrder(5L, 10L);
        assertThat(result).noneMatch(e -> e.seqnum() == 0L);
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

    @Test
    void getLatestSeqnumForTag_returnsNegativeOneWhenEmpty() throws IOException {
        assertThat(adapter.getLatestSeqnumForTag(TAG)).isEqualTo(-1L);
    }

    @Test
    void getLatestSeqnumForTag_includesPendingEntries() throws IOException {
        // Append but do NOT flush — entries live in pending buffer only
        adapter.append(entry(0, TAG));
        adapter.append(entry(5, TAG));
        // Should see pending latest, not delegate latest (-1)
        assertThat(adapter.getLatestSeqnumForTag(TAG)).isEqualTo(5L);
    }

    @Test
    void getLatestSeqnumForTag_afterFlush_returnsFromDelegate() throws IOException {
        adapter.append(entry(0, TAG));
        adapter.flushNow();  // flush to delegate
        assertThat(adapter.getLatestSeqnumForTag(TAG)).isEqualTo(0L);
    }

    @Test
    void setGetTagValue_writeThroughToDelegate() throws IOException {
        adapter.append(entry(0, TAG));
        // KV write goes straight to delegate without needing a flush
        adapter.setTagValue(TAG, "ck", "val".getBytes());
        assertThat(adapter.getTagValue(TAG, "ck")).isEqualTo("val".getBytes());
    }

    @Test
    void deleteTagValue_writeThroughToDelegate() throws IOException {
        adapter.setTagValue(TAG, "ck", "val".getBytes());
        adapter.deleteTagValue(TAG, "ck");
        assertThat(adapter.getTagValue(TAG, "ck")).isNull();
    }
}
