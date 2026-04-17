package com.cajunsystems.gumbo.persistence;

import com.cajunsystems.gumbo.core.LogEntry;
import com.cajunsystems.gumbo.core.LogTag;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class InMemoryPersistenceAdapterTest {

    private InMemoryPersistenceAdapter adapter;
    private static final LogTag TAG_A = LogTag.of("orders");
    private static final LogTag TAG_B = LogTag.of("inventory");

    @BeforeEach
    void setUp() throws IOException {
        adapter = new InMemoryPersistenceAdapter();
        adapter.open();
    }

    @Test
    void emptyAdapterHasNoEntries() throws IOException {
        assertThat(adapter.getLatestSeqnum()).isEqualTo(-1L);
        assertThat(adapter.readAll()).isEmpty();
        assertThat(adapter.readByTag(TAG_A, 0)).isEmpty();
    }

    @Test
    void appendAndReadAll() throws IOException {
        LogEntry e1 = entry(0, 0, TAG_A, "hello");
        LogEntry e2 = entry(1, 1, TAG_A, "world");
        adapter.append(e1);
        adapter.append(e2);

        List<LogEntry> all = adapter.readAll();
        assertThat(all).hasSize(2);
        assertThat(all.get(0).seqnum()).isEqualTo(0);
        assertThat(all.get(1).seqnum()).isEqualTo(1);
    }

    @Test
    void readFromFiltersLowerSeqnums() throws IOException {
        adapter.append(entry(0, 0, TAG_A, "a"));
        adapter.append(entry(1, 1, TAG_A, "b"));
        adapter.append(entry(2, 2, TAG_A, "c"));

        List<LogEntry> result = adapter.readFrom(1);
        assertThat(result).hasSize(2);
        assertThat(result.get(0).seqnum()).isEqualTo(1);
        assertThat(result.get(1).seqnum()).isEqualTo(2);
    }

    @Test
    void readByTagReturnsOnlyMatchingEntries() throws IOException {
        adapter.append(entry(0, 0, TAG_A, "order-event"));
        adapter.append(entry(1, 0, TAG_B, "inventory-event"));
        adapter.append(entry(2, 1, TAG_A, "order-event-2"));

        List<LogEntry> ordersOnly = adapter.readByTag(TAG_A, 0);
        assertThat(ordersOnly).hasSize(2);
        assertThat(ordersOnly).allMatch(e -> e.tags().contains(TAG_A));

        List<LogEntry> inventoryOnly = adapter.readByTag(TAG_B, 0);
        assertThat(inventoryOnly).hasSize(1);
    }

    @Test
    void readByTagWithFromSeqnum() throws IOException {
        adapter.append(entry(0, 0, TAG_A, "e0"));
        adapter.append(entry(1, 1, TAG_A, "e1"));
        adapter.append(entry(2, 2, TAG_A, "e2"));

        List<LogEntry> result = adapter.readByTag(TAG_A, 2);
        assertThat(result).hasSize(1);
        assertThat(result.get(0).seqnum()).isEqualTo(2);
    }

    @Test
    void multiTagEntry() throws IOException {
        LogEntry multi = new LogEntry(0, 0, Set.of(TAG_A, TAG_B), "data".getBytes(), Instant.now());
        adapter.append(multi);

        assertThat(adapter.readByTag(TAG_A, 0)).hasSize(1);
        assertThat(adapter.readByTag(TAG_B, 0)).hasSize(1);
    }

    @Test
    void trimRemovesEntriesBelowSeqnum() throws IOException {
        adapter.append(entry(0, 0, TAG_A, "e0"));
        adapter.append(entry(1, 1, TAG_A, "e1"));
        adapter.append(entry(2, 2, TAG_A, "e2"));

        adapter.trim(2); // remove 0, 1 (exclusive upper bound)

        List<LogEntry> all = adapter.readAll();
        assertThat(all).hasSize(1);
        assertThat(all.get(0).seqnum()).isEqualTo(2);
    }

    @Test
    void latestSeqnumTracksMaximum() throws IOException {
        assertThat(adapter.getLatestSeqnum()).isEqualTo(-1L);
        adapter.append(entry(0, 0, TAG_A, "a"));
        assertThat(adapter.getLatestSeqnum()).isEqualTo(0L);
        adapter.append(entry(5, 1, TAG_A, "b"));
        assertThat(adapter.getLatestSeqnum()).isEqualTo(5L);
    }

    @Test
    void localIdCountReflectsAppendedEntries() throws IOException {
        adapter.append(entry(0, 0, TAG_A, "e0"));
        adapter.append(entry(1, 1, TAG_A, "e1"));
        adapter.append(entry(2, 0, TAG_B, "f0"));

        assertThat(adapter.getLocalIdCountForTag(TAG_A)).isEqualTo(2L);
        assertThat(adapter.getLocalIdCountForTag(TAG_B)).isEqualTo(1L);
    }

    @Test
    void readByTag_fromSeqnum_skipsEntriesBelowBoundary() throws IOException {
        // Non-contiguous seqnums — simulates actor replay from a mid-log checkpoint
        adapter.append(entry(0,  0, TAG_A, "e0"));
        adapter.append(entry(5,  1, TAG_A, "e5"));
        adapter.append(entry(10, 2, TAG_A, "e10"));

        List<LogEntry> result = adapter.readByTag(TAG_A, 5);

        assertThat(result).hasSize(2);
        assertThat(result.get(0).seqnum()).isEqualTo(5L);
        assertThat(result.get(1).seqnum()).isEqualTo(10L);
        // Entry at seqnum 0 must NOT appear
        assertThat(result).noneMatch(e -> e.seqnum() == 0L);
    }

    @Test
    void closedAdapterThrowsOnWrite() throws IOException {
        adapter.close();
        assertThatThrownBy(() -> adapter.append(entry(0, 0, TAG_A, "x")))
                .isInstanceOf(IllegalStateException.class);
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private static LogEntry entry(long seqnum, long localId, LogTag tag, String data) {
        return new LogEntry(seqnum, localId, Set.of(tag), data.getBytes(), Instant.now());
    }
}
