package com.cajunsystems.gumbo.persistence;

import com.cajunsystems.gumbo.core.LogEntry;
import com.cajunsystems.gumbo.core.LogTag;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class FileBasedPersistenceAdapterTest {

    @TempDir
    Path tempDir;

    private FileBasedPersistenceAdapter adapter;

    private static final LogTag TAG_ORDERS    = LogTag.of("orders");
    private static final LogTag TAG_INVENTORY = LogTag.of("inventory");

    @BeforeEach
    void setUp() throws IOException {
        adapter = new FileBasedPersistenceAdapter(tempDir);
        adapter.open();
    }

    @AfterEach
    void tearDown() throws IOException {
        adapter.close();
    }

    @Test
    void emptyAdapterHasNoEntries() throws IOException {
        assertThat(adapter.getLatestSeqnum()).isEqualTo(-1L);
        assertThat(adapter.readAll()).isEmpty();
    }

    @Test
    void appendAndReadAllPreservesData() throws IOException {
        byte[] payload = "hello-world".getBytes();
        adapter.append(entry(0, 0, TAG_ORDERS, payload));

        List<LogEntry> entries = adapter.readAll();
        assertThat(entries).hasSize(1);
        assertThat(entries.get(0).seqnum()).isEqualTo(0);
        assertThat(entries.get(0).data()).isEqualTo(payload);
        assertThat(entries.get(0).tags()).containsExactly(TAG_ORDERS);
    }

    @Test
    void multipleEntriesReadInOrder() throws IOException {
        adapter.append(entry(0, 0, TAG_ORDERS, "a"));
        adapter.append(entry(1, 1, TAG_ORDERS, "b"));
        adapter.append(entry(2, 2, TAG_ORDERS, "c"));

        List<LogEntry> all = adapter.readAll();
        assertThat(all).hasSize(3);
        assertThat(all.get(0).seqnum()).isEqualTo(0);
        assertThat(all.get(1).seqnum()).isEqualTo(1);
        assertThat(all.get(2).seqnum()).isEqualTo(2);
    }

    @Test
    void readByTagReturnsOnlyMatchingEntries() throws IOException {
        adapter.append(entry(0, 0, TAG_ORDERS,    "order-1"));
        adapter.append(entry(1, 0, TAG_INVENTORY, "inv-1"));
        adapter.append(entry(2, 1, TAG_ORDERS,    "order-2"));

        List<LogEntry> orders = adapter.readByTag(TAG_ORDERS, 0);
        assertThat(orders).hasSize(2);
        assertThat(orders).allMatch(e -> e.tags().contains(TAG_ORDERS));

        List<LogEntry> inv = adapter.readByTag(TAG_INVENTORY, 0);
        assertThat(inv).hasSize(1);
    }

    @Test
    void readFromLowerBound() throws IOException {
        adapter.append(entry(0, 0, TAG_ORDERS, "e0"));
        adapter.append(entry(1, 1, TAG_ORDERS, "e1"));
        adapter.append(entry(2, 2, TAG_ORDERS, "e2"));

        List<LogEntry> result = adapter.readFrom(1);
        assertThat(result).hasSize(2);
        assertThat(result.get(0).seqnum()).isEqualTo(1);
    }

    @Test
    void multiTagEntry() throws IOException {
        Set<LogTag> both = Set.of(TAG_ORDERS, TAG_INVENTORY);
        LogEntry e = new LogEntry(0, 0, both, "dual".getBytes(), Instant.now());
        adapter.append(e);

        assertThat(adapter.readByTag(TAG_ORDERS, 0)).hasSize(1);
        assertThat(adapter.readByTag(TAG_INVENTORY, 0)).hasSize(1);
    }

    @Test
    void latestSeqnum() throws IOException {
        assertThat(adapter.getLatestSeqnum()).isEqualTo(-1L);
        adapter.append(entry(0, 0, TAG_ORDERS, "x"));
        assertThat(adapter.getLatestSeqnum()).isEqualTo(0L);
        adapter.append(entry(10, 1, TAG_ORDERS, "y"));
        assertThat(adapter.getLatestSeqnum()).isEqualTo(10L);
    }

    @Test
    void trimHidesEntriesBelow() throws IOException {
        adapter.append(entry(0, 0, TAG_ORDERS, "e0"));
        adapter.append(entry(1, 1, TAG_ORDERS, "e1"));
        adapter.append(entry(2, 2, TAG_ORDERS, "e2"));

        adapter.trim(2); // exclusive: hides 0, 1

        List<LogEntry> all = adapter.readAll();
        assertThat(all).hasSize(1);
        assertThat(all.get(0).seqnum()).isEqualTo(2);
    }

    @Test
    void surviveReopenWithIndexFile() throws IOException {
        adapter.append(entry(0, 0, TAG_ORDERS,    "o1"));
        adapter.append(entry(1, 0, TAG_INVENTORY, "i1"));
        adapter.append(entry(2, 1, TAG_ORDERS,    "o2"));
        adapter.close();

        // Re-open and verify state is recovered
        FileBasedPersistenceAdapter reopened = new FileBasedPersistenceAdapter(tempDir);
        try {
            reopened.open();
            assertThat(reopened.getLatestSeqnum()).isEqualTo(2L);
            assertThat(reopened.readAll()).hasSize(3);
            assertThat(reopened.readByTag(TAG_ORDERS, 0)).hasSize(2);
            assertThat(reopened.readByTag(TAG_INVENTORY, 0)).hasSize(1);
        } finally {
            reopened.close();
        }
    }

    @Test
    void surviveReopenAfterIndexDeleted() throws IOException {
        adapter.append(entry(0, 0, TAG_ORDERS, "o1"));
        adapter.append(entry(1, 1, TAG_ORDERS, "o2"));
        adapter.close();

        // Delete the index to force a full log scan on reopen
        java.nio.file.Files.deleteIfExists(tempDir.resolve("index.dat"));

        FileBasedPersistenceAdapter reopened = new FileBasedPersistenceAdapter(tempDir);
        try {
            reopened.open();
            assertThat(reopened.getLatestSeqnum()).isEqualTo(1L);
            assertThat(reopened.readAll()).hasSize(2);
        } finally {
            reopened.close();
        }
    }

    @Test
    void getLatestSeqnumForTag_returnsNegativeOneWhenEmpty() {
        assertThat(adapter.getLatestSeqnumForTag(TAG_ORDERS)).isEqualTo(-1L);
    }

    @Test
    void getLatestSeqnumForTag_returnsHighestSeqnumForTag() throws IOException {
        adapter.append(entry(0, 0, TAG_ORDERS,    "o0"));
        adapter.append(entry(3, 0, TAG_INVENTORY, "i0"));
        adapter.append(entry(7, 1, TAG_ORDERS,    "o1"));

        assertThat(adapter.getLatestSeqnumForTag(TAG_ORDERS)).isEqualTo(7L);
        assertThat(adapter.getLatestSeqnumForTag(TAG_INVENTORY)).isEqualTo(3L);
    }

    // -------------------------------------------------------------------------
    // KV tests
    // -------------------------------------------------------------------------

    @Test
    void setGetTagValue_roundtrip() throws IOException {
        adapter.setTagValue(TAG_ORDERS, "ck", "42".getBytes());
        assertThat(adapter.getTagValue(TAG_ORDERS, "ck")).isEqualTo("42".getBytes());
    }

    @Test
    void getTagValue_returnsNullWhenAbsent() {
        assertThat(adapter.getTagValue(TAG_ORDERS, "missing")).isNull();
    }

    @Test
    void deleteTagValue_removesValue() throws IOException {
        adapter.setTagValue(TAG_ORDERS, "ck", "42".getBytes());
        adapter.deleteTagValue(TAG_ORDERS, "ck");
        assertThat(adapter.getTagValue(TAG_ORDERS, "ck")).isNull();
    }

    @Test
    void kvPersistsAcrossReopen() throws IOException {
        adapter.setTagValue(TAG_ORDERS, "ck", "persisted".getBytes());
        adapter.close();

        FileBasedPersistenceAdapter adapter2 = new FileBasedPersistenceAdapter(tempDir);
        adapter2.open();
        try {
            assertThat(adapter2.getTagValue(TAG_ORDERS, "ck")).isEqualTo("persisted".getBytes());
        } finally {
            adapter2.close();
        }
    }

    // -------------------------------------------------------------------------

    private static LogEntry entry(long seqnum, long streamVersion, LogTag tag, String data) {
        return entry(seqnum, streamVersion, tag, data.getBytes());
    }

    private static LogEntry entry(long seqnum, long streamVersion, LogTag tag, byte[] data) {
        return new LogEntry(seqnum, streamVersion, Set.of(tag), data, Instant.now());
    }
}
