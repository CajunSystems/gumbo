package com.cajunsystems.gumbo.persistence;

import com.cajunsystems.gumbo.core.LogEntry;
import com.cajunsystems.gumbo.core.LogTag;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Integration tests for {@link FoundationDBPersistenceAdapter}.
 *
 * <p>These tests require a running FoundationDB cluster reachable via the
 * default cluster file ({@code /etc/foundationdb/fdb.cluster} on Linux, or
 * the value of the {@code FDB_CLUSTER_FILE} environment variable).
 *
 * <p>If no FDB cluster is available the tests are <em>skipped</em> rather than
 * failed, so CI pipelines without FDB still pass.
 *
 * <p>Each test class run uses a unique subspace root derived from a random UUID
 * so tests are isolated and leave no lasting state if run in parallel.
 */
class FoundationDBPersistenceAdapterTest {

    private static final LogTag TAG_ORDERS    = LogTag.of("orders");
    private static final LogTag TAG_INVENTORY = LogTag.of("inventory");

    /** Set to true in @BeforeAll if FDB is reachable; used by assumeTrue in each test. */
    private static boolean fdbAvailable = false;

    /** Unique root for this test-class run; keeps each run's data isolated. */
    private static String testRoot;

    private FoundationDBPersistenceAdapter adapter;

    @BeforeAll
    static void checkFdbAvailability() {
        testRoot = "gumbo-test-" + UUID.randomUUID();
        try {
            FoundationDBPersistenceAdapter probe =
                    new FoundationDBPersistenceAdapter(fdbClusterFile(), testRoot + "-probe");
            probe.open();
            probe.close();
            fdbAvailable = true;
        } catch (Throwable e) {
            // FDB not available (missing libfdb_c.so, no cluster file, etc.); all tests will be skipped
            fdbAvailable = false;
        }
    }

    @BeforeEach
    void setUp() throws IOException {
        assumeTrue(fdbAvailable, "FoundationDB cluster not available — skipping test");
        // Fresh subspace per test method
        adapter = new FoundationDBPersistenceAdapter(fdbClusterFile(),
                testRoot + "-" + UUID.randomUUID());
        adapter.open();
    }

    @AfterEach
    void tearDown() throws IOException {
        if (adapter != null) adapter.close();
    }

    // =========================================================================
    // Basic contract
    // =========================================================================

    @Test
    void emptyAdapterReturnsNegativeLatestSeqnum() {
        assertThat(adapter.getLatestSeqnum()).isEqualTo(-1L);
    }

    @Test
    void emptyAdapterReadsEmpty() throws IOException {
        assertThat(adapter.readAll()).isEmpty();
    }

    @Test
    void appendAndReadAllPreservesAllFields() throws IOException {
        Instant ts      = Instant.ofEpochMilli(1_700_000_000_000L);
        byte[]  payload = "hello-fdb".getBytes();
        LogEntry written = new LogEntry(0L, 0L, Set.of(TAG_ORDERS), payload, ts);

        adapter.append(written);

        List<LogEntry> entries = adapter.readAll();
        assertThat(entries).hasSize(1);
        LogEntry read = entries.get(0);
        assertThat(read.seqnum()).isEqualTo(0L);
        assertThat(read.localId()).isEqualTo(0L);
        assertThat(read.tags()).containsExactly(TAG_ORDERS);
        assertThat(read.data()).isEqualTo(payload);
        assertThat(read.timestamp().toEpochMilli()).isEqualTo(ts.toEpochMilli());
    }

    @Test
    void multipleEntriesReadInSeqnumOrder() throws IOException {
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
    void latestSeqnumTracksHighestWrite() throws IOException {
        assertThat(adapter.getLatestSeqnum()).isEqualTo(-1L);
        adapter.append(entry(0, 0, TAG_ORDERS, "x"));
        assertThat(adapter.getLatestSeqnum()).isEqualTo(0L);
        adapter.append(entry(5, 1, TAG_ORDERS, "y"));
        assertThat(adapter.getLatestSeqnum()).isEqualTo(5L);
    }

    // =========================================================================
    // readFrom
    // =========================================================================

    @Test
    void readFromFiltersLowerSeqnums() throws IOException {
        adapter.append(entry(0, 0, TAG_ORDERS, "e0"));
        adapter.append(entry(1, 1, TAG_ORDERS, "e1"));
        adapter.append(entry(2, 2, TAG_ORDERS, "e2"));

        List<LogEntry> result = adapter.readFrom(1);
        assertThat(result).hasSize(2);
        assertThat(result.get(0).seqnum()).isEqualTo(1);
        assertThat(result.get(1).seqnum()).isEqualTo(2);
    }

    // =========================================================================
    // readByTag
    // =========================================================================

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
    void readByTagWithFromSeqnumBound() throws IOException {
        adapter.append(entry(0, 0, TAG_ORDERS, "e0"));
        adapter.append(entry(1, 1, TAG_ORDERS, "e1"));
        adapter.append(entry(2, 2, TAG_ORDERS, "e2"));

        List<LogEntry> result = adapter.readByTag(TAG_ORDERS, 1);
        assertThat(result).hasSize(2);
        assertThat(result.get(0).seqnum()).isEqualTo(1);
    }

    @Test
    void readByTagUnknownTagReturnsEmpty() throws IOException {
        adapter.append(entry(0, 0, TAG_ORDERS, "o"));
        assertThat(adapter.readByTag(TAG_INVENTORY, 0)).isEmpty();
    }

    // =========================================================================
    // Multi-tag entries
    // =========================================================================

    @Test
    void multiTagEntryVisibleFromAllTagViews() throws IOException {
        Set<LogTag> both = Set.of(TAG_ORDERS, TAG_INVENTORY);
        adapter.append(new LogEntry(0, 0, both, "dual".getBytes(), Instant.now()));

        assertThat(adapter.readByTag(TAG_ORDERS, 0)).hasSize(1);
        assertThat(adapter.readByTag(TAG_INVENTORY, 0)).hasSize(1);
    }

    // =========================================================================
    // localIdCount
    // =========================================================================

    @Test
    void localIdCountReflectsHighestLocalIdPlusOne() throws IOException {
        assertThat(adapter.getLocalIdCountForTag(TAG_ORDERS)).isEqualTo(0L);

        adapter.append(entry(0, 0, TAG_ORDERS, "e0"));
        assertThat(adapter.getLocalIdCountForTag(TAG_ORDERS)).isEqualTo(1L);

        adapter.append(entry(1, 1, TAG_ORDERS, "e1"));
        assertThat(adapter.getLocalIdCountForTag(TAG_ORDERS)).isEqualTo(2L);
    }

    // =========================================================================
    // appendBatch
    // =========================================================================

    @Test
    void appendBatchWritesAllEntriesAtomically() throws IOException {
        List<LogEntry> batch = List.of(
                entry(0, 0, TAG_ORDERS, "b0"),
                entry(1, 0, TAG_INVENTORY, "b1"),
                entry(2, 1, TAG_ORDERS, "b2")
        );
        adapter.appendBatch(batch);

        assertThat(adapter.readAll()).hasSize(3);
        assertThat(adapter.getLatestSeqnum()).isEqualTo(2L);
        assertThat(adapter.readByTag(TAG_ORDERS, 0)).hasSize(2);
        assertThat(adapter.readByTag(TAG_INVENTORY, 0)).hasSize(1);
    }

    @Test
    void appendEmptyBatchIsNoop() throws IOException {
        adapter.appendBatch(List.of());
        assertThat(adapter.readAll()).isEmpty();
        assertThat(adapter.getLatestSeqnum()).isEqualTo(-1L);
    }

    // =========================================================================
    // Trim
    // =========================================================================

    @Test
    void trimHidesEntriesBelow() throws IOException {
        adapter.append(entry(0, 0, TAG_ORDERS, "e0"));
        adapter.append(entry(1, 1, TAG_ORDERS, "e1"));
        adapter.append(entry(2, 2, TAG_ORDERS, "e2"));

        adapter.trim(2); // exclusive: hides seqnums 0, 1

        List<LogEntry> all = adapter.readAll();
        assertThat(all).hasSize(1);
        assertThat(all.get(0).seqnum()).isEqualTo(2);
    }

    @Test
    void trimIsIdempotentForSameOrLowerValue() throws IOException {
        adapter.append(entry(0, 0, TAG_ORDERS, "e0"));
        adapter.append(entry(1, 1, TAG_ORDERS, "e1"));

        adapter.trim(1);
        adapter.trim(1); // no-op
        adapter.trim(0); // no-op (lower than current)

        assertThat(adapter.readAll()).hasSize(1);
        assertThat(adapter.readAll().get(0).seqnum()).isEqualTo(1);
    }

    // =========================================================================
    // State survives reopen
    // =========================================================================

    @Test
    void metadataSurvivesCloseAndReopen() throws IOException {
        String sharedRoot = testRoot + "-reopen-" + UUID.randomUUID();

        FoundationDBPersistenceAdapter first =
                new FoundationDBPersistenceAdapter(fdbClusterFile(), sharedRoot);
        first.open();
        first.append(entry(0, 0, TAG_ORDERS,    "o1"));
        first.append(entry(1, 0, TAG_INVENTORY, "i1"));
        first.append(entry(2, 1, TAG_ORDERS,    "o2"));
        first.trim(1);
        first.close();

        FoundationDBPersistenceAdapter second =
                new FoundationDBPersistenceAdapter(fdbClusterFile(), sharedRoot);
        try {
            second.open();
            assertThat(second.getLatestSeqnum()).isEqualTo(2L);
            assertThat(second.getLocalIdCountForTag(TAG_ORDERS)).isEqualTo(2L);
            assertThat(second.getLocalIdCountForTag(TAG_INVENTORY)).isEqualTo(1L);
            // trim=1 means seqnum 0 was removed, 1 and 2 remain
            assertThat(second.readAll()).hasSize(2);
            assertThat(second.readAll().get(0).seqnum()).isEqualTo(1);
        } finally {
            second.close();
        }
    }

    // =========================================================================
    // Encode / decode round-trip (no FDB required)
    // =========================================================================

    @Test
    void encodeDecodeRoundTrip() {
        LogEntry original = new LogEntry(
                42L, 7L,
                Set.of(LogTag.of("ns", "key"), LogTag.of("other")),
                "payload".getBytes(),
                Instant.ofEpochMilli(999_000L));

        byte[]   encoded = FoundationDBPersistenceAdapter.encodeEntry(original);
        LogEntry decoded = FoundationDBPersistenceAdapter.decodeEntry(encoded);

        assertThat(decoded.seqnum()).isEqualTo(42L);
        assertThat(decoded.localId()).isEqualTo(7L);
        assertThat(decoded.tags()).isEqualTo(original.tags());
        assertThat(decoded.data()).isEqualTo("payload".getBytes());
        assertThat(decoded.timestamp().toEpochMilli()).isEqualTo(999_000L);
    }

    @Test
    void encodeDecodeEmptyPayload() {
        LogEntry original = new LogEntry(0L, 0L, Set.of(TAG_ORDERS), new byte[0], Instant.now());
        LogEntry decoded  = FoundationDBPersistenceAdapter.decodeEntry(
                FoundationDBPersistenceAdapter.encodeEntry(original));
        assertThat(decoded.data()).isEmpty();
    }

    // =========================================================================
    // Helpers
    // =========================================================================

    private static LogEntry entry(long seqnum, long localId, LogTag tag, String data) {
        return new LogEntry(seqnum, localId, Set.of(tag), data.getBytes(), Instant.now());
    }

    /** Returns the cluster file path from the env variable, or null for the FDB default. */
    private static String fdbClusterFile() {
        return System.getenv("FDB_CLUSTER_FILE"); // null → fdb.open() uses default
    }
}
