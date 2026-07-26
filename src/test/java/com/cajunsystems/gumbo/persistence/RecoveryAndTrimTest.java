package com.cajunsystems.gumbo.persistence;

import com.cajunsystems.gumbo.core.LogEntry;
import com.cajunsystems.gumbo.core.LogTag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The paths that only run after something has already gone wrong.
 *
 * <p>Mutation testing put the surviving mutants overwhelmingly here — index recovery,
 * trim, and the KV replay — rather than in the append and read paths. That is the same
 * shape as every defect found in review: the code that runs when things are fine is well
 * covered, and the code that runs when they are not is where the gaps are.
 */
class RecoveryAndTrimTest {

    private static final LogTag ORDERS = LogTag.of("orders");

    @TempDir
    Path tempDir;

    /**
     * A truncated {@code index.dat} must be rejected and the index rebuilt from the log.
     *
     * <p>The existing coverage deletes the index, which is the easy case. A crash during
     * an index append leaves a <em>partial</em> record instead — a file whose size is not
     * a multiple of 16 — and the size check that detects it survived every mutation,
     * including replacing the modulus with a multiplication. Nothing was checking that a
     * torn index is noticed rather than loaded.
     */
    @Test
    void aTruncatedIndexIsRebuiltFromTheLog() throws IOException {
        FileBasedPersistenceAdapter adapter = new FileBasedPersistenceAdapter(tempDir);
        adapter.open();
        try {
            for (int i = 0; i < 4; i++) adapter.append(entry(i));
        } finally {
            adapter.close();
        }

        // Chop a partial record off the end, as a crash mid-append would.
        Path index = tempDir.resolve("index.dat");
        long full = Files.size(index);
        assertThat(full % 16).isZero();
        try (FileChannel ch = FileChannel.open(index, StandardOpenOption.WRITE)) {
            ch.truncate(full - 7);
        }
        assertThat(Files.size(index) % 16).isNotZero();

        FileBasedPersistenceAdapter reopened = new FileBasedPersistenceAdapter(tempDir);
        reopened.open();
        try {
            assertThat(reopened.readAll().stream().map(LogEntry::seqnum))
                    .as("a torn index must be discarded and rebuilt, not partially trusted")
                    .containsExactly(0L, 1L, 2L, 3L);
            assertThat(reopened.getLatestSeqnum()).isEqualTo(3L);
            assertThat(reopened.getNextStreamVersion(ORDERS)).isEqualTo(4L);
        } finally {
            reopened.close();
        }
    }

    /** A trim point must survive a reopen, or trimmed entries reappear on restart. */
    @Test
    void aTrimPointSurvivesAReopen() throws IOException {
        FileBasedPersistenceAdapter adapter = new FileBasedPersistenceAdapter(tempDir);
        adapter.open();
        try {
            for (int i = 0; i < 4; i++) adapter.append(entry(i));
            adapter.trim(2);
            assertThat(adapter.readAll().stream().map(LogEntry::seqnum)).containsExactly(2L, 3L);
        } finally {
            adapter.close();
        }

        FileBasedPersistenceAdapter reopened = new FileBasedPersistenceAdapter(tempDir);
        reopened.open();
        try {
            assertThat(reopened.readAll().stream().map(LogEntry::seqnum))
                    .as("the trim point must be durable; entries 0 and 1 are gone for good")
                    .containsExactly(2L, 3L);
            assertThat(reopened.readByTag(ORDERS, 0).stream().map(LogEntry::seqnum))
                    .containsExactly(2L, 3L);
        } finally {
            reopened.close();
        }
    }

    /**
     * The batching decorator must forward {@code trim} to its delegate.
     *
     * <p>Deleting that call left every test passing. Trim is the one operation that
     * destroys data, and a decorator silently dropping it means storage is never
     * reclaimed while the caller is told it was — the failure being invisible is the
     * whole problem.
     */
    @Test
    void batchingForwardsTrimToItsDelegate() throws IOException {
        FileBasedPersistenceAdapter file = new FileBasedPersistenceAdapter(tempDir);
        BatchingPersistenceAdapter adapter = new BatchingPersistenceAdapter(file, 64, 600_000);
        adapter.open();
        try {
            for (int i = 0; i < 4; i++) adapter.append(entry(i));
            adapter.flushNow();
            adapter.trim(2);

            assertThat(file.readAll().stream().map(LogEntry::seqnum))
                    .as("trim must reach the delegate, not stop at the decorator")
                    .containsExactly(2L, 3L);
        } finally {
            adapter.close();
        }
    }

    private static LogEntry entry(long n) {
        return new LogEntry(n, n, Set.of(ORDERS), ("e" + n).getBytes(), Instant.now());
    }
}
