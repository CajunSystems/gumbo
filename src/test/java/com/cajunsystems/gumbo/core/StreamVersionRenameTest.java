package com.cajunsystems.gumbo.core;

import com.cajunsystems.gumbo.persistence.FileBasedPersistenceAdapter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@code localId} became {@code streamVersion}. The rename is source-level only — it
 * changes what the field is called, not what it holds, where it sits on disk, or what
 * value gets assigned. These tests pin all three, because a rename that quietly shifted
 * any of them would invalidate every log already written.
 */
class StreamVersionRenameTest {

    private static final LogTag TAG = LogTag.of("orders");

    @TempDir
    Path tempDir;

    @Test
    @SuppressWarnings("removal")
    void theDeprecatedAccessorsReturnTheSameValue() {
        LogEntry entry = new LogEntry(7, 3, Set.of(TAG), "x".getBytes(), Instant.now());
        assertThat(entry.streamVersion()).isEqualTo(3L);
        assertThat(entry.localId()).isEqualTo(entry.streamVersion());

        AppendResult result = new AppendResult(7, 3, TAG, Instant.now());
        assertThat(result.streamVersion()).isEqualTo(3L);
        assertThat(result.localId()).isEqualTo(result.streamVersion());
    }

    /**
     * The on-disk layout is unchanged: the version still occupies the same 8 bytes at the
     * same offset it did when it was called {@code localId}. This is what makes the
     * rename free — a log written by an earlier version reads back identically, with no
     * migration and no version stamp.
     */
    @Test
    void theOnDiskLayoutIsUnchanged() throws IOException {
        FileBasedPersistenceAdapter adapter = new FileBasedPersistenceAdapter(tempDir);
        adapter.open();
        try {
            adapter.append(new LogEntry(0, 0, Set.of(TAG), "a".getBytes(), Instant.now()));
            adapter.append(new LogEntry(1, 1, Set.of(TAG), "b".getBytes(), Instant.now()));
        } finally {
            adapter.close();
        }

        // Header layout, unchanged from 0.2.0:
        //   magic(4) | seqnum(8) | timestamp(8) | version(8) | numTags(4) | …
        byte[] raw = Files.readAllBytes(tempDir.resolve("log.dat"));
        ByteBuffer buf = ByteBuffer.wrap(raw);
        assertThat(buf.getInt(0)).isEqualTo(0xC0FFEE42);
        assertThat(buf.getLong(4)).isEqualTo(0L);   // seqnum
        assertThat(buf.getLong(20)).isEqualTo(0L);  // version, still the third field

        FileBasedPersistenceAdapter reopened = new FileBasedPersistenceAdapter(tempDir);
        reopened.open();
        try {
            List<LogEntry> entries = reopened.readAll();
            assertThat(entries).hasSize(2);
            assertThat(entries.get(1).streamVersion()).isEqualTo(1L);
            assertThat(reopened.getNextStreamVersion(TAG)).isEqualTo(2L);
        } finally {
            reopened.close();
        }
    }

    /**
     * Versions stay dense from zero and keep counting across a reopen — the property the
     * decision to keep a dense counter (rather than exposing the global seqnum as the
     * version) was made to preserve, and the one a persisted cursor depends on.
     */
    @Test
    void versionsStayDenseAcrossAReopen() throws IOException {
        FileBasedPersistenceAdapter adapter = new FileBasedPersistenceAdapter(tempDir);
        adapter.open();
        try {
            for (int i = 0; i < 3; i++) {
                adapter.append(new LogEntry(i, i, Set.of(TAG), ("e" + i).getBytes(), Instant.now()));
            }
        } finally {
            adapter.close();
        }

        FileBasedPersistenceAdapter reopened = new FileBasedPersistenceAdapter(tempDir);
        reopened.open();
        try {
            assertThat(reopened.readAll().stream().map(LogEntry::streamVersion))
                    .containsExactly(0L, 1L, 2L);
            assertThat(reopened.getNextStreamVersion(TAG))
                    .as("a restart continues the sequence rather than restarting it")
                    .isEqualTo(3L);
        } finally {
            reopened.close();
        }
    }
}
