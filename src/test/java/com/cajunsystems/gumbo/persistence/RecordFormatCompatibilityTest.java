package com.cajunsystems.gumbo.persistence;

import com.cajunsystems.gumbo.core.LogEntry;
import com.cajunsystems.gumbo.core.LogTag;
import com.cajunsystems.gumbo.core.PendingAppend;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.zip.CRC32;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A log written before entries carried a version per tag still reads.
 *
 * <p>This is the guarantee the format change had to keep, and it is no longer checkable by
 * writing a log and reading it back: the adapter only writes the new layout now. So these
 * tests hand-assemble records in the old layout — the only honest way to test compatibility
 * with bytes the current code can no longer produce.
 *
 * <p>The record marker is what distinguishes them, so a single log may hold both. That
 * matters more than it sounds: an existing log does not get rewritten on upgrade, it simply
 * grows new-format records on the end, and every read has to cope with the mixture from
 * then on.
 */
class RecordFormatCompatibilityTest {

    private static final int MAGIC_V1 = 0xC0FFEE42;
    private static final LogTag HISTORY = LogTag.of("history", "wf-1");
    private static final LogTag QUEUE   = LogTag.of("queue");

    @TempDir
    Path tempDir;

    @Test
    void aLogOfOldRecordsReadsBackWithItsVersionsIntact() throws IOException {
        writeV1Log(
                v1Record(0, 0, Set.of(HISTORY), "a"),
                v1Record(1, 1, Set.of(HISTORY), "b"),
                v1Record(2, 2, Set.of(HISTORY), "c"));

        FileBasedPersistenceAdapter adapter = new FileBasedPersistenceAdapter(tempDir);
        adapter.open();
        try {
            assertThat(adapter.readAll()).hasSize(3);
            assertThat(adapter.readAll().stream().map(LogEntry::streamVersion))
                    .containsExactly(0L, 1L, 2L);
            assertThat(adapter.getNextStreamVersion(HISTORY))
                    .as("the counter is rebuilt from records the current code never wrote")
                    .isEqualTo(3L);
            assertThat(adapter.readFromVersion(HISTORY, 1))
                    .as("and version-keyed reads still work over them")
                    .hasSize(2);
        } finally {
            adapter.close();
        }
    }

    /**
     * A single-tag old record is exact and says so. One tag, one number, nothing the record
     * failed to record — {@link LogEntry#hasPerTagVersions()} is about ambiguity, not about
     * which release wrote the bytes.
     */
    @Test
    void anOldSingleTagRecordReportsItsVersionAsGenuinelyItsOwn() throws IOException {
        writeV1Log(v1Record(0, 7, Set.of(HISTORY), "a"));

        FileBasedPersistenceAdapter adapter = new FileBasedPersistenceAdapter(tempDir);
        adapter.open();
        try {
            LogEntry entry = adapter.readAll().get(0);
            assertThat(entry.streamVersion(HISTORY)).isEqualTo(7L);
            assertThat(entry.hasPerTagVersions()).isTrue();
        } finally {
            adapter.close();
        }
    }

    /**
     * An old <em>multi-tag</em> record cannot say which tag its one number counted, because
     * the writer picked the primary with {@code tags.iterator().next()} over a Set whose
     * iteration order Java salts per JVM run. It answers every tag with that number, exactly
     * as it always did, and reports that the answer is not per tag — so a consumer can tell
     * the difference rather than being quietly guessed at.
     */
    @Test
    void anOldMultiTagRecordAnswersEveryTagAndAdmitsTheAmbiguity() throws IOException {
        writeV1Log(v1Record(0, 3, Set.of(HISTORY, QUEUE), "work"));

        FileBasedPersistenceAdapter adapter = new FileBasedPersistenceAdapter(tempDir);
        adapter.open();
        try {
            LogEntry entry = adapter.readAll().get(0);
            assertThat(entry.streamVersion(HISTORY)).isEqualTo(3L);
            assertThat(entry.streamVersion(QUEUE)).isEqualTo(3L);
            assertThat(entry.hasPerTagVersions())
                    .as("one number, two streams — the record cannot say which it counted")
                    .isFalse();
        } finally {
            adapter.close();
        }
    }

    /**
     * The case an upgrade actually produces: old records, then new ones appended after it.
     * Both are read, and the new appends continue each tag's count from where the old
     * records left it rather than restarting.
     */
    @Test
    void aLogMixingBothLayoutsReadsAndKeepsCounting() throws IOException {
        writeV1Log(
                v1Record(0, 0, Set.of(HISTORY), "old-0"),
                v1Record(1, 1, Set.of(HISTORY), "old-1"));

        FileBasedPersistenceAdapter adapter = new FileBasedPersistenceAdapter(tempDir);
        adapter.open();
        try {
            LogEntry fresh = adapter.append(
                    new PendingAppend(2, HISTORY, Set.of(HISTORY, QUEUE), "new".getBytes(),
                            Instant.now()),
                    PersistenceAdapter.ANY_VERSION);

            assertThat(fresh.streamVersion(HISTORY))
                    .as("continues the old records' count")
                    .isEqualTo(2L);
            assertThat(fresh.streamVersion(QUEUE))
                    .as("and starts the queue's own count at zero")
                    .isEqualTo(0L);
            assertThat(fresh.hasPerTagVersions()).isTrue();
        } finally {
            adapter.close();
        }

        // And all of it survives the reopen, which is where a mis-sized cursor would show up:
        // the scan advances record by record, and the two layouts have different strides.
        FileBasedPersistenceAdapter reopened = new FileBasedPersistenceAdapter(tempDir);
        reopened.open();
        try {
            assertThat(reopened.readAll()).hasSize(3);
            assertThat(reopened.getNextStreamVersion(HISTORY)).isEqualTo(3L);
            assertThat(reopened.getNextStreamVersion(QUEUE)).isEqualTo(1L);
            assertThat(reopened.readFromVersion(QUEUE, 0)).hasSize(1);
        } finally {
            reopened.close();
        }
    }

    // -------------------------------------------------------------------------
    // Hand-assembled records in the layout this code no longer writes
    // -------------------------------------------------------------------------

    /**
     * Writes {@code log.dat} and the matching {@code index.dat}, which is what an older
     * adapter would have left behind. Writing only the log would be a fixture no release
     * ever produced — the adapter trusts a present index and would read just the records it
     * lists, so the test would be measuring the gap in its own setup.
     */
    private void writeV1Log(byte[]... records) throws IOException {
        Files.createDirectories(tempDir);
        try (var log = java.nio.channels.FileChannel.open(tempDir.resolve("log.dat"),
                     StandardOpenOption.CREATE, StandardOpenOption.WRITE);
             var idx = java.nio.channels.FileChannel.open(tempDir.resolve("index.dat"),
                     StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
            long offset = 0;
            for (byte[] r : records) {
                log.write(ByteBuffer.wrap(r));
                ByteBuffer e = ByteBuffer.allocate(16);
                e.putLong(ByteBuffer.wrap(r).getLong(4));   // seqnum, from the record itself
                e.putLong(offset);
                idx.write(e.flip());
                offset += r.length;
            }
        }
    }

    /**
     * The same fixture with no {@code index.dat}, forcing the full-log-scan recovery path.
     * That scan advances a cursor record by record, and the two layouts have different
     * strides — so it is the one path where reading an old record with the new code's
     * arithmetic would desynchronise and stop at a bad magic.
     */
    private void writeV1LogWithoutIndex(byte[]... records) throws IOException {
        Files.createDirectories(tempDir);
        try (var ch = java.nio.channels.FileChannel.open(tempDir.resolve("log.dat"),
                StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
            for (byte[] r : records) ch.write(ByteBuffer.wrap(r));
        }
    }

    private static byte[] v1Record(long seqnum, long version, Set<LogTag> tags, String payload) {
        byte[] data = payload.getBytes(StandardCharsets.UTF_8);
        int tagsLen = 0;
        for (LogTag t : tags) {
            tagsLen += 2 + t.namespace().getBytes(StandardCharsets.UTF_8).length;
            tagsLen += 2 + t.key().getBytes(StandardCharsets.UTF_8).length;
        }
        int total = 40 + tagsLen + data.length;   // FIXED_OVERHEAD(40) + tags + data

        ByteBuffer buf = ByteBuffer.allocate(total);
        buf.putInt(MAGIC_V1);
        buf.putLong(seqnum);
        buf.putLong(Instant.now().toEpochMilli());
        buf.putLong(version);
        buf.putInt(tags.size());
        for (LogTag t : tags) {
            byte[] ns  = t.namespace().getBytes(StandardCharsets.UTF_8);
            byte[] key = t.key().getBytes(StandardCharsets.UTF_8);
            buf.putShort((short) ns.length);
            buf.put(ns);
            buf.putShort((short) key.length);
            buf.put(key);
            // deliberately no per-tag version — that is what makes this the old layout
        }
        buf.putInt(data.length);
        buf.put(data);

        CRC32 crc = new CRC32();
        crc.update(buf.array(), 0, total - 4);
        buf.putInt((int) crc.getValue());
        return buf.array();
    }

    /**
     * Recovery by full log scan over a mixed log — no index to lean on, so every record's
     * length has to be computed from the layout it was actually written in. Getting that
     * wrong lands the cursor mid-record and the scan stops early, which is exactly the
     * failure that would silently truncate a log on upgrade.
     */
    @Test
    void aFullScanOverMixedLayoutsFindsEveryRecord() throws IOException {
        writeV1LogWithoutIndex(
                v1Record(0, 0, Set.of(HISTORY), "old-0"),
                v1Record(1, 0, Set.of(HISTORY, QUEUE), "old-multi"),
                v1Record(2, 2, Set.of(HISTORY), "old-2"));

        FileBasedPersistenceAdapter adapter = new FileBasedPersistenceAdapter(tempDir);
        adapter.open();
        try {
            adapter.append(new PendingAppend(3, HISTORY, Set.of(HISTORY), "new".getBytes(),
                    Instant.now()), PersistenceAdapter.ANY_VERSION);
        } finally {
            adapter.close();
        }

        Files.delete(tempDir.resolve("index.dat"));   // force the scan again, now mixed

        FileBasedPersistenceAdapter reopened = new FileBasedPersistenceAdapter(tempDir);
        reopened.open();
        try {
            assertThat(reopened.readAll())
                    .as("a mis-sized cursor stops the scan at the first record it lands inside")
                    .hasSize(4);
        } finally {
            reopened.close();
        }
    }

    /** Sanity: the fixture really is the old layout, or these tests prove nothing. */
    @Test
    void theFixtureIsGenuinelyTheOldLayout() {
        byte[] record = v1Record(0, 0, Set.of(HISTORY), "a");
        assertThat(ByteBuffer.wrap(record).getInt(0)).isEqualTo(MAGIC_V1);

        List<Integer> newMagics = List.of(0xC0FFEE43);
        assertThat(newMagics).doesNotContain(ByteBuffer.wrap(record).getInt(0));
    }
}
