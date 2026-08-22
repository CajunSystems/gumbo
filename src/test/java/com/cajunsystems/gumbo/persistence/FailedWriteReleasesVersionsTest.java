package com.cajunsystems.gumbo.persistence;

import com.cajunsystems.gumbo.core.LogEntry;
import com.cajunsystems.gumbo.core.LogTag;
import com.cajunsystems.gumbo.core.PendingAppend;
import com.cajunsystems.gumbo.core.VersionConflictException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A position is claimed before the entry is written, because the entry has to carry it. If
 * the write or the fsync then fails, that position was consumed by nothing.
 *
 * <p>Two things break if it is not handed back. The stream gets a permanent hole, and
 * density is what every persisted cursor relies on. And the counter sits ahead of what is
 * durable, so a conditional append at the position the log actually ends on is rejected as
 * stale — a fence guarding an entry that was never written.
 *
 * <p>This adapter already draws that line for reads: {@code publish} is separate from
 * {@code writeNoSync} precisely so an entry becomes visible only once its bytes are durable.
 * These tests hold the counter to the same rule.
 */
class FailedWriteReleasesVersionsTest {

    private static final LogTag HISTORY = LogTag.of("history", "wf-1");
    private static final LogTag QUEUE   = LogTag.of("queue");

    @TempDir
    Path tempDir;

    private FailableAdapter adapter;

    @AfterEach
    void tearDown() throws IOException {
        if (adapter != null) adapter.close();
    }

    @Test
    void aFailedSingleTagAppendGivesItsPositionBack() throws IOException {
        open();
        adapter.append(pending(0, HISTORY, Set.of(HISTORY)), PersistenceAdapter.ANY_VERSION);
        assertThat(adapter.getNextStreamVersion(HISTORY)).isEqualTo(1L);

        adapter.failSync = true;
        assertThatThrownBy(() ->
                adapter.append(pending(1, HISTORY, Set.of(HISTORY)), PersistenceAdapter.ANY_VERSION))
                .isInstanceOf(IOException.class);
        adapter.failSync = false;

        assertThat(adapter.getNextStreamVersion(HISTORY))
                .as("the failed entry consumed nothing, so the next append is still 1")
                .isEqualTo(1L);
    }

    /**
     * The multi-tag case, which is why this matters more than it used to: a failed append
     * once burned one position and now touches every tag it named.
     */
    @Test
    void aFailedMultiTagAppendGivesEveryTagsPositionBack() throws IOException {
        open();
        adapter.append(pending(0, HISTORY, Set.of(HISTORY)), PersistenceAdapter.ANY_VERSION);

        adapter.failSync = true;
        assertThatThrownBy(() -> adapter.append(
                pending(1, HISTORY, Set.of(HISTORY, QUEUE)), PersistenceAdapter.ANY_VERSION))
                .isInstanceOf(IOException.class);
        adapter.failSync = false;

        assertThat(adapter.getNextStreamVersion(HISTORY)).isEqualTo(1L);
        assertThat(adapter.getNextStreamVersion(QUEUE))
                .as("the queue never received an entry, so its stream has not started")
                .isEqualTo(0L);
    }

    /**
     * The consequence a caller actually hits: after a failed write, a conditional append at
     * the position the log really ends on must be accepted. A leaked counter rejects it as
     * stale, and the writer is not stale — the log is exactly where it thinks it is.
     */
    @Test
    void aConditionalAppendAtTheRealTipIsAcceptedAfterAFailedWrite() throws IOException {
        open();
        adapter.append(pending(0, HISTORY, Set.of(HISTORY)), PersistenceAdapter.ANY_VERSION);

        adapter.failSync = true;
        assertThatThrownBy(() ->
                adapter.append(pending(1, HISTORY, Set.of(HISTORY)), PersistenceAdapter.ANY_VERSION))
                .isInstanceOf(IOException.class);
        adapter.failSync = false;

        assertThatCode(() -> adapter.append(pending(2, HISTORY, Set.of(HISTORY)), 1L))
                .as("version 1 is genuinely free; a fence must not guard an entry never written")
                .doesNotThrowAnyException();
    }

    @Test
    void aFailedBatchGivesBackEveryPositionItClaimed() throws IOException {
        open();
        adapter.failSync = true;
        assertThatThrownBy(() -> adapter.appendBatchAssigningVersions(List.of(
                pending(0, HISTORY, Set.of(HISTORY, QUEUE)),
                pending(1, HISTORY, Set.of(HISTORY)),
                pending(2, QUEUE, Set.of(QUEUE)))))
                .isInstanceOf(IOException.class);
        adapter.failSync = false;

        assertThat(adapter.getNextStreamVersion(HISTORY))
                .as("two claims in the batch, both handed back")
                .isEqualTo(0L);
        assertThat(adapter.getNextStreamVersion(QUEUE)).isEqualTo(0L);

        // And the streams still number from zero once a write does land.
        LogEntry ok = adapter.append(pending(3, HISTORY, Set.of(HISTORY, QUEUE)),
                PersistenceAdapter.ANY_VERSION);
        assertThat(ok.streamVersion(HISTORY)).isEqualTo(0L);
        assertThat(ok.streamVersion(QUEUE)).isEqualTo(0L);
    }

    /** A rejected fence must not consume anything either — it never got as far as a write. */
    @Test
    void aRejectedConditionalAppendConsumesNothing() throws IOException {
        open();
        adapter.append(pending(0, HISTORY, Set.of(HISTORY)), PersistenceAdapter.ANY_VERSION);

        assertThatThrownBy(() -> adapter.append(
                pending(1, HISTORY, Set.of(HISTORY, QUEUE)), 7L))
                .isInstanceOf(VersionConflictException.class);

        assertThat(adapter.getNextStreamVersion(HISTORY)).isEqualTo(1L);
        assertThat(adapter.getNextStreamVersion(QUEUE))
                .as("the queue was named by an append that was refused before it began")
                .isEqualTo(0L);
    }

    private void open() throws IOException {
        adapter = new FailableAdapter(tempDir);
        adapter.open();
    }

    private static PendingAppend pending(long seqnum, LogTag primary, Set<LogTag> tags) {
        return new PendingAppend(seqnum, primary, tags, ("e" + seqnum).getBytes(), Instant.now());
    }

    /**
     * Makes the fsync fail on demand. {@code syncChannels} is package-private for exactly
     * this: it is the one durability boundary that cannot be provoked from outside the class.
     */
    private static final class FailableAdapter extends FileBasedPersistenceAdapter {
        volatile boolean failSync = false;

        FailableAdapter(Path dir) { super(dir); }

        @Override
        void syncChannels() throws IOException {
            if (failSync) throw new IOException("fsync failed");
            super.syncChannels();
        }
    }
}
