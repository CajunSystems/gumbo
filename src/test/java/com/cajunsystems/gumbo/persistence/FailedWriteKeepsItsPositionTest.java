package com.cajunsystems.gumbo.persistence;

import com.cajunsystems.gumbo.core.LogEntry;
import com.cajunsystems.gumbo.core.LogTag;
import com.cajunsystems.gumbo.core.PendingAppend;
import com.cajunsystems.gumbo.core.VersionConflictException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * An append that fails keeps the positions it claimed, and this is the deliberate choice
 * rather than the absence of one.
 *
 * <p>Handing them back is the tidier-looking option and was written first: it keeps the
 * stream dense and stops the counter sitting ahead of what is durable. It is not safe,
 * because <strong>a failed fsync does not mean the record is gone</strong> — the bytes are
 * already in both channels by then, and whether they reach the platter is exactly what the
 * failed call leaves undecided. Release the position and a retry reuses it; if the first
 * record did land, two entries end up at the same position in the same stream.
 *
 * <p>So the trade is a hole against a duplicate, and they are not equal. A hole breaks
 * density — a documented invariant — but a cursor still advances monotonically and nothing
 * is delivered twice. A duplicate makes a version-keyed consumer deliver one slot twice,
 * silently, with the entry that ought to be there indistinguishable from the one that
 * replaced it. And a hole heals on reopen, because the counters are rebuilt from the log.
 *
 * <p>The first test below is the evidence for all of that: after an injected sync failure
 * the record is still there.
 */
class FailedWriteKeepsItsPositionTest {

    private static final LogTag HISTORY = LogTag.of("history", "wf-1");
    private static final LogTag QUEUE   = LogTag.of("queue");

    @TempDir
    Path tempDir;

    /**
     * The premise, measured rather than assumed: a record whose fsync failed is still in the
     * log afterwards. Everything else here follows from this, and if it ever stopped being
     * true the reasoning would need revisiting rather than the code.
     */
    @Test
    void aRecordWhoseSyncFailedIsStillInTheLog() throws IOException {
        FailableAdapter adapter = new FailableAdapter(tempDir);
        adapter.open();
        try {
            adapter.failSync = true;
            assertThatThrownBy(() ->
                    adapter.append(pending(0, HISTORY, Set.of(HISTORY)), PersistenceAdapter.ANY_VERSION))
                    .isInstanceOf(IOException.class);
        } finally {
            adapter.failSync = false;
            adapter.close();
        }

        FileBasedPersistenceAdapter reopened = new FileBasedPersistenceAdapter(tempDir);
        reopened.open();
        try {
            assertThat(reopened.readAll())
                    .as("the bytes were in the channel before the fsync was even attempted")
                    .hasSize(1);
        } finally {
            reopened.close();
        }
    }

    /**
     * Because that record may exist, its position stays consumed: the next append gets the
     * one after it, not the same one. Reusing it is what would produce two entries sharing a
     * position.
     */
    @Test
    void theNextAppendDoesNotReuseAFailedAppendsPosition() throws IOException {
        FailableAdapter adapter = new FailableAdapter(tempDir);
        adapter.open();
        try {
            adapter.failSync = true;
            assertThatThrownBy(() ->
                    adapter.append(pending(0, HISTORY, Set.of(HISTORY)), PersistenceAdapter.ANY_VERSION))
                    .isInstanceOf(IOException.class);
            adapter.failSync = false;

            LogEntry next = adapter.append(pending(1, HISTORY, Set.of(HISTORY)),
                    PersistenceAdapter.ANY_VERSION);
            assertThat(next.streamVersion(HISTORY))
                    .as("position 0 may already be taken by the record whose sync failed")
                    .isEqualTo(1L);
        } finally {
            adapter.close();
        }
    }

    /**
     * And the ambiguity is resolved by reading, not by guessing: a reopen rebuilds the
     * counters from the records actually in the log. Here the failed record did land, so the
     * count reflects it and the stream is dense after all.
     */
    @Test
    void aReopenRebuildsTheCountFromWhatIsActuallyThere() throws IOException {
        FailableAdapter adapter = new FailableAdapter(tempDir);
        adapter.open();
        try {
            adapter.failSync = true;
            assertThatThrownBy(() ->
                    adapter.append(pending(0, HISTORY, Set.of(HISTORY, QUEUE)),
                            PersistenceAdapter.ANY_VERSION))
                    .isInstanceOf(IOException.class);
        } finally {
            adapter.failSync = false;
            adapter.close();
        }

        FileBasedPersistenceAdapter reopened = new FileBasedPersistenceAdapter(tempDir);
        reopened.open();
        try {
            assertThat(reopened.getNextStreamVersion(HISTORY)).isEqualTo(1L);
            assertThat(reopened.getNextStreamVersion(QUEUE))
                    .as("the entry was in both streams, so both counts reflect it")
                    .isEqualTo(1L);
            assertThat(reopened.readFromVersion(QUEUE, 0)).hasSize(1);
        } finally {
            reopened.close();
        }
    }

    /**
     * A fence rejected before the write is a different case entirely, and it consumes
     * nothing — the check happens before any counter moves, so a stale writer cannot burn a
     * position in each of the other tags on its way to being refused.
     */
    @Test
    void aRejectedConditionalAppendConsumesNothing() throws IOException {
        FileBasedPersistenceAdapter adapter = new FileBasedPersistenceAdapter(tempDir);
        adapter.open();
        try {
            adapter.append(pending(0, HISTORY, Set.of(HISTORY)), PersistenceAdapter.ANY_VERSION);

            assertThatThrownBy(() ->
                    adapter.append(pending(1, HISTORY, Set.of(HISTORY, QUEUE)), 7L))
                    .isInstanceOf(VersionConflictException.class);

            assertThat(adapter.getNextStreamVersion(HISTORY)).isEqualTo(1L);
            assertThat(adapter.getNextStreamVersion(QUEUE))
                    .as("the queue was named by an append refused before it began")
                    .isEqualTo(0L);
        } finally {
            adapter.close();
        }
    }

    /** A failed batch behaves the same way, for the same reason: its records may be there. */
    @Test
    void aFailedBatchKeepsThePositionsItClaimed() throws IOException {
        FailableAdapter adapter = new FailableAdapter(tempDir);
        adapter.open();
        try {
            adapter.failSync = true;
            assertThatThrownBy(() -> adapter.appendBatchAssigningVersions(List.of(
                    pending(0, HISTORY, Set.of(HISTORY, QUEUE)),
                    pending(1, HISTORY, Set.of(HISTORY)))))
                    .isInstanceOf(IOException.class);
            adapter.failSync = false;

            assertThat(adapter.getNextStreamVersion(HISTORY)).isEqualTo(2L);
            assertThat(adapter.getNextStreamVersion(QUEUE)).isEqualTo(1L);
        } finally {
            adapter.close();
        }
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
