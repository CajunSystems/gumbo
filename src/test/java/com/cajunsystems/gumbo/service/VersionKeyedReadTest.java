package com.cajunsystems.gumbo.service;

import com.cajunsystems.gumbo.api.LogView;
import com.cajunsystems.gumbo.core.AppendRequest;
import com.cajunsystems.gumbo.core.AppendResult;
import com.cajunsystems.gumbo.core.LogEntry;
import com.cajunsystems.gumbo.persistence.BatchingPersistenceAdapter;
import com.cajunsystems.gumbo.core.LogTag;
import com.cajunsystems.gumbo.persistence.FileBasedPersistenceAdapter;
import com.cajunsystems.gumbo.persistence.InMemoryPersistenceAdapter;
import com.cajunsystems.gumbo.persistence.PersistenceAdapter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.nio.file.Path;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Version-keyed reads: {@code readFromVersion} addresses a tag's own stream, where
 * {@code readByTag} addresses the shared global sequence.
 *
 * <p>Every test here uses <strong>two</strong> tags in one log, deliberately. One tag in
 * a fresh log is the single configuration where a per-stream version and a global seqnum
 * are indistinguishable — which is why a seqnum-keyed cursor read looks correct until a
 * second stream shows up.
 */
class VersionKeyedReadTest {

    private static final LogTag ORDERS    = LogTag.of("orders");
    private static final LogTag INVENTORY = LogTag.of("inventory");

    @TempDir
    Path tempDir;

    private SharedLogService service;

    @AfterEach
    void tearDown() {
        if (service != null) service.close();
    }

    static Stream<Object[]> adapters() {
        return Stream.of(
                new Object[]{"in-memory",
                        (Function<Path, PersistenceAdapter>) dir -> new InMemoryPersistenceAdapter()},
                new Object[]{"file-based",
                        (Function<Path, PersistenceAdapter>) FileBasedPersistenceAdapter::new},
                new Object[]{"batching(file-based)",
                        (Function<Path, PersistenceAdapter>) dir ->
                                BatchingPersistenceAdapter.of(new FileBasedPersistenceAdapter(dir))});
    }

    /**
     * The defect this API exists to fix: a consumer holding a cursor into one stream asks
     * for everything after it and gets its whole history back, because the number it holds
     * is a version and the API it called takes a seqnum.
     */
    @ParameterizedTest(name = "{0}")
    @MethodSource("adapters")
    void seqnumKeyedTailReadOverReturnsWhenTheLogHasTwoTags(
            String name, Function<Path, PersistenceAdapter> factory) throws IOException {
        open(factory);
        // Interleaved, so ORDERS occupies seqnums 0,2,4 and versions 0,1,2.
        append(ORDERS, "o0"); append(INVENTORY, "i0");
        append(ORDERS, "o1"); append(INVENTORY, "i1");
        append(ORDERS, "o2"); append(INVENTORY, "i2");

        LogView orders = service.getView(ORDERS);

        // Consumed through version 1; ask for the rest. The seqnum-keyed read hands back
        // versions 1 and 2 — one already-consumed entry, silently reprocessed.
        assertThat(versionsOf(orders.readAfter(1).join())).containsExactly(1L, 2L);

        // The version-keyed read answers the question that was actually asked.
        assertThat(versionsOf(orders.readAfterVersion(1).join())).containsExactly(2L);
        assertThat(payloadsOf(orders.readAfterVersion(1).join())).containsExactly("o2");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("adapters")
    void readFromVersionReturnsExactlyTheTagsOwnSuffix(
            String name, Function<Path, PersistenceAdapter> factory) throws IOException {
        open(factory);
        append(ORDERS, "o0"); append(INVENTORY, "i0");
        append(ORDERS, "o1"); append(INVENTORY, "i1");
        append(ORDERS, "o2");

        LogView orders = service.getView(ORDERS);

        assertThat(payloadsOf(orders.readFromVersion(0).join())).containsExactly("o0", "o1", "o2");
        assertThat(payloadsOf(orders.readFromVersion(1).join())).containsExactly("o1", "o2");
        assertThat(payloadsOf(orders.readFromVersion(2).join())).containsExactly("o2");
        assertThat(orders.readFromVersion(3).join()).isEmpty();

        // The other tag is numbered independently: its version 1 is a different entry.
        LogView inventory = service.getView(INVENTORY);
        assertThat(payloadsOf(inventory.readFromVersion(1).join())).containsExactly("i1");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("adapters")
    void readAfterVersionIsExclusiveAndMinusOneMeansEverything(
            String name, Function<Path, PersistenceAdapter> factory) throws IOException {
        open(factory);
        append(INVENTORY, "i0");
        append(ORDERS, "o0");
        append(ORDERS, "o1");

        LogView orders = service.getView(ORDERS);

        assertThat(payloadsOf(orders.readAfterVersion(-1).join())).containsExactly("o0", "o1");
        assertThat(payloadsOf(orders.readAfterVersion(0).join())).containsExactly("o1");
        assertThat(orders.readAfterVersion(1).join()).isEmpty();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("adapters")
    void versionKeyedAndSeqnumKeyedAgreeWhenTheLogHasOneTag(
            String name, Function<Path, PersistenceAdapter> factory) throws IOException {
        open(factory);
        append(ORDERS, "o0");
        append(ORDERS, "o1");
        append(ORDERS, "o2");

        LogView orders = service.getView(ORDERS);

        // The blind spot, stated as an assertion: with one tag the two number spaces
        // coincide, so a seqnum-keyed cursor read looks correct here and only here.
        assertThat(payloadsOf(orders.readAfter(0).join()))
                .isEqualTo(payloadsOf(orders.readAfterVersion(0).join()));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("adapters")
    void getLatestVersionTracksTheTagNotTheLog(
            String name, Function<Path, PersistenceAdapter> factory) throws IOException {
        open(factory);
        LogView orders = service.getView(ORDERS);
        assertThat(orders.getLatestVersion()).isEqualTo(-1L);

        append(ORDERS, "o0");
        append(INVENTORY, "i0"); append(INVENTORY, "i1"); append(INVENTORY, "i2");
        append(ORDERS, "o1");

        // Four entries landed after ORDERS' first, but only one of them was its own.
        assertThat(orders.getLatestVersion()).isEqualTo(1L);
        assertThat(orders.getLatestSeqnum()).isEqualTo(4L);

        // Round-trip: read the tip, drain up to it, and the stream is exhausted.
        long tip = orders.getLatestVersion();
        assertThat(payloadsOf(orders.readFromVersion(0).join())).hasSize((int) tip + 1);
        assertThat(orders.readAfterVersion(tip).join()).isEmpty();
    }

    /**
     * One atomic append into two tags now takes a position in <em>each</em> stream, and
     * both stay dense from zero.
     *
     * <p>This test used to assert the opposite, as a property: *both streams cannot be
     * dense from 0*, because an entry carried one version and every tag it touched was told
     * that number. A tag carried only as a secondary therefore inherited another stream's
     * numbering — not dense, not starting at zero, and able to go backwards relative to
     * entries already delivered, so a consumer cursoring a shared queue could silently skip
     * work. It was written as a property rather than fixed expectations because which tag
     * ends up primary is {@code tags.iterator().next()} over a {@code Set.copyOf}, whose
     * iteration order Java salts per JVM run.
     *
     * <p>That salting is why the assertion can now be exact in a way it could not be
     * before: with a position per tag, neither stream's numbering depends on which tag won
     * the iteration order.
     */
    @ParameterizedTest(name = "{0}")
    @MethodSource("adapters")
    void anAtomicMultiTagAppendNumbersBothStreamsFromZero(
            String name, Function<Path, PersistenceAdapter> factory) throws IOException {
        LogTag history = LogTag.of("history", "wf-1");
        LogTag queue   = LogTag.of("queue");
        open(factory);

        for (int i = 0; i < 3; i++) append(history, "h" + i);
        // One atomic append to both — the pattern a workflow engine uses to record history
        // and enqueue work with no window where one is visible without the other.
        AppendResult r = service.append(AppendRequest.to(
                new LinkedHashSet<>(List.of(history, queue)), "work".getBytes())).join();

        assertThat(payloadsOf(service.getView(queue).readFromVersion(0).join()))
                .containsExactly("work");

        assertThat(isDenseFromZero(history))
                .as("history continues its own count: 0,1,2 then 3 (primary this run: %s)", r.primaryTag())
                .isTrue();
        assertThat(isDenseFromZero(queue))
                .as("and the queue starts its own at 0 rather than inheriting history's 3")
                .isTrue();

        assertThat(versionsIn(queue, service.getView(queue).readFromVersion(0).join()))
                .containsExactly(0L);

        // The next queue-only append continues the queue's count, not history's.
        append(queue, "next");
        assertThat(versionsIn(queue, service.getView(queue).readFromVersion(0).join()))
                .as("a queue cursor advances by one per queue entry")
                .containsExactly(0L, 1L);
    }

    /**
     * The consumer-facing consequence, and the reason this was worth a format change: a
     * worker cursoring a shared fan-out tag sees every item exactly once.
     *
     * <p>The worker consumes <em>between</em> enqueues, which is what makes the old defect
     * visible. Under the old numbering the dual-tagged entry took its workflow's history
     * position, so a worker that had advanced its cursor to 8 would be handed nothing when a
     * second workflow — whose history sat at 4 — enqueued next: the item is numbered below
     * where the cursor already is, and a version-keyed tail read skips it. Work silently
     * never claimed, and nothing in the log looking wrong.
     *
     * <p>Draining everything first and only then advancing hides it, because the items are
     * all above the initial cursor whatever their order.
     */
    @ParameterizedTest(name = "{0}")
    @MethodSource("adapters")
    void aWorkerCursoringAFanOutTagSeesEveryItemExactlyOnce(
            String name, Function<Path, PersistenceAdapter> factory) throws IOException {
        LogTag queue = LogTag.of("queue");
        open(factory);

        // History lengths descend, so the positions the queue would have inherited descend
        // too: 8, then 4, then 0 — each below the cursor the worker already holds.
        int[] historyLengths = {8, 4, 0};
        List<String> claimed = new java.util.ArrayList<>();
        long cursor = -1;

        for (int w = 0; w < historyLengths.length; w++) {
            LogTag history = LogTag.of("history", "wf-" + w);
            for (int i = 0; i < historyLengths[w]; i++) append(history, "h" + w + "-" + i);

            // Fence on the history tag explicitly, which also names the tag the entry is
            // numbered by. Left implicit, that choice is tags.iterator().next() over a Set —
            // salted per JVM run — so the old defect would land on whichever tag won.
            service.append(
                    AppendRequest.to(new LinkedHashSet<>(List.of(history, queue)),
                            ("work-" + w).getBytes()),
                    history, historyLengths[w]).join();

            // The worker drains what it can see, then advances — before the next enqueue.
            for (LogEntry e : service.getView(queue).readAfterVersion(cursor).join()) {
                claimed.add(new String(e.dataUnsafe()));
                cursor = Math.max(cursor, e.streamVersion(queue));
            }
        }

        assertThat(claimed)
                .as("every item claimed exactly once — none skipped by a cursor compared"
                        + " against another stream's numbering, none redelivered")
                .containsExactly("work-0", "work-1", "work-2");
        assertThat(cursor).as("and the cursor is the queue's own last position").isEqualTo(2L);
    }

    /** True if the tag's entries carry versions 0, 1, 2, … with no gap or repeat. */
    private boolean isDenseFromZero(LogTag tag) {
        List<Long> versions = versionsIn(tag, service.getView(tag).readFromVersion(0).join());
        for (int i = 0; i < versions.size(); i++) {
            if (versions.get(i) != i) return false;
        }
        return true;
    }

    /**
     * {@code Long.MAX_VALUE} is this codebase's sentinel for "the very end" — it is what
     * {@code DefaultLogView.checkTail} passes. Naively adding one to it wraps to
     * {@code Long.MIN_VALUE}, which every version filter reads as "no lower bound", so
     * asking for the entries after everything would return the entire stream: the exact
     * inverse of the answer, delivered silently.
     */
    @ParameterizedTest(name = "{0}")
    @MethodSource("adapters")
    void aCursorAtTheMaximumVersionReturnsNothingRatherThanEverything(
            String name, Function<Path, PersistenceAdapter> factory) throws IOException {
        open(factory);
        append(ORDERS, "o0"); append(INVENTORY, "i0"); append(ORDERS, "o1");

        LogView orders = service.getView(ORDERS);
        assertThat(orders.readAfterVersion(Long.MAX_VALUE).join()).isEmpty();
        assertThat(orders.readAfterVersion(Long.MAX_VALUE - 1).join()).isEmpty();

        // The tag is not empty — the reads above are bounded, not broken.
        assertThat(payloadsOf(orders.readAfterVersion(-1).join())).containsExactly("o0", "o1");
    }

    @Test
    void versionsSurviveAReopenOfTheLog() throws IOException {
        open(FileBasedPersistenceAdapter::new);
        append(ORDERS, "o0"); append(INVENTORY, "i0"); append(ORDERS, "o1");
        service.close();

        open(FileBasedPersistenceAdapter::new);
        LogView orders = service.getView(ORDERS);
        assertThat(payloadsOf(orders.readFromVersion(1).join())).containsExactly("o1");

        // A version assigned before the restart still addresses the same entry after it.
        append(ORDERS, "o2");
        assertThat(payloadsOf(orders.readAfterVersion(1).join())).containsExactly("o2");
    }

    // -------------------------------------------------------------------------

    private void open(Function<Path, PersistenceAdapter> factory) throws IOException {
        service = SharedLogService.open(SharedLogConfig.builder()
                .persistenceAdapter(factory.apply(tempDir))
                .build());
    }

    private void append(LogTag tag, String data) {
        service.append(AppendRequest.to(tag, data.getBytes())).join();
    }

    /**
     * Positions as seen from one stream. A read through a tag view has to ask for that
     * tag's position: {@link LogEntry#streamVersion()} answers for the entry's primary tag,
     * which for a multi-tag entry is whichever the Set iterated first this run.
     */
    private static List<Long> versionsIn(LogTag tag, List<LogEntry> entries) {
        return entries.stream().map(e -> e.streamVersion(tag)).toList();
    }

    /** Positions as the entries themselves report them; exact for single-tag streams. */
    private static List<Long> versionsOf(List<LogEntry> entries) {
        return entries.stream().map(LogEntry::streamVersion).toList();
    }

    private static List<String> payloadsOf(List<LogEntry> entries) {
        return entries.stream().map(e -> new String(e.data())).toList();
    }
}
