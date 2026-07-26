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
     * The boundary of what a version means today, pinned rather than papered over.
     *
     * <p>An entry carries <em>one</em> {@code streamVersion}, drawn from its primary tag's
     * counter, so an atomic multi-tag append leaves one of the two streams mis-numbered:
     * the fan-out tag's first entry is numbered 3 rather than 0 if the history tag is
     * primary, and the history tag's stream reads {@code 0,1,2,0} — a repeated version —
     * if the queue tag is. Which one happens is not the caller's choice: the primary is
     * {@code tags.iterator().next()} over a {@code Set.copyOf}, whose iteration order
     * Java salts per JVM run, so the same program numbers its streams differently across
     * restarts. The assertion below is written on the property rather than on either
     * outcome for exactly that reason.
     *
     * <p>Nothing in a read-side change can fix this — it needs a version per tag per
     * entry, i.e. storage-owned per-tag versions. So the contract says version-keyed
     * reads address a tag's own primary stream, and this test is what starts failing
     * when that lands.
     */
    @Test
    void anAtomicMultiTagAppendLeavesOneStreamMisNumbered() throws IOException {
        LogTag history = LogTag.of("history", "wf-1");
        LogTag queue   = LogTag.of("queue");
        open(dir -> new InMemoryPersistenceAdapter());

        for (int i = 0; i < 3; i++) append(history, "h" + i);
        // One atomic append to both — the pattern a workflow engine uses to record
        // history and enqueue work with no window where one is visible without the other.
        AppendResult r = service.append(AppendRequest.to(
                new LinkedHashSet<>(List.of(history, queue)), "work".getBytes())).join();

        assertThat(payloadsOf(service.getView(queue).readFromVersion(0).join()))
                .containsExactly("work");

        assertThat(isDenseFromZero(history) && isDenseFromZero(queue))
                .as("both streams cannot be densely numbered from 0 — one entry, one version"
                        + " (primary tag this run: %s)", r.primaryTag())
                .isFalse();

        // The primary tag's own stream is the one that stays correct.
        assertThat(isDenseFromZero(r.primaryTag())).isTrue();
    }

    /** True if the tag's entries carry versions 0, 1, 2, … with no gap or repeat. */
    private boolean isDenseFromZero(LogTag tag) {
        List<Long> versions = versionsOf(service.getView(tag).readFromVersion(0).join());
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

    private static List<Long> versionsOf(List<LogEntry> entries) {
        return entries.stream().map(LogEntry::streamVersion).toList();
    }

    private static List<String> payloadsOf(List<LogEntry> entries) {
        return entries.stream().map(e -> new String(e.data())).toList();
    }
}
