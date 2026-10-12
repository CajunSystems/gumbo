package com.cajunsystems.gumbo.persistence;

import com.cajunsystems.gumbo.core.LogEntry;
import com.cajunsystems.gumbo.core.LogTag;
import com.cajunsystems.gumbo.core.PendingAppend;
import com.cajunsystems.gumbo.core.VersionConflictException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Two {@link FoundationDBPersistenceAdapter}s on one root, in one JVM, so each case can be
 * interleaved by hand and the outcome is deterministic. Everything here is about what one
 * instance sees of another's writes; what happens when they genuinely race is measured
 * across two JVMs in {@code FoundationDBMultiWriterTest}.
 *
 * <p>Two of these pin defects found by reading the adapter against Catalyst's Dist-0 list
 * (#38) and then confirmed here before being fixed: three metadata reads answered from
 * per-process caches, and a {@code meta/latest} write that could move backwards. The rest
 * state, as assertions, what the FDB adapter is relied on for and had only prose for: the
 * capability declarations it is the sole adapter to make, and that the file adapter's D2
 * (a per-process index that clobbers the other process's view) does not apply to it.
 */
class FoundationDBMultiInstanceTest {

    private static final LogTag ORDERS    = LogTag.of("orders");
    private static final LogTag INVENTORY = LogTag.of("inventory");

    private static boolean fdbAvailable;

    private String root;
    private FoundationDBPersistenceAdapter a;
    private FoundationDBPersistenceAdapter b;
    private final List<FoundationDBPersistenceAdapter> opened = new ArrayList<>();

    @BeforeAll
    static void checkFdbAvailability() {
        fdbAvailable = FdbTestSupport.probe("gumbo-mi");
    }

    @BeforeEach
    void setUp() throws IOException {
        FdbTestSupport.assumeAvailable(fdbAvailable);
        root = FdbTestSupport.freshRoot("gumbo-mi");
        a = open();
        b = open();
    }

    @AfterEach
    void tearDown() throws IOException {
        for (FoundationDBPersistenceAdapter adapter : opened) adapter.close();
    }

    // -------------------------------------------------------------------------
    // Ask 2 of #38: per-process caches
    // -------------------------------------------------------------------------

    /**
     * Both instances were open before either wrote, so a value loaded at open time cannot
     * explain a correct answer here: it has to come from the store on each call.
     */
    @Test
    void metadataIsReadFromTheStoreNotFromWhatThisProcessWrote() throws IOException {
        a.append(pending(0, ORDERS), PersistenceAdapter.ANY_VERSION);

        assertThat(b.getLatestSeqnum()).as("b never wrote; the seqnum is a's").isEqualTo(0L);
        assertThat(b.getNextStreamVersion(ORDERS)).isEqualTo(1L);
        assertThat(b.getLatestSeqnumForTag(ORDERS)).isEqualTo(0L);

        b.append(pending(1, ORDERS), PersistenceAdapter.ANY_VERSION);

        assertThat(a.getLatestSeqnum()).isEqualTo(1L);
        assertThat(a.getNextStreamVersion(ORDERS)).isEqualTo(2L);
        assertThat(a.getLatestSeqnumForTag(ORDERS)).isEqualTo(1L);
    }

    /** The three ways an entry reaches the store. Each used to publish a local maximum. */
    static Stream<Object[]> writePaths() {
        return Stream.of(
                new Object[]{"append(pending, expectedVersion)", (Write) (adapter, seqnum) ->
                        adapter.append(pending(seqnum, ORDERS), PersistenceAdapter.ANY_VERSION)},
                new Object[]{"appendBatchAssigningVersions", (Write) (adapter, seqnum) ->
                        adapter.appendBatchAssigningVersions(List.of(pending(seqnum, ORDERS)))},
                new Object[]{"appendBatch(entries)", (Write) (adapter, seqnum) ->
                        adapter.appendBatch(List.of(pending(seqnum, ORDERS).withVersion(0)))});
    }

    /**
     * The interleaving from #38, read off the old code: B claims seqnum 9, A claims 10 and
     * commits first, then B commits. {@code meta/latest} was written as
     * {@code max(localLatest, seqnum)}, and B's local latest had never seen 10 — so the stored
     * value went from 10 back to 9, and a fresh process would open with a durability boundary
     * below an entry that is durable.
     *
     * <p>Seqnums are supplied directly here, because the point is the order of <em>commits</em>
     * relative to the order of <em>claims</em>, and a sequencer would couple them.
     */
    @ParameterizedTest(name = "{0}")
    @MethodSource("writePaths")
    void latestSeqnumNeverMovesBackwardsWhenALowerClaimCommitsLater(String name, Write write)
            throws IOException {
        write.apply(a, 10);
        write.apply(b, 9);

        assertThat(a.getLatestSeqnum()).isEqualTo(10L);
        assertThat(b.getLatestSeqnum()).isEqualTo(10L);
        assertThat(open().getLatestSeqnum())
                .as("a fresh instance reads the stored value, which the later, lower commit must not have lowered")
                .isEqualTo(10L);
    }

    /**
     * Same shape for the trim point: a stale instance cannot move it back. The regression is
     * invisible through reads (what sat below the real point is already cleared), which is
     * why this reads the stored point directly — an assertion on {@code readAll()} passed
     * against the old code and proved nothing.
     */
    @Test
    void trimIsComparedAgainstTheStoredPointNotThisProcessesHint() throws IOException {
        for (int i = 0; i < 6; i++) a.append(pending(i, ORDERS), PersistenceAdapter.ANY_VERSION);

        a.trim(4);
        b.trim(2);   // b last saw the trim point at 0

        assertThat(b.storedTrimSeqnum()).as("b's lower trim did not overwrite a's").isEqualTo(4L);
        assertThat(seqnumsOf(open().readAll())).containsExactly(4L, 5L);
        assertThat(seqnumsOf(b.readAll())).containsExactly(4L, 5L);
    }

    // -------------------------------------------------------------------------
    // Review findings on #40
    // -------------------------------------------------------------------------

    /**
     * What {@code db.run} does after {@code commit_unknown_result}: run the same lambda again
     * with the same pending, the first attempt possibly having committed. Calling the method
     * twice with one {@code PendingAppend} is that retry, minus the lost reply. The second
     * call must return the committed entry — not a conflict for the conditional form, which
     * would tell a caller "nothing written" about an entry that is durable, and not a second
     * record one version up for the unconditional form, which would leave a hole.
     */
    @Test
    void aRetryOfACommittedAppendReturnsTheCommittedEntryNotAConflictOrAHole() throws IOException {
        PendingAppend fenced = pending(0, ORDERS);
        LogEntry first = a.append(fenced, 0L);
        LogEntry again = a.append(fenced, 0L);
        assertThat(sameEntry(again, first)).as("the retry is handed what the first attempt committed").isTrue();

        PendingAppend unfenced = pending(1, ORDERS);
        a.append(unfenced, PersistenceAdapter.ANY_VERSION);
        a.append(unfenced, PersistenceAdapter.ANY_VERSION);

        List<PendingAppend> batch = List.of(pending(2, ORDERS), pending(3, INVENTORY));
        List<LogEntry> stored = a.appendBatchAssigningVersions(batch);
        List<LogEntry> retried = a.appendBatchAssigningVersions(batch);
        assertThat(retried).hasSize(2);
        assertThat(sameEntry(retried.get(0), stored.get(0))).isTrue();
        assertThat(sameEntry(retried.get(1), stored.get(1))).isTrue();

        assertThat(versionsOf(b.readByTag(ORDERS, 0L), ORDERS))
                .as("one record per append, no hole, as seen from the other instance")
                .containsExactly(0L, 1L, 2L);
        assertThat(versionsOf(b.readByTag(INVENTORY, 0L), INVENTORY)).containsExactly(0L);
        assertThat(b.getNextStreamVersion(ORDERS)).isEqualTo(3L);
    }

    /**
     * The other thing a record already at this seqnum can mean: two writers behind a
     * sequencer that is not distributed, handed the same number. That configuration does not
     * report {@code multiWriter}, and used to overwrite the first record silently.
     */
    @Test
    void aDifferentEntryAtAnAlreadyUsedSeqnumIsRefusedNotOverwritten() throws IOException {
        a.append(pending(0, ORDERS), PersistenceAdapter.ANY_VERSION);

        PendingAppend collider = new PendingAppend(0, INVENTORY, Set.of(INVENTORY), bytes("other"), Instant.now());
        assertThatThrownBy(() -> b.append(collider, PersistenceAdapter.ANY_VERSION))
                .isInstanceOf(IOException.class)
                .hasStackTraceContaining("already holds a different entry");

        assertThat(a.readAll()).hasSize(1);
        assertThat(a.readAll().get(0).data()).isEqualTo(bytes("e0"));
        assertThat(a.readByTag(INVENTORY, 0L)).isEmpty();
    }

    /**
     * {@code BYTE_MAX} with no entries would be {@code BYTE_MAX} with -1, whose bytes are all
     * {@code 0xFF} and outrank every real seqnum: {@code meta/latest} would read -1 in every
     * process on this root, forever. The service never sends an empty batch; the adapter is
     * public API and must not depend on that.
     */
    @Test
    void anEmptyBatchLeavesLatestSeqnumAlone() throws IOException {
        a.append(pending(3, ORDERS), PersistenceAdapter.ANY_VERSION);

        a.appendBatchAssigningVersions(List.of());
        a.appendBatch(List.of());

        assertThat(a.getLatestSeqnum()).isEqualTo(3L);
        assertThat(open().getLatestSeqnum()).isEqualTo(3L);
    }

    // -------------------------------------------------------------------------
    // Ask 3 of #38: D2 does not apply
    // -------------------------------------------------------------------------

    /**
     * D2 on the file adapter: each process writes its own {@code index.dat}, so the last one
     * to close overwrites the other's view and the log reports three of six appends while all
     * six are on disk. The FDB adapter's index is the {@code tag} subspace, written in the
     * same transaction as the entry, so there is no per-process view to clobber. Two writers
     * interleave, both close, and a third instance sees everything — dense, in order.
     */
    @Test
    void aFreshInstanceSeesEveryEntryBothInstancesAppended() throws IOException {
        for (int i = 0; i < 6; i++) {
            FoundationDBPersistenceAdapter writer = i % 2 == 0 ? a : b;
            writer.append(pending(i, ORDERS), PersistenceAdapter.ANY_VERSION);
        }
        a.close();
        b.close();

        FoundationDBPersistenceAdapter c = open();
        List<LogEntry> all = c.readByTag(ORDERS, 0L);
        assertThat(seqnumsOf(all)).containsExactly(0L, 1L, 2L, 3L, 4L, 5L);
        assertThat(versionsOf(all, ORDERS)).as("one dense stream, not two interleaved opinions")
                .containsExactly(0L, 1L, 2L, 3L, 4L, 5L);
        assertThat(versionsOf(c.readFromVersion(ORDERS, 3L), ORDERS)).containsExactly(3L, 4L, 5L);
        assertThat(c.getNextStreamVersion(ORDERS)).isEqualTo(6L);
    }

    // -------------------------------------------------------------------------
    // Capability honesty, across instances — the half LogCapabilityHonestyTest cannot run
    // -------------------------------------------------------------------------

    /**
     * {@code conditionalAppend} with the reach {@code multiWriter} adds: the writer that is
     * fenced out is a different instance from the one that moved the tag. Nothing in
     * {@code LogCapabilityHonestyTest} can show that, since its adapters cannot share a log.
     */
    @Test
    void theFenceHoldsAgainstAnotherInstance() throws IOException {
        assertThat(a.capabilities().conditionalAppend()).isTrue();
        assertThat(a.capabilities().multiWriter()).isTrue();

        a.append(pending(0, ORDERS), 0L);

        assertThatThrownBy(() -> b.append(pending(1, ORDERS), 0L))
                .as("b still believes the tag is at 0; a moved it")
                .isInstanceOf(VersionConflictException.class)
                .satisfies(e -> {
                    VersionConflictException conflict = (VersionConflictException) e;
                    assertThat(conflict.expectedVersion()).isEqualTo(0L);
                    assertThat(conflict.actualVersion()).isEqualTo(1L);
                });
        assertThat(b.readByTag(ORDERS, 0L)).as("the rejected append wrote nothing").hasSize(1);

        assertThat(b.append(pending(1, ORDERS), 1L).streamVersion())
                .as("re-reading the version and retrying is the protocol")
                .isEqualTo(1L);
    }

    /** {@code compareAndSet}: one claimant per instance, exactly one told it won. */
    @Test
    void theKvClaimIsArbitratedBetweenInstances() throws IOException {
        assertThat(a.capabilities().compareAndSet()).isTrue();

        assertThat(a.setTagValueIfAbsent(ORDERS, "owner", bytes("a"))).isTrue();
        assertThat(b.setTagValueIfAbsent(ORDERS, "owner", bytes("b"))).as("b arrives second").isFalse();
        assertThat(b.getTagValue(ORDERS, "owner")).isEqualTo(bytes("a"));

        assertThat(b.compareAndSetTagValue(ORDERS, "owner", bytes("a"), bytes("b")))
                .as("a takeover that names the current holder succeeds")
                .isTrue();
        assertThat(a.compareAndSetTagValue(ORDERS, "owner", bytes("a"), bytes("a-again")))
                .as("a's release names a holder that is no longer the holder")
                .isFalse();
        assertThat(a.getTagValue(ORDERS, "owner")).isEqualTo(bytes("b"));

        assertThat(a.incrementTagValue(ORDERS, "count", 1)).isEqualTo(1L);
        assertThat(b.incrementTagValue(ORDERS, "count", 1)).isEqualTo(2L);
    }

    /**
     * {@code atomicMultiTagAppend} together with per-tag versions, across instances: each
     * instance fences on its own tag while both feed one queue, and the queue is dense.
     */
    @Test
    void aFanOutTagIsDenseAcrossInstancesWhileEachFencesOnItsOwnTag() throws IOException {
        LogTag queue = LogTag.of("queue");
        long seqnum = 0;
        for (int i = 0; i < 3; i++) {
            a.append(pending(seqnum++, ORDERS, new LinkedHashSet<>(List.of(ORDERS, queue))), i);
            b.append(pending(seqnum++, INVENTORY, new LinkedHashSet<>(List.of(INVENTORY, queue))), i);
        }

        assertThat(versionsOf(a.readByTag(queue, 0L), queue)).containsExactly(0L, 1L, 2L, 3L, 4L, 5L);
        assertThat(versionsOf(b.readByTag(ORDERS, 0L), ORDERS)).containsExactly(0L, 1L, 2L);
        assertThat(versionsOf(b.readByTag(INVENTORY, 0L), INVENTORY)).containsExactly(0L, 1L, 2L);
    }

    /** The {@code multiWriter} branch of the honesty test: a second opener is admitted and shares the log. */
    @Test
    void aSecondInstanceIsAdmittedAndSharesTheLog() throws IOException {
        a.append(pending(0, ORDERS), PersistenceAdapter.ANY_VERSION);
        assertThat(b.readByTag(ORDERS, 0L)).isNotEmpty();
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    @FunctionalInterface
    interface Write {
        void apply(FoundationDBPersistenceAdapter adapter, long seqnum) throws IOException;
    }

    private FoundationDBPersistenceAdapter open() throws IOException {
        FoundationDBPersistenceAdapter adapter =
                new FoundationDBPersistenceAdapter(FdbTestSupport.clusterFile(), root);
        adapter.open();
        opened.add(adapter);
        return adapter;
    }

    private static PendingAppend pending(long seqnum, LogTag tag) {
        return pending(seqnum, tag, Set.of(tag));
    }

    private static PendingAppend pending(long seqnum, LogTag primary, Set<LogTag> tags) {
        return new PendingAppend(seqnum, primary, tags, bytes("e" + seqnum), Instant.now());
    }

    /** Same record: an entry handed back from the store carries its timestamp at millisecond precision. */
    private static boolean sameEntry(LogEntry x, LogEntry y) {
        return x.seqnum() == y.seqnum()
                && x.streamVersions().equals(y.streamVersions())
                && x.tags().equals(y.tags())
                && java.util.Arrays.equals(x.dataUnsafe(), y.dataUnsafe())
                && x.timestamp().toEpochMilli() == y.timestamp().toEpochMilli();
    }

    private static List<Long> seqnumsOf(List<LogEntry> entries) {
        return entries.stream().map(LogEntry::seqnum).toList();
    }

    private static List<Long> versionsOf(List<LogEntry> entries, LogTag tag) {
        return entries.stream().map(e -> e.streamVersion(tag)).toList();
    }

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }
}
