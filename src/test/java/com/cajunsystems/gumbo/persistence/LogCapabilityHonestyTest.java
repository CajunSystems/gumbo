package com.cajunsystems.gumbo.persistence;

import com.cajunsystems.gumbo.core.LogCapabilities;
import com.cajunsystems.gumbo.core.LogEntry;
import com.cajunsystems.gumbo.core.LogTag;
import com.cajunsystems.gumbo.core.PendingAppend;
import com.cajunsystems.gumbo.core.VersionConflictException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Every capability an adapter declares is exercised against the adapter, in both
 * directions: what it claims must work, and what it disclaims must fail loudly rather
 * than silently do nothing.
 *
 * <p>A declaration nobody checks is prose with a return type. The reason this file exists
 * is that prose is what a downstream consumer had to rely on when it used a seqnum-keyed
 * read as a version-keyed one, and the resulting fold double-counted with no error
 * anywhere. Capabilities only help if a wrong one fails a build.
 *
 * <p><strong>"Fails loudly when disclaimed" applies to two of the six</strong>, and it is worth
 * being exact about which, because the earlier version of this comment claimed it of all of them.
 * {@code conditionalAppend} and {@code compareAndSet} each have an optional <em>method</em>, so
 * disclaiming one means calling it throws. The rest name a <em>property</em> of methods that exist
 * either way: there is no separate call to refuse for a non-atomic multi-tag append, and no
 * exception a single-writer log can raise at the moment a second process in another JVM writes.
 * Those are asserted by behaviour instead — and where an adapter disclaims one, the test says so
 * rather than passing silently, so a skipped branch cannot read as a checked one.
 *
 * <p>Note the shape of each test: it branches on the declaration and asserts the matching
 * behaviour, rather than asserting a fixed expected value per adapter. So an adapter that
 * flips a flag without changing its behaviour fails here — which is the mutation that
 * matters, and the only one a hard-coded expectation would miss.
 *
 * <p>{@link FoundationDBPersistenceAdapter} is absent for the usual reason: it needs a
 * live cluster. It is the one adapter declaring {@code multiWriter}, and that claim is
 * covered by {@link FoundationDBPersistenceAdapterTest} where a cluster is available.
 */
class LogCapabilityHonestyTest {

    private static final LogTag ORDERS    = LogTag.of("orders");
    private static final LogTag INVENTORY = LogTag.of("inventory");

    @TempDir
    Path tempDir;

    private PersistenceAdapter adapter;
    private long nextSeqnum = 0;

    @AfterEach
    void tearDown() throws IOException {
        if (adapter != null) adapter.close();
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

    // -------------------------------------------------------------------------
    // Per-capability honesty
    // -------------------------------------------------------------------------

    /**
     * The fence: an append at a version the tag has moved past must be rejected, not
     * accepted at whatever the version happens to be now. An adapter that cannot compare
     * and increment atomically must say so by throwing, because a non-atomic compare that
     * usually works is worse than none at all — it is believed.
     */
    @ParameterizedTest(name = "{0}")
    @MethodSource("adapters")
    void conditionalAppendWorksExactlyWhenDeclared(
            String name, Function<Path, PersistenceAdapter> factory) throws IOException {
        open(factory);

        if (adapter.capabilities().conditionalAppend()) {
            adapter.append(pending(ORDERS), PersistenceAdapter.ANY_VERSION);

            assertThat(adapter.append(pending(ORDERS), 1L).streamVersion())
                    .as("an append at the tag's current version is accepted")
                    .isEqualTo(1L);

            assertThatThrownBy(() -> adapter.append(pending(ORDERS), 1L))
                    .as("a writer still holding version 1 has been overtaken")
                    .isInstanceOf(VersionConflictException.class);

            assertThat(adapter.readByTag(ORDERS, 0L))
                    .as("the rejected append wrote nothing")
                    .hasSize(2);
        } else {
            assertThatThrownBy(() -> adapter.append(pending(ORDERS), 0L))
                    .isInstanceOf(UnsupportedOperationException.class);
        }
    }

    /**
     * The claim: of two callers who both believe a key is theirs, exactly one is told it
     * won. Same reasoning as the append fence, on the other store.
     */
    @ParameterizedTest(name = "{0}")
    @MethodSource("adapters")
    void compareAndSetWorksExactlyWhenDeclared(
            String name, Function<Path, PersistenceAdapter> factory) throws IOException {
        open(factory);

        if (adapter.capabilities().compareAndSet()) {
            assertThat(adapter.setTagValueIfAbsent(ORDERS, "owner", bytes("node-a"))).isTrue();
            assertThat(adapter.setTagValueIfAbsent(ORDERS, "owner", bytes("node-b")))
                    .as("the second claimant loses")
                    .isFalse();
            assertThat(adapter.getTagValue(ORDERS, "owner")).isEqualTo(bytes("node-a"));
        } else {
            assertThatThrownBy(() -> adapter.compareAndSetTagValue(ORDERS, "owner", null, bytes("x")))
                    .isInstanceOf(UnsupportedOperationException.class);
        }
    }

    /**
     * Version-keyed reads address the tag's own stream. Verified with <strong>two</strong>
     * tags in the log, because one tag in a fresh log is the single configuration where a
     * per-stream version and a global seqnum are the same number — the blind spot that hid
     * this defect in the first place.
     */
    @ParameterizedTest(name = "{0}")
    @MethodSource("adapters")
    void versionedReadsWorkExactlyWhenDeclared(
            String name, Function<Path, PersistenceAdapter> factory) throws IOException {
        open(factory);
        for (int i = 0; i < 3; i++) adapter.append(pending(INVENTORY), PersistenceAdapter.ANY_VERSION);
        for (int i = 0; i < 3; i++) adapter.append(pending(ORDERS), PersistenceAdapter.ANY_VERSION);

        assertThat(adapter.capabilities().versionedReads())
                .as("every adapter has at least the interface's filtering default")
                .isTrue();

        assertThat(versionsOf(adapter.readAfterVersion(ORDERS, 0L)))
                .as("the tail of ORDERS' own stream, not of the log")
                .containsExactly(1L, 2L);
    }

    /**
     * One append, several tags, no window between them: the entry is visible under every
     * tag it carries or under none, and it is one entry — the same seqnum — rather than a
     * copy per tag.
     */
    @ParameterizedTest(name = "{0}")
    @MethodSource("adapters")
    void atomicMultiTagAppendWorksExactlyWhenDeclared(
            String name, Function<Path, PersistenceAdapter> factory) throws IOException {
        open(factory);
        // Unlike the fence and the KV swap, this capability has no method to refuse: an adapter
        // that cannot write several tags indivisibly still accepts the same append and simply does
        // not promise the property. So a disclaimer is recorded as a skip with its reason, never as
        // a silent pass — every adapter here declares it, and if one stops, this says which.
        assumeTrue(adapter.capabilities().atomicMultiTagAppend(),
                name + " disclaims atomicMultiTagAppend; there is no throwing surface to assert on");

        LogEntry stored = adapter.append(
                pending(ORDERS, new LinkedHashSet<>(List.of(ORDERS, INVENTORY))),
                PersistenceAdapter.ANY_VERSION);

        assertThat(seqnumsOf(adapter.readByTag(ORDERS, 0L))).containsExactly(stored.seqnum());
        assertThat(seqnumsOf(adapter.readByTag(INVENTORY, 0L)))
                .as("both tags see the same entry, not two entries")
                .containsExactly(stored.seqnum());
    }

    /**
     * A log that does not declare {@code multiWriter} must not quietly acquire a second
     * writer. There are two honest ways to be single-writer and this accepts either: refuse
     * the second opener outright (the file adapter takes an exclusive directory lock), or
     * have nothing to share in the first place (an in-memory adapter's log is its own heap,
     * so a second instance is a different log, not a second writer of this one).
     *
     * <p>What it rules out is the third case, which is the dangerous one: two writers both
     * opening successfully, both handed the same versions, and neither told.
     */
    @ParameterizedTest(name = "{0}")
    @MethodSource("adapters")
    void aSingleWriterLogEitherRefusesASecondWriterOrIsADifferentLog(
            String name, Function<Path, PersistenceAdapter> factory) throws IOException {
        open(factory);
        adapter.append(pending(ORDERS), PersistenceAdapter.ANY_VERSION);

        PersistenceAdapter second = factory.apply(tempDir);
        boolean refused = false;
        try {
            second.open();
        } catch (IOException | RuntimeException expected) {
            refused = true;
        }
        try {
            if (adapter.capabilities().multiWriter()) {
                assertThat(refused).as("a multi-writer log admits a second writer").isFalse();
                assertThat(second.readByTag(ORDERS, 0L))
                        .as("and that writer shares this log")
                        .isNotEmpty();
            } else if (!refused) {
                assertThat(second.readByTag(ORDERS, 0L))
                        .as("a second opener that was not refused must be a different log,"
                                + " not an unarbitrated co-writer of this one")
                        .isEmpty();
            }
        } finally {
            second.close();
        }
    }

    // -------------------------------------------------------------------------
    // The interface's own default
    // -------------------------------------------------------------------------

    /**
     * A third-party adapter that implements only the abstract methods inherits a
     * declaration, and that declaration has to describe what the interface actually gives
     * it: version-keyed reads (there is a working filtering default) and nothing else
     * (conditional append and the conditional KV both throw).
     *
     * <p>This is the case that decides whether the default is safe to have at all. It is —
     * because it under-reports rather than over-reports. An adapter that quietly gained a
     * cheerful default here would be believed by exactly the callers who cannot afford it.
     */
    @Test
    void theInheritedDefaultDescribesWhatTheInterfaceItselfProvides() throws IOException {
        PersistenceAdapter minimal = new MinimalAdapter();
        LogCapabilities declared = minimal.capabilities();

        assertThat(declared.versionedReads()).isTrue();
        minimal.append(new PendingAppend(0, ORDERS, Set.of(ORDERS), bytes("a"), Instant.now())
                .withVersion(0));
        assertThat(minimal.readFromVersion(ORDERS, 0L)).hasSize(1);

        assertThat(declared.conditionalAppend()).isFalse();
        assertThatThrownBy(() -> minimal.append(pending(ORDERS), 0L))
                .isInstanceOf(UnsupportedOperationException.class);

        assertThat(declared.compareAndSet()).isFalse();
        assertThatThrownBy(() -> minimal.compareAndSetTagValue(ORDERS, "k", null, bytes("v")))
                .isInstanceOf(UnsupportedOperationException.class);

        assertThat(declared.multiWriter()).isFalse();
        // The interface cannot promise an arbitrary adapter writes several tags indivisibly, so the
        // inherited answer withholds it. Nothing throws — the append still works — which is exactly
        // why this one is asserted as a declaration rather than as a refusal.
        assertThat(declared.atomicMultiTagAppend()).isFalse();
        assertThat(minimal.readByTag(INVENTORY, 0L))
                .as("a disclaimed guarantee does not disable the method")
                .isEmpty();
        assertThat(declared.pushSubscriptions())
                .as("delivery is the service's, and an adapter never answers for it")
                .isFalse();
    }

    /** A decorator narrows its delegate's reach to one process, and says so. */
    @Test
    void batchingDeclaresItsDelegatesCapabilitiesExceptMultiWriter() throws IOException {
        PersistenceAdapter delegate = new AlwaysMultiWriterAdapter();
        LogCapabilities batched = BatchingPersistenceAdapter.of(delegate).capabilities();

        assertThat(batched.multiWriter())
                .as("a version claimed here lands at flush time; across processes the"
                        + " compare no longer guards the write")
                .isFalse();
        assertThat(batched.conditionalAppend()).isTrue();
        assertThat(batched.compareAndSet()).isTrue();
        assertThat(batched.versionedReads()).isTrue();
        assertThat(batched.atomicMultiTagAppend()).isTrue();
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private void open(Function<Path, PersistenceAdapter> factory) throws IOException {
        adapter = factory.apply(tempDir);
        adapter.open();
    }

    private PendingAppend pending(LogTag tag) {
        return pending(tag, Set.of(tag));
    }

    private PendingAppend pending(LogTag primary, Set<LogTag> tags) {
        return new PendingAppend(nextSeqnum++, primary, tags,
                bytes("e" + nextSeqnum), Instant.now());
    }

    private static List<Long> versionsOf(List<LogEntry> entries) {
        return entries.stream().map(LogEntry::streamVersion).toList();
    }

    private static List<Long> seqnumsOf(List<LogEntry> entries) {
        return entries.stream().map(LogEntry::seqnum).toList();
    }

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    /** Implements the abstract methods and nothing else, to pin the inherited declaration. */
    private static final class MinimalAdapter implements PersistenceAdapter {
        private final List<LogEntry> entries = new ArrayList<>();

        @Override public void open() {}
        @Override public void close() {}
        @Override public void append(LogEntry entry) { entries.add(entry); }
        @Override public List<LogEntry> readAll() { return List.copyOf(entries); }

        @Override public List<LogEntry> readFrom(long fromSeqnum) {
            return entries.stream().filter(e -> e.seqnum() >= fromSeqnum).toList();
        }

        @Override public List<LogEntry> readByTag(LogTag tag, long fromSeqnum) {
            return entries.stream()
                    .filter(e -> e.tags().contains(tag) && e.seqnum() >= fromSeqnum)
                    .toList();
        }

        @Override public void trim(long upToSeqnum) {}

        @Override public long getLatestSeqnum() {
            return entries.isEmpty() ? -1L : entries.get(entries.size() - 1).seqnum();
        }

        @Override public long getNextStreamVersion(LogTag tag) {
            return entries.stream().filter(e -> e.tags().contains(tag)).count();
        }
    }

    /** A delegate that declares everything, so the decorator's narrowing is visible. */
    private static final class AlwaysMultiWriterAdapter extends InMemoryPersistenceAdapter {
        @Override public LogCapabilities capabilities() {
            return LogCapabilities.builder(super.capabilities()).multiWriter(true).build();
        }
    }
}
