package com.cajunsystems.gumbo.service;

import com.cajunsystems.gumbo.core.AppendRequest;
import com.cajunsystems.gumbo.core.AppendResult;
import com.cajunsystems.gumbo.core.LogEntry;
import com.cajunsystems.gumbo.core.LogTag;
import com.cajunsystems.gumbo.core.PendingAppend;
import com.cajunsystems.gumbo.core.VersionConflictException;
import com.cajunsystems.gumbo.persistence.BatchingPersistenceAdapter;
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
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Storage assigns the version, and a conditional append is decided where the write lands.
 *
 * <p>The point of moving assignment into the adapter is that a version now describes the
 * stream rather than one process's opinion of it. These tests check both halves: that the
 * number comes from storage and survives a restart, and that conditioning on it actually
 * rejects a stale writer.
 */
class ConditionalAppendTest {

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

    // -------------------------------------------------------------------------
    // Conditional append
    // -------------------------------------------------------------------------

    @ParameterizedTest(name = "{0}")
    @MethodSource("adapters")
    void anAppendAtTheExpectedVersionSucceeds(
            String name, Function<Path, PersistenceAdapter> factory) throws IOException {
        open(factory);
        assertThat(append(ORDERS, "o0", 0).streamVersion()).isEqualTo(0L);
        assertThat(append(ORDERS, "o1", 1).streamVersion()).isEqualTo(1L);
        assertThat(append(ORDERS, "o2", 2).streamVersion()).isEqualTo(2L);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("adapters")
    void anAppendAtAStaleVersionIsRejectedAndWritesNothing(
            String name, Function<Path, PersistenceAdapter> factory) throws IOException {
        open(factory);
        append(ORDERS, "o0", 0);
        append(ORDERS, "o1", 1);

        // A writer that last saw version 0 has been overtaken.
        assertThatThrownBy(() -> append(ORDERS, "stale", 0))
                .rootCause()
                .isInstanceOf(VersionConflictException.class)
                .hasMessageContaining("expected version 0")
                .hasMessageContaining("is at 2");

        assertThat(payloads(ORDERS)).containsExactly("o0", "o1");
    }

    /**
     * Report test #4: two writers at the same expected version, exactly one wins.
     *
     * <p>The losing append must be <em>rejected</em>, not silently accepted — an accepted
     * loser is how two nodes both believe they own a stream and neither finds out.
     */
    @ParameterizedTest(name = "{0}")
    @MethodSource("adapters")
    void twoWritersAtTheSameExpectedVersionProduceExactlyOneWinner(
            String name, Function<Path, PersistenceAdapter> factory) throws Exception {
        open(factory);
        append(ORDERS, "base", 0);          // tag is now at version 1

        int writers = 8;
        CountDownLatch ready = new CountDownLatch(writers);
        CountDownLatch go = new CountDownLatch(1);
        AtomicInteger accepted = new AtomicInteger();
        AtomicInteger rejected = new AtomicInteger();

        List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < writers; i++) {
            final int n = i;
            threads.add(Thread.ofVirtual().start(() -> {
                ready.countDown();
                try {
                    go.await(10, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                try {
                    append(ORDERS, "w" + n, 1);   // all expect the same version
                    accepted.incrementAndGet();
                } catch (RuntimeException e) {
                    rejected.incrementAndGet();
                }
            }));
        }
        assertThat(ready.await(10, TimeUnit.SECONDS)).isTrue();
        go.countDown();
        for (Thread t : threads) t.join(10_000);

        assertThat(accepted.get()).as("exactly one writer may win version 1").isEqualTo(1);
        assertThat(rejected.get()).isEqualTo(writers - 1);
        assertThat(payloads(ORDERS)).hasSize(2);   // "base" plus the single winner
        assertThat(versions(ORDERS)).containsExactly(0L, 1L);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("adapters")
    void aRejectedAppendLeavesTheVersionFreeForTheNextWriter(
            String name, Function<Path, PersistenceAdapter> factory) throws IOException {
        open(factory);
        append(ORDERS, "o0", 0);

        assertThatThrownBy(() -> append(ORDERS, "wrong", 5)).isInstanceOf(RuntimeException.class);

        // The failed attempt must not have consumed version 1.
        assertThat(append(ORDERS, "o1", 1).streamVersion()).isEqualTo(1L);
        assertThat(payloads(ORDERS)).containsExactly("o0", "o1");
    }

    /** Each tag is fenced independently: a conflict on one says nothing about another. */
    @ParameterizedTest(name = "{0}")
    @MethodSource("adapters")
    void theConditionAppliesPerTagNotPerLog(
            String name, Function<Path, PersistenceAdapter> factory) throws IOException {
        open(factory);
        append(ORDERS, "o0", 0);
        append(ORDERS, "o1", 1);

        // INVENTORY has its own numbering and is still at 0, despite the log being at
        // seqnum 2 — which is exactly why the fence is a version and not a seqnum.
        assertThat(append(INVENTORY, "i0", 0).streamVersion()).isEqualTo(0L);
    }

    /**
     * A multi-tag conditional append must name the tag it is fencing.
     *
     * <p>The primary tag of a multi-tag request is {@code tags.iterator().next()} over an
     * immutable {@code Set}, whose iteration order Java salts per JVM run. Left implicit,
     * the fence would apply to whichever stream that yielded — rejecting a valid entity
     * update in one run and accepting a stale one in the next, with nothing in the code to
     * show which. Refused up front instead.
     */
    @ParameterizedTest(name = "{0}")
    @MethodSource("adapters")
    void aMultiTagConditionalAppendMustNameTheFencedTag(
            String name, Function<Path, PersistenceAdapter> factory) throws IOException {
        open(factory);
        AppendRequest both = AppendRequest.to(Set.of(ORDERS, INVENTORY), "work".getBytes());

        assertThatThrownBy(() -> service.append(both, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must name the tag to fence");

        assertThatThrownBy(() -> service.append(both, LogTag.of("unrelated"), 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("is not among the request's tags");
    }

    /**
     * With the tag named, both the fence and the numbering follow the caller's choice
     * rather than a set's iteration order — the workflow shape: fence the entity's
     * history, fan out to a queue that needs no fence.
     */
    @ParameterizedTest(name = "{0}")
    @MethodSource("adapters")
    void namingTheFencedTagDecidesBothTheFenceAndTheVersion(
            String name, Function<Path, PersistenceAdapter> factory) throws IOException {
        LogTag history = LogTag.of("history", "wf-1");
        LogTag queue   = LogTag.of("queue");
        open(factory);

        for (int i = 0; i < 3; i++) {
            service.append(AppendRequest.to(history, ("h" + i).getBytes())).join();
        }

        AppendRequest both = AppendRequest.to(Set.of(history, queue), "work".getBytes());

        // Stale expectation on the named tag is rejected, whichever way the set iterates.
        assertThatThrownBy(() -> service.append(both, history, 0).join())
                .hasRootCauseInstanceOf(VersionConflictException.class);

        AppendResult r = service.append(both, history, 3).join();
        assertThat(r.primaryTag()).isEqualTo(history);
        assertThat(r.streamVersion()).isEqualTo(3L);
    }

    /**
     * A multi-tag append must never lower a secondary tag's version count.
     *
     * <p>An entry carries one version — its primary tag's — so on a multi-tag append a
     * secondary tag may already be further along its own sequence. Writing
     * {@code thisEntry.version + 1} as that tag's count rewinds it, which hands out
     * versions that already exist and, now that the count is what a conditional append
     * reads, lets a stale writer pass a fence it should have failed.
     *
     * <p>The in-memory and file adapters take a max and were always safe; FoundationDB
     * overwrote. The invariant is asserted here for every adapter rather than only where
     * it was broken, so it is checked wherever the suite runs.
     */
    @ParameterizedTest(name = "{0}")
    @MethodSource("adapters")
    void aMultiTagAppendNeverRewindsASecondaryTagsVersion(
            String name, Function<Path, PersistenceAdapter> factory) throws IOException {
        open(factory);

        // INVENTORY is well ahead; ORDERS has nothing.
        for (int i = 0; i < 3; i++) {
            service.append(AppendRequest.to(INVENTORY, ("i" + i).getBytes())).join();
        }

        // One entry on both, fenced on (and numbered by) ORDERS, whose version is 0.
        AppendResult r = service.append(
                AppendRequest.to(Set.of(ORDERS, INVENTORY), "both".getBytes()),
                ORDERS, 0).join();
        assertThat(r.streamVersion()).isEqualTo(0L);

        // INVENTORY's next version must not have been rewound to 1.
        assertThatThrownBy(() -> append(INVENTORY, "stale", 1))
                .as("a rewound count would let this stale writer through the fence")
                .isInstanceOf(RuntimeException.class);

        assertThat(append(INVENTORY, "i3", 3).streamVersion()).isEqualTo(3L);
    }

    // -------------------------------------------------------------------------
    // Storage-owned assignment
    // -------------------------------------------------------------------------

    /**
     * The version comes from the log, not from a counter in this process. A fresh service
     * over the same directory must continue the sequence rather than restart it — that is
     * the property a caller-side counter cannot provide, since it is seeded once and then
     * drifts.
     */
    @Test
    void versionsContinueAcrossARestartWithNoInMemoryCounter() throws IOException {
        open(FileBasedPersistenceAdapter::new);
        assertThat(append(ORDERS, "o0", 0).streamVersion()).isEqualTo(0L);
        assertThat(append(ORDERS, "o1", 1).streamVersion()).isEqualTo(1L);
        service.close();

        open(FileBasedPersistenceAdapter::new);
        // A stale expectation is still rejected after the restart: the tag's version was
        // recovered from storage, not reset to zero.
        assertThatThrownBy(() -> append(ORDERS, "stale", 0)).isInstanceOf(RuntimeException.class);
        assertThat(append(ORDERS, "o2", 2).streamVersion()).isEqualTo(2L);
        assertThat(payloads(ORDERS)).containsExactly("o0", "o1", "o2");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("adapters")
    void unconditionalAppendsStayDenseAndInterleaveIndependently(
            String name, Function<Path, PersistenceAdapter> factory) throws IOException {
        open(factory);
        for (int i = 0; i < 3; i++) {
            service.append(AppendRequest.to(ORDERS, ("o" + i).getBytes())).join();
            service.append(AppendRequest.to(INVENTORY, ("i" + i).getBytes())).join();
        }
        assertThat(versions(ORDERS)).containsExactly(0L, 1L, 2L);
        assertThat(versions(INVENTORY)).containsExactly(0L, 1L, 2L);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("adapters")
    void batchedAppendsTakeConsecutiveVersionsWithinTheBatch(
            String name, Function<Path, PersistenceAdapter> factory) throws IOException {
        open(factory);
        List<AppendResult> results = service.appendBatch(List.of(
                AppendRequest.to(ORDERS, "a".getBytes()),
                AppendRequest.to(INVENTORY, "x".getBytes()),
                AppendRequest.to(ORDERS, "b".getBytes()),
                AppendRequest.to(ORDERS, "c".getBytes()))).join();

        assertThat(results.stream().map(AppendResult::streamVersion))
                .containsExactly(0L, 0L, 1L, 2L);
        assertThat(versions(ORDERS)).containsExactly(0L, 1L, 2L);
        assertThat(versions(INVENTORY)).containsExactly(0L);
    }

    // -------------------------------------------------------------------------
    // The SPI contract for adapters that cannot fence
    // -------------------------------------------------------------------------

    /**
     * An adapter that cannot compare-and-increment atomically must refuse rather than
     * write unconditionally. Silently ignoring {@code expectedVersion} would make a log
     * look like it was participating in the fencing protocol while providing none of it —
     * surfacing later as corruption rather than now as an error.
     */
    @Test
    void anAdapterWithoutAtomicCompareAndIncrementRefusesToPretend() {
        PersistenceAdapter bare = new BareAdapter();
        PendingAppend pending = new PendingAppend(
                0, ORDERS, Set.of(ORDERS), "x".getBytes(), Instant.now());

        assertThatThrownBy(() -> bare.append(pending, 0))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("conditional append");

        // ...but it still handles an unconditional append through the same entry point.
        assertThat(catching(() -> bare.append(pending, PersistenceAdapter.ANY_VERSION)))
                .isNotNull();
    }

    /** Implements only the required SPI methods, inheriting every default. */
    private static final class BareAdapter implements PersistenceAdapter {
        private final List<LogEntry> entries = new ArrayList<>();
        @Override public void open() {}
        @Override public void close() {}
        @Override public void append(LogEntry e) { entries.add(e); }
        @Override public List<LogEntry> readAll() { return List.copyOf(entries); }
        @Override public List<LogEntry> readFrom(long s) { return List.copyOf(entries); }
        @Override public List<LogEntry> readByTag(LogTag t, long s) { return List.copyOf(entries); }
        @Override public void trim(long s) {}
        @Override public long getLatestSeqnum() { return entries.isEmpty() ? -1 : entries.size() - 1; }
        @Override public long getNextStreamVersion(LogTag t) { return entries.size(); }
    }

    // -------------------------------------------------------------------------

    private void open(Function<Path, PersistenceAdapter> factory) throws IOException {
        service = SharedLogService.open(SharedLogConfig.builder()
                .persistenceAdapter(factory.apply(tempDir))
                .build());
    }

    private AppendResult append(LogTag tag, String data, long expectedVersion) {
        return service.append(AppendRequest.to(tag, data.getBytes()), expectedVersion).join();
    }

    private List<String> payloads(LogTag tag) {
        return service.readAll(tag).join().stream().map(e -> new String(e.data())).toList();
    }

    private List<Long> versions(LogTag tag) {
        return service.readAll(tag).join().stream().map(LogEntry::streamVersion).toList();
    }

    private static Object catching(ThrowingSupplier s) {
        try {
            return s.get();
        } catch (Exception e) {
            throw new AssertionError("unconditional append should have been accepted", e);
        }
    }

    private interface ThrowingSupplier {
        Object get() throws Exception;
    }
}
