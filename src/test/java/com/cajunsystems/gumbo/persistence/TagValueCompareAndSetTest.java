package com.cajunsystems.gumbo.persistence;

import com.cajunsystems.gumbo.core.CounterValues;
import com.cajunsystems.gumbo.core.LogEntry;
import com.cajunsystems.gumbo.core.LogTag;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The tag KV arbitrates claims, so a contended key has exactly one winner.
 *
 * <p>The KV was already load-bearing — idempotency indexes and checkpoints live in it — but
 * every mutation overwrote unconditionally, so two callers who both believed a key was
 * theirs both got their write, and neither was told. These tests are about the pair
 * (compare, write) being indivisible: what makes a claim decided by storage rather than by
 * whichever caller happened to read first.
 */
class TagValueCompareAndSetTest {

    private static final LogTag ORDERS    = LogTag.of("orders");
    private static final LogTag INVENTORY = LogTag.of("inventory");

    @TempDir
    Path tempDir;

    private PersistenceAdapter adapter;

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
    // Compare-and-set
    // -------------------------------------------------------------------------

    @ParameterizedTest(name = "{0}")
    @MethodSource("adapters")
    void aSwapAtTheExpectedValueSucceeds(
            String name, Function<Path, PersistenceAdapter> factory) throws IOException {
        open(factory);
        adapter.setTagValue(ORDERS, "owner", bytes("node-a"));

        assertThat(adapter.compareAndSetTagValue(ORDERS, "owner", bytes("node-a"), bytes("node-b")))
                .isTrue();
        assertThat(adapter.getTagValue(ORDERS, "owner")).isEqualTo(bytes("node-b"));
    }

    /**
     * The comparison is by content, not identity. A caller that read a value back, or
     * reconstructed it from its own state, holds a different array with the same bytes —
     * which is the normal case, not the exception.
     */
    @ParameterizedTest(name = "{0}")
    @MethodSource("adapters")
    void theComparisonIsByContentNotIdentity(
            String name, Function<Path, PersistenceAdapter> factory) throws IOException {
        open(factory);
        adapter.setTagValue(ORDERS, "owner", bytes("node-a"));

        byte[] equalButDistinct = bytes("node-a");
        assertThat(adapter.compareAndSetTagValue(ORDERS, "owner", equalButDistinct, bytes("node-b")))
                .isTrue();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("adapters")
    void aSwapAtAStaleValueWritesNothing(
            String name, Function<Path, PersistenceAdapter> factory) throws IOException {
        open(factory);
        adapter.setTagValue(ORDERS, "owner", bytes("node-b"));

        // A caller that last saw node-a has been overtaken.
        assertThat(adapter.compareAndSetTagValue(ORDERS, "owner", bytes("node-a"), bytes("node-c")))
                .isFalse();
        assertThat(adapter.getTagValue(ORDERS, "owner"))
                .as("a rejected swap must leave the value exactly as it was")
                .isEqualTo(bytes("node-b"));
    }

    /**
     * {@code null} expected means "must be absent", which is what makes a claim on an
     * unclaimed key expressible at all. A stored value is never null, so there is no
     * ambiguity between "absent" and "present but empty" — an empty array is a value.
     */
    @ParameterizedTest(name = "{0}")
    @MethodSource("adapters")
    void absenceIsExpressedAsNullAndAnEmptyValueIsStillAValue(
            String name, Function<Path, PersistenceAdapter> factory) throws IOException {
        open(factory);

        assertThat(adapter.compareAndSetTagValue(ORDERS, "owner", null, new byte[0])).isTrue();
        assertThat(adapter.getTagValue(ORDERS, "owner")).isEmpty();

        // Now that the key exists, "must be absent" must fail even though the value is empty.
        assertThat(adapter.compareAndSetTagValue(ORDERS, "owner", null, bytes("node-b"))).isFalse();
        assertThat(adapter.getTagValue(ORDERS, "owner")).isEmpty();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("adapters")
    void aNullValueRemovesTheKey(
            String name, Function<Path, PersistenceAdapter> factory) throws IOException {
        open(factory);
        adapter.setTagValue(ORDERS, "owner", bytes("node-a"));

        assertThat(adapter.compareAndSetTagValue(ORDERS, "owner", bytes("node-a"), null)).isTrue();
        assertThat(adapter.getTagValue(ORDERS, "owner")).isNull();
        // And the key is genuinely absent, not holding a tombstone that reads as a value.
        assertThat(adapter.setTagValueIfAbsent(ORDERS, "owner", bytes("node-b"))).isTrue();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("adapters")
    void keysAreScopedPerTag(
            String name, Function<Path, PersistenceAdapter> factory) throws IOException {
        open(factory);

        assertThat(adapter.setTagValueIfAbsent(ORDERS, "owner", bytes("node-a"))).isTrue();
        // The same key under another tag is a different claim, and still unclaimed.
        assertThat(adapter.setTagValueIfAbsent(INVENTORY, "owner", bytes("node-b"))).isTrue();

        assertThat(adapter.getTagValue(ORDERS, "owner")).isEqualTo(bytes("node-a"));
        assertThat(adapter.getTagValue(INVENTORY, "owner")).isEqualTo(bytes("node-b"));
    }

    // -------------------------------------------------------------------------
    // Claim and release
    // -------------------------------------------------------------------------

    /**
     * The claim operation, contended: exactly one of N callers may win.
     *
     * <p>This is the KV analogue of the report's conditional-append test. An accepted loser
     * is how two nodes both believe they own a stream and neither finds out — so the
     * assertion is on the <em>count</em> of winners, not merely on the value being one of
     * the candidates.
     */
    @ParameterizedTest(name = "{0}")
    @MethodSource("adapters")
    void exactlyOneOfManyContendingClaimantsWins(
            String name, Function<Path, PersistenceAdapter> factory) throws Exception {
        open(factory);

        int claimants = 8;
        CountDownLatch ready = new CountDownLatch(claimants);
        CountDownLatch go = new CountDownLatch(1);
        AtomicInteger won = new AtomicInteger();

        List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < claimants; i++) {
            final int n = i;
            threads.add(Thread.ofVirtual().start(() -> {
                ready.countDown();
                try {
                    if (!go.await(10, TimeUnit.SECONDS)) return;
                    if (adapter.setTagValueIfAbsent(ORDERS, "owner", bytes("node-" + n))) {
                        won.incrementAndGet();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } catch (IOException e) {
                    throw new AssertionError(e);
                }
            }));
        }
        assertThat(ready.await(10, TimeUnit.SECONDS)).isTrue();
        go.countDown();
        for (Thread t : threads) t.join(10_000);

        assertThat(won.get()).as("exactly one claimant may take the key").isEqualTo(1);
        assertThat(adapter.getTagValue(ORDERS, "owner")).isNotNull();
    }

    /**
     * A holder that has been taken over must not be able to release the claim.
     *
     * <p>Unconditional delete is the bug this closes: a node whose lease expired, coming
     * back from a pause and tidying up after itself, would remove the new owner's claim and
     * leave the key free for a third.
     */
    @ParameterizedTest(name = "{0}")
    @MethodSource("adapters")
    void aStaleHolderCannotReleaseSomeoneElsesClaim(
            String name, Function<Path, PersistenceAdapter> factory) throws IOException {
        open(factory);
        adapter.setTagValue(ORDERS, "owner", bytes("node-a"));

        // Taken over.
        assertThat(adapter.compareAndSetTagValue(ORDERS, "owner", bytes("node-a"), bytes("node-b")))
                .isTrue();

        // node-a tidies up, and must be refused.
        assertThat(adapter.deleteTagValueIf(ORDERS, "owner", bytes("node-a"))).isFalse();
        assertThat(adapter.getTagValue(ORDERS, "owner")).isEqualTo(bytes("node-b"));

        // The actual holder can.
        assertThat(adapter.deleteTagValueIf(ORDERS, "owner", bytes("node-b"))).isTrue();
        assertThat(adapter.getTagValue(ORDERS, "owner")).isNull();
    }

    // -------------------------------------------------------------------------
    // Counters
    // -------------------------------------------------------------------------

    @ParameterizedTest(name = "{0}")
    @MethodSource("adapters")
    void anAbsentCounterStartsAtZero(
            String name, Function<Path, PersistenceAdapter> factory) throws IOException {
        open(factory);

        assertThat(adapter.incrementTagValue(ORDERS, "attempts", 1)).isEqualTo(1L);
        assertThat(adapter.incrementTagValue(ORDERS, "attempts", 4)).isEqualTo(5L);
        assertThat(adapter.incrementTagValue(ORDERS, "attempts", -5)).isEqualTo(0L);
    }

    /** The stored bytes are the documented encoding, so a client can read the key directly. */
    @ParameterizedTest(name = "{0}")
    @MethodSource("adapters")
    void aCounterIsStoredInTheDocumentedEncoding(
            String name, Function<Path, PersistenceAdapter> factory) throws IOException {
        open(factory);
        adapter.incrementTagValue(ORDERS, "attempts", 7);

        byte[] raw = adapter.getTagValue(ORDERS, "attempts");
        assertThat(raw).hasSize(CounterValues.WIDTH);
        assertThat(CounterValues.toLong(raw)).isEqualTo(7L);

        // ...and a counter seeded by a client through setTagValue continues from there.
        adapter.setTagValue(ORDERS, "seeded", CounterValues.toBytes(41));
        assertThat(adapter.incrementTagValue(ORDERS, "seeded", 1)).isEqualTo(42L);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("adapters")
    void incrementingSomethingThatIsNotACounterFailsRatherThanGuessing(
            String name, Function<Path, PersistenceAdapter> factory) throws IOException {
        open(factory);
        adapter.setTagValue(ORDERS, "owner", bytes("node-a"));

        assertThatThrownBy(() -> adapter.incrementTagValue(ORDERS, "owner", 1))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("not a counter");
        assertThat(adapter.getTagValue(ORDERS, "owner")).isEqualTo(bytes("node-a"));
    }

    /**
     * No increment may be lost under contention. A read-modify-write that is not atomic
     * loses one per collision, which is invisible in the result and wrong by an amount that
     * depends on timing — the failure mode of the unconditional KV this replaces.
     */
    @ParameterizedTest(name = "{0}")
    @MethodSource("adapters")
    void concurrentIncrementsLoseNothing(
            String name, Function<Path, PersistenceAdapter> factory) throws Exception {
        open(factory);

        int threads = 8, each = 10;
        CountDownLatch ready = new CountDownLatch(threads);
        CountDownLatch go = new CountDownLatch(1);

        List<Thread> workers = new ArrayList<>();
        for (int i = 0; i < threads; i++) {
            workers.add(Thread.ofVirtual().start(() -> {
                ready.countDown();
                try {
                    if (!go.await(10, TimeUnit.SECONDS)) return;
                    for (int n = 0; n < each; n++) {
                        adapter.incrementTagValue(ORDERS, "attempts", 1);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } catch (IOException e) {
                    throw new AssertionError(e);
                }
            }));
        }
        assertThat(ready.await(10, TimeUnit.SECONDS)).isTrue();
        go.countDown();
        for (Thread t : workers) t.join(30_000);

        assertThat(CounterValues.toLong(adapter.getTagValue(ORDERS, "attempts")))
                .isEqualTo((long) threads * each);
    }

    // -------------------------------------------------------------------------
    // Durability
    // -------------------------------------------------------------------------

    /**
     * A won claim and a counter both survive a reopen on the file adapter.
     *
     * <p>A claim that is acknowledged but not durable is worse than no claim: the log
     * entries written under its authority survive the crash and the record of who was
     * allowed to write them does not, so the next owner cannot tell whether it is resuming
     * its own work or overwriting someone else's.
     */
    @Test
    void aClaimAndACounterSurviveAReopen() throws IOException {
        open(FileBasedPersistenceAdapter::new);
        assertThat(adapter.setTagValueIfAbsent(ORDERS, "owner", bytes("node-a"))).isTrue();
        adapter.incrementTagValue(ORDERS, "attempts", 3);
        adapter.close();

        open(FileBasedPersistenceAdapter::new);
        assertThat(adapter.getTagValue(ORDERS, "owner")).isEqualTo(bytes("node-a"));
        assertThat(CounterValues.toLong(adapter.getTagValue(ORDERS, "attempts"))).isEqualTo(3L);

        // The recovered value is the one a swap has to match, not a fresh start.
        assertThat(adapter.setTagValueIfAbsent(ORDERS, "owner", bytes("node-b"))).isFalse();
        assertThat(adapter.compareAndSetTagValue(ORDERS, "owner", bytes("node-a"), bytes("node-b")))
                .isTrue();
        assertThat(adapter.incrementTagValue(ORDERS, "attempts", 1)).isEqualTo(4L);
    }

    @Test
    void aConditionalDeleteSurvivesAReopen() throws IOException {
        open(FileBasedPersistenceAdapter::new);
        adapter.setTagValue(ORDERS, "owner", bytes("node-a"));
        assertThat(adapter.deleteTagValueIf(ORDERS, "owner", bytes("node-a"))).isTrue();
        adapter.close();

        open(FileBasedPersistenceAdapter::new);
        assertThat(adapter.getTagValue(ORDERS, "owner"))
                .as("the tombstone must replay, or a released claim comes back on restart")
                .isNull();
    }

    /**
     * The in-memory adapter's stored value is an array, so it is the one adapter where a
     * caller could reach the stored state after writing it. Copying in and out is what stops
     * a caller reusing a buffer from silently changing a value another caller is comparing
     * against — a comparison protocol cannot be built on state its participants can edit.
     */
    @Test
    void anInMemoryValueIsNotAliasedToTheCallersArray() throws IOException {
        open(dir -> new InMemoryPersistenceAdapter());

        byte[] mutable = bytes("node-a");
        adapter.setTagValue(ORDERS, "owner", mutable);
        mutable[0] = 'X';

        assertThat(adapter.getTagValue(ORDERS, "owner")).isEqualTo(bytes("node-a"));

        byte[] readBack = adapter.getTagValue(ORDERS, "owner");
        readBack[0] = 'Y';
        assertThat(adapter.compareAndSetTagValue(ORDERS, "owner", bytes("node-a"), bytes("node-b")))
                .as("a mutated read-back must not have changed the stored value")
                .isTrue();
    }

    // -------------------------------------------------------------------------
    // The SPI contract for adapters that cannot compare atomically
    // -------------------------------------------------------------------------

    /**
     * An adapter that cannot compare and write atomically must refuse, for the same reason
     * conditional append does: a KV that silently ignored {@code expected} would hand every
     * contender a {@code true} and report two owners as success.
     *
     * <p>All four conditional methods must refuse, not only the primitive — a default that
     * degraded on the way to it would be the same failure one call further down.
     */
    @Test
    void anAdapterWithoutAtomicCompareAndSetRefusesToPretend() {
        PersistenceAdapter bare = new BareAdapter();

        assertThatThrownBy(() -> bare.compareAndSetTagValue(ORDERS, "k", null, bytes("v")))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("compareAndSetTagValue");
        assertThatThrownBy(() -> bare.setTagValueIfAbsent(ORDERS, "k", bytes("v")))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> bare.deleteTagValueIf(ORDERS, "k", bytes("v")))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> bare.incrementTagValue(ORDERS, "k", 1))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    /**
     * A plausible third-party adapter: it has a working KV, but nothing that can compare and
     * write in one step. Deliberately not a stub that throws from every KV method — that
     * would pass this test without the refusal coming from the right place. Here
     * {@code incrementTagValue}'s read succeeds and the swap is what refuses.
     */
    private static final class BareAdapter implements PersistenceAdapter {
        private final java.util.Map<String, byte[]> kv = new java.util.HashMap<>();
        @Override public void open() {}
        @Override public void close() {}
        @Override public void append(LogEntry e) {}
        @Override public List<LogEntry> readAll() { return List.of(); }
        @Override public List<LogEntry> readFrom(long s) { return List.of(); }
        @Override public List<LogEntry> readByTag(LogTag t, long s) { return List.of(); }
        @Override public void trim(long s) {}
        @Override public long getLatestSeqnum() { return -1L; }
        @Override public long getNextStreamVersion(LogTag t) { return 0L; }
        @Override public void setTagValue(LogTag t, String k, byte[] v) { kv.put(t + k, v); }
        @Override public byte[] getTagValue(LogTag t, String k) { return kv.get(t + k); }
        @Override public void deleteTagValue(LogTag t, String k) { kv.remove(t + k); }
    }

    // -------------------------------------------------------------------------

    private void open(Function<Path, PersistenceAdapter> factory) throws IOException {
        if (adapter != null) adapter.close();
        adapter = factory.apply(tempDir);
        adapter.open();
    }

    private static byte[] bytes(String s) {
        return s.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }
}
