package com.cajunsystems.gumbo.service;

import com.cajunsystems.gumbo.api.LogView;
import com.cajunsystems.gumbo.api.TypedLogView;
import com.cajunsystems.gumbo.core.AppendRequest;
import com.cajunsystems.gumbo.core.CounterValues;
import com.cajunsystems.gumbo.core.LogTag;
import com.cajunsystems.gumbo.core.VersionConflictException;
import com.cajunsystems.gumbo.persistence.BatchingPersistenceAdapter;
import com.cajunsystems.gumbo.persistence.FileBasedPersistenceAdapter;
import com.cajunsystems.gumbo.persistence.InMemoryPersistenceAdapter;
import com.cajunsystems.gumbo.persistence.PersistenceAdapter;
import com.cajunsystems.gumbo.serialization.KryoLogSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
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
 * The conditional KV through a {@link LogView}: claims, releases and counters on the tag a
 * view is scoped to.
 *
 * <p>The point of these being at the service level rather than the adapter level is the
 * combination they are for. A claim says a node owned the stream a moment ago; a conditional
 * append says it still does at the instant the entry lands. Neither substitutes for the
 * other, and the pair is what a lease is actually made of.
 */
class TagValueCoordinationTest {

    private static final LogTag EXEC_1 = LogTag.of("execution", "exec-1");
    private static final LogTag EXEC_2 = LogTag.of("execution", "exec-2");

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
    // Claims through a view
    // -------------------------------------------------------------------------

    @ParameterizedTest(name = "{0}")
    @MethodSource("adapters")
    void aClaimIsTakenOnceAndSeenBySubsequentReaders(
            String name, Function<Path, PersistenceAdapter> factory) throws IOException {
        open(factory);
        LogView view = service.getView(EXEC_1);

        assertThat(view.setValueIfAbsent("owner", bytes("node-a")).join()).isTrue();
        assertThat(view.setValueIfAbsent("owner", bytes("node-b")).join()).isFalse();
        assertThat(view.getValue("owner").join()).isEqualTo(bytes("node-a"));
    }

    /** A view's KV is scoped to its tag, so two executions claim independently. */
    @ParameterizedTest(name = "{0}")
    @MethodSource("adapters")
    void claimsOnDifferentViewsDoNotCollide(
            String name, Function<Path, PersistenceAdapter> factory) throws IOException {
        open(factory);

        assertThat(service.getView(EXEC_1).setValueIfAbsent("owner", bytes("node-a")).join()).isTrue();
        assertThat(service.getView(EXEC_2).setValueIfAbsent("owner", bytes("node-b")).join()).isTrue();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("adapters")
    void exactlyOneViewLevelClaimantWins(
            String name, Function<Path, PersistenceAdapter> factory) throws Exception {
        open(factory);
        LogView view = service.getView(EXEC_1);

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
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                if (view.setValueIfAbsent("owner", bytes("node-" + n)).join()) {
                    won.incrementAndGet();
                }
            }));
        }
        assertThat(ready.await(10, TimeUnit.SECONDS)).isTrue();
        go.countDown();
        for (Thread t : threads) t.join(30_000);

        assertThat(won.get()).isEqualTo(1);
    }

    /**
     * The lease shape end to end: an expired lease is taken over by compare-and-set, and the
     * previous holder's write is then rejected by the version fence rather than by the lease.
     *
     * <p>That second half is the reason the fence exists. The takeover happened because a
     * clock said the lease had expired, and a clock is exactly the thing that can be wrong —
     * so if correctness rested on the lease alone, skew between the two nodes would be enough
     * for both to write. With the append fenced on the version, the loser is refused by
     * storage and the skew costs duplicated effort instead of a corrupted stream.
     */
    @ParameterizedTest(name = "{0}")
    @MethodSource("adapters")
    void anExpiredLeaseIsTakenOverAndTheOldHolderIsStillFencedOut(
            String name, Function<Path, PersistenceAdapter> factory) throws IOException {
        open(factory);
        LogView view = service.getView(EXEC_1);

        byte[] holderA = bytes("node-a@expires=1");
        assertThat(view.setValueIfAbsent("owner", holderA).join()).isTrue();
        service.append(AppendRequest.to(EXEC_1, bytes("step-0")), 0).join();

        // node-b sees an expired lease and takes it over.
        byte[] held = view.getValue("owner").join();
        assertThat(held).isEqualTo(holderA);
        byte[] holderB = bytes("node-b@expires=9");
        assertThat(view.compareAndSetValue("owner", held, holderB).join()).isTrue();

        // node-a, unaware, tries the takeover it thinks it is entitled to — and loses,
        // because the value it expects is no longer there.
        assertThat(view.compareAndSetValue("owner", holderA, bytes("node-a@expires=2")).join())
                .isFalse();

        // Its append loses too, on the version rather than on the lease.
        service.append(AppendRequest.to(EXEC_1, bytes("step-1")), 1).join();   // node-b's write
        assertThatThrownBy(() -> service.append(AppendRequest.to(EXEC_1, bytes("step-1'")), 1).join())
                .hasRootCauseInstanceOf(VersionConflictException.class);

        assertThat(view.getValue("owner").join()).isEqualTo(holderB);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("adapters")
    void onlyTheCurrentHolderCanRelease(
            String name, Function<Path, PersistenceAdapter> factory) throws IOException {
        open(factory);
        LogView view = service.getView(EXEC_1);
        view.setValue("owner", bytes("node-b")).join();

        assertThat(view.deleteValueIf("owner", bytes("node-a")).join()).isFalse();
        assertThat(view.getValue("owner").join()).isEqualTo(bytes("node-b"));

        assertThat(view.deleteValueIf("owner", bytes("node-b")).join()).isTrue();
        assertThat(view.getValue("owner").join()).isNull();
    }

    // -------------------------------------------------------------------------
    // Counters through a view
    // -------------------------------------------------------------------------

    @ParameterizedTest(name = "{0}")
    @MethodSource("adapters")
    void aCounterAccumulatesAcrossConcurrentCallers(
            String name, Function<Path, PersistenceAdapter> factory) throws Exception {
        open(factory);
        LogView view = service.getView(EXEC_1);

        int callers = 6, each = 10;
        CountDownLatch ready = new CountDownLatch(callers);
        CountDownLatch go = new CountDownLatch(1);

        List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < callers; i++) {
            threads.add(Thread.ofVirtual().start(() -> {
                ready.countDown();
                try {
                    if (!go.await(10, TimeUnit.SECONDS)) return;
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                for (int n = 0; n < each; n++) view.incrementValue("attempts", 1).join();
            }));
        }
        assertThat(ready.await(10, TimeUnit.SECONDS)).isTrue();
        go.countDown();
        for (Thread t : threads) t.join(30_000);

        assertThat(view.incrementValue("attempts", 0).join()).isEqualTo((long) callers * each);
        assertThat(CounterValues.toLong(view.getValue("attempts").join()))
                .isEqualTo((long) callers * each);
    }

    /** A typed view keeps the same KV — the bookkeeping is bytes, not the payload type. */
    @Test
    void aTypedViewOffersTheSameConditionalKv() throws IOException {
        open(dir -> new InMemoryPersistenceAdapter());
        TypedLogView<String> typed =
                service.getTypedView(EXEC_1, new KryoLogSerializer<>(String.class));

        assertThat(typed.setValueIfAbsent("owner", bytes("node-a")).join()).isTrue();
        assertThat(typed.setValueIfAbsent("owner", bytes("node-b")).join()).isFalse();
        assertThat(typed.compareAndSetValue("owner", bytes("node-a"), bytes("node-b")).join())
                .isTrue();
        assertThat(typed.incrementValue("attempts", 2).join()).isEqualTo(2L);
        assertThat(typed.deleteValueIf("owner", bytes("node-b")).join()).isTrue();
        assertThat(typed.getValue("owner").join()).isNull();
    }

    // -------------------------------------------------------------------------

    private void open(Function<Path, PersistenceAdapter> factory) throws IOException {
        service = SharedLogService.open(SharedLogConfig.builder()
                .persistenceAdapter(factory.apply(tempDir))
                .build());
    }

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }
}
