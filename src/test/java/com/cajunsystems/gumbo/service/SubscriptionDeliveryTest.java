package com.cajunsystems.gumbo.service;

import com.cajunsystems.gumbo.api.SharedLog;
import com.cajunsystems.gumbo.core.AppendRequest;
import com.cajunsystems.gumbo.core.LogPosition;
import com.cajunsystems.gumbo.core.LogTag;
import com.cajunsystems.gumbo.persistence.InMemoryPersistenceAdapter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Push subscriptions have a handover: a backlog read, then live entries. These tests
 * cover the seam, where entries were being lost.
 *
 * <p>None of them race for the window — each drives the interleaving directly with a
 * listener that blocks, so a failure is a real defect rather than a slow machine.
 */
class SubscriptionDeliveryTest {

    private static final LogTag ORDERS = LogTag.of("orders");

    private SharedLogService service;

    @BeforeEach
    void setUp() throws IOException {
        service = SharedLogService.open(SharedLogConfig.builder()
                .persistenceAdapter(new InMemoryPersistenceAdapter())
                .build());
    }

    @AfterEach
    void tearDown() {
        service.close();
    }

    /**
     * The defect: an entry appended while the subscriber is still working through its
     * backlog reached it by neither route. The backlog read had already happened, so it
     * was not in there, and the live path skipped every subscriber whose backlog was not
     * yet marked done. It was dropped silently — no error, no retry, nothing in the log.
     */
    @Test
    void anEntryAppendedDuringBacklogDeliveryIsNotLost() {
        for (int i = 0; i < 3; i++) append("backlog" + i);

        List<String> received = new CopyOnWriteArrayList<>();
        CountDownLatch firstDelivered = new CountDownLatch(1);
        CountDownLatch releaseListener = new CountDownLatch(1);
        CountDownLatch allFour = new CountDownLatch(4);

        SharedLog.Subscription sub = service.subscribe(ORDERS, LogPosition.BEGINNING, e -> {
            received.add(new String(e.data()));
            firstDelivered.countDown();
            allFour.countDown();
            awaitQuietly(releaseListener);   // hold the backlog open
        });
        try {
            awaitQuietly(firstDelivered);          // backlog delivery is now in progress
            append("live-during-backlog");         // ...and this lands mid-handover
            releaseListener.countDown();

            assertThat(awaitLatch(allFour))
                    .as("the live entry must arrive, not vanish; received=%s", received)
                    .isTrue();
            assertThat(received).containsExactly(
                    "backlog0", "backlog1", "backlog2", "live-during-backlog");
        } finally {
            sub.close();
        }
    }

    /** The same seam, entered from {@code subscribeTail} — an empty backlog. */
    @Test
    void subscribeTailLosesNothingAppendedImmediatelyAfterSubscribing() {
        append("past");

        List<String> received = new CopyOnWriteArrayList<>();
        CountDownLatch two = new CountDownLatch(2);

        SharedLog.Subscription sub = service.subscribeTail(ORDERS, e -> {
            received.add(new String(e.data()));
            two.countDown();
        });
        try {
            append("future-1");
            append("future-2");

            assertThat(awaitLatch(two)).as("received=%s", received).isTrue();
            assertThat(received).containsExactly("future-1", "future-2");
            assertThat(received).doesNotContain("past");
        } finally {
            sub.close();
        }
    }

    /**
     * An entry appended between registration and the backlog read is in both, so the
     * naive fix for losing entries trades it for delivering them twice. Hammered,
     * because that collision is timing-dependent in the other direction.
     */
    @Test
    void anEntryIsNeverDeliveredTwiceAcrossTheHandover() {
        for (int i = 0; i < 50; i++) append("e" + i);

        List<String> received = new CopyOnWriteArrayList<>();
        SharedLog.Subscription sub = service.subscribe(
                ORDERS, LogPosition.BEGINNING, e -> received.add(new String(e.data())));
        try {
            for (int i = 50; i < 100; i++) append("e" + i);   // straddles the handover

            await().atMost(10, TimeUnit.SECONDS).until(() -> received.size() >= 100);
            Thread.sleep(200);                                 // let any duplicate land

            assertThat(received).doesNotHaveDuplicates();
            assertThat(received).hasSize(100);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            sub.close();
        }
    }

    /**
     * Delivery is in seqnum order. It previously spawned a thread per entry, leaving the
     * order to the scheduler — so an ordered read of a log was not something a subscriber
     * could rely on, only something it usually got.
     */
    @Test
    void deliveryIsInSeqnumOrder() {
        List<String> received = new CopyOnWriteArrayList<>();
        SharedLog.Subscription sub = service.subscribe(
                ORDERS, LogPosition.BEGINNING, e -> received.add(new String(e.data())));
        try {
            for (int i = 0; i < 200; i++) append("e" + i);

            await().atMost(10, TimeUnit.SECONDS).until(() -> received.size() == 200);

            List<String> expected = new java.util.ArrayList<>();
            for (int i = 0; i < 200; i++) expected.add("e" + i);
            assertThat(received).containsExactlyElementsOf(expected);
        } finally {
            sub.close();
        }
    }

    /** A listener is never called concurrently with itself, so it needs no locking. */
    @Test
    void theListenerIsNeverCalledConcurrentlyWithItself() {
        AtomicInteger inFlight = new AtomicInteger();
        AtomicInteger maxInFlight = new AtomicInteger();
        AtomicInteger seen = new AtomicInteger();

        SharedLog.Subscription sub = service.subscribe(ORDERS, LogPosition.BEGINNING, e -> {
            maxInFlight.accumulateAndGet(inFlight.incrementAndGet(), Math::max);
            try { Thread.sleep(2); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
            inFlight.decrementAndGet();
            seen.incrementAndGet();
        });
        try {
            for (int i = 0; i < 50; i++) append("e" + i);
            await().atMost(10, TimeUnit.SECONDS).until(() -> seen.get() == 50);
            assertThat(maxInFlight.get()).isEqualTo(1);
        } finally {
            sub.close();
        }
    }

    /** Closing stops delivery and releases the thread blocked waiting for entries. */
    @Test
    void closingASubscriptionStopsDeliveryAndUnblocksItsThread() {
        List<String> received = new CopyOnWriteArrayList<>();
        SharedLog.Subscription sub = service.subscribe(
                ORDERS, LogPosition.BEGINNING, e -> received.add(new String(e.data())));

        append("before-close");
        await().atMost(5, TimeUnit.SECONDS).until(() -> received.size() == 1);

        sub.close();
        assertThat(sub.isActive()).isFalse();

        append("after-close");
        await().during(300, TimeUnit.MILLISECONDS)
               .atMost(2, TimeUnit.SECONDS)
               .until(() -> received.size() == 1);
        assertThat(received).containsExactly("before-close");
    }

    /**
     * A listener that throws must not take the subscription down with it.
     *
     * <p>{@code Error} matters more than it looks here. One thread now serves the whole
     * subscription, so an {@code Error} escaping the listener would kill delivery for
     * every later entry — where the previous thread-per-entry design lost only its own.
     * And it would fail quietly: {@code isActive()} would keep reporting true while
     * entries piled up in a queue nobody drains.
     */
    @Test
    void aListenerThrowingAnErrorDoesNotStopTheSubscription() {
        List<String> received = new CopyOnWriteArrayList<>();
        SharedLog.Subscription sub = service.subscribe(ORDERS, LogPosition.BEGINNING, e -> {
            String data = new String(e.data());
            received.add(data);
            if (data.equals("boom")) throw new AssertionError("listener blew up");
        });
        try {
            append("first");
            append("boom");
            append("third");
            append("fourth");

            await().atMost(10, TimeUnit.SECONDS).until(() -> received.size() == 4);
            assertThat(received).containsExactly("first", "boom", "third", "fourth");
            assertThat(sub.isActive()).isTrue();
        } finally {
            sub.close();
        }
    }

    /**
     * Once {@code close()} returns, the listener is not running and will not run again.
     * Callers release what the listener closes over on the very next line, so a delivery
     * still in flight — or one already dequeued — is a use-after-free waiting to happen.
     */
    @Test
    void closeWaitsForAnInFlightDeliveryAndDeliversNothingAfter() throws Exception {
        List<String> received = new CopyOnWriteArrayList<>();
        CountDownLatch inListener = new CountDownLatch(1);
        CountDownLatch releaseListener = new CountDownLatch(1);
        AtomicInteger completedCalls = new AtomicInteger();

        SharedLog.Subscription sub = service.subscribe(ORDERS, LogPosition.BEGINNING, e -> {
            received.add(new String(e.data()));
            inListener.countDown();
            awaitQuietly(releaseListener);
            completedCalls.incrementAndGet();
        });

        append("in-flight");
        assertThat(inListener.await(5, TimeUnit.SECONDS)).isTrue();
        append("queued-behind-it");     // waiting in the queue while the listener blocks

        Thread closer = Thread.ofVirtual().start(sub::close);
        Thread.sleep(200);
        assertThat(closer.isAlive())
                .as("close() must wait for the in-flight listener call, not race past it")
                .isTrue();

        releaseListener.countDown();
        closer.join(10_000);
        assertThat(closer.isAlive()).isFalse();

        // close() returned only after the in-flight call finished, and the entry queued
        // behind it was never delivered.
        assertThat(completedCalls.get()).isEqualTo(1);
        assertThat(received).containsExactly("in-flight");

        append("after-close");
        Thread.sleep(300);
        assertThat(received).containsExactly("in-flight");
    }

    /**
     * A listener that honours interruption must be reaped <em>before</em> close returns.
     *
     * <p>Close escalates: sentinel, wait, interrupt, wait again. Dropping that second
     * wait is the easy mistake — the interrupt is sent and the method returns in the same
     * breath, so the one chance the interrupt exists to give the listener is denied and
     * the caller frees resources under a listener that is still unwinding.
     */
    @Test
    void closeWaitsAgainAfterInterruptingAListenerThatHonoursIt() {
        CountDownLatch inListener = new CountDownLatch(1);
        AtomicBoolean finishedUnwinding = new AtomicBoolean(false);

        SharedLog.Subscription sub = service.subscribe(ORDERS, LogPosition.BEGINNING, e -> {
            inListener.countDown();
            try {
                Thread.sleep(60_000);            // blocks until interrupted
            } catch (InterruptedException ie) {
                sleepUninterruptibly(300);       // a measurable unwind
                finishedUnwinding.set(true);
                Thread.currentThread().interrupt();
            }
        });

        append("blocks-the-listener");
        awaitQuietly(inListener);

        long start = System.nanoTime();
        sub.close();
        long elapsedMs = (System.nanoTime() - start) / 1_000_000;

        assertThat(finishedUnwinding.get())
                .as("close() returned while the listener was still unwinding from its interrupt")
                .isTrue();
        // Interrupted at the half-way point, not left for the whole budget.
        assertThat(elapsedMs).isLessThan(5_000);
    }

    /**
     * An interrupt on the <em>calling</em> thread must not cut close's wait short.
     *
     * <p>Shutdown paths routinely run on an already-interrupted thread — a {@code finally}
     * after an {@code InterruptedException}, say. {@link Thread#join(long)} throws
     * immediately for such a caller, so a close that honours it skips its waits entirely
     * and returns while the listener runs on: the failure mode is worst exactly when the
     * caller is about to tear down what the listener is using. The interrupt is deferred
     * and restored, not obeyed and not swallowed.
     */
    @Test
    void anInterruptedCallerStillGetsTheFullCloseWait() {
        CountDownLatch inListener = new CountDownLatch(1);
        AtomicBoolean listenerFinished = new AtomicBoolean(false);

        SharedLog.Subscription sub = service.subscribe(ORDERS, LogPosition.BEGINNING, e -> {
            inListener.countDown();
            sleepUninterruptibly(500);
            listenerFinished.set(true);
        });

        append("blocks-the-listener");
        awaitQuietly(inListener);

        Thread.currentThread().interrupt();   // caller arrives at close() already interrupted
        try {
            sub.close();

            assertThat(listenerFinished.get())
                    .as("close() skipped its wait because the caller was interrupted")
                    .isTrue();
            assertThat(Thread.currentThread().isInterrupted())
                    .as("the caller's interrupt must be restored, not swallowed")
                    .isTrue();
        } finally {
            Thread.interrupted();   // clear, so the flag does not leak into teardown
        }
    }

    /** Closing from inside the listener must not wait for the thread doing the closing. */
    @Test
    void aListenerMayCloseItsOwnSubscriptionWithoutDeadlocking() {
        List<String> received = new CopyOnWriteArrayList<>();
        AtomicReference<SharedLog.Subscription> self = new AtomicReference<>();

        SharedLog.Subscription sub = service.subscribe(ORDERS, LogPosition.BEGINNING, e -> {
            received.add(new String(e.data()));
            self.get().close();          // unsubscribe on first entry
        });
        self.set(sub);
        try {
            append("first");
            await().atMost(5, TimeUnit.SECONDS).until(() -> received.size() == 1);
            await().atMost(5, TimeUnit.SECONDS).until(() -> !sub.isActive());

            append("second");
            Thread.sleep(300);
            assertThat(received).containsExactly("first");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            sub.close();
        }
    }

    // -------------------------------------------------------------------------

    private void append(String data) {
        service.append(AppendRequest.to(ORDERS, data.getBytes())).join();
    }

    private static boolean awaitLatch(CountDownLatch latch) {
        try {
            return latch.await(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    /** Sleeps without letting an interrupt cut it short, so the unwind is observable. */
    private static void sleepUninterruptibly(long ms) {
        long deadline = System.nanoTime() + ms * 1_000_000L;
        long remaining;
        while ((remaining = deadline - System.nanoTime()) > 0) {
            try {
                Thread.sleep(remaining / 1_000_000L, (int) (remaining % 1_000_000L));
            } catch (InterruptedException ignored) {
                // deliberate: this models a listener that finishes its cleanup
            }
        }
    }

    private static void awaitQuietly(CountDownLatch latch) {
        try {
            latch.await(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
