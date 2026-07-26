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
import java.util.concurrent.atomic.AtomicInteger;

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

    private static void awaitQuietly(CountDownLatch latch) {
        try {
            latch.await(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
