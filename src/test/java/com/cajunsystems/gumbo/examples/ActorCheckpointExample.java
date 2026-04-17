package com.cajunsystems.gumbo.examples;

import com.cajunsystems.gumbo.api.LogView;
import com.cajunsystems.gumbo.api.SharedLog;
import com.cajunsystems.gumbo.core.AppendRequest;
import com.cajunsystems.gumbo.core.LogEntry;
import com.cajunsystems.gumbo.core.LogPosition;
import com.cajunsystems.gumbo.core.LogTag;
import com.cajunsystems.gumbo.persistence.InMemoryPersistenceAdapter;
import com.cajunsystems.gumbo.service.SharedLogConfig;
import com.cajunsystems.gumbo.service.SharedLogService;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Demonstrates the actor checkpoint pattern: an actor persists its last-processed
 * seqnum via the KV store so it can resume from exactly that point on restart,
 * then switch seamlessly to live delivery via subscribeTail().
 *
 * <p>Pattern:
 * <ol>
 *   <li>On start, read checkpoint seqnum from KV ({@code getValue("checkpoint")}).</li>
 *   <li>Replay missed entries via {@code readFrom(checkpoint + 1, ...)}.</li>
 *   <li>Switch to live delivery via {@code subscribeTail(listener)}.</li>
 *   <li>After each message, persist the new seqnum via {@code setValue("checkpoint", ...)}.</li>
 * </ol>
 *
 * <p>Run with: {@code mvn test -Dtest=ActorCheckpointExample}
 */
class ActorCheckpointExample {

    private static final LogTag ORDERS = LogTag.of("actor", "orders");
    private static final String CHECKPOINT_KEY = "checkpoint";

    private static byte[] toBytes(long v) {
        return ByteBuffer.allocate(8).putLong(v).array();
    }

    private static long fromBytes(byte[] b) {
        return ByteBuffer.wrap(b).getLong();
    }

    @Test
    void actorResumesFromCheckpointAndSwitchesToLive() throws Exception {
        try (SharedLogService service = SharedLogService.open(
                SharedLogConfig.builder()
                        .persistenceAdapter(new InMemoryPersistenceAdapter())
                        .build())) {

            LogView view = service.getView(ORDERS);

            // ── Phase 1: Pre-populate messages ──────────────────────────────────
            service.append(AppendRequest.to(ORDERS, "msg-1".getBytes())).join();
            service.append(AppendRequest.to(ORDERS, "msg-2".getBytes())).join();
            service.append(AppendRequest.to(ORDERS, "msg-3".getBytes())).join();

            // ── Phase 2: Save checkpoint at msg-1 ───────────────────────────────
            // Read all to find msg-1's seqnum; then simulate: actor processed
            // msg-1 and checkpointed that seqnum.
            List<LogEntry> allEntries = view.readAll().join();
            long msg1Seqnum = allEntries.get(0).seqnum();

            view.setValue(CHECKPOINT_KEY, toBytes(msg1Seqnum)).join();

            // ── Phase 3: Simulate restart — read checkpoint and replay ───────────
            byte[] savedCheckpoint = view.getValue(CHECKPOINT_KEY).join();
            long resumeFrom = (savedCheckpoint != null)
                    ? fromBytes(savedCheckpoint) + 1  // resume AFTER last processed
                    : 0L;

            List<String> replayed = new CopyOnWriteArrayList<>();
            List<LogEntry> backlog = view.readFrom(new LogPosition(resumeFrom), 100).join();
            for (LogEntry e : backlog) {
                replayed.add(new String(e.data()));
                view.setValue(CHECKPOINT_KEY, toBytes(e.seqnum())).join();
            }

            // Only msg-2 and msg-3 should be replayed (msg-1 was already checkpointed)
            assertThat(replayed).containsExactly("msg-2", "msg-3");

            // ── Phase 4: Switch to live delivery via subscribeTail ───────────────
            List<String> live = new CopyOnWriteArrayList<>();
            SharedLog.Subscription sub = view.subscribeTail(
                    e -> live.add(new String(e.data())));

            try {
                service.append(AppendRequest.to(ORDERS, "msg-4".getBytes())).join();
                service.append(AppendRequest.to(ORDERS, "msg-5".getBytes())).join();

                await().atMost(3, TimeUnit.SECONDS).until(() -> live.size() == 2);

                assertThat(live).containsExactly("msg-4", "msg-5");
                assertThat(live).doesNotContain("msg-1", "msg-2", "msg-3");
            } finally {
                sub.close();
            }
        }
    }
}
