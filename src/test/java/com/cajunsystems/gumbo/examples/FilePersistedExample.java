package com.cajunsystems.gumbo.examples;

import com.cajunsystems.gumbo.core.AppendRequest;
import com.cajunsystems.gumbo.core.AppendResult;
import com.cajunsystems.gumbo.core.LogEntry;
import com.cajunsystems.gumbo.core.LogTag;
import com.cajunsystems.gumbo.persistence.FileBasedPersistenceAdapter;
import com.cajunsystems.gumbo.service.SharedLogConfig;
import com.cajunsystems.gumbo.service.SharedLogService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Demonstrates durable persistence and crash recovery using
 * {@link FileBasedPersistenceAdapter}.
 *
 * <h2>What it shows</h2>
 * <ul>
 *   <li>Entries written in one JVM run are fully recoverable after restart.</li>
 *   <li>Per-tag indices are rebuilt from the on-disk WAL — no separate checkpoint
 *       file needed.</li>
 *   <li>The sequencer is reseeded from the latest persisted seqnum so new appends
 *       continue from where they left off, never reusing a seqnum.</li>
 * </ul>
 *
 * <p>Run with: {@code mvn test -Dtest=FilePersistedExample}
 */
class FilePersistedExample {

    // -------------------------------------------------------------------------

    @Test
    void entriesSurviveRestart(@TempDir Path dataDir) throws Exception {
        LogTag events = LogTag.of("events");
        String path   = dataDir.toString();

        // ── First "run": write three entries, then close ──────────────────────
        try (SharedLogService log = openFileBacked(path)) {

            log.append(AppendRequest.to(events, "event-A".getBytes())).join();
            log.append(AppendRequest.to(events, "event-B".getBytes())).join();
            AppendResult last = log.append(AppendRequest.to(events, "event-C".getBytes())).join();

            System.out.println("First run: wrote 3 entries, latest seqnum=" + last.seqnum());
        }
        // log.dat, index.dat, and trim.dat are now on disk; the JVM process has "crashed".

        // ── Second "run": reopen the same directory and verify full recovery ──
        try (SharedLogService log = openFileBacked(path)) {

            List<LogEntry> recovered = log.readAll(events).join();
            assertThat(recovered).hasSize(3);
            assertThat(recovered.stream().map(e -> new String(e.data())).toList())
                    .containsExactly("event-A", "event-B", "event-C");

            System.out.println("Second run: recovered " + recovered.size() + " entries:");
            for (LogEntry e : recovered) {
                System.out.printf("  [seqnum=%d, localId=%d] %s%n",
                        e.seqnum(), e.localId(), new String(e.data()));
            }

            // The sequencer resumes after the last persisted seqnum
            assertThat(log.getLatestSeqnum()).isEqualTo(2); // seqnums 0, 1, 2

            // New appends continue from seqnum 3 — no reuse of old seqnums
            AppendResult r4 = log.append(AppendRequest.to(events, "event-D".getBytes())).join();
            assertThat(r4.seqnum()).isEqualTo(3);
            assertThat(log.readAll(events).join()).hasSize(4);
        }
    }

    // -------------------------------------------------------------------------

    @Test
    void perTagIndicesRebuiltCorrectly(@TempDir Path dataDir) throws Exception {
        String path      = dataDir.toString();
        LogTag orders    = LogTag.of("orders");
        LogTag inventory = LogTag.of("inventory");

        // Write entries on two different tags
        try (SharedLogService log = openFileBacked(path)) {
            log.append(AppendRequest.to(orders,    "order-placed".getBytes())).join();
            log.append(AppendRequest.to(inventory, "stock-decremented".getBytes())).join();
            log.append(AppendRequest.to(orders,    "order-confirmed".getBytes())).join();
        }

        // After recovery, each tag's view returns only its own entries
        try (SharedLogService log = openFileBacked(path)) {
            List<LogEntry> orderEntries = log.readAll(orders).join();
            List<LogEntry> stockEntries = log.readAll(inventory).join();

            assertThat(orderEntries).hasSize(2);
            assertThat(stockEntries).hasSize(1);

            // localIds are tag-local: orders has localIds 0 and 1; inventory has 0
            assertThat(orderEntries.get(0).localId()).isEqualTo(0);
            assertThat(orderEntries.get(1).localId()).isEqualTo(1);
            assertThat(stockEntries.get(0).localId()).isEqualTo(0);

            System.out.printf("Recovered: %d order entries, %d inventory entries%n",
                    orderEntries.size(), stockEntries.size());
        }
    }

    // -------------------------------------------------------------------------

    private static SharedLogService openFileBacked(String path) throws Exception {
        return SharedLogService.open(SharedLogConfig.builder()
                .persistenceAdapter(new FileBasedPersistenceAdapter(path))
                .build());
    }
}
