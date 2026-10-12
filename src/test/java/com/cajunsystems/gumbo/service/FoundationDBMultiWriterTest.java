package com.cajunsystems.gumbo.service;

import com.cajunsystems.gumbo.core.CounterValues;
import com.cajunsystems.gumbo.core.LogEntry;
import com.cajunsystems.gumbo.core.LogTag;
import com.cajunsystems.gumbo.persistence.FdbTestSupport;
import com.cajunsystems.gumbo.persistence.FoundationDBPersistenceAdapter;
import com.cajunsystems.gumbo.sequencer.FoundationDBSequencer;
import com.cajunsystems.gumbo.service.MultiWriterPeer.Pair;
import com.cajunsystems.gumbo.service.MultiWriterPeer.Report;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static com.cajunsystems.gumbo.service.MultiWriterPeer.FANOUT_APPENDS;
import static com.cajunsystems.gumbo.service.MultiWriterPeer.FENCE_ROUNDS;
import static com.cajunsystems.gumbo.service.MultiWriterPeer.LEASE_ROUNDS;
import static com.cajunsystems.gumbo.service.MultiWriterPeer.SHARED_APPENDS;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@code multiWriter}, measured: two JVMs, each its own {@link SharedLogService} over its own
 * {@link FoundationDBPersistenceAdapter} and {@link FoundationDBSequencer}, on one cluster and
 * one root, appending at the same time. This is the configuration the FDB adapter declares
 * {@code multiWriter} for and the service composes it from, and until now nothing had run it
 * with more than one writer — the adapter's tests are single-instance, and the sequencer only
 * appeared behind a stand-in. Catalyst's distributed execution (Dist-0, #38) will not trust
 * the flag until it comes from a measured configuration, which is the right instinct: a
 * capability nobody has exercised is prose with a return type.
 *
 * <p>The script both writers run is {@link MultiWriterPeer}. This class launches the second
 * JVM, runs the first on a thread, and asserts over both reports plus what a third, fresh
 * service reads afterwards.
 *
 * <p>Skipped without a cluster; CI runs it against a service container with
 * {@code GUMBO_REQUIRE_FDB=true}, so there a skip is a failure. {@code scripts/fdb-tests.sh}
 * runs it locally in Docker.
 */
class FoundationDBMultiWriterTest {

    private static boolean fdbAvailable;

    @BeforeAll
    static void checkFdbAvailability() {
        fdbAvailable = FdbTestSupport.probe("gumbo-mw");
    }

    @Test
    @Timeout(value = 5, unit = TimeUnit.MINUTES)
    void twoProcessesShareOneLogAndStorageArbitratesEveryClaim() throws Exception {
        FdbTestSupport.assumeAvailable(fdbAvailable);
        String clusterFile = FdbTestSupport.clusterFile();
        String root = FdbTestSupport.freshRoot("gumbo-mw");

        Process child = launchPeer("B", clusterFile, root);
        StringBuilder childOut = new StringBuilder();
        Thread drain = Thread.ofVirtual().start(() -> {
            try (BufferedReader r = new BufferedReader(
                    new InputStreamReader(child.getInputStream(), StandardCharsets.UTF_8))) {
                String line;
                while ((line = r.readLine()) != null) childOut.append(line).append('\n');
            } catch (Exception e) {
                childOut.append("<<drain failed: ").append(e).append(">>\n");
            }
        });

        Report a;
        try {
            a = MultiWriterPeer.run("A", clusterFile, root);
        } catch (Throwable t) {
            child.destroyForcibly();
            throw t;
        }
        assertThat(child.waitFor(3, TimeUnit.MINUTES)).as("writer B finished").isTrue();
        drain.join();
        assertThat(child.exitValue()).as("writer B exited cleanly; its stderr is above").isEqualTo(0);
        Report b = parse(childOut.toString());

        // ---- The configuration under test is the one that reports the flag -------------
        assertThat(a.multiWriter()).as("A: FDB adapter + FDB sequencer composes to multiWriter").isTrue();
        assertThat(b.multiWriter()).as("B: same").isTrue();

        // ---- 1. Same tag: dense across both writers, readable from both --------------------
        List<Long> sharedVersions = new ArrayList<>();
        a.shared().forEach(p -> sharedVersions.add(p.version()));
        b.shared().forEach(p -> sharedVersions.add(p.version()));
        assertThat(sharedVersions).hasSize(2 * SHARED_APPENDS);
        assertThat(new HashSet<>(sharedVersions))
                .as("streamVersions handed to the two writers are dense 0..n-1 with no duplicate")
                .isEqualTo(range(0, 2L * SHARED_APPENDS));
        assertThat(a.shared().stream().map(Pair::version).toList())
                .as("each writer's own versions arrive in increasing order")
                .isSorted();
        assertThat(b.shared().stream().map(Pair::version).toList()).isSorted();

        List<Long> dense = LongStream.range(0, 2L * SHARED_APPENDS).boxed().toList();
        assertThat(a.sharedSeen()).as("A reads every append, including B's, in version order").isEqualTo(dense);
        assertThat(b.sharedSeen()).as("B reads every append, including A's, in version order").isEqualTo(dense);

        // ---- 2 (of #38): metadata is read through, not cached per process ----------------
        long maxSharedSeqnum = Math.max(maxSeqnum(a.shared()), maxSeqnum(b.shared()));
        assertThat(a.latestVersionSeen()).as("A's getLatestVersion() includes B's appends").isEqualTo(2L * SHARED_APPENDS - 1);
        assertThat(b.latestVersionSeen()).as("B's getLatestVersion() includes A's appends").isEqualTo(2L * SHARED_APPENDS - 1);
        assertThat(a.latestSeqnumSeen()).as("A's getLatestSeqnum() is the log's, not A's").isEqualTo(maxSharedSeqnum);
        assertThat(b.latestSeqnumSeen()).as("B's getLatestSeqnum() is the log's, not B's").isEqualTo(maxSharedSeqnum);

        // ---- 3. Conditional append: exactly one winner per round -------------------------
        assertThat(a.fenceWon()).hasSize(FENCE_ROUNDS);
        assertThat(b.fenceWon()).hasSize(FENCE_ROUNDS);
        for (int round = 0; round < FENCE_ROUNDS; round++) {
            assertThat(a.fenceWon().get(round) ^ b.fenceWon().get(round))
                    .as("round %d: exactly one of the two writers was accepted at version %d", round, round)
                    .isTrue();
        }
        List<Long> fenceVersions = new ArrayList<>();
        a.fenceWins().forEach(p -> fenceVersions.add(p.version()));
        b.fenceWins().forEach(p -> fenceVersions.add(p.version()));
        assertThat(new HashSet<>(fenceVersions)).as("the winners' versions are exactly the rounds").isEqualTo(range(0, FENCE_ROUNDS));

        // ---- 4. Multi-tag fan-out: own tags dense, queue dense, fence on the named tag only -
        assertThat(a.fanout().stream().map(Pair::version).toList())
                .as("A fenced every append on its own tag at the expected version and was never refused")
                .isEqualTo(LongStream.range(0, FANOUT_APPENDS).boxed().toList());
        assertThat(b.fanout().stream().map(Pair::version).toList()).isEqualTo(LongStream.range(0, FANOUT_APPENDS).boxed().toList());
        List<Long> queueDense = LongStream.range(0, 2L * FANOUT_APPENDS).boxed().toList();
        assertThat(a.queueSeen().stream().map(Pair::version).toList())
                .as("the queue tag has its own dense numbering across both writers")
                .isEqualTo(queueDense);
        assertThat(b.queueSeen()).as("both writers read the same queue").isEqualTo(a.queueSeen());
        Set<Long> fanoutSeqnums = new HashSet<>();
        a.fanout().forEach(p -> fanoutSeqnums.add(p.seqnum()));
        b.fanout().forEach(p -> fanoutSeqnums.add(p.seqnum()));
        assertThat(a.queueSeen().stream().map(Pair::seqnum).collect(Collectors.toSet()))
                .as("the queue holds exactly the fan-out entries, by seqnum")
                .isEqualTo(fanoutSeqnums);

        // ---- 5. KV CAS: exactly one winner per round, for the claim and for the takeover ---
        assertThat(a.leaseWon()).hasSize(LEASE_ROUNDS);
        assertThat(b.leaseWon()).hasSize(LEASE_ROUNDS);
        for (int round = 0; round < LEASE_ROUNDS; round++) {
            assertThat(a.leaseWon().get(round) ^ b.leaseWon().get(round))
                    .as("lease round %d: exactly one setValueIfAbsent won", round).isTrue();
            assertThat(a.casWon().get(round) ^ b.casWon().get(round))
                    .as("cas round %d: exactly one compareAndSetValue from the same holder won", round).isTrue();
        }

        // ---- 6. Global seqnums never collide ----------------------------------------------
        List<Long> allSeqnums = new ArrayList<>();
        for (Report r : List.of(a, b)) {
            r.shared().forEach(p -> allSeqnums.add(p.seqnum()));
            r.fenceWins().forEach(p -> allSeqnums.add(p.seqnum()));
            r.fanout().forEach(p -> allSeqnums.add(p.seqnum()));
        }
        int expectedEntries = 2 * SHARED_APPENDS + FENCE_ROUNDS + 2 * FANOUT_APPENDS;
        assertThat(allSeqnums).hasSize(expectedEntries);
        assertThat(new HashSet<>(allSeqnums)).as("two FoundationDBSequencers never issue the same seqnum").hasSize(expectedEntries);

        // ---- A third, fresh service sees it all (D2 does not apply) and the metadata agrees -
        FoundationDBSequencer sequencerC = new FoundationDBSequencer(clusterFile, root + "-seq");
        sequencerC.open();
        SharedLogService c = SharedLogService.open(SharedLogConfig.builder()
                .persistenceAdapter(new FoundationDBPersistenceAdapter(clusterFile, root))
                .sequencer(sequencerC)
                .build());
        try {
            assertThat(versions(c.readFromVersion(MultiWriterPeer.SHARED, 0L).join(), MultiWriterPeer.SHARED))
                    .as("a process that wrote nothing reads both writers' appends, dense")
                    .isEqualTo(dense);
            List<LogEntry> fence = c.readFromVersion(MultiWriterPeer.FENCE, 0L).join();
            assertThat(versions(fence, MultiWriterPeer.FENCE)).isEqualTo(LongStream.range(0, FENCE_ROUNDS).boxed().toList());
            for (int round = 0; round < FENCE_ROUNDS; round++) {
                String winner = a.fenceWon().get(round) ? "A" : "B";
                assertThat(MultiWriterPeer.text(fence.get(round)))
                        .as("the entry at version %d was written by the writer that was told it won", round)
                        .isEqualTo(winner + ":fence:" + round);
            }
            assertThat(versions(c.readFromVersion(MultiWriterPeer.QUEUE, 0L).join(), MultiWriterPeer.QUEUE)).isEqualTo(queueDense);
            assertThat(c.getLatestSeqnum())
                    .as("meta/latest is the highest seqnum any writer committed, never a lower one that committed later")
                    .isEqualTo(allSeqnums.stream().mapToLong(Long::longValue).max().orElseThrow());
            assertThat(c.getView(MultiWriterPeer.SHARED).getLatestVersion()).isEqualTo(2L * SHARED_APPENDS - 1);
            // A refused conditional append has already claimed its seqnum from the sequencer by
            // the time storage rejects it, so each losing round leaves one seqnum unused. That
            // is true of every adapter, not only FDB, and harmless: a seqnum is a position in
            // the global order, not a count of entries. What must hold is that nothing else
            // was issued — the counter advanced by exactly the entries plus the refusals.
            assertThat(sequencerC.currentGlobal())
                    .as("the shared counter issued one seqnum per entry plus one per refused append")
                    .isEqualTo(expectedEntries + FENCE_ROUNDS - 1L);

            for (String name : MultiWriterPeer.barrierNames()) {
                assertThat(CounterValues.toLong(c.getView(MultiWriterPeer.BARRIER).getValue(name).join()))
                        .as("barrier %s: two increments from two processes, no lost or doubled update", name)
                        .isEqualTo(2L);
            }
        } finally {
            c.close();
            sequencerC.close();
        }
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    /** The second JVM: same java, same classpath, {@link MultiWriterPeer#main}. */
    private static Process launchPeer(String writerId, String clusterFile, String root) throws Exception {
        String java = ProcessHandle.current().info().command().orElse("java");
        List<String> command = List.of(
                java, "-cp", System.getProperty("java.class.path"),
                MultiWriterPeer.class.getName(),
                writerId, clusterFile == null ? "-" : clusterFile, root);
        return new ProcessBuilder(command)
                .redirectError(ProcessBuilder.Redirect.INHERIT)
                .start();
    }

    private static Report parse(String stdout) {
        List<String> lines = stdout.lines().toList();
        int begin = lines.indexOf(MultiWriterPeer.REPORT_BEGIN);
        int end   = lines.indexOf(MultiWriterPeer.REPORT_END);
        assertThat(begin).as("writer B printed a report; its stdout was:%n%s", stdout).isNotNegative();
        assertThat(end).isGreaterThan(begin);
        return Report.decode(lines.subList(begin + 1, end));
    }

    private static Set<Long> range(long fromInclusive, long toExclusive) {
        return LongStream.range(fromInclusive, toExclusive).boxed().collect(Collectors.toSet());
    }

    private static long maxSeqnum(List<Pair> pairs) {
        return pairs.stream().mapToLong(Pair::seqnum).max().orElse(-1L);
    }

    private static List<Long> versions(List<LogEntry> entries, LogTag tag) {
        return entries.stream().map(e -> e.streamVersion(tag)).toList();
    }
}
