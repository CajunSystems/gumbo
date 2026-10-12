package com.cajunsystems.gumbo.service;

import com.cajunsystems.gumbo.api.LogView;
import com.cajunsystems.gumbo.core.AppendRequest;
import com.cajunsystems.gumbo.core.AppendResult;
import com.cajunsystems.gumbo.core.CounterValues;
import com.cajunsystems.gumbo.core.LogEntry;
import com.cajunsystems.gumbo.core.LogTag;
import com.cajunsystems.gumbo.core.VersionConflictException;
import com.cajunsystems.gumbo.persistence.FoundationDBPersistenceAdapter;
import com.cajunsystems.gumbo.sequencer.FoundationDBSequencer;

import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionException;
import java.util.stream.Collectors;

/**
 * One writer of the two in {@link FoundationDBMultiWriterTest}: a {@link SharedLogService}
 * over its own {@link FoundationDBPersistenceAdapter} and {@link FoundationDBSequencer}, on
 * a cluster and root it shares with the other writer.
 *
 * <p>Both writers run <em>this</em> code. The test JVM runs it as writer {@code A} on a
 * thread; writer {@code B} is a second JVM launched on the same classpath, running
 * {@link #main}, which prints its {@link Report} between two marker lines for the test to
 * parse. The phases are separated by barriers so the two really do contend, rather than one
 * finishing before the other starts. The barriers are counters in the log's own tag KV —
 * {@code incrementValue} across processes is one of the things under test, and a barrier
 * that never fills times out loudly rather than passing.
 *
 * <p>What each phase measures, against #38's list:
 * <ol>
 *   <li><b>Same tag</b> — {@value #SHARED_APPENDS} unconditional appends each to one tag.</li>
 *   <li><b>Conditional append</b> — {@value #FENCE_ROUNDS} rounds of both racing
 *       {@code append(request, tag, v)} for the same {@code v}.</li>
 *   <li><b>Multi-tag fan-out</b> — {@value #FANOUT_APPENDS} appends each into
 *       {@code (own execution tag, shared queue tag)}, fenced on the own tag.</li>
 *   <li><b>KV CAS</b> — {@value #LEASE_ROUNDS} rounds of racing {@code setValueIfAbsent},
 *       then racing {@code compareAndSetValue} from the same current value.</li>
 * </ol>
 * The global seqnum check and the read-through metadata check come out of the same data.
 */
final class MultiWriterPeer {

    static final int SHARED_APPENDS = 150;
    static final int FENCE_ROUNDS   = 20;
    static final int FANOUT_APPENDS = 100;
    static final int LEASE_ROUNDS   = 20;

    static final LogTag SHARED  = LogTag.of("mw", "shared");
    static final LogTag FENCE   = LogTag.of("mw", "fence");
    static final LogTag QUEUE   = LogTag.of("mw", "queue");
    static final LogTag LEASE   = LogTag.of("mw", "lease");
    static final LogTag BARRIER = LogTag.of("mw", "barrier");

    static LogTag ownTag(String writerId) {
        return LogTag.of("mw", "exec-" + writerId);
    }

    static final String REPORT_BEGIN = "MW-REPORT-BEGIN";
    static final String REPORT_END   = "MW-REPORT-END";

    private static final Duration BARRIER_TIMEOUT = Duration.ofSeconds(90);

    /** A seqnum and a version the writer was handed for one append. */
    record Pair(long seqnum, long version) {}

    /** Everything one writer saw, in a form the other JVM can print and this one can parse. */
    record Report(
            String writerId,
            boolean multiWriter,
            List<Pair> shared,              // phase 1: (seqnum, version) per append
            List<Long> sharedSeen,          // phase 1: versions read back after both finished
            long latestVersionSeen,         // phase 1: LogView.getLatestVersion() after both finished
            long latestSeqnumSeen,          // phase 1: SharedLog.getLatestSeqnum() after both finished
            List<Boolean> fenceWon,         // phase 2: per round
            List<Pair> fenceWins,           // phase 2: (seqnum, version) for each round won
            List<Pair> fanout,              // phase 3: (seqnum, own-tag version) per append
            List<Pair> queueSeen,           // phase 3: (seqnum, queue version) read back after both finished
            List<Boolean> leaseWon,         // phase 4: setValueIfAbsent per round
            List<Boolean> casWon) {         // phase 4: compareAndSetValue per round

        List<String> encode() {
            return List.of(
                    "writerId=" + writerId,
                    "multiWriter=" + multiWriter,
                    "shared=" + pairs(shared),
                    "sharedSeen=" + longs(sharedSeen),
                    "latestVersionSeen=" + latestVersionSeen,
                    "latestSeqnumSeen=" + latestSeqnumSeen,
                    "fenceWon=" + bools(fenceWon),
                    "fenceWins=" + pairs(fenceWins),
                    "fanout=" + pairs(fanout),
                    "queueSeen=" + pairs(queueSeen),
                    "leaseWon=" + bools(leaseWon),
                    "casWon=" + bools(casWon));
        }

        static Report decode(List<String> lines) {
            Map<String, String> f = lines.stream()
                    .filter(l -> l.contains("="))
                    .collect(Collectors.toMap(l -> l.substring(0, l.indexOf('=')),
                                              l -> l.substring(l.indexOf('=') + 1)));
            return new Report(
                    f.get("writerId"),
                    Boolean.parseBoolean(f.get("multiWriter")),
                    parsePairs(f.get("shared")),
                    parseLongs(f.get("sharedSeen")),
                    Long.parseLong(f.get("latestVersionSeen")),
                    Long.parseLong(f.get("latestSeqnumSeen")),
                    parseBools(f.get("fenceWon")),
                    parsePairs(f.get("fenceWins")),
                    parsePairs(f.get("fanout")),
                    parsePairs(f.get("queueSeen")),
                    parseBools(f.get("leaseWon")),
                    parseBools(f.get("casWon")));
        }

        private static String pairs(List<Pair> ps) {
            return ps.stream().map(p -> p.seqnum() + ":" + p.version()).collect(Collectors.joining(","));
        }
        private static String longs(List<Long> ls) {
            return ls.stream().map(String::valueOf).collect(Collectors.joining(","));
        }
        private static String bools(List<Boolean> bs) {
            return bs.stream().map(String::valueOf).collect(Collectors.joining(","));
        }
        private static List<Pair> parsePairs(String s) {
            if (s == null || s.isEmpty()) return List.of();
            return Arrays.stream(s.split(",")).map(p -> {
                String[] halves = p.split(":");
                return new Pair(Long.parseLong(halves[0]), Long.parseLong(halves[1]));
            }).toList();
        }
        private static List<Long> parseLongs(String s) {
            if (s == null || s.isEmpty()) return List.of();
            return Arrays.stream(s.split(",")).map(Long::parseLong).toList();
        }
        private static List<Boolean> parseBools(String s) {
            if (s == null || s.isEmpty()) return List.of();
            return Arrays.stream(s.split(",")).map(Boolean::parseBoolean).toList();
        }
    }

    // -------------------------------------------------------------------------
    // The second JVM's entry point
    // -------------------------------------------------------------------------

    /** {@code args}: writerId, cluster file or {@code -} for the default, root. */
    public static void main(String[] args) throws Exception {
        String writerId    = args[0];
        String clusterFile = "-".equals(args[1]) ? null : args[1];
        String root        = args[2];

        Report report = run(writerId, clusterFile, root);

        PrintStream out = System.out;
        out.println(REPORT_BEGIN);
        for (String line : report.encode()) out.println(line);
        out.println(REPORT_END);
        out.flush();
        // The FDB client's network thread is not a daemon; an explicit exit is the clean way out.
        System.exit(0);
    }

    // -------------------------------------------------------------------------
    // The script both writers run
    // -------------------------------------------------------------------------

    static Report run(String writerId, String clusterFile, String root) throws Exception {
        FoundationDBSequencer sequencer = new FoundationDBSequencer(clusterFile, root + "-seq");
        sequencer.open();
        SharedLogService service = SharedLogService.open(SharedLogConfig.builder()
                .persistenceAdapter(new FoundationDBPersistenceAdapter(clusterFile, root))
                .sequencer(sequencer)
                .build());
        try {
            return new MultiWriterPeer(writerId, service).script();
        } finally {
            service.close();
            sequencer.close();
        }
    }

    private final String writerId;
    private final SharedLogService service;
    private final LogTag own;

    private MultiWriterPeer(String writerId, SharedLogService service) {
        this.writerId = writerId;
        this.service  = service;
        this.own      = ownTag(writerId);
    }

    private Report script() throws Exception {
        boolean multiWriter = service.capabilities().multiWriter();
        barrier("start");

        // Phase 1 — same tag, unconditional, flat out.
        List<Pair> shared = new ArrayList<>();
        for (int i = 0; i < SHARED_APPENDS; i++) {
            AppendResult r = service.append(AppendRequest.to(SHARED, bytes(writerId + ":shared:" + i))).join();
            shared.add(new Pair(r.seqnum(), r.streamVersion()));
        }
        barrier("p1-done");
        List<Long> sharedSeen = service.readFromVersion(SHARED, 0L).join().stream()
                .map(e -> e.streamVersion(SHARED)).toList();
        long latestVersionSeen = service.getView(SHARED).getLatestVersion();
        long latestSeqnumSeen  = service.getLatestSeqnum();
        barrier("p1-read");   // nobody claims another seqnum until both have read the metadata

        // Phase 2 — both race the same expected version, every round.
        List<Boolean> fenceWon  = new ArrayList<>();
        List<Pair>    fenceWins = new ArrayList<>();
        for (int round = 0; round < FENCE_ROUNDS; round++) {
            barrier("fence-" + round);
            try {
                AppendResult r = service.append(
                        AppendRequest.to(FENCE, bytes(writerId + ":fence:" + round)), FENCE, round).join();
                fenceWon.add(true);
                fenceWins.add(new Pair(r.seqnum(), r.streamVersion()));
            } catch (CompletionException e) {
                if (!causedByConflict(e)) throw e;
                fenceWon.add(false);
            }
        }
        barrier("p2-done");

        // Phase 3 — one append into (own execution tag, shared queue tag), fenced on own.
        List<Pair> fanout = new ArrayList<>();
        for (int i = 0; i < FANOUT_APPENDS; i++) {
            AppendResult r = service.append(
                    AppendRequest.to(Set.of(own, QUEUE), bytes(writerId + ":fanout:" + i)), own, i).join();
            fanout.add(new Pair(r.seqnum(), r.streamVersion()));
        }
        barrier("p3-done");
        List<Pair> queueSeen = service.readFromVersion(QUEUE, 0L).join().stream()
                .map(e -> new Pair(e.seqnum(), e.streamVersion(QUEUE))).toList();
        barrier("p3-read");

        // Phase 4 — the claim, then the takeover, each raced.
        LogView lease = service.getView(LEASE);
        List<Boolean> leaseWon = new ArrayList<>();
        List<Boolean> casWon   = new ArrayList<>();
        for (int round = 0; round < LEASE_ROUNDS; round++) {
            String key = "lease-" + round;
            barrier("lease-" + round);
            leaseWon.add(lease.setValueIfAbsent(key, bytes(writerId)).join());

            byte[] holder = lease.getValue(key).join();   // the same value for both, after the barrier
            barrier("cas-" + round);
            casWon.add(lease.compareAndSetValue(key, holder, bytes(writerId + ":took")).join());
        }
        barrier("p4-done");

        return new Report(writerId, multiWriter, shared, sharedSeen, latestVersionSeen, latestSeqnumSeen,
                fenceWon, fenceWins, fanout, queueSeen, leaseWon, casWon);
    }

    /** Every barrier name in order, so the test can check each counted exactly two arrivals. */
    static List<String> barrierNames() {
        List<String> names = new ArrayList<>(List.of("start", "p1-done", "p1-read"));
        for (int r = 0; r < FENCE_ROUNDS; r++) names.add("fence-" + r);
        names.addAll(List.of("p2-done", "p3-done", "p3-read"));
        for (int r = 0; r < LEASE_ROUNDS; r++) {
            names.add("lease-" + r);
            names.add("cas-" + r);
        }
        names.add("p4-done");
        return names;
    }

    /**
     * Arrive, then wait for the other writer. Counted in the tag KV: each arrival is one
     * {@code incrementValue}, and the barrier opens when the counter reads two. A count above
     * two means an increment was applied twice, which is its own failure.
     */
    private void barrier(String name) throws InterruptedException {
        LogView view = service.getView(BARRIER);
        view.incrementValue(name, 1).join();
        long deadline = System.nanoTime() + BARRIER_TIMEOUT.toNanos();
        while (true) {
            long arrived = CounterValues.toLong(view.getValue(name).join());
            if (arrived == 2) return;
            if (arrived > 2) throw new IllegalStateException("barrier " + name + " counted " + arrived + " arrivals");
            if (System.nanoTime() > deadline) {
                throw new IllegalStateException("barrier " + name + " timed out with " + arrived + " arrival(s)");
            }
            Thread.sleep(5);
        }
    }

    private static boolean causedByConflict(Throwable t) {
        for (Throwable c = t; c != null; c = c.getCause()) {
            if (c instanceof VersionConflictException) return true;
        }
        return false;
    }

    static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    static String text(LogEntry e) {
        return new String(e.dataUnsafe(), StandardCharsets.UTF_8);
    }
}
