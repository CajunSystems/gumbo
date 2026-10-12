package com.cajunsystems.gumbo.sequencer;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.cajunsystems.gumbo.core.OperationToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * A distributed {@link Sequencer} backed by FoundationDB.
 *
 * <h2>Mechanism</h2>
 * <p>A single FDB key — {@code {root} / "seq"} — holds the current counter value
 * as an 8-byte big-endian long.  Each call to {@link #next()} runs a read-modify-
 * write transaction that atomically increments the counter and returns the value
 * it claimed.  FDB's optimistic concurrency control serialises concurrent callers
 * automatically; conflicting transactions are transparently retried by the client.
 *
 * <p>The client also retries on {@code commit_unknown_result}, with the commit possibly
 * already applied. A plain read-modify-write run twice would claim twice and skip a
 * number, so each claim records an {@link OperationToken} and its base under
 * {@code {root} / "claim" / readVersion / token} in the same transaction, and a retry looks
 * up its own earlier attempts and returns the base it already holds.
 *
 * <h2>Why FDB over an AtomicLong</h2>
 * <p>In a single-process deployment {@link LocalSequencer} is sufficient and
 * much faster.  {@code FoundationDBSequencer} is intended for <em>multi-node</em>
 * scenarios where several writer processes must share a single, monotonic seqnum
 * space — mirroring the role the FDB-backed metalog plays in the original Boki
 * design (SOSP 2021).
 *
 * <h2>Throughput characteristics</h2>
 * <p>Each {@link #next()} call is a round-trip to the FDB cluster.  Typical
 * latency is 1–5 ms on a local cluster.  If many writers contend on the same
 * counter key you will see retry-driven latency spikes; consider batching seqnum
 * reservations (claiming a range at once) for high-throughput workloads.
 *
 * <h2>Lifecycle</h2>
 * <p>Call {@link #open()} before use and {@link #close()} when done.  If a
 * pre-opened {@link Database} was supplied the caller owns its lifecycle and
 * {@link #close()} is a no-op for the database.
 */
public class FoundationDBSequencer implements Sequencer, Closeable {

    private static final Logger log = LoggerFactory.getLogger(FoundationDBSequencer.class);

    private static final int FDB_API_VERSION = 730;

    private final String  clusterFilePath;
    private final boolean ownsDatabase;
    private final byte[]  counterKey;

    /**
     * {@code {root} / "claim" / readVersion / token → base}: one record per claim attempt, so
     * a retry after {@code commit_unknown_result} finds the base its earlier attempt was
     * handed rather than claiming again and leaving a gap. Written in the same transaction as
     * the counter; not a single slot, since another process's claim in between would
     * overwrite it. Every claim range-clears records older than {@link #claimHorizonVersions}.
     */
    private final Subspace claims;

    /** As the persistence adapter's token horizon: an hour of versions, far beyond any retry. */
    static final long DEFAULT_CLAIM_HORIZON_VERSIONS = 3_600L * 1_000_000L;
    volatile long claimHorizonVersions = DEFAULT_CLAIM_HORIZON_VERSIONS;

    private Database db;

    /** Last value returned by next(); -1 if none issued yet. */
    private volatile long lastIssued = -1L;

    // =========================================================================
    // Constructors
    // =========================================================================

    /**
     * Uses the default FDB cluster file; counter key lives at
     * {@code ("gumbo_seq", "seq")}.
     */
    public FoundationDBSequencer() {
        this((String) null, "gumbo_seq");
    }

    /**
     * Uses the specified cluster file; counter key at {@code (rootPrefix, "seq")}.
     *
     * @param clusterFilePath path to {@code fdb.cluster}, or {@code null} for the default
     * @param rootPrefix      subspace prefix for the counter key
     */
    public FoundationDBSequencer(String clusterFilePath, String rootPrefix) {
        this.clusterFilePath = clusterFilePath;
        this.ownsDatabase    = true;
        this.counterKey      = new Subspace(Tuple.from(rootPrefix)).pack(Tuple.from("seq"));
        this.claims          = new Subspace(Tuple.from(rootPrefix)).subspace(Tuple.from("claim"));
    }

    /**
     * Uses a pre-opened {@link Database}. Caller owns the database lifecycle;
     * {@link #close()} will not close it.
     *
     * @param database   an already-open FDB database
     * @param rootPrefix subspace prefix for the counter key
     */
    public FoundationDBSequencer(Database database, String rootPrefix) {
        this.clusterFilePath = null;
        this.ownsDatabase    = false;
        this.db              = database;
        this.counterKey      = new Subspace(Tuple.from(rootPrefix)).pack(Tuple.from("seq"));
        this.claims          = new Subspace(Tuple.from(rootPrefix)).subspace(Tuple.from("claim"));
    }

    // =========================================================================
    // Lifecycle
    // =========================================================================

    /**
     * Opens the FDB connection (when {@code ownsDatabase}) and reads the current
     * counter value to initialise {@link #lastIssued}.
     */
    public void open() throws IOException {
        try {
            if (ownsDatabase) {
                FDB fdb = FDB.selectAPIVersion(FDB_API_VERSION);
                db = (clusterFilePath != null) ? fdb.open(clusterFilePath) : fdb.open();
            }
            // Seed lastIssued from whatever is already in FDB
            db.run(tr -> {
                byte[] val = tr.get(counterKey).join();
                if (val != null) lastIssued = ByteBuffer.wrap(val).getLong() - 1;
                return null;
            });
            log.info("FoundationDBSequencer opened: lastIssued={}", lastIssued);
        } catch (Exception e) {
            throw new IOException("Failed to open FoundationDBSequencer", e);
        }
    }

    @Override
    public void close() throws IOException {
        if (ownsDatabase && db != null) {
            try {
                db.close();
            } catch (Exception e) {
                throw new IOException("Error closing FoundationDBSequencer", e);
            }
        }
    }

    // =========================================================================
    // Sequencer
    // =========================================================================

    /**
     * Claims the next sequence number.
     *
     * <p>Runs a serialisable read-modify-write transaction:
     * <ol>
     *   <li>Read the current counter (defaulting to {@code 0} if absent).</li>
     *   <li>Write {@code current + 1} back to the same key.</li>
     *   <li>Return {@code current} as the claimed seqnum.</li>
     * </ol>
     *
     * <p>FDB's optimistic concurrency will transparently retry on conflict, so
     * the returned value is always unique and globally monotonic.
     */
    /**
     * True: the counter lives in FoundationDB under a single key, incremented in a transaction, so
     * every process issuing seqnums is drawing from the same sequence. This is the half of
     * multi-writer safety that storage cannot provide on its own.
     */
    @Override
    public boolean distributed() {
        return true;
    }

    @Override
    public long next() {
        return claim(1, OperationToken.fresh(), new ArrayList<>());
    }

    /**
     * Claims {@code count} consecutive seqnums and returns the first. The read-modify-write
     * and the claim record are one transaction; see {@link #claims}. Package-private so a
     * test can replay the retry {@code db.run} performs after {@code commit_unknown_result}
     * — the same call with the same token and the same accumulating list of attempts, and
     * another process's claim in between — and check the counter moved once.
     *
     * @param attempts read versions of this call's earlier attempts; appended to by each run
     */
    long claim(int count, byte[] token, List<Long> attempts) {
        long base = db.run(tr -> {
            // An earlier attempt of this call that landed: one point read each, none on the
            // first attempt. Reading them also makes a late-landing earlier attempt conflict
            // with this commit rather than let it claim twice.
            for (long readVersion : attempts) {
                byte[] landed = tr.get(claims.pack(Tuple.from(readVersion, token))).join();
                if (landed != null) return ByteBuffer.wrap(landed).getLong();
            }
            byte[] val     = tr.get(counterKey).join();
            long   current = (val == null) ? 0L : ByteBuffer.wrap(val).getLong();
            tr.set(counterKey, ByteBuffer.allocate(8).putLong(current + count).array());

            long readVersion = tr.getReadVersion().join();
            attempts.add(readVersion);
            tr.set(claims.pack(Tuple.from(readVersion, token)), ByteBuffer.allocate(8).putLong(current).array());
            tr.clear(claims.range().begin, claims.pack(Tuple.from(readVersion - claimHorizonVersions)));
            return current;
        });
        lastIssued = base + count - 1;
        return base;
    }

    /** The claim records currently kept, for tests of the clearing. */
    long storedClaimRecords() {
        return db.run(tr -> {
            long n = 0;
            for (com.apple.foundationdb.KeyValue ignored : tr.getRange(claims.range())) n++;
            return n;
        });
    }

    /**
     * Claims {@code count} consecutive sequence numbers in a <strong>single</strong>
     * FDB read-modify-write transaction, rather than one transaction per seqnum.
     *
     * <p>This is the Boki metalog batch-reservation optimisation: for an N-entry
     * write batch the sequencer cost drops from N FDB round-trips to 1, which is
     * the dominant throughput gain when using
     * {@link com.cajunsystems.gumbo.persistence.BatchingPersistenceAdapter} with
     * the FDB persistence adapter.
     *
     * @param count number of consecutive seqnums to claim; must be &gt; 0
     * @return array of length {@code count} with consecutive seqnums starting at {@code base}
     */
    @Override
    public long[] nextBatch(int count) {
        if (count <= 0) throw new IllegalArgumentException("count must be > 0");
        long base = claim(count, OperationToken.fresh(), new ArrayList<>());
        long[] seqnums = new long[count];
        for (int i = 0; i < count; i++) seqnums[i] = base + i;
        return seqnums;
    }

    /**
     * Returns the last seqnum issued by this instance, or {@code -1} if none has
     * been issued in this session.
     *
     * <p>Note: in a multi-node deployment other writers may have advanced the
     * global counter beyond this value.  Use {@link #currentGlobal()} for the
     * authoritative global view.
     */
    @Override
    public long current() {
        return lastIssued;
    }

    /**
     * Reads the highest seqnum that has been claimed globally (across all nodes)
     * directly from FDB.  This is a read-only transaction and reflects the state
     * at a consistent FDB snapshot.
     *
     * @return the global counter value minus one, or {@code -1} if nothing has
     *         been issued yet
     */
    public long currentGlobal() {
        return db.run(tr -> {
            byte[] val = tr.get(counterKey).join();
            if (val == null) return -1L;
            return ByteBuffer.wrap(val).getLong() - 1;
        });
    }
}
