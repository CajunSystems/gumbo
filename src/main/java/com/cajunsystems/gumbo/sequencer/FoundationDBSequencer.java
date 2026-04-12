package com.cajunsystems.gumbo.sequencer;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;

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
    @Override
    public long next() {
        long claimed = db.run(tr -> {
            byte[] val     = tr.get(counterKey).join();
            long   current = (val == null) ? 0L : ByteBuffer.wrap(val).getLong();
            long   next    = current + 1;
            tr.set(counterKey, ByteBuffer.allocate(8).putLong(next).array());
            return current;
        });
        lastIssued = claimed;
        return claimed;
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
