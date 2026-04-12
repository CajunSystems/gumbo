package com.cajunsystems.gumbo.sequencer;

/**
 * Assigns globally monotonic sequence numbers to log entries.
 *
 * <p>The sequencer is the sole authority for {@code seqnum} values. All
 * implementations must guarantee:
 * <ol>
 *   <li><strong>Monotonicity</strong>: successive calls to {@link #next()} always
 *       return strictly increasing values.</li>
 *   <li><strong>Uniqueness</strong>: no two entries ever share the same seqnum.</li>
 * </ol>
 *
 * <p>In a single-node deployment {@link LocalSequencer} suffices. A distributed
 * deployment would plug in a network-backed implementation (e.g. Zookeeper,
 * etcd, or a dedicated sequencer service as used in Boki).
 */
public interface Sequencer {

    /**
     * Returns the next globally unique, monotonically increasing sequence number.
     * Thread-safe; may be called concurrently.
     */
    long next();

    /**
     * Returns the last sequence number that was issued, or {@code -1} if none
     * has been issued yet.
     */
    long current();

    /**
     * Claims {@code count} consecutive sequence numbers in a single atomic
     * operation and returns them as an array.  Element {@code i} of the returned
     * array holds {@code base + i}, where {@code base} is the first seqnum of
     * the claimed range.  All values are globally unique and monotonically
     * increasing relative to any previously issued seqnums.
     *
     * <p>This is the Boki metalog batch-reservation optimisation: instead of
     * one round-trip per entry, a distributed sequencer (e.g.
     * {@link com.cajunsystems.gumbo.sequencer.FoundationDBSequencer}) can claim
     * an entire batch in a single read-modify-write transaction, reducing
     * sequencer contention from O(N) to O(1) round-trips for an N-entry batch.
     *
     * <p>The default implementation calls {@link #next()} {@code count} times.
     * Implementations backed by a remote sequencer should override this to
     * perform the reservation in one network round-trip.
     *
     * @param count number of sequence numbers to claim; must be &gt; 0
     * @return array of length {@code count} with consecutive seqnums
     * @throws IllegalArgumentException if {@code count <= 0}
     */
    default long[] nextBatch(int count) {
        if (count <= 0) throw new IllegalArgumentException("count must be > 0");
        long[] seqnums = new long[count];
        for (int i = 0; i < count; i++) seqnums[i] = next();
        return seqnums;
    }
}
