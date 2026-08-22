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

    /**
     * Whether this sequencer's uniqueness guarantee holds across <em>processes</em>, not merely
     * across threads.
     *
     * <p>Both guarantees above are stated unconditionally, and for {@link LocalSequencer} the second
     * one is only true within one JVM: its counter is an {@code AtomicLong} seeded per process, so
     * two processes issue the same seqnums and neither is told. That is not a hypothetical — the
     * adapters index entries <em>by</em> seqnum ({@code globalIndex}, {@code tagSeqnums}), so a
     * collision overwrites an index entry and the earlier record stops being readable while its
     * bytes sit on disk. Losing the view rather than the data is the signature failure of this whole
     * layer.
     *
     * <p>This matters to a caller through {@link com.cajunsystems.gumbo.core.LogCapabilities#multiWriter()},
     * which is a property of the whole log and therefore needs <em>both</em> halves: storage that
     * assigns per-tag versions across processes, and a sequencer whose global numbering spans them
     * too. A storage-side fence cannot rescue a seqnum collision, because nothing about the seqnum
     * passes through it.
     *
     * <p>Defaults to {@code false}: an implementation that has not considered the question is
     * assumed not to satisfy it, so a caller declines a capability it might have had rather than
     * assuming one it does not.
     */
    default boolean distributed() {
        return false;
    }
}
