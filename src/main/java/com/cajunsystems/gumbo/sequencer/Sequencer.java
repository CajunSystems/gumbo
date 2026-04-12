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
}
