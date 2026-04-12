package com.cajunsystems.gumbo.sequencer;

import java.util.concurrent.atomic.AtomicLong;

/**
 * A single-node {@link Sequencer} backed by an {@link AtomicLong}.
 *
 * <p>Suitable for in-process deployments where a single JVM is the sole writer.
 * The counter starts at {@code 0} by default but can be seeded from a recovered
 * value (e.g. the latest persisted seqnum + 1) to avoid re-using sequence numbers
 * after a restart.
 */
public class LocalSequencer implements Sequencer {

    private final AtomicLong counter;

    /** Starts the sequencer at {@code 0}. */
    public LocalSequencer() {
        this(0L);
    }

    /**
     * Starts the sequencer at {@code initialValue}.
     * The first call to {@link #next()} will return {@code initialValue}.
     *
     * @param initialValue the first seqnum to issue; must be {@code >= 0}
     */
    public LocalSequencer(long initialValue) {
        if (initialValue < 0) throw new IllegalArgumentException("initialValue must be >= 0");
        this.counter = new AtomicLong(initialValue);
    }

    @Override
    public long next() {
        return counter.getAndIncrement();
    }

    @Override
    public long current() {
        long v = counter.get() - 1;
        return v < 0 ? -1L : v;
    }

    /**
     * Advances the counter so that the next {@link #next()} call returns at least
     * {@code minNext}. Has no effect if the counter is already at or beyond
     * {@code minNext}. Used by {@code SharedLogService} to reseed from a persisted
     * latest seqnum after a restart.
     *
     * @param minNext the minimum value the next {@link #next()} should return
     */
    public void advanceTo(long minNext) {
        counter.updateAndGet(current -> Math.max(current, minNext));
    }
}
