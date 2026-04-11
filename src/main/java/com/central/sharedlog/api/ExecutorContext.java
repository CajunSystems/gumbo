package com.central.sharedlog.api;

import com.central.sharedlog.core.LogPosition;
import com.central.sharedlog.core.LogTag;

import java.time.Instant;

/**
 * Runtime context supplied to an {@link Executor} during its
 * {@linkplain Executor#execute execute} call.
 *
 * <p>The context exposes the {@link LogView} the executor is processing,
 * the current position (i.e. the seqnum of the last entry that was included
 * in the state snapshot), and the wall-clock time of the execution cycle.
 *
 * <p>Executors may also open views for other tags through
 * {@link #openView(LogTag)} to produce cross-tag entries (fan-out).
 */
public interface ExecutorContext {

    /** The primary view this executor is processing. */
    LogView getView();

    /**
     * The position up to which (inclusive) the log has been read to build
     * the current state.  Appending with a seqnum past this position indicates
     * new work.
     */
    LogPosition getCurrentPosition();

    /** Wall-clock time at which this execution cycle started. */
    Instant getCycleTimestamp();

    /**
     * Opens a {@link LogView} for an arbitrary tag. Useful for executors that
     * need to read or write cross-entity log streams.
     */
    LogView openView(LogTag tag);
}
