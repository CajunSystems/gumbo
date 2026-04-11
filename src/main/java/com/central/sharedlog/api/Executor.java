package com.central.sharedlog.api;

import com.central.sharedlog.core.AppendRequest;
import com.central.sharedlog.core.LogEntry;
import com.central.sharedlog.core.LogTag;

import java.util.List;

/**
 * A <em>stateless</em> executor that derives its state entirely from the shared
 * log and appends new entries back to it.
 *
 * <h2>Contract</h2>
 * <p>The {@link com.central.sharedlog.service.ExecutorEngine} drives a
 * read-rebuild-execute-append cycle:
 * <ol>
 *   <li>Read all (or incremental) log entries for {@link #getInputTag()}.</li>
 *   <li>Fold them into {@code S} using {@link #initialState()} + {@link #apply}.</li>
 *   <li>Call {@link #execute(Object, ExecutorContext)} to produce new entries.</li>
 *   <li>Append those entries to the log (possibly to different tags).</li>
 * </ol>
 *
 * <p>Because state is always rebuilt from the log, the executor is crash-safe
 * with no additional persistence: restart it and replay. Idempotency of
 * {@link #execute} is the executor author's responsibility.
 *
 * <h2>Type parameter</h2>
 * <p>{@code S} is the in-memory state type. It need not be serializable; it
 * exists only during a single execution cycle.
 *
 * <h2>Virtual threads</h2>
 * <p>The engine runs each executor cycle on a virtual thread, so
 * {@link #apply} and {@link #execute} may perform blocking I/O freely.
 *
 * @param <S> the in-memory state type
 */
public interface Executor<S> {

    /**
     * The tag whose log entries this executor reads and folds to produce state.
     * The engine will subscribe to this tag and trigger cycles when new entries
     * arrive.
     */
    LogTag getInputTag();

    /**
     * A human-readable name for this executor, used in logs and metrics.
     * Defaults to the class simple name.
     */
    default String getName() {
        return getClass().getSimpleName();
    }

    // -------------------------------------------------------------------------
    // State-building (functional fold)
    // -------------------------------------------------------------------------

    /**
     * Returns the zero/empty state before any log entries have been applied.
     * Called once per execution cycle before folding entries.
     */
    S initialState();

    /**
     * Applies a single log entry to the current state, returning the new state.
     * This is the "reduce" step of the read-rebuild cycle.
     *
     * <p>Must be pure (no side effects).  It will be called for every entry in
     * the view in seqnum order.
     *
     * @param currentState the accumulated state so far
     * @param entry        the next entry to fold in
     * @return the updated state
     */
    S apply(S currentState, LogEntry entry);

    // -------------------------------------------------------------------------
    // Execution
    // -------------------------------------------------------------------------

    /**
     * Given the fully-rebuilt state, decides what new entries to append.
     *
     * <p>The engine will append the returned {@link AppendRequest requests} to
     * the shared log in order.  Return an empty list if there is nothing to do
     * this cycle (the engine will retry when new entries arrive for
     * {@link #getInputTag()}).
     *
     * <p>Implementations may read other views via
     * {@link ExecutorContext#openView(LogTag)} and may block freely (virtual
     * thread).
     *
     * @param state   the state rebuilt from all log entries up to
     *                {@link ExecutorContext#getCurrentPosition()}
     * @param context runtime context (views, position, timestamp)
     * @return the entries to append; may be empty
     */
    List<AppendRequest> execute(S state, ExecutorContext context);
}
