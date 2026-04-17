package com.cajunsystems.gumbo.service;

import com.cajunsystems.gumbo.api.LogView;
import com.cajunsystems.gumbo.api.SharedLog;
import com.cajunsystems.gumbo.core.AppendRequest;
import com.cajunsystems.gumbo.core.AppendResult;
import com.cajunsystems.gumbo.core.LogEntry;
import com.cajunsystems.gumbo.core.LogPosition;
import com.cajunsystems.gumbo.core.LogTag;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * Default {@link LogView} implementation backed by {@link SharedLogService}.
 *
 * <p>In addition to the standard {@link LogView} contract, this class exposes
 * Boki-style {@link #readNextAfter} and {@link #readPrevBefore} operations:
 * efficient forward/backward searches over the tag's sorted seqnum index,
 * equivalent to Boki's {@code SharedLogReadNext} / {@code SharedLogReadPrev}.
 */
public class DefaultLogView implements LogView {

    private final LogTag tag;
    private final SharedLogService service;

    DefaultLogView(LogTag tag, SharedLogService service) {
        this.tag = tag;
        this.service = service;
    }

    // -------------------------------------------------------------------------
    // LogView
    // -------------------------------------------------------------------------

    @Override
    public LogTag getTag() { return tag; }

    @Override
    public CompletableFuture<List<LogEntry>> readFrom(LogPosition from, int maxEntries) {
        return service.read(tag, from, maxEntries);
    }

    @Override
    public long getLatestSeqnum() {
        try {
            return service.adapter().getLatestSeqnumForTag(tag);
        } catch (IOException e) {
            throw new SharedLogService.LogReadException("getLatestSeqnum failed for tag=" + tag, e);
        }
    }

    @Override
    public CompletableFuture<AppendResult> append(byte[] data) {
        return service.append(AppendRequest.to(tag, data));
    }

    @Override
    public SharedLog.Subscription subscribe(LogPosition from, Consumer<LogEntry> listener) {
        return service.addSubscription(tag, from, listener);
    }

    // -------------------------------------------------------------------------
    // Boki-style ReadNext / ReadPrev
    // -------------------------------------------------------------------------

    /**
     * Returns the first entry for this tag whose {@code seqnum >= minSeqnum}.
     * Equivalent to Boki's {@code SharedLogReadNext}.
     *
     * @param minSeqnum inclusive lower bound on the global seqnum
     * @return the matching entry, or empty if no such entry exists yet
     */
    public CompletableFuture<Optional<LogEntry>> readNextAfter(long minSeqnum) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                List<LogEntry> entries = service.adapter().readByTag(tag, minSeqnum);
                return entries.isEmpty() ? Optional.empty() : Optional.of(entries.get(0));
            } catch (IOException e) {
                throw new SharedLogService.LogReadException(
                        "readNextAfter failed for tag=" + tag + " minSeqnum=" + minSeqnum, e);
            }
        }, service.asyncPool());
    }

    /**
     * Returns the last entry for this tag whose {@code seqnum <= maxSeqnum}.
     * Equivalent to Boki's {@code SharedLogReadPrev}.
     *
     * @param maxSeqnum inclusive upper bound on the global seqnum
     * @return the matching entry, or empty if no such entry exists
     */
    public CompletableFuture<Optional<LogEntry>> readPrevBefore(long maxSeqnum) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                // Read all entries up to and including maxSeqnum, return the last one
                List<LogEntry> all = service.adapter().readByTag(tag, 0L);
                LogEntry prev = null;
                for (LogEntry e : all) {
                    if (e.seqnum() > maxSeqnum) break;
                    prev = e;
                }
                return Optional.ofNullable(prev);
            } catch (IOException e) {
                throw new SharedLogService.LogReadException(
                        "readPrevBefore failed for tag=" + tag + " maxSeqnum=" + maxSeqnum, e);
            }
        }, service.asyncPool());
    }

    /**
     * Equivalent to Boki's {@code SharedLogCheckTail}: returns the latest entry
     * for this tag, or empty if the tag has no entries.
     */
    public CompletableFuture<Optional<LogEntry>> checkTail() {
        return readPrevBefore(Long.MAX_VALUE);
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    @Override
    public String toString() {
        return "LogView{tag=" + tag + '}';
    }
}
