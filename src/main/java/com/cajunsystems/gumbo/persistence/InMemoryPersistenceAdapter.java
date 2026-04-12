package com.cajunsystems.gumbo.persistence;

import com.cajunsystems.gumbo.core.LogEntry;
import com.cajunsystems.gumbo.core.LogTag;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * An in-memory {@link PersistenceAdapter} backed by a {@link ConcurrentSkipListMap}.
 *
 * <p>All data is lost when the JVM exits. Intended for testing and local
 * development.  It is fully thread-safe: reads can proceed concurrently while a
 * write lock is held by the service layer (the skip-list provides cheap
 * concurrent reads regardless).
 *
 * <p>A secondary per-tag index ({@code ConcurrentSkipListMap<localId, seqnum>})
 * enables O(log n) tag-scoped reads without scanning the full log.
 */
public class InMemoryPersistenceAdapter implements PersistenceAdapter {

    /** Global log: seqnum → entry (skip-list preserves insertion / seqnum order). */
    private final ConcurrentSkipListMap<Long, LogEntry> log = new ConcurrentSkipListMap<>();

    /**
     * Per-tag index: tag → (localId → seqnum).
     * Entries within a tag are ordered by localId which is also seqnum-ordered
     * because we assign localIds at the same time we assign seqnums.
     */
    private final ConcurrentHashMap<LogTag, ConcurrentSkipListMap<Long, Long>> tagIndex =
            new ConcurrentHashMap<>();

    /** Per-tag local-id counter (mirrors the counters in SharedLogService). */
    private final ConcurrentHashMap<LogTag, AtomicLong> tagLocalIdCount =
            new ConcurrentHashMap<>();

    private volatile boolean open = false;

    // -------------------------------------------------------------------------
    // Lifecycle
    // -------------------------------------------------------------------------

    @Override
    public void open() {
        open = true;
    }

    @Override
    public void close() {
        open = false;
    }

    // -------------------------------------------------------------------------
    // Write
    // -------------------------------------------------------------------------

    @Override
    public void append(LogEntry entry) {
        ensureOpen();
        log.put(entry.seqnum(), entry);
        for (LogTag tag : entry.tags()) {
            // Use seqnum as the tagIndex key — it is globally unique across all tags,
            // avoiding collisions when entries have different primary tags but share a
            // secondary tag (which would cause localId-keyed entries to overwrite each other).
            tagIndex
                    .computeIfAbsent(tag, k -> new ConcurrentSkipListMap<>())
                    .put(entry.seqnum(), entry.seqnum());
            tagLocalIdCount
                    .computeIfAbsent(tag, k -> new AtomicLong(0))
                    .updateAndGet(current -> Math.max(current, entry.localId() + 1));
        }
    }

    // -------------------------------------------------------------------------
    // Read
    // -------------------------------------------------------------------------

    @Override
    public List<LogEntry> readAll() {
        return List.copyOf(log.values());
    }

    @Override
    public List<LogEntry> readFrom(long fromSeqnum) {
        return List.copyOf(log.tailMap(fromSeqnum).values());
    }

    @Override
    public List<LogEntry> readByTag(LogTag tag, long fromSeqnum) {
        ConcurrentSkipListMap<Long, Long> idx = tagIndex.get(tag);
        if (idx == null || idx.isEmpty()) return Collections.emptyList();

        // The tag index maps localId → seqnum; we need entries with seqnum >= fromSeqnum.
        // Walk the tag index and skip entries below fromSeqnum.
        List<LogEntry> result = new ArrayList<>();
        for (long seqnum : idx.values()) {
            if (seqnum >= fromSeqnum) {
                LogEntry e = log.get(seqnum);
                if (e != null) result.add(e);
            }
        }
        // Result may be out of seqnum order if localIds were assigned non-monotonically
        // (they shouldn't be, but sort for safety).
        result.sort((a, b) -> Long.compare(a.seqnum(), b.seqnum()));
        return Collections.unmodifiableList(result);
    }

    // -------------------------------------------------------------------------
    // Housekeeping
    // -------------------------------------------------------------------------

    @Override
    public void trim(long upToSeqnum) {
        NavigableMap<Long, LogEntry> toRemove = log.headMap(upToSeqnum, false);
        for (Map.Entry<Long, LogEntry> e : toRemove.entrySet()) {
            LogEntry entry = e.getValue();
            for (LogTag tag : entry.tags()) {
                ConcurrentSkipListMap<Long, Long> idx = tagIndex.get(tag);
                if (idx != null) idx.remove(entry.seqnum());
            }
        }
        toRemove.clear();
    }

    // -------------------------------------------------------------------------
    // Metadata
    // -------------------------------------------------------------------------

    @Override
    public long getLatestSeqnum() {
        if (log.isEmpty()) return -1L;
        return log.lastKey();
    }

    @Override
    public long getLocalIdCountForTag(LogTag tag) {
        AtomicLong counter = tagLocalIdCount.get(tag);
        return counter == null ? 0L : counter.get();
    }

    // -------------------------------------------------------------------------
    // Internal helpers
    // -------------------------------------------------------------------------

    private void ensureOpen() {
        if (!open) throw new IllegalStateException("Adapter is not open");
    }
}
