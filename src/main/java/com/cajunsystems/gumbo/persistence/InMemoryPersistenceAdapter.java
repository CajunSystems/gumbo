package com.cajunsystems.gumbo.persistence;

import com.cajunsystems.gumbo.core.LogCapabilities;
import com.cajunsystems.gumbo.core.LogEntry;
import com.cajunsystems.gumbo.core.PendingAppend;
import com.cajunsystems.gumbo.core.VersionConflictException;
import com.cajunsystems.gumbo.core.LogTag;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.LinkedHashMap;
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
 * <p>A secondary per-tag index ({@code ConcurrentSkipListMap<streamVersion, seqnum>})
 * enables O(log n) tag-scoped reads without scanning the full log.
 */
public class InMemoryPersistenceAdapter implements PersistenceAdapter {

    /** Global log: seqnum → entry (skip-list preserves insertion / seqnum order). */
    private final ConcurrentSkipListMap<Long, LogEntry> log = new ConcurrentSkipListMap<>();

    /**
     * Per-tag index: tag → (seqnum → seqnum).
     * Keys and values are both the global seqnum; using seqnum as key enables
     * O(log N) positional seeks via tailMap.
     */
    private final ConcurrentHashMap<LogTag, ConcurrentSkipListMap<Long, Long>> tagIndex =
            new ConcurrentHashMap<>();

    /** Per-tag local-id counter (mirrors the counters in SharedLogService). */
    private final ConcurrentHashMap<LogTag, AtomicLong> tagVersionCount =
            new ConcurrentHashMap<>();

    /** Per-tag key-value store for arbitrary metadata. */
    private final ConcurrentHashMap<LogTag, ConcurrentHashMap<String, byte[]>> kvStore =
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
            // secondary tag (which would cause version-keyed entries to overwrite each other).
            tagIndex
                    .computeIfAbsent(tag, k -> new ConcurrentSkipListMap<>())
                    .put(entry.seqnum(), entry.seqnum());
            // That tag's own position, not the entry's primary one. Raising every tag's
            // counter to the primary's version is what used to drag a shared queue tag's
            // numbering along behind whichever stream happened to be appended with it.
            tagVersionCount
                    .computeIfAbsent(tag, k -> new AtomicLong(0))
                    .updateAndGet(current -> Math.max(current, entry.streamVersion(tag) + 1));
        }
    }

    /**
     * Assigns the version and writes under one lock, so the compare and the increment
     * cannot be interleaved with another writer's.
     */
    @Override
    public synchronized LogEntry append(PendingAppend pending, long expectedVersion)
            throws VersionConflictException {
        LogEntry entry = pending.withVersions(claimVersions(pending, expectedVersion));
        append(entry);
        return entry;
    }

    @Override
    public synchronized List<LogEntry> appendBatchAssigningVersions(List<PendingAppend> pendings)
            throws VersionConflictException {
        List<LogEntry> entries = new ArrayList<>(pendings.size());
        for (PendingAppend p : pendings) {
            entries.add(p.withVersions(claimVersions(p, PersistenceAdapter.ANY_VERSION)));
        }
        for (LogEntry e : entries) append(e);
        return entries;
    }

    /**
     * Reserves the next version in <em>every</em> tag the append touches, enforcing
     * {@code expectedVersion} on the fenced tag if one is given.
     *
     * <p>The fence is checked before anything is consumed, so a rejected append leaves no
     * tag's counter advanced — otherwise a stale writer would burn a position in each of
     * the other streams on its way to being refused.
     */
    private Map<LogTag, Long> claimVersions(PendingAppend pending, long expectedVersion)
            throws VersionConflictException {
        LogTag fenced = pending.primaryTag();
        if (expectedVersion != PersistenceAdapter.ANY_VERSION) {
            long next = tagVersionCount.computeIfAbsent(fenced, k -> new AtomicLong(0)).get();
            if (expectedVersion != next) {
                throw new VersionConflictException(fenced, expectedVersion, next);
            }
        }
        Map<LogTag, Long> versions = new LinkedHashMap<>();
        for (LogTag tag : pending.tags()) {
            versions.put(tag, tagVersionCount.computeIfAbsent(tag, k -> new AtomicLong(0))
                    .getAndIncrement());
        }
        return versions;
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

        NavigableMap<Long, Long> range = idx.tailMap(fromSeqnum, true);
        List<LogEntry> result = new ArrayList<>(range.size());
        for (long seqnum : range.keySet()) {
            LogEntry e = log.get(seqnum);
            if (e != null) result.add(e);
        }
        // No sort needed: ConcurrentSkipListMap.tailMap preserves ascending key order
        return Collections.unmodifiableList(result);
    }

    /**
     * Walks the per-tag index in seqnum order and keeps the entries at or past
     * {@code fromVersion} <em>in this tag's own stream</em>. The version is read off the
     * entry rather than the index: this adapter stores the seqnum as the index value (see
     * {@link #append}), so the index cannot answer the version question by itself.
     */
    @Override
    public List<LogEntry> readFromVersion(LogTag tag, long fromVersion) {
        ConcurrentSkipListMap<Long, Long> idx = tagIndex.get(tag);
        if (idx == null || idx.isEmpty()) return Collections.emptyList();

        List<LogEntry> result = new ArrayList<>();
        for (long seqnum : idx.keySet()) {
            LogEntry entry = log.get(seqnum);
            if (entry != null && entry.streamVersion(tag) >= fromVersion) result.add(entry);
        }
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
    public long getNextStreamVersion(LogTag tag) {
        AtomicLong counter = tagVersionCount.get(tag);
        return counter == null ? 0L : counter.get();
    }

    @Override
    public long getLatestSeqnumForTag(LogTag tag) {
        ConcurrentSkipListMap<Long, Long> idx = tagIndex.get(tag);
        if (idx == null || idx.isEmpty()) return -1L;
        return idx.lastKey();
    }

    // -------------------------------------------------------------------------
    // Capabilities
    // -------------------------------------------------------------------------

    /**
     * Everything except multi-writer, which is not a limitation of this adapter so much as
     * a category error against it: the log <em>is</em> the heap of one process, so a second
     * writer would be a second log rather than a competing writer of this one. There is
     * nothing to refuse and nothing to arbitrate.
     *
     * <p>The rest hold because every mutating path is {@code synchronized} on this adapter,
     * which is the whole store — a compare and the write it guards cannot be separated.
     */
    @Override
    public LogCapabilities capabilities() {
        return LogCapabilities.builder()
                .conditionalAppend(true)
                .compareAndSet(true)
                .versionedReads(true)
                .atomicMultiTagAppend(true)
                .multiWriter(false)
                .build();
    }

    // -------------------------------------------------------------------------
    // Key-Value
    // -------------------------------------------------------------------------

    /*
     * Values are copied in and out: here the array *is* the stored state, so handing out a
     * reference to it would let a caller change a value nobody wrote — including one another
     * caller is comparing against.
     *
     * The same rule applies to any adapter that answers reads from memory, which is both
     * this one and FileBasedPersistenceAdapter's kvStore cache. Only FoundationDB is exempt,
     * because the bytes leave the process when the transaction sets them.
     */

    @Override
    public void setTagValue(LogTag tag, String key, byte[] value) {
        kvStore.computeIfAbsent(tag, k -> new ConcurrentHashMap<>()).put(key, value.clone());
    }

    @Override
    public byte[] getTagValue(LogTag tag, String key) {
        ConcurrentHashMap<String, byte[]> tagKv = kvStore.get(tag);
        if (tagKv == null) return null;
        byte[] stored = tagKv.get(key);
        return stored == null ? null : stored.clone();
    }

    @Override
    public void deleteTagValue(LogTag tag, String key) {
        ConcurrentHashMap<String, byte[]> tagKv = kvStore.get(tag);
        if (tagKv != null) tagKv.remove(key);
    }

    /**
     * {@code compute} on a {@link ConcurrentHashMap} holds the bin lock across the
     * comparison and the write, which is the atomicity this method has to provide.
     * Returning {@code null} from it removes the mapping, so a conditional delete is the
     * same call as a conditional set.
     */
    @Override
    public boolean compareAndSetTagValue(LogTag tag, String key, byte[] expected, byte[] value) {
        ConcurrentHashMap<String, byte[]> tagKv =
                kvStore.computeIfAbsent(tag, k -> new ConcurrentHashMap<>());
        boolean[] swapped = {false};
        tagKv.compute(key, (k, current) -> {
            if (!Arrays.equals(current, expected)) {
                return current;   // no match: leave it exactly as it was (null stays absent)
            }
            swapped[0] = true;
            return value == null ? null : value.clone();
        });
        return swapped[0];
    }

    // -------------------------------------------------------------------------
    // Internal helpers
    // -------------------------------------------------------------------------

    private void ensureOpen() {
        if (!open) throw new IllegalStateException("Adapter is not open");
    }
}
