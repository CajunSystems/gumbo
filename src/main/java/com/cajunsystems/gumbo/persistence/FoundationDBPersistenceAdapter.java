package com.cajunsystems.gumbo.persistence;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.cajunsystems.gumbo.core.CounterValues;
import com.cajunsystems.gumbo.core.LogEntry;
import com.cajunsystems.gumbo.core.PendingAppend;
import com.cajunsystems.gumbo.core.VersionConflictException;
import com.cajunsystems.gumbo.core.LogTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A {@link PersistenceAdapter} backed by FoundationDB.
 *
 * <h2>Subspace layout</h2>
 * <pre>
 * {root} / "log"  / seqnum (long)                          → entry bytes
 * {root} / "tag"  / namespace / key / seqnum (long)        → version  (8 bytes, big-endian)
 * {root} / "meta" / "trim"                                 → trimSeqnum (8 bytes, big-endian)
 * {root} / "meta" / "latest"                               → latestSeqnum (8 bytes, big-endian)
 * {root} / "meta" / "tagcount" / namespace / key           → versionCount (8 bytes, big-endian)
 * </pre>
 *
 * <h2>Entry value format</h2>
 * <p>No magic bytes or CRC — FDB provides integrity via its own checksumming and
 * 3-way replication. The value is a compact binary blob:
 * <pre>
 * seqnum    8 bytes  big-endian long
 * timestamp 8 bytes  epoch-millis, big-endian long
 * version   8 bytes  big-endian long
 * numTags   4 bytes  big-endian int
 *   per tag:
 *     nsLen   2 bytes  unsigned big-endian short
 *     ns      nsLen bytes  UTF-8
 *     keyLen  2 bytes  unsigned big-endian short
 *     key     keyLen bytes  UTF-8
 * dataLen   4 bytes  big-endian int
 * data      dataLen bytes
 * </pre>
 *
 * <h2>Durability</h2>
 * <p>Every committed FDB transaction is durable across the full replication group
 * before the commit future resolves. No additional fsync handling is required.
 *
 * <h2>Batch writes</h2>
 * <p>{@link #appendBatch} writes all entries in a single FDB transaction.
 * If the encoded payload would exceed {@value #MAX_BATCH_BYTES}, the batch is
 * automatically chunked; each chunk is individually committed and durable.
 *
 * <h2>Read scaling</h2>
 * <p>{@link #readByTag} performs one range scan over the tag index to collect
 * seqnums, then issues all log-subspace point reads in parallel within the same
 * snapshot transaction — typically two FDB round-trips regardless of result set
 * size.
 *
 * <h2>Thread safety</h2>
 * <p>Writes are externally serialized by
 * {@link com.cajunsystems.gumbo.service.SharedLogService}. Reads use FDB
 * snapshot transactions and are fully concurrent.
 *
 * <h2>Large result sets</h2>
 * <p>FDB transactions have a default 5 MB read limit. {@link #readAll()} and
 * {@link #readFrom} may exceed this for very large logs. Callers should prefer
 * paginated reads via {@link #readFrom} with explicit bounds in those cases.
 */
public class FoundationDBPersistenceAdapter implements PersistenceAdapter {

    private static final Logger log = LoggerFactory.getLogger(FoundationDBPersistenceAdapter.class);

    /** FDB API version to bind to. Matches the 7.3.x client library. */
    private static final int FDB_API_VERSION = 730;

    /**
     * Maximum encoded bytes per FDB transaction in {@link #appendBatch}.
     * FDB's hard limit is 10 MB; we leave a 2 MB margin for key overhead.
     */
    static final int MAX_BATCH_BYTES = 8 * 1024 * 1024;

    // Subspace name tokens
    private static final String LOG_NS      = "log";
    private static final String TAG_NS      = "tag";
    private static final String META_NS     = "meta";
    private static final String TRIM_KEY    = "trim";
    private static final String LATEST_KEY  = "latest";
    private static final String TAGCOUNT_NS  = "tagcount";
    private static final String TAGLATEST_NS = "taglatest";
    private static final String KV_NS        = "kv";

    // -------------------------------------------------------------------------
    // Configuration
    // -------------------------------------------------------------------------

    private final String  clusterFilePath; // null → default cluster file
    private final boolean ownsDatabase;    // true when we opened db ourselves
    private final Subspace root;

    private Database db;
    private Subspace logSubspace;
    private Subspace tagSubspace;
    private Subspace metaSubspace;
    private Subspace tagCountSubspace;
    private Subspace tagLatestSubspace;
    private Subspace kvSubspace;

    // -------------------------------------------------------------------------
    // In-memory caches (populated on open, kept current on every write)
    // -------------------------------------------------------------------------

    /** Highest persisted seqnum; -1 if nothing has been written. */
    private volatile long latestSeqnum = -1L;

    /** Highest trimmed seqnum (exclusive lower bound for reads). */
    private volatile long trimSeqnum = 0L;

    /** Per-tag entry counts; mirrors what is persisted under the tagcount subspace. */
    private final ConcurrentHashMap<LogTag, AtomicLong> tagVersionCount = new ConcurrentHashMap<>();

    /** Per-tag latest seqnum; mirrors what is persisted under the taglatest subspace. */
    private final ConcurrentHashMap<LogTag, AtomicLong> tagLatestSeqnum = new ConcurrentHashMap<>();

    private volatile boolean opened = false;

    // =========================================================================
    // Constructors
    // =========================================================================

    /**
     * Uses the default FDB cluster file with {@code "gumbo"} as the root subspace.
     */
    public FoundationDBPersistenceAdapter() {
        this((String) null, "gumbo");
    }

    /**
     * Uses the supplied cluster file with {@code "gumbo"} as the root subspace.
     *
     * @param clusterFilePath path to {@code fdb.cluster}, or {@code null} for the default
     */
    public FoundationDBPersistenceAdapter(String clusterFilePath) {
        this(clusterFilePath, "gumbo");
    }

    /**
     * Full constructor — opens its own {@link Database} connection on {@link #open()}.
     *
     * @param clusterFilePath path to {@code fdb.cluster}, or {@code null} for the default
     * @param rootSubspaceName prefix under which all gumbo keys are stored
     */
    public FoundationDBPersistenceAdapter(String clusterFilePath, String rootSubspaceName) {
        this.clusterFilePath = clusterFilePath;
        this.ownsDatabase    = true;
        this.root            = new Subspace(Tuple.from(rootSubspaceName));
    }

    /**
     * Uses a pre-opened {@link Database} instance (shared with, e.g.,
     * {@link FoundationDBSequencer}).  The caller is responsible for the
     * {@code Database} lifecycle; {@link #close()} will NOT close it.
     *
     * @param database         an already-open FDB database
     * @param rootSubspaceName prefix under which all gumbo keys are stored
     */
    public FoundationDBPersistenceAdapter(Database database, String rootSubspaceName) {
        this.clusterFilePath = null;
        this.ownsDatabase    = false;
        this.db              = database;
        this.root            = new Subspace(Tuple.from(rootSubspaceName));
    }

    // =========================================================================
    // Lifecycle
    // =========================================================================

    @Override
    public void open() throws IOException {
        try {
            if (ownsDatabase) {
                FDB fdb = FDB.selectAPIVersion(FDB_API_VERSION);
                db = (clusterFilePath != null) ? fdb.open(clusterFilePath) : fdb.open();
            }
            initSubspaces();
            loadMetadata();
            opened = true;
            log.info("FoundationDBPersistenceAdapter opened: root={}, latestSeqnum={}, trimSeqnum={}",
                    root, latestSeqnum, trimSeqnum);
        } catch (Exception e) {
            throw new IOException("Failed to open FoundationDB connection", e);
        }
    }

    private void initSubspaces() {
        logSubspace      = root.subspace(Tuple.from(LOG_NS));
        tagSubspace      = root.subspace(Tuple.from(TAG_NS));
        metaSubspace     = root.subspace(Tuple.from(META_NS));
        tagCountSubspace  = metaSubspace.subspace(Tuple.from(TAGCOUNT_NS));
        tagLatestSubspace = metaSubspace.subspace(Tuple.from(TAGLATEST_NS));
        kvSubspace        = root.subspace(Tuple.from(KV_NS));
    }

    private void loadMetadata() {
        db.run(tr -> {
            byte[] trimVal   = tr.get(metaSubspace.pack(Tuple.from(TRIM_KEY))).join();
            byte[] latestVal = tr.get(metaSubspace.pack(Tuple.from(LATEST_KEY))).join();

            if (trimVal   != null) trimSeqnum   = ByteBuffer.wrap(trimVal).getLong();
            if (latestVal != null) latestSeqnum = ByteBuffer.wrap(latestVal).getLong();

            // Rebuild per-tag counts from the compact tagcount subspace
            Range tcRange = tagCountSubspace.range();
            for (KeyValue kv : tr.getRange(tcRange)) {
                Tuple suffix = tagCountSubspace.unpack(kv.getKey());
                String ns  = suffix.getString(0);
                String key = suffix.getString(1);
                long count = ByteBuffer.wrap(kv.getValue()).getLong();
                tagVersionCount.put(LogTag.of(ns, key), new AtomicLong(count));
            }

            // Rebuild per-tag latest seqnum from the taglatest subspace
            Range tlRange = tagLatestSubspace.range();
            for (KeyValue kv : tr.getRange(tlRange)) {
                Tuple suffix = tagLatestSubspace.unpack(kv.getKey());
                String ns  = suffix.getString(0);
                String key = suffix.getString(1);
                long latest = ByteBuffer.wrap(kv.getValue()).getLong();
                tagLatestSeqnum.put(LogTag.of(ns, key), new AtomicLong(latest));
            }
            return null;
        });
    }

    @Override
    public void close() throws IOException {
        if (!opened) return;
        opened = false;
        if (ownsDatabase && db != null) {
            try {
                db.close();
            } catch (Exception e) {
                throw new IOException("Error closing FoundationDB connection", e);
            }
        }
        log.info("FoundationDBPersistenceAdapter closed");
    }

    // =========================================================================
    // Write
    // =========================================================================

    @Override
    public void append(LogEntry entry) throws IOException {
        appendBatch(List.of(entry));
    }

    /**
     * Writes all entries in as few FDB transactions as possible.
     *
     * <p>Entries are packed into chunks up to {@value #MAX_BATCH_BYTES}. Each
     * chunk is one FDB transaction, so the entire batch is durable after this
     * method returns even when chunking occurs.
     */
    @Override
    public void appendBatch(List<LogEntry> entries) throws IOException {
        ensureOpen();
        if (entries.isEmpty()) return;

        List<LogEntry> chunk     = new ArrayList<>();
        int            chunkSize = 0;

        for (LogEntry entry : entries) {
            int encodedLen = encodedSize(entry);
            if (!chunk.isEmpty() && chunkSize + encodedLen > MAX_BATCH_BYTES) {
                commitChunk(chunk);
                chunk     = new ArrayList<>();
                chunkSize = 0;
            }
            chunk.add(entry);
            chunkSize += encodedLen;
        }
        if (!chunk.isEmpty()) {
            commitChunk(chunk);
        }
    }

    /**
     * Reads the tag's current version, checks it, and writes — all inside <em>one FDB
     * transaction</em>. This is the adapter where storage-owned versioning is fully
     * realised: FDB serialises conflicting transactions, so two processes appending to the
     * same tag cannot both be assigned the same version, and a conditional append is
     * rejected by the store rather than by anything a client believes about the store.
     *
     * <p>The version is read from the transaction rather than from {@link #tagVersionCount},
     * which is a local cache and therefore exactly the kind of per-process opinion this is
     * meant to stop trusting.
     */
    @Override
    public LogEntry append(PendingAppend pending, long expectedVersion) throws IOException {
        ensureOpen();
        LogTag primary = pending.primaryTag();
        byte[] countKey = tagCountSubspace.pack(Tuple.from(primary.namespace(), primary.key()));
        try {
            LogEntry entry = db.run(tr -> {
                byte[] raw = tr.get(countKey).join();
                long next = raw == null ? 0L : ByteBuffer.wrap(raw).getLong();
                if (expectedVersion != ANY_VERSION && expectedVersion != next) {
                    throw new ConflictSignal(new VersionConflictException(primary, expectedVersion, next));
                }
                LogEntry e = pending.withVersion(next);
                writeEntry(tr, e);
                tr.set(metaSubspace.pack(Tuple.from(LATEST_KEY)),
                       longBytes(Math.max(latestSeqnum, e.seqnum())));
                return e;
            });
            cacheAfterCommit(entry);
            return entry;
        } catch (ConflictSignal cs) {
            throw cs.conflict;
        } catch (Exception e) {
            throw new IOException("FDB append failed for tag=" + primary, e);
        }
    }

    @Override
    public List<LogEntry> appendBatchAssigningVersions(List<PendingAppend> pendings)
            throws IOException {
        ensureOpen();
        try {
            List<LogEntry> entries = db.run(tr -> {
                java.util.Map<LogTag, Long> next = new java.util.HashMap<>();
                List<LogEntry> out = new ArrayList<>(pendings.size());
                long maxSeqnum = latestSeqnum;
                for (PendingAppend p : pendings) {
                    LogTag primary = p.primaryTag();
                    long v = next.computeIfAbsent(primary, t -> {
                        byte[] raw = tr.get(tagCountSubspace.pack(
                                Tuple.from(t.namespace(), t.key()))).join();
                        return raw == null ? 0L : ByteBuffer.wrap(raw).getLong();
                    });
                    next.put(primary, v + 1);
                    LogEntry e = p.withVersion(v);
                    writeEntry(tr, e);
                    out.add(e);
                    if (e.seqnum() > maxSeqnum) maxSeqnum = e.seqnum();
                }
                tr.set(metaSubspace.pack(Tuple.from(LATEST_KEY)), longBytes(maxSeqnum));
                return out;
            });
            for (LogEntry e : entries) cacheAfterCommit(e);
            return entries;
        } catch (Exception e) {
            throw new IOException("FDB batch append failed", e);
        }
    }

    /** Writes one entry's log record, tag index, per-tag count and per-tag latest seqnum. */
    private void writeEntry(com.apple.foundationdb.Transaction tr, LogEntry entry) {
        tr.set(logSubspace.pack(Tuple.from(entry.seqnum())), encodeEntry(entry));
        for (LogTag tag : entry.tags()) {
            tr.set(tagSubspace.pack(Tuple.from(tag.namespace(), tag.key(), entry.seqnum())),
                   longBytes(entry.streamVersion()));

            // Raise the tag's count, never lower it. An entry carries one version — its
            // primary tag's — so on a multi-tag append a secondary tag may already be
            // further along its own sequence. Overwriting its count with this entry's
            // version + 1 would hand out versions that already exist on that tag, and,
            // now that the conditional append reads this key as its fence, would let a
            // stale writer pass a check it should have failed.
            byte[] countKey = tagCountSubspace.pack(Tuple.from(tag.namespace(), tag.key()));
            byte[] rawCount = tr.get(countKey).join();
            long currentCount = rawCount == null ? 0L : ByteBuffer.wrap(rawCount).getLong();
            long candidate = entry.streamVersion() + 1;
            if (candidate > currentCount) tr.set(countKey, longBytes(candidate));

            // Same reasoning for the tag's latest seqnum. Seqnums only grow, so this
            // rarely differs — but "rarely" is not a reason to write a lower value.
            byte[] latestKey = tagLatestSubspace.pack(Tuple.from(tag.namespace(), tag.key()));
            byte[] rawLatest = tr.get(latestKey).join();
            long currentLatest = rawLatest == null ? -1L : ByteBuffer.wrap(rawLatest).getLong();
            if (entry.seqnum() > currentLatest) tr.set(latestKey, longBytes(entry.seqnum()));
        }
    }

    /** Refreshes the local caches once a commit is durable. */
    private void cacheAfterCommit(LogEntry entry) {
        if (entry.seqnum() > latestSeqnum) latestSeqnum = entry.seqnum();
        for (LogTag tag : entry.tags()) {
            tagVersionCount.computeIfAbsent(tag, k -> new AtomicLong(0))
                    .updateAndGet(c -> Math.max(c, entry.streamVersion() + 1));
            tagLatestSeqnum.computeIfAbsent(tag, k -> new AtomicLong(-1L))
                    .updateAndGet(c -> Math.max(c, entry.seqnum()));
        }
    }

    /** Carries a conflict out of the FDB retry loop, which only propagates RuntimeExceptions. */
    private static final class ConflictSignal extends RuntimeException {
        final VersionConflictException conflict;
        ConflictSignal(VersionConflictException conflict) { super(conflict); this.conflict = conflict; }
    }

    private void commitChunk(List<LogEntry> entries) throws IOException {
        try {
            db.run(tr -> {
                long maxSeqnum = latestSeqnum;

                for (LogEntry entry : entries) {
                    // Shared with the version-assigning append so both raise per-tag
                    // metadata rather than overwriting it — one copy of that rule, not two.
                    writeEntry(tr, entry);
                    if (entry.seqnum() > maxSeqnum) maxSeqnum = entry.seqnum();
                }

                tr.set(metaSubspace.pack(Tuple.from(LATEST_KEY)), longBytes(maxSeqnum));
                return null;
            });

            // Update in-memory caches after commit
            for (LogEntry entry : entries) {
                if (entry.seqnum() > latestSeqnum) latestSeqnum = entry.seqnum();
                for (LogTag tag : entry.tags()) {
                    tagVersionCount
                        .computeIfAbsent(tag, k -> new AtomicLong(0))
                        .updateAndGet(c -> Math.max(c, entry.streamVersion() + 1));
                    tagLatestSeqnum
                        .computeIfAbsent(tag, k -> new AtomicLong(-1L))
                        .updateAndGet(c -> Math.max(c, entry.seqnum()));
                }
            }
        } catch (Exception e) {
            throw new IOException("FDB transaction failed during appendBatch", e);
        }
    }

    // =========================================================================
    // Read
    // =========================================================================

    @Override
    public List<LogEntry> readAll() throws IOException {
        return readFrom(0L);
    }

    @Override
    public List<LogEntry> readFrom(long fromSeqnum) throws IOException {
        ensureOpen();
        long effectiveFrom = Math.max(fromSeqnum, trimSeqnum);
        try {
            return db.run(tr -> {
                byte[] startKey = logSubspace.pack(Tuple.from(effectiveFrom));
                byte[] endKey   = logSubspace.range().end;

                List<LogEntry> result = new ArrayList<>();
                for (KeyValue kv : tr.getRange(startKey, endKey)) {
                    result.add(decodeEntry(kv.getValue()));
                }
                return Collections.unmodifiableList(result);
            });
        } catch (Exception e) {
            throw new IOException("FDB read failed in readFrom(" + fromSeqnum + ")", e);
        }
    }

    /**
     * Reads entries for the given tag in two FDB round-trips:
     * <ol>
     *   <li>Range scan over the tag index to collect qualifying seqnums.</li>
     *   <li>All log-subspace point reads issued in parallel within the same snapshot.</li>
     * </ol>
     */
    @Override
    public List<LogEntry> readByTag(LogTag tag, long fromSeqnum) throws IOException {
        ensureOpen();
        long effectiveFrom = Math.max(fromSeqnum, trimSeqnum);
        try {
            return db.run(tr -> {
                // Range scan: (namespace, key, seqnum >= effectiveFrom)
                byte[] startKey = tagSubspace.pack(Tuple.from(tag.namespace(), tag.key(), effectiveFrom));
                byte[] endKey   = tagSubspace.subspace(Tuple.from(tag.namespace(), tag.key())).range().end;

                List<Long> seqnums = new ArrayList<>();
                for (KeyValue kv : tr.getRange(startKey, endKey)) {
                    Tuple suffix = tagSubspace.unpack(kv.getKey());
                    seqnums.add(suffix.getLong(2));
                }

                if (seqnums.isEmpty()) return Collections.<LogEntry>emptyList();

                // Parallel point reads — FDB batches these in a single network round-trip
                List<CompletableFuture<byte[]>> futures = new ArrayList<>(seqnums.size());
                for (long seqnum : seqnums) {
                    futures.add(tr.get(logSubspace.pack(Tuple.from(seqnum))));
                }

                List<LogEntry> result = new ArrayList<>(seqnums.size());
                for (CompletableFuture<byte[]> f : futures) {
                    byte[] val = f.join();
                    if (val != null) result.add(decodeEntry(val));
                }
                return Collections.unmodifiableList(result);
            });
        } catch (Exception e) {
            throw new IOException("FDB read failed in readByTag(" + tag + ", " + fromSeqnum + ")", e);
        }
    }

    /**
     * Same two round-trips as {@link #readByTag}, filtered on the tag's own version.
     *
     * <p>The tag index is keyed {@code (namespace, key, seqnum) → streamVersion}, so the
     * version is the value rather than part of the key and the scan cannot start at it
     * directly. The scan therefore covers the tag's key range but reads only 8-byte
     * values; the expensive part — the point reads into the log subspace — is still
     * proportional to the result. A {@code (namespace, key, streamVersion)} index would make
     * the scan seekable too, at the cost of a second index and a migration for logs
     * already written.
     */
    @Override
    public List<LogEntry> readFromVersion(LogTag tag, long fromVersion) throws IOException {
        ensureOpen();
        try {
            return db.run(tr -> {
                byte[] startKey = tagSubspace.pack(Tuple.from(tag.namespace(), tag.key(), trimSeqnum));
                byte[] endKey   = tagSubspace.subspace(Tuple.from(tag.namespace(), tag.key())).range().end;

                List<Long> seqnums = new ArrayList<>();
                for (KeyValue kv : tr.getRange(startKey, endKey)) {
                    if (ByteBuffer.wrap(kv.getValue()).getLong() < fromVersion) continue;
                    seqnums.add(tagSubspace.unpack(kv.getKey()).getLong(2));
                }

                if (seqnums.isEmpty()) return Collections.<LogEntry>emptyList();

                List<CompletableFuture<byte[]>> futures = new ArrayList<>(seqnums.size());
                for (long seqnum : seqnums) {
                    futures.add(tr.get(logSubspace.pack(Tuple.from(seqnum))));
                }

                List<LogEntry> result = new ArrayList<>(seqnums.size());
                for (CompletableFuture<byte[]> f : futures) {
                    byte[] val = f.join();
                    if (val != null) result.add(decodeEntry(val));
                }
                return Collections.unmodifiableList(result);
            });
        } catch (Exception e) {
            throw new IOException("FDB read failed in readFromVersion(" + tag + ", " + fromVersion + ")", e);
        }
    }

    // =========================================================================
    // Housekeeping
    // =========================================================================

    /**
     * Clears all log entries with {@code seqnum < upToSeqnum} from the log
     * subspace in a single FDB {@code clearRange} call and durably updates the
     * trim metadata.
     *
     * <p>Tag-index entries below the trim boundary become "dead weight" but are
     * never returned by reads (the {@code effectiveFrom} guard filters them out).
     * Callers that need precise storage reclamation can invoke this periodically;
     * tag-index cleanup is deferred to avoid large-transaction fan-out.
     */
    @Override
    public void trim(long upToSeqnum) throws IOException {
        ensureOpen();
        if (upToSeqnum <= trimSeqnum) return;
        try {
            final long newTrim = upToSeqnum;
            db.run(tr -> {
                // Clear log entries below the trim boundary
                tr.clear(logSubspace.range().begin,
                         logSubspace.pack(Tuple.from(newTrim)));
                // Persist trim point
                tr.set(metaSubspace.pack(Tuple.from(TRIM_KEY)), longBytes(newTrim));
                return null;
            });
            trimSeqnum = upToSeqnum;
            log.debug("Trimmed log up to seqnum {}", upToSeqnum);
        } catch (Exception e) {
            throw new IOException("FDB trim failed", e);
        }
    }

    // =========================================================================
    // Metadata
    // =========================================================================

    @Override
    public long getLatestSeqnum() {
        return latestSeqnum;
    }

    @Override
    public long getNextStreamVersion(LogTag tag) {
        AtomicLong c = tagVersionCount.get(tag);
        return c == null ? 0L : c.get();
    }

    @Override
    public long getLatestSeqnumForTag(LogTag tag) {
        AtomicLong c = tagLatestSeqnum.get(tag);
        return c == null ? -1L : c.get();
    }

    @Override
    public void setTagValue(LogTag tag, String key, byte[] value) {
        db.run(tr -> {
            tr.set(kvSubspace.pack(Tuple.from(tag.namespace(), tag.key(), key)), value);
            return null;
        });
    }

    @Override
    public byte[] getTagValue(LogTag tag, String key) {
        return db.run(tr ->
            tr.get(kvSubspace.pack(Tuple.from(tag.namespace(), tag.key(), key))).join()
        );
    }

    @Override
    public void deleteTagValue(LogTag tag, String key) {
        db.run(tr -> {
            tr.clear(kvSubspace.pack(Tuple.from(tag.namespace(), tag.key(), key)));
            return null;
        });
    }

    /**
     * The read, the comparison and the write happen in one transaction, so the fence holds
     * across processes rather than only within this JVM: FoundationDB adds the key to the
     * transaction's read conflict set, and a concurrent writer of the same key makes one of
     * the two commits fail and retry. This is the adapter where a claim is genuinely
     * arbitrated by storage.
     */
    @Override
    public boolean compareAndSetTagValue(LogTag tag, String key, byte[] expected, byte[] value) {
        byte[] k = kvSubspace.pack(Tuple.from(tag.namespace(), tag.key(), key));
        return db.run(tr -> {
            byte[] current = tr.get(k).join();
            if (!Arrays.equals(current, expected)) return false;
            if (value == null) {
                tr.clear(k);
            } else {
                tr.set(k, value);
            }
            return true;
        });
    }

    /**
     * One transaction, rather than the interface's compare-and-set loop.
     *
     * <p>Not FoundationDB's native {@code MutationType.ADD}, though it exists and would be a
     * single mutation with no read: it interprets the value as <em>little</em>-endian, so a
     * counter it maintained would disagree byte-for-byte with the big-endian
     * {@link CounterValues} encoding every other adapter uses and every client decodes. A
     * read-modify-write inside a serialisable transaction is the same guarantee at the cost
     * of one read.
     */
    @Override
    public long incrementTagValue(LogTag tag, String key, long delta) {
        byte[] k = kvSubspace.pack(Tuple.from(tag.namespace(), tag.key(), key));
        return db.run(tr -> {
            long next = CounterValues.toLong(tr.get(k).join()) + delta;
            tr.set(k, CounterValues.toBytes(next));
            return next;
        });
    }

    // =========================================================================
    // Entry encoding / decoding
    // =========================================================================

    /**
     * Encodes a {@link LogEntry} to its FDB value representation.
     * No magic header or checksum — FDB ensures integrity at the storage layer.
     */
    static byte[] encodeEntry(LogEntry entry) {
        List<byte[]> nsBufs  = new ArrayList<>();
        List<byte[]> keyBufs = new ArrayList<>();
        int tagsLen = 0;

        for (LogTag tag : entry.tags()) {
            byte[] ns  = tag.namespace().getBytes(StandardCharsets.UTF_8);
            byte[] key = tag.key().getBytes(StandardCharsets.UTF_8);
            nsBufs.add(ns);
            keyBufs.add(key);
            tagsLen += 2 + ns.length + 2 + key.length;
        }

        byte[] data     = entry.dataUnsafe();
        int    totalLen = 8 + 8 + 8 + 4 + tagsLen + 4 + data.length;

        ByteBuffer buf = ByteBuffer.allocate(totalLen);
        buf.putLong(entry.seqnum());
        buf.putLong(entry.timestamp().toEpochMilli());
        buf.putLong(entry.streamVersion());
        buf.putInt(entry.tags().size());

        for (int i = 0; i < nsBufs.size(); i++) {
            buf.putShort((short) nsBufs.get(i).length);
            buf.put(nsBufs.get(i));
            buf.putShort((short) keyBufs.get(i).length);
            buf.put(keyBufs.get(i));
        }

        buf.putInt(data.length);
        buf.put(data);
        return buf.array();
    }

    static LogEntry decodeEntry(byte[] bytes) {
        ByteBuffer buf    = ByteBuffer.wrap(bytes);
        long       seqnum = buf.getLong();
        long       tsMs   = buf.getLong();
        long       version= buf.getLong();
        int        nTags  = buf.getInt();

        Set<LogTag> tags = new HashSet<>(nTags);
        for (int i = 0; i < nTags; i++) {
            int    nsLen = Short.toUnsignedInt(buf.getShort());
            byte[] ns    = new byte[nsLen];
            buf.get(ns);
            int    keyLen = Short.toUnsignedInt(buf.getShort());
            byte[] key    = new byte[keyLen];
            buf.get(key);
            tags.add(LogTag.of(new String(ns, StandardCharsets.UTF_8),
                               new String(key, StandardCharsets.UTF_8)));
        }

        int    dataLen = buf.getInt();
        byte[] data    = new byte[dataLen];
        if (dataLen > 0) buf.get(data);

        return new LogEntry(seqnum, version, tags, data, Instant.ofEpochMilli(tsMs));
    }

    /** Estimates encoded byte size for chunking decisions. */
    private static int encodedSize(LogEntry entry) {
        int tagsLen = 0;
        for (LogTag tag : entry.tags()) {
            tagsLen += 2 + tag.namespace().getBytes(StandardCharsets.UTF_8).length;
            tagsLen += 2 + tag.key().getBytes(StandardCharsets.UTF_8).length;
        }
        return 8 + 8 + 8 + 4 + tagsLen + 4 + entry.dataUnsafe().length;
    }

    // =========================================================================
    // Misc helpers
    // =========================================================================

    private static byte[] longBytes(long v) {
        return ByteBuffer.allocate(8).putLong(v).array();
    }

    private void ensureOpen() {
        if (!opened) throw new IllegalStateException("Adapter is not open");
    }
}
