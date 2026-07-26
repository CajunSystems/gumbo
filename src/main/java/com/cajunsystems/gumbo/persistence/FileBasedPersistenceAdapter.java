package com.cajunsystems.gumbo.persistence;

import com.cajunsystems.gumbo.core.LogEntry;
import com.cajunsystems.gumbo.core.PendingAppend;
import com.cajunsystems.gumbo.core.VersionConflictException;
import com.cajunsystems.gumbo.core.LogTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.CRC32;

/**
 * A durable, append-only {@link PersistenceAdapter} backed by a pair of flat files.
 *
 * <h2>File layout</h2>
 * <pre>
 * {dataDir}/
 *   log.dat     – append-only stream of binary-encoded {@link LogEntry} records
 *   index.dat   – append-only global index: [seqnum:8][fileOffset:8] = 16 B/entry
 *   trim.dat    – single 8-byte little-endian trim seqnum (0 = nothing trimmed)
 *   lock        – empty file; holds the exclusive writer lock for the directory
 * </pre>
 *
 * <h2>Log entry binary format</h2>
 * <pre>
 * [MAGIC    : 4 bytes  = 0xC0FFEE42  ]
 * [seqnum   : 8 bytes, big-endian    ]
 * [timestamp: 8 bytes, millis epoch  ]
 * [version  : 8 bytes, big-endian    ]
 * [numTags  : 4 bytes, big-endian    ]
 *   per tag:
 *     [nsLen  : 2 bytes unsigned     ]
 *     [ns     : nsLen UTF-8 bytes    ]
 *     [keyLen : 2 bytes unsigned     ]
 *     [key    : keyLen UTF-8 bytes   ]
 * [dataLen  : 4 bytes, big-endian    ]
 * [data     : dataLen bytes          ]
 * [checksum : 4 bytes CRC32 of all above]
 * </pre>
 *
 * <h2>Recovery</h2>
 * <p>On {@link #open()}, the adapter loads the global index into memory and rebuilds
 * per-tag indices by scanning only the indexed entries (avoiding full data reads).
 * If the index file is absent or truncated, it falls back to a full log scan.
 *
 * <h2>Thread safety</h2>
 * <p>Writes are guarded externally by the service layer. Reads use in-memory
 * skip-list maps that are safe for concurrent reads.
 *
 * <h2>Single-writer</h2>
 * <p>This adapter is single-writer: local ids are assigned from process-local counters
 * and {@code index.dat} is written per process, so two adapters sharing a directory
 * assign colliding ids and overwrite each other's index. {@link #open()} therefore takes
 * an exclusive {@link java.nio.channels.FileLock} on {@code lock} and fails with
 * {@link LogAlreadyOpenException} rather than corrupting the log silently.
 */
public class FileBasedPersistenceAdapter implements PersistenceAdapter {

    private static final Logger log = LoggerFactory.getLogger(FileBasedPersistenceAdapter.class);

    private static final int MAGIC = 0xC0FFEE42;
    /** Fixed-size overhead per entry: magic(4)+seqnum(8)+ts(8)+version(8)+numTags(4)+dataLen(4)+crc(4) = 40 bytes */
    private static final int FIXED_OVERHEAD = 40;

    private final Path dataDir;
    private final Path logFile;
    private final Path indexFile;
    private final Path trimFile;
    private final Path lockFile;

    /** Channel holding {@link #dirLock}; kept open for the lifetime of the lock. */
    private FileChannel lockChannel;
    /** Exclusive lock on {@link #lockFile}, held from open() to close(). */
    private java.nio.channels.FileLock dirLock;

    /** Durable write channel for the log; opened in APPEND mode. */
    private FileChannel logChannel;
    /** Durable write channel for the index; opened in APPEND mode. */
    private FileChannel indexChannel;
    /** Durable write channel for the KV store; opened in APPEND mode. */
    private FileChannel kvChannel;
    private final ConcurrentHashMap<LogTag, ConcurrentHashMap<String, byte[]>> kvStore = new ConcurrentHashMap<>();

    /** In-memory global index: seqnum → file offset in log.dat */
    private final ConcurrentSkipListMap<Long, Long> globalIndex = new ConcurrentSkipListMap<>();

    /** In-memory per-tag index: tag → (seqnum → streamVersion) */
    private final ConcurrentHashMap<LogTag, ConcurrentSkipListMap<Long, Long>> tagSeqnums =
            new ConcurrentHashMap<>();

    /** Per-tag local-id counter. */
    private final ConcurrentHashMap<LogTag, AtomicLong> tagVersionCount =
            new ConcurrentHashMap<>();

    private volatile long trimSeqnum = 0L;
    private volatile boolean open = false;

    // -------------------------------------------------------------------------
    // Constructor
    // -------------------------------------------------------------------------

    public FileBasedPersistenceAdapter(Path dataDir) {
        this.dataDir = dataDir;
        this.logFile   = dataDir.resolve("log.dat");
        this.indexFile = dataDir.resolve("index.dat");
        this.trimFile  = dataDir.resolve("trim.dat");
        this.lockFile  = dataDir.resolve("lock");
    }

    public FileBasedPersistenceAdapter(String dataDir) {
        this(Path.of(dataDir));
    }

    // -------------------------------------------------------------------------
    // Lifecycle
    // -------------------------------------------------------------------------

    @Override
    public void open() throws IOException {
        if (open) throw new IllegalStateException("Adapter is already open: " + dataDir);
        Files.createDirectories(dataDir);
        acquireDirectoryLock();
        try {
            openLocked();
        } catch (IOException | RuntimeException e) {
            // Unwind everything, not just the lock. openLocked() assigns the three write
            // channels one at a time, so a failure part-way through leaves the earlier
            // ones open — and with `open` still false, close() would return before
            // reaching them, leaking a descriptor per failed attempt.
            closeQuietly(logChannel);   logChannel   = null;
            closeQuietly(indexChannel); indexChannel = null;
            closeQuietly(kvChannel);    kvChannel    = null;
            releaseDirectoryLock();
            throw e;
        }
    }

    /**
     * Takes the exclusive writer lock on the data directory, so a second adapter fails
     * loudly here instead of silently duplicating local ids and clobbering the index.
     *
     * <p>Both failure modes map to the same error: {@code tryLock} returns {@code null}
     * when another <em>process</em> holds the lock, and throws
     * {@link java.nio.channels.OverlappingFileLockException} when another adapter in
     * <em>this</em> JVM does (file locks are held per JVM, not per channel).
     */
    private void acquireDirectoryLock() throws IOException {
        // Held in a local until the lock is actually taken. Publishing the channel to the
        // field first would let a failed acquisition overwrite a channel this adapter is
        // already holding a lock through, orphaning its descriptor.
        FileChannel channel = FileChannel.open(lockFile,
                StandardOpenOption.CREATE, StandardOpenOption.WRITE);
        java.nio.channels.FileLock lock;
        try {
            lock = channel.tryLock();
        } catch (java.nio.channels.OverlappingFileLockException e) {
            closeQuietly(channel);
            throw new LogAlreadyOpenException(dataDir, e);
        } catch (IOException | RuntimeException e) {
            closeQuietly(channel);
            throw e;
        }
        if (lock == null) {
            closeQuietly(channel);
            throw new LogAlreadyOpenException(dataDir, null);
        }
        this.lockChannel = channel;
        this.dirLock = lock;
    }

    private void releaseDirectoryLock() {
        if (dirLock != null) {
            try { dirLock.release(); } catch (IOException ignored) { /* channel close covers it */ }
            dirLock = null;
        }
        closeQuietly(lockChannel);
        lockChannel = null;
    }

    private void openLocked() throws IOException {
        // Load trim seqnum
        if (Files.exists(trimFile)) {
            byte[] trimBytes = Files.readAllBytes(trimFile);
            if (trimBytes.length >= 8) {
                trimSeqnum = ByteBuffer.wrap(trimBytes).getLong(0);
            }
        }

        // Load or rebuild global index
        boolean indexValid = tryLoadGlobalIndex();
        if (!indexValid) {
            log.warn("Index file missing or corrupt; rebuilding from log scan");
            rebuildIndexFromLog();
        }

        // Rebuild per-tag indices from the global index entries
        rebuildTagIndices();

        // Open append channels
        logChannel = FileChannel.open(logFile,
                StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND);
        indexChannel = FileChannel.open(indexFile,
                StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND);

        Path kvFile = dataDir.resolve("kv.dat");
        if (Files.exists(kvFile)) {
            loadKvFile(kvFile);
        }
        kvChannel = FileChannel.open(kvFile, StandardOpenOption.CREATE, StandardOpenOption.APPEND);

        open = true;
        log.info("FileBasedPersistenceAdapter opened: dir={}, entries={}, latestSeqnum={}",
                dataDir, globalIndex.size(), getLatestSeqnum());
    }

    @Override
    public void close() throws IOException {
        if (!open) return;
        open = false;
        closeQuietly(logChannel);
        closeQuietly(indexChannel);
        closeQuietly(kvChannel);
        releaseDirectoryLock();
        log.info("FileBasedPersistenceAdapter closed");
    }

    // -------------------------------------------------------------------------
    // Write
    // -------------------------------------------------------------------------

    @Override
    public void append(LogEntry entry) throws IOException {
        writeNoSync(entry);
        syncChannels();
    }

    /**
     * Writes all entries to the log and index files then calls {@code fdatasync}
     * <em>once</em> for both — the group-commit path.  N entries cost 2
     * {@code fdatasync} calls instead of {@code 2N}.
     */
    @Override
    public void appendBatch(List<LogEntry> entries) throws IOException {
        for (LogEntry entry : entries) {
            writeNoSync(entry);
        }
        syncChannels();
    }

    // Writes bytes to both channels and updates in-memory indices without
    // calling force() — the caller is responsible for the fdatasync.
    private void writeNoSync(LogEntry entry) throws IOException {
        ensureOpen();
        byte[] encoded = encode(entry);

        // Current position = end of log file (channel opened in APPEND mode)
        long offset = logChannel.size();

        ByteBuffer buf = ByteBuffer.wrap(encoded);
        while (buf.hasRemaining()) {
            logChannel.write(buf);
        }

        // Append seqnum→offset record to index
        ByteBuffer idxBuf = ByteBuffer.allocate(16);
        idxBuf.putLong(entry.seqnum());
        idxBuf.putLong(offset);
        idxBuf.flip();
        while (idxBuf.hasRemaining()) {
            indexChannel.write(idxBuf);
        }

        // Update in-memory indices immediately so concurrent reads see the entry
        globalIndex.put(entry.seqnum(), offset);
        for (LogTag tag : entry.tags()) {
            tagSeqnums
                    .computeIfAbsent(tag, k -> new ConcurrentSkipListMap<>())
                    .put(entry.seqnum(), entry.streamVersion());
            tagVersionCount
                    .computeIfAbsent(tag, k -> new AtomicLong(0))
                    .updateAndGet(c -> Math.max(c, entry.streamVersion() + 1));
        }
    }

    // fdatasync on both WAL files; metadata update not required.
    private void syncChannels() throws IOException {
        logChannel.force(false);
        indexChannel.force(false);
    }

    /**
     * Assigns the version from this adapter's own durable state and writes, both under one
     * lock — so the version comes from the log rather than from a caller's counter, and
     * the compare and increment of a conditional append cannot be split.
     *
     * <p>Within this process that makes the pair atomic. Across processes it is the
     * directory lock taken in {@link #open()} that holds the line: this adapter is
     * single-writer, and a second one is refused rather than allowed to race here.
     */
    @Override
    public synchronized LogEntry append(PendingAppend pending, long expectedVersion)
            throws IOException {
        LogEntry entry = pending.withVersion(claimVersion(pending.primaryTag(), expectedVersion));
        writeNoSync(entry);
        syncChannels();
        return entry;
    }

    @Override
    public synchronized List<LogEntry> appendBatchAssigningVersions(List<PendingAppend> pendings)
            throws IOException {
        List<LogEntry> entries = new ArrayList<>(pendings.size());
        for (PendingAppend p : pendings) {
            entries.add(p.withVersion(claimVersion(p.primaryTag(), ANY_VERSION)));
        }
        for (LogEntry e : entries) writeNoSync(e);
        syncChannels();   // one fdatasync for the batch, as before
        return entries;
    }

    /**
     * Reserves the tag's next version, enforcing {@code expectedVersion} if given.
     *
     * <p>Reads from {@code tagVersionCount}, which is rebuilt from the log on open, so a
     * restart continues the sequence rather than restarting it.
     */
    private long claimVersion(LogTag tag, long expectedVersion) throws VersionConflictException {
        AtomicLong counter = tagVersionCount.computeIfAbsent(tag, k -> new AtomicLong(0));
        long next = counter.get();
        if (expectedVersion != ANY_VERSION && expectedVersion != next) {
            throw new VersionConflictException(tag, expectedVersion, next);
        }
        counter.set(next + 1);
        return next;
    }

    // -------------------------------------------------------------------------
    // Read
    // -------------------------------------------------------------------------

    @Override
    public List<LogEntry> readAll() throws IOException {
        return readFrom(trimSeqnum);
    }

    @Override
    public List<LogEntry> readFrom(long fromSeqnum) throws IOException {
        long effectiveFrom = Math.max(fromSeqnum, trimSeqnum);
        NavigableMap<Long, Long> range = globalIndex.tailMap(effectiveFrom, true);
        return readEntries(range.values());
    }

    @Override
    public List<LogEntry> readByTag(LogTag tag, long fromSeqnum) throws IOException {
        ConcurrentSkipListMap<Long, Long> idx = tagSeqnums.get(tag);
        if (idx == null || idx.isEmpty()) return Collections.emptyList();

        long effectiveFrom = Math.max(fromSeqnum, trimSeqnum);
        NavigableMap<Long, Long> range = idx.tailMap(effectiveFrom, true);
        // range: seqnum → streamVersion; we need offsets from the global index
        List<Long> offsets = new ArrayList<>(range.size());
        for (long seqnum : range.keySet()) {
            Long offset = globalIndex.get(seqnum);
            if (offset != null) offsets.add(offset);
        }
        return readEntries(offsets);
    }

    /**
     * Resolves the version range against the in-memory per-tag index and reads only the
     * matching entries from {@code log.dat} — so the cost on storage is proportional to
     * the result, not to the tag's history.
     *
     * <p>The index is keyed by seqnum with the version as its value, and within a tag the
     * two ascend together (versions are handed out in append order), so walking it in
     * seqnum order yields versions in ascending order too.
     */
    @Override
    public List<LogEntry> readFromVersion(LogTag tag, long fromVersion) throws IOException {
        ConcurrentSkipListMap<Long, Long> idx = tagSeqnums.get(tag);
        if (idx == null || idx.isEmpty()) return Collections.emptyList();

        List<Long> offsets = new ArrayList<>();
        for (Map.Entry<Long, Long> e : idx.entrySet()) {  // seqnum → streamVersion, ascending seqnum
            if (e.getValue() < fromVersion) continue;
            if (e.getKey() < trimSeqnum) continue;
            Long offset = globalIndex.get(e.getKey());
            if (offset != null) offsets.add(offset);
        }
        return readEntries(offsets);
    }

    // -------------------------------------------------------------------------
    // Housekeeping
    // -------------------------------------------------------------------------

    @Override
    public void trim(long upToSeqnum) throws IOException {
        if (upToSeqnum <= trimSeqnum) return;
        trimSeqnum = upToSeqnum;

        // Persist trim point atomically (write to temp then rename)
        Path tmp = trimFile.resolveSibling("trim.dat.tmp");
        ByteBuffer buf = ByteBuffer.allocate(8).putLong(upToSeqnum);
        buf.flip();
        try (FileChannel tc = FileChannel.open(tmp,
                StandardOpenOption.CREATE, StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING)) {
            while (buf.hasRemaining()) tc.write(buf);
            tc.force(true);
        }
        Files.move(tmp, trimFile, java.nio.file.StandardCopyOption.ATOMIC_MOVE,
                java.nio.file.StandardCopyOption.REPLACE_EXISTING);

        // Purge from in-memory indices (storage reclamation is lazy)
        globalIndex.headMap(upToSeqnum, false).clear();
        for (var tidx : tagSeqnums.values()) {
            tidx.headMap(upToSeqnum, false).clear();
        }
    }

    // -------------------------------------------------------------------------
    // Metadata
    // -------------------------------------------------------------------------

    @Override
    public long getLatestSeqnum() {
        if (globalIndex.isEmpty()) return -1L;
        return globalIndex.lastKey();
    }

    @Override
    public long getNextStreamVersion(LogTag tag) {
        AtomicLong c = tagVersionCount.get(tag);
        return c == null ? 0L : c.get();
    }

    @Override
    public long getLatestSeqnumForTag(LogTag tag) {
        ConcurrentSkipListMap<Long, Long> idx = tagSeqnums.get(tag);
        if (idx == null || idx.isEmpty()) return -1L;
        return idx.lastKey();
    }

    // -------------------------------------------------------------------------
    // Key-Value
    // -------------------------------------------------------------------------

    @Override
    public void setTagValue(LogTag tag, String key, byte[] value) throws IOException {
        kvStore.computeIfAbsent(tag, k -> new ConcurrentHashMap<>()).put(key, value);
        writeKvRecord(tag, key, value);
    }

    @Override
    public byte[] getTagValue(LogTag tag, String key) {
        ConcurrentHashMap<String, byte[]> tagKv = kvStore.get(tag);
        return tagKv == null ? null : tagKv.get(key);
    }

    @Override
    public void deleteTagValue(LogTag tag, String key) throws IOException {
        ConcurrentHashMap<String, byte[]> tagKv = kvStore.get(tag);
        if (tagKv != null) tagKv.remove(key);
        writeKvRecord(tag, key, null);  // null → tombstone (valLen = -1)
    }

    private void loadKvFile(Path kvFile) throws IOException {
        byte[] bytes = Files.readAllBytes(kvFile);
        ByteBuffer buf = ByteBuffer.wrap(bytes);
        while (buf.remaining() >= 2) {
            if (buf.remaining() < 2) break;
            String ns     = readShortString(buf);
            if (buf.remaining() < 2) break;
            String tagKey = readShortString(buf);
            if (buf.remaining() < 2) break;
            String kvKey  = readShortString(buf);
            if (buf.remaining() < 4) break;
            int valLen = buf.getInt();
            LogTag tag = LogTag.of(ns, tagKey);
            if (valLen < 0) {
                // tombstone — delete
                ConcurrentHashMap<String, byte[]> m = kvStore.get(tag);
                if (m != null) m.remove(kvKey);
            } else {
                if (buf.remaining() < valLen) break;
                byte[] value = new byte[valLen];
                buf.get(value);
                kvStore.computeIfAbsent(tag, k -> new ConcurrentHashMap<>()).put(kvKey, value);
            }
        }
    }

    private static String readShortString(ByteBuffer buf) {
        int len = Short.toUnsignedInt(buf.getShort());
        byte[] b = new byte[len];
        buf.get(b);
        return new String(b, StandardCharsets.UTF_8);
    }

    private void writeKvRecord(LogTag tag, String kvKey, byte[] value) throws IOException {
        byte[] nsBytes  = tag.namespace().getBytes(StandardCharsets.UTF_8);
        byte[] tagBytes = tag.key().getBytes(StandardCharsets.UTF_8);
        byte[] keyBytes = kvKey.getBytes(StandardCharsets.UTF_8);
        int valueLen = (value == null) ? -1 : value.length;
        int capacity = 2 + nsBytes.length + 2 + tagBytes.length + 2 + keyBytes.length + 4
                       + (valueLen > 0 ? valueLen : 0);
        ByteBuffer buf = ByteBuffer.allocate(capacity);
        buf.putShort((short) nsBytes.length);  buf.put(nsBytes);
        buf.putShort((short) tagBytes.length); buf.put(tagBytes);
        buf.putShort((short) keyBytes.length); buf.put(keyBytes);
        buf.putInt(valueLen);
        if (valueLen > 0) buf.put(value);
        buf.flip();
        while (buf.hasRemaining()) kvChannel.write(buf);
    }

    // -------------------------------------------------------------------------
    // Encoding / decoding
    // -------------------------------------------------------------------------

    private byte[] encode(LogEntry entry) {
        // Pre-compute variable-length parts
        List<byte[]> nsBytes  = new ArrayList<>();
        List<byte[]> keyBytes = new ArrayList<>();
        int tagsBytesLen = 0;
        for (LogTag tag : entry.tags()) {
            byte[] ns  = tag.namespace().getBytes(StandardCharsets.UTF_8);
            byte[] key = tag.key().getBytes(StandardCharsets.UTF_8);
            nsBytes.add(ns);
            keyBytes.add(key);
            tagsBytesLen += 2 + ns.length + 2 + key.length;
        }
        byte[] data = entry.dataUnsafe();
        int totalLen = FIXED_OVERHEAD + tagsBytesLen + data.length;

        ByteBuffer buf = ByteBuffer.allocate(totalLen);
        int checksumEnd = totalLen - 4;

        buf.putInt(MAGIC);
        buf.putLong(entry.seqnum());
        buf.putLong(entry.timestamp().toEpochMilli());
        buf.putLong(entry.streamVersion());
        buf.putInt(entry.tags().size());
        for (int i = 0; i < nsBytes.size(); i++) {
            buf.putShort((short) nsBytes.get(i).length);
            buf.put(nsBytes.get(i));
            buf.putShort((short) keyBytes.get(i).length);
            buf.put(keyBytes.get(i));
        }
        buf.putInt(data.length);
        buf.put(data);

        // CRC32 over everything except the trailing 4 bytes
        CRC32 crc = new CRC32();
        crc.update(buf.array(), 0, checksumEnd);
        buf.putInt((int) crc.getValue());

        return buf.array();
    }

    /**
     * Fixed-size prefix before the variable-length tag section:
     * magic(4) + seqnum(8) + timestamp(8) + version(8) + numTags(4) = 32 bytes.
     * Note: FIXED_OVERHEAD (40) = PREFIX_SIZE(32) + dataLen(4) + checksum(4),
     * used for total entry size calculation but NOT as the cursor start.
     */
    private static final int PREFIX_SIZE = 32;

    private LogEntry decodeAt(FileChannel channel, long offset) throws IOException {
        // Read only the fixed prefix (32 bytes) before the variable tag section
        ByteBuffer header = ByteBuffer.allocate(PREFIX_SIZE);
        readFully(channel, header, offset);
        header.flip();

        int magic = header.getInt();
        if (magic != MAGIC) throw new IOException("Bad magic at offset " + offset);

        long seqnum        = header.getLong();
        long tsMillis      = header.getLong();
        long streamVersion = header.getLong();
        int  numTags       = header.getInt();

        // Variable-length tag section starts immediately after the 32-byte prefix
        long cursor = offset + PREFIX_SIZE;

        // Read tags
        Set<LogTag> tags = new java.util.HashSet<>(numTags);
        for (int i = 0; i < numTags; i++) {
            ByteBuffer lenBuf = ByteBuffer.allocate(2);
            readFully(channel, lenBuf, cursor); cursor += 2;
            int nsLen = Short.toUnsignedInt(lenBuf.flip().getShort());
            byte[] nsBytes = new byte[nsLen];
            readFully(channel, ByteBuffer.wrap(nsBytes), cursor); cursor += nsLen;

            lenBuf = ByteBuffer.allocate(2);
            readFully(channel, lenBuf, cursor); cursor += 2;
            int keyLen = Short.toUnsignedInt(lenBuf.flip().getShort());
            byte[] keyBytes = new byte[keyLen];
            readFully(channel, ByteBuffer.wrap(keyBytes), cursor); cursor += keyLen;

            tags.add(LogTag.of(
                    new String(nsBytes, StandardCharsets.UTF_8),
                    new String(keyBytes, StandardCharsets.UTF_8)));
        }

        // dataLen
        ByteBuffer dataLenBuf = ByteBuffer.allocate(4);
        readFully(channel, dataLenBuf, cursor); cursor += 4;
        int dataLen = dataLenBuf.flip().getInt();

        // data
        byte[] data = new byte[dataLen];
        if (dataLen > 0) {
            readFully(channel, ByteBuffer.wrap(data), cursor);
            cursor += dataLen;
        }

        // checksum (skip verification here for read performance; it's done on open/recovery)
        // cursor += 4;

        return new LogEntry(seqnum, streamVersion, tags, data, Instant.ofEpochMilli(tsMillis));
    }

    // -------------------------------------------------------------------------
    // Recovery
    // -------------------------------------------------------------------------

    private boolean tryLoadGlobalIndex() {
        if (!Files.exists(indexFile)) return false;
        try {
            long size = Files.size(indexFile);
            if (size == 0) return true; // empty log, valid
            if (size % 16 != 0) {
                log.warn("Index file size {} is not a multiple of 16; truncated?", size);
                return false;
            }
            try (FileChannel ch = FileChannel.open(indexFile, StandardOpenOption.READ)) {
                ByteBuffer buf = ByteBuffer.allocate((int) Math.min(size, 16 * 1024 * 64)); // 1 MB chunks
                long remaining = size;
                while (remaining > 0) {
                    int toRead = (int) Math.min(buf.capacity(), remaining);
                    buf.limit(toRead);
                    buf.rewind();
                    readFully(ch, buf, size - remaining);
                    buf.flip();
                    while (buf.remaining() >= 16) {
                        long seqnum = buf.getLong();
                        long fileOffset = buf.getLong();
                        globalIndex.put(seqnum, fileOffset);
                    }
                    remaining -= toRead;
                }
            }
            return true;
        } catch (IOException e) {
            log.warn("Failed to load index file: {}", e.getMessage());
            return false;
        }
    }

    private void rebuildIndexFromLog() throws IOException {
        globalIndex.clear();
        if (!Files.exists(logFile)) return;
        try (FileChannel ch = FileChannel.open(logFile, StandardOpenOption.READ)) {
            long fileSize = ch.size();
            long cursor = 0;
            while (cursor < fileSize) {
                long entryOffset = cursor;
                // Try to read magic + seqnum to validate
                ByteBuffer hdr = ByteBuffer.allocate(12);
                try {
                    readFully(ch, hdr, cursor);
                } catch (IOException e) {
                    log.warn("Partial entry at offset {}; stopping scan", cursor);
                    break;
                }
                hdr.flip();
                int magic = hdr.getInt();
                if (magic != MAGIC) {
                    log.warn("Bad magic at offset {}; stopping scan", cursor);
                    break;
                }
                long seqnum = hdr.getLong();
                globalIndex.put(seqnum, entryOffset);

                // Skip to next entry: decode to find length
                try {
                    LogEntry entry = decodeAt(ch, entryOffset);
                    cursor = entryOffset + entrySize(entry);
                } catch (IOException e) {
                    log.warn("Decode error at offset {}; stopping scan: {}", cursor, e.getMessage());
                    break;
                }
            }
        }
        log.info("Rebuilt index: {} entries", globalIndex.size());
    }

    private void rebuildTagIndices() throws IOException {
        tagSeqnums.clear();
        tagVersionCount.clear();
        if (globalIndex.isEmpty() || !Files.exists(logFile)) return;

        try (FileChannel ch = FileChannel.open(logFile, StandardOpenOption.READ)) {
            for (Map.Entry<Long, Long> e : globalIndex.entrySet()) {
                if (e.getKey() < trimSeqnum) continue;
                try {
                    LogEntry entry = decodeAt(ch, e.getValue());
                    for (LogTag tag : entry.tags()) {
                        tagSeqnums
                                .computeIfAbsent(tag, k -> new ConcurrentSkipListMap<>())
                                .put(entry.seqnum(), entry.streamVersion());
                        tagVersionCount
                                .computeIfAbsent(tag, k -> new AtomicLong(0))
                                .updateAndGet(c -> Math.max(c, entry.streamVersion() + 1));
                    }
                } catch (IOException ex) {
                    log.warn("Could not decode entry at offset {}; skipping: {}", e.getValue(), ex.getMessage());
                }
            }
        }
    }

    // -------------------------------------------------------------------------
    // Read helpers
    // -------------------------------------------------------------------------

    private List<LogEntry> readEntries(Iterable<Long> offsets) throws IOException {
        if (!Files.exists(logFile)) return Collections.emptyList();
        List<LogEntry> result = new ArrayList<>();
        try (FileChannel ch = FileChannel.open(logFile, StandardOpenOption.READ)) {
            for (long offset : offsets) {
                result.add(decodeAt(ch, offset));
            }
        }
        return Collections.unmodifiableList(result);
    }

    private static void readFully(FileChannel ch, ByteBuffer buf, long position) throws IOException {
        long pos = position;
        while (buf.hasRemaining()) {
            int n = ch.read(buf, pos);
            if (n < 0) throw new IOException("Unexpected EOF at position " + pos);
            pos += n;
        }
    }

    // -------------------------------------------------------------------------
    // Size calculation (for recovery cursor advance)
    // -------------------------------------------------------------------------

    private int entrySize(LogEntry entry) {
        int tagsLen = 0;
        for (LogTag tag : entry.tags()) {
            tagsLen += 2 + tag.namespace().getBytes(StandardCharsets.UTF_8).length;
            tagsLen += 2 + tag.key().getBytes(StandardCharsets.UTF_8).length;
        }
        return FIXED_OVERHEAD + tagsLen + entry.dataUnsafe().length;
    }

    // -------------------------------------------------------------------------
    // Misc
    // -------------------------------------------------------------------------

    private void ensureOpen() {
        if (!open) throw new IllegalStateException("Adapter is not open");
    }

    private static void closeQuietly(AutoCloseable c) {
        if (c != null) try { c.close(); } catch (Exception ignored) {}
    }
}
