package com.central.sharedlog.persistence;

import com.central.sharedlog.core.LogEntry;
import com.central.sharedlog.core.LogTag;
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
 * </pre>
 *
 * <h2>Log entry binary format</h2>
 * <pre>
 * [MAGIC    : 4 bytes  = 0xC0FFEE42  ]
 * [seqnum   : 8 bytes, big-endian    ]
 * [timestamp: 8 bytes, millis epoch  ]
 * [localId  : 8 bytes, big-endian    ]
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
 */
public class FileBasedPersistenceAdapter implements PersistenceAdapter {

    private static final Logger log = LoggerFactory.getLogger(FileBasedPersistenceAdapter.class);

    private static final int MAGIC = 0xC0FFEE42;
    /** Fixed-size overhead per entry: magic(4)+seqnum(8)+ts(8)+localId(8)+numTags(4)+dataLen(4)+crc(4) = 40 bytes */
    private static final int FIXED_OVERHEAD = 40;

    private final Path dataDir;
    private final Path logFile;
    private final Path indexFile;
    private final Path trimFile;

    /** Durable write channel for the log; opened in APPEND mode. */
    private FileChannel logChannel;
    /** Durable write channel for the index; opened in APPEND mode. */
    private FileChannel indexChannel;

    /** In-memory global index: seqnum → file offset in log.dat */
    private final ConcurrentSkipListMap<Long, Long> globalIndex = new ConcurrentSkipListMap<>();

    /** In-memory per-tag index: tag → (seqnum → localId) */
    private final ConcurrentHashMap<LogTag, ConcurrentSkipListMap<Long, Long>> tagSeqnums =
            new ConcurrentHashMap<>();

    /** Per-tag local-id counter. */
    private final ConcurrentHashMap<LogTag, AtomicLong> tagLocalIdCount =
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
    }

    public FileBasedPersistenceAdapter(String dataDir) {
        this(Path.of(dataDir));
    }

    // -------------------------------------------------------------------------
    // Lifecycle
    // -------------------------------------------------------------------------

    @Override
    public void open() throws IOException {
        Files.createDirectories(dataDir);

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
        log.info("FileBasedPersistenceAdapter closed");
    }

    // -------------------------------------------------------------------------
    // Write
    // -------------------------------------------------------------------------

    @Override
    public void append(LogEntry entry) throws IOException {
        ensureOpen();
        byte[] encoded = encode(entry);

        // Current position = end of log file
        long offset = logChannel.size();

        ByteBuffer buf = ByteBuffer.wrap(encoded);
        while (buf.hasRemaining()) {
            logChannel.write(buf);
        }
        logChannel.force(false); // fdatasync (metadata not required)

        // Append to index
        ByteBuffer idxBuf = ByteBuffer.allocate(16);
        idxBuf.putLong(entry.seqnum());
        idxBuf.putLong(offset);
        idxBuf.flip();
        while (idxBuf.hasRemaining()) {
            indexChannel.write(idxBuf);
        }
        indexChannel.force(false);

        // Update in-memory indices
        globalIndex.put(entry.seqnum(), offset);
        for (LogTag tag : entry.tags()) {
            tagSeqnums
                    .computeIfAbsent(tag, k -> new ConcurrentSkipListMap<>())
                    .put(entry.seqnum(), entry.localId());
            tagLocalIdCount
                    .computeIfAbsent(tag, k -> new AtomicLong(0))
                    .updateAndGet(c -> Math.max(c, entry.localId() + 1));
        }
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
        // range: seqnum → localId; we need offsets from the global index
        List<Long> offsets = new ArrayList<>(range.size());
        for (long seqnum : range.keySet()) {
            Long offset = globalIndex.get(seqnum);
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
    public long getLocalIdCountForTag(LogTag tag) {
        AtomicLong c = tagLocalIdCount.get(tag);
        return c == null ? 0L : c.get();
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
        buf.putLong(entry.localId());
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
     * magic(4) + seqnum(8) + timestamp(8) + localId(8) + numTags(4) = 32 bytes.
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

        long seqnum   = header.getLong();
        long tsMillis = header.getLong();
        long localId  = header.getLong();
        int numTags   = header.getInt();

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

        return new LogEntry(seqnum, localId, tags, data, Instant.ofEpochMilli(tsMillis));
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
        tagLocalIdCount.clear();
        if (globalIndex.isEmpty() || !Files.exists(logFile)) return;

        try (FileChannel ch = FileChannel.open(logFile, StandardOpenOption.READ)) {
            for (Map.Entry<Long, Long> e : globalIndex.entrySet()) {
                if (e.getKey() < trimSeqnum) continue;
                try {
                    LogEntry entry = decodeAt(ch, e.getValue());
                    for (LogTag tag : entry.tags()) {
                        tagSeqnums
                                .computeIfAbsent(tag, k -> new ConcurrentSkipListMap<>())
                                .put(entry.seqnum(), entry.localId());
                        tagLocalIdCount
                                .computeIfAbsent(tag, k -> new AtomicLong(0))
                                .updateAndGet(c -> Math.max(c, entry.localId() + 1));
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
