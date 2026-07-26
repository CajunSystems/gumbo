# Persistence Evolution

This document traces the persistence architecture from its starting point
through current optimisations and maps a concrete path toward io_uring-class
throughput on the JVM.

---

## Stage 1 — `InMemoryPersistenceAdapter` (baseline)

**Location**: `persistence/InMemoryPersistenceAdapter.java`

```
append(entry)
  → ConcurrentSkipListMap.put(seqnum, entry)    // O(log n), no syscall
  → tagIndex[tag].put(seqnum, streamVersion)
```

Pure in-memory using `ConcurrentSkipListMap` for a lock-free read path and
a monotonic `seqnum → entry` global index.  Zero I/O; no durability.

**Use-case**: unit tests, local development, short-lived projections.

---

## Stage 2 — `FileBasedPersistenceAdapter` (durable WAL)

**Location**: `persistence/FileBasedPersistenceAdapter.java`

```
append(entry)
  → encode(entry)                // ByteBuffer packing: header + tags + data + CRC32
  → logChannel.write(buf)        // pwrite64() — write to log.dat
  → logChannel.force(false)      // fdatasync() ← BOTTLENECK
  → indexChannel.write(idxBuf)   // pwrite64() — write [seqnum:8][offset:8] to index.dat
  → indexChannel.force(false)    // fdatasync() ← BOTTLENECK
  → globalIndex.put(seqnum, offset)
  → tagSeqnums[tag].put(seqnum, streamVersion)
```

**Two `fdatasync` calls per entry** is the dominant cost.  On NVMe this is
~50–200 µs per call; on spinning disk ~5 ms.  At 1 000 writes/s with 200 µs
latency, the sync path alone consumes 400 ms/s of I/O time.

### Binary wire format

```
┌──────────┬──────────┬──────────────┬──────────┬────────────────┬──────────┬───────────┬──────────┐
│ MAGIC    │ seqnum   │ timestamp    │ version  │ tags (var)     │ dataLen  │ data      │ CRC32    │
│ 4 bytes  │ 8 bytes  │ 8 bytes      │ 8 bytes  │ nsLen+ns+…     │ 4 bytes  │ N bytes   │ 4 bytes  │
└──────────┴──────────┴──────────────┴──────────┴────────────────┴──────────┴───────────┴──────────┘
```

`index.dat` stores `[seqnum:8][fileOffset:8]` records for O(log n) random
access without scanning the log.  `trim.dat` is an 8-byte atomic file that
records the trim horizon (written via temp-rename for crash-safety).

**Recovery**: On `open()`, the adapter loads the index file; if absent or
truncated it falls back to a full `log.dat` scan and rebuilds from scratch
(same approach Boki uses when an engine node restarts cold).

---

## Stage 3 — `BatchingPersistenceAdapter` (group-commit, current)

**Location**: `persistence/BatchingPersistenceAdapter.java`

```
append(entry)   →  pendingBatch.add(entry)          // no I/O
                   if (pendingBatch.size() >= N)
                     delegate.appendBatch(batch)     // ← one fdatasync per N entries
                                                     //   instead of one per entry
```

Wraps any `PersistenceAdapter` and flushes via `appendBatch()`, which
`FileBasedPersistenceAdapter` overrides to call `syncChannels()` once after
writing all N entries.

**Group-commit benefit**: N entries cost **2 `fdatasync` calls** instead of 2N.

### Throughput comparison

| Adapter | Writes/s (NVMe, 200 µs fdatasync) | Writes/s (HDD, 5 ms fdatasync) |
|---|---|---|
| `FileBasedPersistenceAdapter` (per-entry sync) | ~2 500 | ~100 |
| `BatchingPersistenceAdapter` (batch=64, 10 ms) | ~64 000 | ~12 800 |

### Parameters

| Parameter | Default | Notes |
|---|---|---|
| `maxBatchSize` | 64 | Size-triggered flush |
| `maxDelayMs` | 10 ms | Time-triggered flush by a background virtual thread |

### Durability window

Entries sit in heap memory between `append()` and the next flush.  A JVM crash
within the delay window loses those entries.  Configure `maxDelayMs=0` +
`maxBatchSize=1` to match single-entry durability.

```java
// High-throughput, ~10 ms durability window
BatchingPersistenceAdapter.of(new FileBasedPersistenceAdapter(path))

// Strict durability (same as no batching)
new BatchingPersistenceAdapter(new FileBasedPersistenceAdapter(path), 1, 0)
```

---

## FoundationDB Path — Recommended for production

**Location**: `persistence/FoundationDBPersistenceAdapter.java`

This is the **recommended persistence backend for any multi-node or production
deployment**.  Rather than optimising how bytes hit a local disk, it replaces
the storage medium entirely with a distributed, replicated key-value store.
The local-file stages (2–5) remain fully supported for single-node and
low-latency use cases.

```
append(entry)
  → FDB transaction:
      logSubspace.set(seqnum, encodeEntry(entry))   // O(log n), distributed write
      tagSubspace.set((ns, key, seqnum), streamVersion)   // tag index entry
      metaSubspace.set("latest", seqnum)            // cached high-water mark
  → db.run() → commit()                             // 3-way replicated, crash-safe
```

### FDB subspace layout

```
{root} / "log"  / seqnum (long)                  → entry value bytes
{root} / "tag"  / namespace / key / seqnum        → streamVersion (8 bytes)
{root} / "meta" / "latest"                        → latestSeqnum
{root} / "meta" / "trim"                          → trimSeqnum
{root} / "meta" / "tagcount" / namespace / key    → versionCount
```

FDB's tuple-layer encoding preserves the natural ordering of `seqnum` keys, so
`readFrom(n)` is a single range scan with no separate index file.  `readByTag`
uses two FDB round-trips: one range scan of the tag index, then parallel point
reads on the log subspace within the same snapshot transaction.

### No MAGIC / CRC

`FileBasedPersistenceAdapter` embeds a 4-byte MAGIC sentinel and a 4-byte CRC32
per entry to detect partial writes and corruption on local disk.  FDB omits
these: every committed transaction is verified end-to-end by the FDB storage
layer before the commit future resolves.  The value format is therefore 8 bytes
shorter per entry.

### Durability model

| Adapter | Durability unit | Failure domain |
|---|---|---|
| `FileBasedPersistenceAdapter` | `fdatasync` on local disk | Single machine |
| `BatchingPersistenceAdapter` | `fdatasync` once per N entries | Single machine |
| `FoundationDBPersistenceAdapter` | FDB commit (3-way replication) | Cluster-wide |

A committed FDB transaction is durable across the replication group before
`db.run()` returns.  No `fdatasync` logic is needed in the adapter.

### Throughput

```
appendBatch(N entries)
  → 1 FDB transaction (up to 8 MB of encoded data)
  → ~1–5 ms round-trip latency per transaction
```

| Operation | Throughput |
|---|---|
| Single `append()` per transaction | ~200–1 000 writes/s |
| `appendBatch(64)` — one transaction per 64 entries | ~12 000–64 000 writes/s |
| `BatchingPersistenceAdapter` wrapping FDB adapter | Same as appendBatch above |

The 10 MB per-transaction write limit is enforced by `appendBatch`: batches that
would exceed 8 MB are automatically split into multiple consecutive transactions,
each individually durable.

### Comparison: when to use FDB vs local file adapters

| Requirement | Local file adapter | FDB adapter |
|---|---|---|
| Single writer process | ✓ Simpler, lower latency | ✓ Works but adds RTT overhead |
| Multiple concurrent writers | ✗ Unsafe — duplicate seqnums | ✓ Only safe option |
| Data survives single-node failure | ✗ | ✓ 3-way replication |
| Sub-millisecond P99 write latency | ✓ (NVMe, ~50–200 µs fdatasync) | ✗ Network RTT adds 1–5 ms |
| Cross-datacenter DR | ✗ Manual rsync / replication | ✓ FDB built-in DR config |
| No external cluster to operate | ✓ | ✗ Requires FDB cluster |
| FaaS / stateless compute (any node writes) | ✗ | ✓ Designed for this model |

### Batch seqnum reservation (Boki metalog optimisation)

`FoundationDBSequencer` now implements `Sequencer.nextBatch(int count)`, which
claims `count` seqnums in a **single FDB read-modify-write transaction** instead
of one transaction per seqnum.  `SharedLogService.appendBatch(List<AppendRequest>)`
uses this automatically.

```
Without appendBatch (N = 64 entries):
  64 × sequencer.next()         → 64 FDB transactions (sequencer)
  +  appendBatch on adapter     → 1  FDB transaction  (data)
  ─────────────────────────────────────────────────────
  Total: 65 FDB transactions

With appendBatch (N = 64 entries):
  sequencer.nextBatch(64)       → 1  FDB transaction  (sequencer)
  +  adapter.appendBatch(64)    → 1  FDB transaction  (data)
  ─────────────────────────────────────────────────────
  Total: 2 FDB transactions
```

This is the same technique the Boki metalog uses: the sequencer issues a single
cut vector covering N records rather than touching FDB for each individual record.

### Pairing with FoundationDBSequencer

For a fully distributed write path, use `FoundationDBSequencer` alongside the
adapter. Both components can share a single `Database` connection:

```java
FDB      fdb     = FDB.selectAPIVersion(730);
Database db      = fdb.open("/etc/foundationdb/fdb.cluster");

FoundationDBSequencer         seq   = new FoundationDBSequencer(db, "myapp");
FoundationDBPersistenceAdapter store = new FoundationDBPersistenceAdapter(db, "myapp");
seq.open();
store.open();

SharedLogConfig config = SharedLogConfig.builder()
    .sequencer(seq)
    .persistenceAdapter(store)
    .build();
```

`FoundationDBSequencer.next()` is a read-modify-write FDB transaction on a
single counter key. FDB's OCC serialises concurrent callers across nodes —
this is the same mechanism Boki uses for its metalog sequencer.

### Installation

```bash
# Debian / Ubuntu
apt install foundationdb-clients   # installs libfdb_c.so
```

```xml
<!-- pom.xml — already present as optional -->
<dependency>
    <groupId>org.foundationdb</groupId>
    <artifactId>fdb-java</artifactId>
    <version>7.3.43</version>
    <optional>true</optional>
</dependency>
```

---

## Stage 4 — `MmapPersistenceAdapter` (planned)

**Key idea**: map `log.dat` into the process address space via
`FileChannel.map(READ_WRITE)`.  Writes become `MappedByteBuffer.put(bytes)` —
a `memcpy` into the OS page cache with **zero syscalls**.  A single
`MappedByteBuffer.force()` (i.e. `msync`) drains the entire batch.

```
append(entry)
  → encode(entry)
  → logBuffer.put(encoded)     // memcpy to page cache — no syscall
  → writePointer += encoded.length

// on flush (batch full or timeout):
  → logBuffer.force()          // msync once for all accumulated entries
```

**Why this beats `FileChannel`**: every `FileChannel.write()` call requires a
syscall (`pwrite64`).  With `MappedByteBuffer`, there is no per-write syscall;
the OS transparently pages out dirty pages.

Chronicle Queue and LMAX Disruptor use this approach to achieve < 1 µs write
latency.

**Trade-offs**:
- File must be pre-allocated to a known maximum (extend with
  `FileChannel.truncate(newSize)` when approaching the limit).
- On a 32-bit JVM, address space limits the maximum mappable size.
  On 64-bit this is not an issue.
- `msync` and `mmap` are Linux/macOS specific; behaviour on Windows differs.

**Implementation sketch** (future `MmapPersistenceAdapter`):

```java
// open
logBuffer = logChannel.map(READ_WRITE, 0, INITIAL_SIZE);

// append (no fdatasync until explicit flush)
private void writeNoSync(LogEntry entry) {
    byte[] encoded = encode(entry);
    logBuffer.put(logWritePos, encoded);
    logWritePos += encoded.length;
    // update in-memory index immediately
}

// batch flush
public void syncChannels() {
    logBuffer.force();   // msync
    idxBuffer.force();
}
```

---

## Stage 5 — `IoUringPersistenceAdapter` (future, Linux 5.1+)

### What Boki actually does with io_uring

Boki's C++ storage nodes (`storage/logfile.cpp`) submit all I/O through an
io_uring submission ring.  A single `io_uring_submit()` call batches:
1. `IORING_OP_WRITE` — log bytes
2. `IORING_OP_WRITE` — index record
3. `IORING_OP_FSYNC` — durability

This costs **one syscall for the entire batch** (the submit call itself),
regardless of how many entries are in it.  Between submissions, the engine
thread polls the completion queue without blocking.

### JVM options

#### Option A — Java 21 Panama FFI (recommended)

Java 21 ships a stable Foreign Function Interface (`java.lang.foreign`).  No
JNI, no native compilation:

```java
// Load liburing
Linker linker = Linker.nativeLinker();
SymbolLookup uring = SymbolLookup.libraryLookup("liburing.so", Arena.global());

// io_uring_queue_init(32, &ring, 0)
MethodHandle queueInit = linker.downcallHandle(
    uring.find("io_uring_queue_init").orElseThrow(),
    FunctionDescriptor.of(ValueLayout.JAVA_INT,
        ValueLayout.JAVA_INT, ValueLayout.ADDRESS, ValueLayout.JAVA_INT));

// Submit SQEs for write + write + fsync in one call
// Poll CQEs for completions without blocking
```

**Key advantage**: no native build step.  `liburing` must be installed on the
host (`apt install liburing-dev`).  Requires Linux 5.1+.

#### Option B — Netty io_uring transport (network layer only)

`netty-incubator-codec-native-io_uring` provides io_uring for TCP/UDP.
Relevant for a **distributed sequencer** (sequencer ↔ storage node messages),
not for the WAL file path.

```xml
<dependency>
    <groupId>io.netty.incubator</groupId>
    <artifactId>netty-incubator-codec-native-io_uring</artifactId>
    <version>0.0.25.Final</version>
    <classifier>linux-x86_64</classifier>
</dependency>
```

#### Option C — Virtual threads + blocking I/O (already in place)

Java 21 virtual threads park on blocking I/O calls without consuming OS threads.
On Linux, the JVM uses epoll internally for network I/O.  For file I/O, the
JVM submits blocking calls to a thread pool (file I/O is POSIX-synchronous).
This is the current architecture and already benefits from virtual-thread parking.

### Roadmap summary

```
Distributed path (multi-node) — RECOMMENDED for production
──────────────────────────────────────────────────────────────────
FoundationDBPersistenceAdapter   3-way replicated, 1 FDB txn / batch
+ FoundationDBSequencer          distributed seqnum via FDB OCC
+ SharedLog.appendBatch(N)       1 seqnum txn + 1 data txn = 2 FDB RTTs per N entries

Local-disk path (single-node / low-latency)
──────────────────────────────────────────────────────────────────
Stage 1   InMemoryPersistenceAdapter     tests, dev (no durability)
Stage 2   FileBasedPersistenceAdapter    durable WAL (2 fdatasyncs / entry)
Stage 3   BatchingPersistenceAdapter     CURRENT: 2 fdatasyncs / N entries
Stage 4   MmapPersistenceAdapter         PLANNED: msync / N entries, zero pwrite64s
Stage 5   IoUringPersistenceAdapter      FUTURE: 1 io_uring_submit / batch
```

All stages share the same `PersistenceAdapter` interface.  Switching is a
one-line config change.  The distributed and local-disk paths are fully
independent — choose based on your deployment requirements.

---

## Serialization evolution

### Stage 1 — Raw `byte[]`

All initial API surfaces (`AppendRequest.to(tag, byte[])`,
`LogView.append(byte[])`, `LogEntry.data()`) pass opaque byte arrays.  Callers
must handle encoding/decoding themselves.

### Stage 2 — `TypedLogView<T>` + `KryoLogSerializer` (current)

`TypedLogView<T>` wraps a `LogView` and a `LogSerializer<T>` to give type-safe
append/read/subscribe without exposing byte arrays at call sites.

```
User code
   ↓  TypedLogView<T>.append(object)
      → KryoLogSerializer.serialize(object) → byte[]
      → LogView.append(byte[])
      → SharedLogService → PersistenceAdapter

User code ← TypedLogView<T>.readAll()
   ↑       ← KryoLogSerializer.deserialize(entry.data())
            ← LogView.readAll() → List<LogEntry>
```

`KryoLogSerializer` maintains a `Pool<Kryo>` so it is fully thread-safe and
avoids per-call Kryo instance creation.

### Stage 3 — Alternative serializers (possible)

The `LogSerializer<T>` interface is format-agnostic.  Plugging in Jackson,
Protobuf, or Avro requires only implementing two methods:

```java
LogSerializer<OrderEvent> proto = new LogSerializer<>() {
    public byte[]     serialize(OrderEvent v) { return v.toByteArray(); }
    public OrderEvent deserialize(byte[] b)   { return OrderEvent.parseFrom(b); }
};
```

---

*For background on Boki's original io_uring and sequencer design see the
[SOSP 2021 paper](https://www.cs.utexas.edu/~witchel/pubs/jia21sosp-boki.pdf)
and the [Boki source](https://github.com/ut-osa/boki).*
