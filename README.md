# Gumbo

A shared log library for Java 21 that provides a single append-only, totally-ordered
log as the source of truth for your entire system. Stateless **executors** derive
all their state by replaying the log, process it, and write new entries back — making
every component crash-safe and independently restartable with no coordination.

**Inspired by [Boki](https://github.com/ut-osa/boki)** (SOSP 2021), Gumbo brings the shared log primitive to Java with a focus on simplicity, flexibility, and production readiness. See [Boki Comparison](docs/BOKI_COMPARISON.md) for details on how Gumbo relates to and differs from Boki.

---

## Features

- **Single source of truth**: All state derived from an append-only log
- **Crash-safe by design**: Replay the log to recover exact state after any failure
- **Flexible deployment**: Single-node (zero dependencies) to multi-node (FoundationDB)
- **Type-safe API**: `TypedLogView<T>` with pluggable serialization (Kryo included)
- **Virtual threads**: Lightweight concurrency for executors and subscriptions
- **Pluggable persistence**: In-memory, file-based, or FoundationDB backends
- **Tag-based filtering**: Multiple logical streams in one physical log
- **Stateless executors**: Functional fold pattern for deterministic state rebuilding

---

## Quick start

```java
// 1. Open the log with in-memory storage
SharedLogConfig config = SharedLogConfig.builder()
    .persistenceAdapter(new InMemoryPersistenceAdapter())
    .build();

try (SharedLogService log = SharedLogService.open(config)) {

    LogTag orders = LogTag.of("orders");

    // 2. Append
    AppendResult r = log.append(AppendRequest.to(orders, "order-placed".getBytes())).join();
    System.out.println("seqnum=" + r.seqnum() + " streamVersion=" + r.streamVersion());

    // 3. Read
    List<LogEntry> entries = log.readAll(orders).join();

    // 4. Subscribe (runs listener on a virtual thread)
    SharedLog.Subscription sub = log.subscribe(orders, LogPosition.BEGINNING, entry ->
        System.out.println("received: " + new String(entry.data())));

    // 5. Use a view
    LogView view = log.getView(orders);
    view.append("order-confirmed".getBytes()).join();

    sub.close();
}
```

Switch to file-backed storage:

```java
SharedLogConfig config = SharedLogConfig.builder()
    .persistenceAdapter(new FileBasedPersistenceAdapter("/var/data/myapp"))
    .build();
```

---

## Table of contents

1. [Installation](#installation)
2. [Core concepts](#core-concepts)
3. [API overview](#api-overview)
4. [Persistence adapters](#persistence-adapters)
5. [Sequencers](#sequencers)
6. [Typed log views](#typed-log-views-kryo)
7. [Executor model](#executor-model)
8. [Actor checkpoints](#actor-checkpoints)
9. [Virtual threads](#virtual-threads)
10. [Architecture](#architecture)
11. [Module map](#module-map)
12. [Data model](#data-model)
13. [Examples](#examples)
14. [Building](#building)
15. [Further reading](#further-reading)

---

## Installation

Gumbo is distributed via [JitPack](https://jitpack.io). No local build required.

### Maven

Add the JitPack repository to your `pom.xml`:

```xml
<repositories>
    <repository>
        <id>jitpack.io</id>
        <url>https://jitpack.io</url>
    </repository>
</repositories>
```

Then add the dependency:

```xml
<dependency>
    <groupId>com.github.CajunSystems</groupId>
    <artifactId>gumbo</artifactId>
    <version>0.4.0</version>
</dependency>
```

### Gradle

```groovy
repositories {
    maven { url 'https://jitpack.io' }
}

dependencies {
    implementation 'com.github.CajunSystems:gumbo:0.4.0'
}
```

> **Note**: Gumbo requires Java 21+.

### Which groupId — `com.github.CajunSystems` or `com.cajunsystems`?

Both name the same jar, and which one resolves depends on where you got it. **Use
`com.github.CajunSystems` unless you built gumbo yourself.**

| How you obtained it | Coordinate |
|---|---|
| Fetched from JitPack (the snippets above) | `com.github.CajunSystems:gumbo` |
| Built locally with `mvn install` | `com.cajunsystems:gumbo` — what [`pom.xml`](pom.xml) declares |

JitPack rewrites the groupId to `com.github.{owner}` when it publishes, so the coordinate in
gumbo's own pom is *not* the one you depend on when you fetch it. Depending on
`com.cajunsystems:gumbo` works on a machine where gumbo has been installed by hand and fails
everywhere else — including CI — with `Could not find artifact ... in central`. That is not
hypothetical: a downstream build was pinned that way and could only be built by whoever had
run the install.

---

## Core concepts

### The log as a single source of truth

Every meaningful event in the system is represented as an immutable `LogEntry`. Nothing
is stored anywhere else. If you need the current state of an entity, you read all log
entries for that entity's tag and fold them together. This is exactly how event sourcing
and CQRS work, but with the log itself as the only durable store.

```
time ──────────────────────────────────────────────────────────▶
seqnum    0          1          2          3          4
          │          │          │          │          │
tag       orders     inventory  orders     orders     inventory
          │          │          │          │          │
data      placed     -10 units  confirmed  shipped    +20 units
```

Any reader who wants the state of `orders` filters to seqnums `{0, 2, 3}` and replays.
Any reader who wants `inventory` gets `{1, 4}`. The global order is preserved; tag
filtering is a read-time projection.

### Tags and log views

A `LogTag` has a **namespace** (entity type) and an optional **key** (entity instance):

```java
LogTag allOrders   = LogTag.of("orders");           // namespace-wide
LogTag singleOrder = LogTag.of("orders", "order-42"); // instance-scoped
```

One log entry can carry **multiple tags** so it is visible from several views:

```java
AppendRequest.to(
    Set.of(LogTag.of("orders"), LogTag.of("orders", "order-42")),
    payload);
```

A `LogView` is a lightweight, tag-scoped window over the shared log. It supports
reads, appends (which write back to the underlying log with the view's tag included),
and push subscriptions.

### Sequence numbers and stream versions

Every entry has two identifiers:

- **`seqnum`** — globally unique, monotonically increasing across all tags. Provides
  total ordering. Assigned by the `Sequencer`.
- **`streamVersion`** — monotonically increasing *within* a tag's stream, dense from
  zero. The per-entity cursor. (Called `localId` before 0.3.0, after Boki's `localid`;
  renamed because Boki's is a per-*engine* write-path id, superseded once the sequencer
  assigns a seqnum, rather than the permanent per-*tag* position this has always been.
  `localId()` still works and is deprecated for removal.)

In Boki, `seqnum = [logspace_id:32 | position:32]` encodes the physical log shard.
Here it is a plain `long` from an `AtomicLong`; a distributed implementation would
encode the node and term in the high bits.

**Read with the number you are holding.** The two are interchangeable only while a log
holds a single tag — the configuration most tests and examples use, and the reason the
distinction is easy to miss. Once a log carries two streams, their seqnums interleave
while their versions stay independent:

```
seqnum    0        1        2        3        4
tag       orders   inv      orders   inv      orders
version   0        0        1        1        2
```

A consumer of `orders` that has processed through its version 1 and asks
`readAfter(1)` — a seqnum-keyed read — gets back versions 1 *and* 2, and reprocesses an
entry it has already seen. `readAfterVersion(1)` returns only version 2. Use the
version-keyed reads whenever the position came from this tag's own stream:

```java
LogView orders = log.getView(LogTag.of("orders"));

List<LogEntry> backlog = orders.readAfterVersion(cursor).join();  // this tag's numbering
List<LogEntry> delta   = orders.readAfter(seqnum).join();         // the global sequence

long tip = orders.getLatestVersion();   // -1 when the tag is empty
```

Both forms exist on `SharedLog`, `LogView` and `TypedLogView`.

### Conditional append

A version is also a fence. `append(request, expectedVersion)` writes only if the primary tag
is still where the writer last saw it:

```java
List<LogEntry> history = log.readAll(tag).join();
long at = history.isEmpty() ? 0 : history.getLast().streamVersion() + 1;

log.append(AppendRequest.to(tag, decide(fold(history))), at).join();
// VersionConflictException if anyone appended in between
```

The check happens **in storage**, in the same operation as the write. That matters because a
lock service can tell a node it *holds* a lock but never that it still holds it at the instant
the write lands — a pause between those two moments is enough for two writers to both believe
they own the stream. Conditioning on the version closes that gap: the loser is rejected by the
store rather than by a coordination layer that may itself be stale.

The version is assigned by the adapter, never by the caller — a caller-side counter is seeded
once and then drifts from every other writer's copy with nothing to reconcile them.

Two limits worth knowing:

- **The condition applies to the primary tag only.** That is the shape real callers have — one
  entity stream that needs fencing, one fan-out tag that does not — and conditioning on every
  tag would fail appends for reasons unrelated to the entity being written.
- **Not every adapter can fence.** It needs an atomic compare-and-increment in the same store.
  FoundationDB does it in one transaction, so it holds across processes. The file and
  in-memory adapters do it under their own lock, which is atomic within the single-writer
  configuration they support. An adapter that cannot throws `UnsupportedOperationException`
  rather than writing unconditionally.

> **One caveat.** An entry carries a single `streamVersion`, taken from its *primary* tag, so a
> tag that appears only as a secondary tag on an atomic multi-tag append inherits the
> other stream's numbering rather than counting its own. Version-keyed reads are meant for
> a tag whose entries are written with it as the primary tag — a per-entity stream. For a
> shared fan-out tag fed by multi-tag appends, keep using the seqnum-keyed reads.

---

## API overview

Gumbo provides three levels of API abstraction:

### 1. SharedLog — Core log operations

The main interface for interacting with the shared log:

```java
SharedLogService log = SharedLogService.open(config);

// Append entries
AppendResult result = log.append(AppendRequest.to(tag, data)).join();

// Read entries
List<LogEntry> all = log.readAll(tag).join();
List<LogEntry> from = log.readFrom(tag, position, maxEntries).join();

// Subscribe to new entries
Subscription sub = log.subscribe(tag, position, entry -> {
    // Process entry on virtual thread
});

// Batch operations
List<AppendResult> results = log.appendBatch(requests).join();

// Trim old entries
log.trim(upToSeqnum).join();
```

### 2. LogView — Tag-scoped window

A lightweight view over a specific tag:

```java
LogView view = log.getView(LogTag.of("orders"));

// Append (automatically includes the view's tag)
view.append(data).join();

// Read operations
List<LogEntry> entries = view.readAll().join();
LogEntry next = view.readNextAfter(seqnum).join();
LogEntry prev = view.readPrevBefore(seqnum).join();
long tail = view.checkTail().join();

// Subscribe to new entries only (no backlog)
Subscription sub = view.subscribeTail(entry -> process(entry));

// Durable KV storage (for checkpoints)
view.setValue("checkpoint", bytes).join();
byte[] saved = view.getValue("checkpoint").join();

// ...and conditionally, for claims and counters (see "Claiming a stream" below)
boolean claimed = view.setValueIfAbsent("owner", nodeId).join();
long attempts = view.incrementValue("attempts", 1).join();
```

### 3. TypedLogView — Type-safe operations

Eliminates byte[] handling with automatic serialization:

```java
LogSerializer<OrderEvent> serializer = new KryoLogSerializer<>(OrderEvent.class);
TypedLogView<OrderEvent> view = log.getTypedView(tag, serializer);

// Append domain objects directly
view.append(new OrderEvent("order-42", "placed")).join();

// Read typed objects
List<OrderEvent> events = view.readAll().join();

// Typed subscriptions
Subscription sub = view.subscribeTail(event -> 
    System.out.println("Order: " + event.orderId()));
```

### Core data types

```java
// Tag: identifies a logical stream
LogTag tag = LogTag.of("orders");              // namespace-wide
LogTag tag = LogTag.of("orders", "order-42");  // instance-scoped

// Entry: immutable log record
record LogEntry(
    long seqnum,        // global sequence number
    long streamVersion, // position within the primary tag's stream
    Set<LogTag> tags,   // tags this entry belongs to
    byte[] data,        // payload
    long timestamp      // append timestamp
)

// Position: read cursor
LogPosition pos = new LogPosition(seqnum);
LogPosition.BEGINNING  // start from seqnum 0
LogPosition.END        // start from latest

// Append request
AppendRequest req = AppendRequest.to(tag, data);
AppendRequest req = AppendRequest.to(Set.of(tag1, tag2), data);  // multi-tag

// Append result
record AppendResult(
    long seqnum,
    long streamVersion,
    LogTag primaryTag,
    long timestamp
)
```

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                        SharedLogService                             │
│                                                                     │
│  append(AppendRequest)                                              │
│    │                                                                │
│    ├── writeLock.lock()                                             │
│    ├── sequencer.next()          ←── LocalSequencer (AtomicLong)    │
│    ├── versionCounters[tag]++                                       │
│    ├── new LogEntry(seqnum, version, tags, data, now)               │
│    ├── persistenceAdapter.append(entry)                             │
│    └── notifySubscribers(entry)  ──▶  [virtual thread per listener] │
│                                                                     │
│  read(tag, from, max)                                               │
│    └── persistenceAdapter.readByTag(tag, from.seqnum())             │
│         (lock-free, concurrent)                                     │
│                                                                     │
│  getView(tag) ──▶ DefaultLogView                                    │
│    ├── readNextAfter(minSeqnum)   ← Boki SharedLogReadNext          │
│    ├── readPrevBefore(maxSeqnum)  ← Boki SharedLogReadPrev          │
│    └── checkTail()               ← Boki SharedLogCheckTail          │
└─────────────────────────────────────────────────────────────────────┘
           │                              │
           ▼                              ▼
┌──────────────────────┐      ┌──────────────────────────────────────┐
│  PersistenceAdapter  │      │           ExecutorEngine             │
│  (pluggable)         │      │                                      │
│                      │      │  register(Executor<S>)               │
│  ┌────────────────┐  │      │  start() → one virtual thread each   │
│  │  InMemory      │  │      │                                      │
│  │  (test/dev)    │  │      │  ┌──────────────────────────────┐    │
│  └────────────────┘  │      │  │  ExecutorRunner<S>           │    │
│                      │      │  │                              │    │
│  ┌────────────────┐  │      │  │  1. buildFullState()         │    │
│  │  FileBased     │  │      │  │     readAll(inputTag)        │    │
│  │  (production)  │  │      │  │     fold: apply(state, e)*   │    │
│  │                │  │      │  │                              │    │
│  │  log.dat       │  │      │  │  2. subscribe(inputTag)      │    │
│  │  index.dat     │  │      │  │     park on inbox.poll()     │    │
│  │  trim.dat      │  │      │  │                              │    │
│  └────────────────┘  │      │  │  3. on new entries:          │    │
└──────────────────────┘      │  │     apply(state, entry)*     │    │
                              │  │     execute(state, ctx)      │    │
                              │  │     → List<AppendRequest>    │    │
                              │  │     append each to log       │    │
                              │  └──────────────────────────────┘    │
                              └──────────────────────────────────────┘
```

### Write path

1. Caller invokes `sharedLog.append(request)` — returns a `CompletableFuture`.
2. The async pool submits the work to a virtual-thread executor.
3. Inside the write lock: `sequencer.next()` issues the next seqnum; the per-tag
   `streamVersion` counter increments; a `LogEntry` is constructed.
4. `persistenceAdapter.append(entry)` persists durably (fsync for file adapter).
5. Active subscribers for the entry's tags are notified on individual virtual threads.
6. `AppendResult(seqnum, streamVersion, primaryTag, timestamp)` is returned.

### Read path

Reads are **lock-free**. The persistence adapter's in-memory indices (`ConcurrentSkipListMap`)
serve tag-scoped reads without touching the write lock.

---

## Module map

```
src/main/java/com/cajunsystems/gumbo/
│
├── core/                       Pure value types — no dependencies
│   ├── LogTag.java             Logical stream identifier (namespace + key)
│   ├── LogEntry.java           Immutable log record (seqnum, streamVersion, tags, data)
│   ├── LogPosition.java        Read cursor (wraps seqnum)
│   ├── AppendResult.java       Result of a successful append
│   └── AppendRequest.java      Payload + target tags for an append call
│
├── api/                        Interfaces — depend only on core/
│   ├── SharedLog.java          Main log interface (append/read/subscribe/trim)
│   ├── LogView.java            Tag-scoped read/append window (byte[])
│   ├── TypedLogView.java       Type-safe wrapper over LogView (uses LogSerializer)
│   ├── Executor.java           Stateless executor interface (fold + execute)
│   └── ExecutorContext.java    Runtime context injected into execute()
│
├── serialization/              Pluggable object ↔ byte[] conversion
│   ├── LogSerializer.java      Interface — serialize(T) / deserialize(byte[])
│   └── KryoLogSerializer.java  Kryo 5 implementation with internal Pool<Kryo>
│
├── persistence/                Storage back-ends
│   ├── PersistenceAdapter.java Interface — open/close/append/appendBatch/read/trim
│   ├── InMemoryPersistenceAdapter.java
│   ├── FileBasedPersistenceAdapter.java        WAL + index + trim files; group-commit via appendBatch
│   ├── BatchingPersistenceAdapter.java         Batches writes → one fdatasync per N entries
│   └── FoundationDBPersistenceAdapter.java     FDB-backed; distributed durability, optional dep
│
├── sequencer/                  Sequence-number generation
│   ├── Sequencer.java          Interface — next() / current()
│   ├── LocalSequencer.java     AtomicLong; advanceTo() for post-crash reseeding
│   └── FoundationDBSequencer.java  Distributed; FDB read-modify-write transaction per seqnum
│
└── service/                    Wiring
    ├── SharedLogConfig.java    Builder-pattern configuration
    ├── SharedLogService.java   Main implementation; getTypedView() factory
    ├── DefaultLogView.java     LogView backed by SharedLogService
    ├── DefaultTypedLogView.java TypedLogView backed by LogView + LogSerializer
    └── ExecutorEngine.java     Runs Executor<S> instances on virtual threads
```

---

## Data model

### LogEntry binary format (FileBasedPersistenceAdapter)

```
┌──────────┬──────────┬──────────────┬──────────┬──────────────────────────┬──────────┬───────────┬──────────┐
│ MAGIC    │ seqnum   │ timestamp    │ version  │ tags (variable)          │ dataLen  │ data      │ CRC32    │
│ 4 bytes  │ 8 bytes  │ 8 bytes      │ 8 bytes  │ see below                │ 4 bytes  │ N bytes   │ 4 bytes  │
│ 0xC0FFEE42│ big-end │ millis epoch │ big-end  │                          │ big-end  │           │          │
└──────────┴──────────┴──────────────┴──────────┴──────────────────────────┴──────────┴───────────┴──────────┘

Tag (per-tag block):
  ┌──────────────┬──────────────────┬──────────┬──────────────────┐
  │ nsLen 2 B    │ namespace UTF-8  │ keyLen   │ key UTF-8        │
  └──────────────┴──────────────────┴──────────┴──────────────────┘

Index file (index.dat): [seqnum : 8 bytes][fileOffset : 8 bytes]  × N entries
Trim file  (trim.dat):  [trimSeqnum : 8 bytes]  (single value, atomically replaced)
```

The index file provides O(log n) seek-to-seqnum without scanning the log. On startup,
if the index is absent or truncated the adapter falls back to a full log scan to
rebuild it (same recovery path Boki uses when an engine node restarts cold).

---

## Persistence adapters

### InMemoryPersistenceAdapter

Backed by a `ConcurrentSkipListMap<seqnum, LogEntry>` plus per-tag
`ConcurrentSkipListMap<seqnum, streamVersion>` indices. Fully concurrent reads; writes are
externally serialised by `SharedLogService`'s write lock. All data is lost on JVM
exit. Use for unit tests and local development.

### FileBasedPersistenceAdapter

Append-only binary WAL (`log.dat`) with:

- **Durability**: `FileChannel.force(false)` (fdatasync) after every entry and every
  index record.
- **Global index** (`index.dat`): `[seqnum:8][offset:8]` appended after each write;
  loaded entirely into memory on open for O(log n) random access.
- **Trim** (`trim.dat`): The trim seqnum is written to a temp file and atomically
  renamed, so a crash during trim never corrupts the log.
- **Crash recovery**: On open, loads the index file; if it is missing or not a
  multiple of 16 bytes, falls back to scanning `log.dat` and rebuilding from scratch.
  Per-tag in-memory indices are always rebuilt from the verified log entries.
- **Single-writer, enforced**: `open()` takes an exclusive `FileLock` on `{dataDir}/lock`
  and throws `LogAlreadyOpenException` if another adapter — in this JVM or another
  process — already holds it. See below.

**Why single-writer.** `localId` is assigned from a process-local counter and `index.dat`
is written per process, so two adapters sharing a directory hand out the same ids and the
second to close overwrites the first's view of the log. Both failures are silent: the
entries are on disk but unreadable, and the duplicate ids address two different entries.
The lock turns that into an error at `open()` instead:

```java
var first  = new FileBasedPersistenceAdapter("/var/data");
first.open();

var second = new FileBasedPersistenceAdapter("/var/data");
second.open();   // throws LogAlreadyOpenException
```

The lock is released by `close()` and by process exit (including a crash — the OS drops
it), so a restart reopens the directory normally. A stale `lock` file is never a problem
and never needs removing by hand; it is the OS lock, not the file's existence, that
guards the directory.

### BatchingPersistenceAdapter

Wraps any `PersistenceAdapter` and accumulates writes in memory, flushing to
the delegate as a single `appendBatch()` call.  With `FileBasedPersistenceAdapter`
as the delegate, **N entries cost 2 `fdatasync` calls instead of 2N**:

```java
PersistenceAdapter base     = new FileBasedPersistenceAdapter("/var/data");
PersistenceAdapter batching = BatchingPersistenceAdapter.of(base); // 64 entries / 10 ms defaults

// or with explicit tuning:
PersistenceAdapter batching = new BatchingPersistenceAdapter(base, 128, 5 /* ms */);

SharedLogConfig config = SharedLogConfig.builder()
    .persistenceAdapter(batching)
    .build();
```

| Parameter | Default | Effect |
|---|---|---|
| `maxBatchSize` | 64 | Flush immediately when pending entries reach this count |
| `maxDelayMs` | 10 ms | Background virtual-thread fires and flushes after this interval |

**Durability tradeoff**: entries appended but not yet flushed are in memory only.
A JVM crash within the delay window loses those entries.  For strict durability
keep the default `FileBasedPersistenceAdapter` (1 `fdatasync` per entry) or use
`BatchingPersistenceAdapter` with `maxDelayMs=0` / `maxBatchSize=1`.

See [docs/PERSISTENCE_EVOLUTION.md](docs/PERSISTENCE_EVOLUTION.md) for a full
analysis of the persistence roadmap including memory-mapped files and io_uring.

### FoundationDBPersistenceAdapter

Backed by [FoundationDB](https://www.foundationdb.org/), this adapter replaces the
local WAL with a distributed, replicated key-value store.  It is the recommended
persistence back-end for **multi-node or production-scale deployments**.

#### Subspace layout

```
{root} / "log"  / seqnum                         → entry bytes
{root} / "tag"  / namespace / key / seqnum        → streamVersion (8 bytes)
{root} / "meta" / "latest"                        → latestSeqnum (8 bytes)
{root} / "meta" / "trim"                          → trimSeqnum (8 bytes)
{root} / "meta" / "tagcount" / namespace / key    → versionCount (8 bytes)
```

FDB's tuple-layer key encoding preserves ordering, so range reads over the log
subspace naturally return entries in `seqnum` order without a separate index file.

#### Quick start

```java
// Minimal — uses the default fdb.cluster file
PersistenceAdapter fdb = new FoundationDBPersistenceAdapter();

SharedLogConfig config = SharedLogConfig.builder()
    .persistenceAdapter(fdb)
    .build();
```

#### Production setup — shared connection with FoundationDBSequencer

For true multi-node operation, use `FoundationDBSequencer` alongside the adapter.
Sharing a single `Database` instance avoids opening two connections:

```java
FDB      fdb = FDB.selectAPIVersion(730);
Database db  = fdb.open("/etc/foundationdb/fdb.cluster");

// Both components share the same connection
FoundationDBSequencer        seq     = new FoundationDBSequencer(db, "myapp");
FoundationDBPersistenceAdapter store = new FoundationDBPersistenceAdapter(db, "myapp");
seq.open();
store.open();

SharedLogConfig config = SharedLogConfig.builder()
    .sequencer(store)        // seqnums assigned by FDB — safe across nodes
    .persistenceAdapter(store)
    .build();
```

> **`fdb-java` is an optional Maven dependency.** It pulls in a platform-specific
> native library (`libfdb_c.so` on Linux).  Projects that do not use the FDB
> adapter are not affected.

#### When to use FoundationDB in production

| Scenario | Recommendation |
|---|---|
| Single-node service, moderate write rate | `BatchingPersistenceAdapter` wrapping `FileBasedPersistenceAdapter` — simpler, lower latency |
| Multiple writer processes / nodes sharing one log | **`FoundationDBPersistenceAdapter` + `FoundationDBSequencer`** — only option that is safe |
| Need cross-datacenter replication | **FDB** — configure FDB's built-in DR replication |
| Write latency is the primary concern (< 1 ms P99) | Local file adapter — FDB adds a network round-trip (~1–5 ms) per commit |
| Log data must outlive the JVM process and survive node failure | **FDB** — 3-way replication by default |
| Operational simplicity (no cluster to manage) | Local file adapter |
| FaaS / serverless: any node can be the writer | **FDB** — distributed seqnum assignment is mandatory |

#### Throughput characteristics

| Configuration | Approximate writes/s |
|---|---|
| Single `append()` (one FDB transaction each) | ~200–1 000 |
| `appendBatch(64 entries)` (one FDB transaction) | ~12 000–64 000 |
| `BatchingPersistenceAdapter` wrapping FDB adapter | Same as appendBatch above |

FDB transactions have a **10 MB write limit**; `appendBatch` automatically chunks
batches that would exceed 8 MB.

#### Operational requirements

- FoundationDB cluster running and reachable (default: `/etc/foundationdb/fdb.cluster`)
- `libfdb_c.so` 7.x installed on every JVM host (`apt install foundationdb-clients` on Debian/Ubuntu)
- Java binding: `org.foundationdb:fdb-java:7.3.43` (already in `pom.xml` as optional)

---

### Implementing your own adapter

```java
public class RocksDbPersistenceAdapter implements PersistenceAdapter {
    @Override public void open()   throws IOException { /* ... */ }
    @Override public void close()  throws IOException { /* ... */ }
    @Override public void append(LogEntry entry) throws IOException { /* ... */ }
    @Override public List<LogEntry> readAll() throws IOException { /* ... */ }
    @Override public List<LogEntry> readFrom(long fromSeqnum) throws IOException { /* ... */ }
    @Override public List<LogEntry> readByTag(LogTag tag, long fromSeqnum) throws IOException { /* ... */ }
    @Override public void trim(long upToSeqnum) throws IOException { /* ... */ }
    @Override public long getLatestSeqnum() { /* ... */ }
    @Override public long getLocalIdCountForTag(LogTag tag) { /* ... */ }
}

SharedLogConfig config = SharedLogConfig.builder()
    .persistenceAdapter(new RocksDbPersistenceAdapter(path))
    .build();
```

---

## Sequencers

The `Sequencer` interface has two implementations:

### LocalSequencer

`AtomicLong`-backed. Suitable for any **single-process** deployment. Starts at
`0` by default; call `advanceTo(latestSeqnum + 1)` after a restart to avoid
reusing seqnums.

```java
// SharedLogService does this automatically on startup
sequencer.advanceTo(persistenceAdapter.getLatestSeqnum() + 1);
```

### FoundationDBSequencer

Uses a **read-modify-write FDB transaction** to atomically claim the next seqnum.
FDB's optimistic concurrency control serialises concurrent callers across all nodes
transparently — conflicting transactions are retried by the FDB client.

```java
FoundationDBSequencer seq = new FoundationDBSequencer("/etc/foundationdb/fdb.cluster", "myapp");
seq.open();

// next() — claims one seqnum, blocks for one FDB round-trip (~1–5 ms)
long seqnum = seq.next();

// currentGlobal() — reads the authoritative global counter without claiming
long highWatermark = seq.currentGlobal();
```

**When to use `FoundationDBSequencer`**:

- Multiple writer processes share the same log — `LocalSequencer` would produce
  duplicate seqnums across processes.
- You want the sequencer's counter to survive JVM restarts without manual
  `advanceTo` reseeding (it is stored durably in FDB).
- You are already using `FoundationDBPersistenceAdapter` and want the entire
  write path to be distributed.

**Batch seqnum reservation**: `FoundationDBSequencer` implements
`Sequencer.nextBatch(int count)`, which claims `count` seqnums in a **single**
FDB transaction. `SharedLog.appendBatch(List<AppendRequest>)` uses this
automatically — an N-entry write costs 2 FDB round-trips total (1 seqnum claim
+ 1 data commit) instead of N + 1. This is the same optimisation Boki uses in
its metalog to prevent the sequencer from becoming a throughput bottleneck.

---

## Typed log views (Kryo)

Every method in `SharedLog` and `LogView` works with raw `byte[]`.  A
`TypedLogView<T>` wraps a `LogView` and uses a `LogSerializer<T>` to
transparently handle serialization so call sites never touch `byte[]`:

```java
// 1. Pick a serializer
LogSerializer<OrderEvent> s = new KryoLogSerializer<>(OrderEvent.class);

// 2. Get a typed view
TypedLogView<OrderEvent> view = service.getTypedView(LogTag.of("orders"), s);

// 3. Append domain objects directly — no .getBytes()
view.append(new OrderEvent("ord-42", "placed")).join();

// 4. Read back typed objects — no new String(e.data())
List<OrderEvent> events = view.readAll().join();
```

Subscriptions are also typed:

```java
SharedLog.Subscription sub = view.subscribeTail(event ->
        System.out.println("received: " + event.orderId()));
```

### LogSerializer

`LogSerializer<T>` is a single-responsibility interface with two methods:

```java
public interface LogSerializer<T> {
    byte[] serialize(T value);
    T      deserialize(byte[] data);
}
```

Plug in any format — JSON, Protobuf, Avro — by implementing this interface.
`KryoLogSerializer<T>` ships out of the box:

```java
// Default (no registration required — works for most types and records)
new KryoLogSerializer<>(MyClass.class)

// Pre-registered (smaller wire format, faster)
new KryoLogSerializer<>(MyClass.class, kryo -> {
    kryo.register(MyClass.class, 10);
    kryo.register(MyEnum.class,  11);
})
```

Kryo instances are pooled internally so the serializer is fully thread-safe.

---

## Actor checkpoints

An actor built on gumbo maps to a `LogTag` inbox. After startup it must replay
messages it missed (backlog), then switch seamlessly to live delivery.

Three APIs make this efficient:

| API | What it does | Complexity |
|-----|-------------|------------|
| `logView.getLatestSeqnum()` | Highest seqnum in the tag's log | O(1) |
| `logView.readFrom(position, max)` | Read entries starting at a seqnum | O(K) |
| `logView.subscribeTail(listener)` | Deliver only future entries, no backlog | O(1) |
| `logView.setValue(key, value)` | Persist a durable KV checkpoint | O(1) |
| `logView.getValue(key)` | Read a durable KV checkpoint | O(1) |

### Checkpoint → replay → live

```java
LogView inbox = service.getView(LogTag.of("actor", "orders"));

// 1. Read saved checkpoint (null on first start)
byte[] saved = inbox.getValue("checkpoint").join();
long resumeFrom = saved != null
    ? ByteBuffer.wrap(saved).getLong() + 1  // resume after last processed seqnum
    : 0L;

// 2. Replay the backlog from checkpoint
List<LogEntry> backlog = inbox.readFrom(new LogPosition(resumeFrom), 1000).join();
for (LogEntry e : backlog) {
    process(e);
    inbox.setValue("checkpoint", ByteBuffer.allocate(8).putLong(e.seqnum()).array()).join();
}

// 3. Switch to live delivery (no backlog, no polling)
SharedLog.Subscription sub = inbox.subscribeTail(e -> {
    process(e);
    inbox.setValue("checkpoint", ByteBuffer.allocate(8).putLong(e.seqnum()).array()).join();
});
```

The checkpoint seqnum is stored in the tag's per-actor KV store — durable in
`FileBasedPersistenceAdapter` and `FoundationDBPersistenceAdapter`, in-memory only in
`InMemoryPersistenceAdapter`.

See [`ActorCheckpointExample.java`](src/test/java/com/cajunsystems/gumbo/examples/ActorCheckpointExample.java) for a runnable version.

### Claiming a stream: conditional KV

The same KV also arbitrates *who* is allowed to write a stream. Each mutation below compares
before it writes, and the comparison happens where the write lands rather than in the caller:

| API | What it does |
|-----|-------------|
| `logView.setValueIfAbsent(key, value)` | Claim — of N contenders exactly one gets `true` |
| `logView.compareAndSetValue(key, expected, value)` | Take over from a known holder; `expected == null` requires absence, `value == null` removes the key |
| `logView.deleteValueIf(key, expected)` | Release, but only if still the holder |
| `logView.incrementValue(key, delta)` | Counter; absent reads as `0`, encoded by [`CounterValues`](src/main/java/com/cajunsystems/gumbo/core/CounterValues.java) |

```java
LogView stream = service.getView(LogTag.of("execution", "exec-1"));
byte[] mine = (nodeId + "@" + expiresAt).getBytes();

if (!stream.setValueIfAbsent("owner", mine).join()) {
    byte[] held = stream.getValue("owner").join();
    if (!expired(held) || !stream.compareAndSetValue("owner", held, mine).join()) {
        return;   // someone else owns this stream
    }
}

// Owning the lease is not enough on its own: fence the write on the stream version too.
// The lease says we owned it a moment ago; the fence says we still do where it matters.
service.append(AppendRequest.to(tag, data), expectedVersion).join();
```

That second step is the point. Lease expiry needs a clock, and a clock is the part that can
be wrong — but with the append fenced on the version, clock skew that lets two nodes both
decide a lease is free costs duplicated work rather than a corrupted stream, because only one
of them can commit.

On `FoundationDBPersistenceAdapter` the read, comparison and write are a single transaction,
so a claim holds across processes. `FileBasedPersistenceAdapter` decides it under the
directory lock it already holds, which is atomic within the single writer it enforces. An
adapter that cannot do it atomically throws `UnsupportedOperationException` rather than
silently accepting every contender.

---

## Executor model

Executors are the **stateless workers** of the system. Each executor is associated
with one input `LogTag`. The `ExecutorEngine` drives a continuous cycle:

```
read all entries for inputTag
       │
       ▼
fold: state = executor.initialState()
      for each entry: state = executor.apply(state, entry)
       │
       ▼
execute: List<AppendRequest> = executor.execute(state, context)
       │
       ▼
append each AppendRequest to the shared log
       │
       ▼
wait for next entry (blocking park on virtual thread)
       │
       └──▶ repeat with incremental state update
```

Because state is always derived from the log, executors survive crashes without any
additional checkpointing. Restart the executor, replay the log, and it resumes exactly
where it left off.

### Example: order fulfilment executor

```java
record OrderState(List<Order> pending, List<Order> fulfilled) {}

public class FulfilmentExecutor implements Executor<OrderState> {

    @Override
    public LogTag getInputTag() { return LogTag.of("orders"); }

    @Override
    public OrderState initialState() {
        return new OrderState(List.of(), List.of());
    }

    @Override
    public OrderState apply(OrderState state, LogEntry entry) {
        var event = OrderEvent.parse(entry.data());
        return switch (event.type()) {
            case PLACED    -> state.withPending(append(state.pending(), event.order()));
            case FULFILLED -> state.moveToPending(event.orderId());
            default        -> state;
        };
    }

    @Override
    public List<AppendRequest> execute(OrderState state, ExecutorContext ctx) {
        // Fulfil the first pending order, if any
        return state.pending().stream()
            .limit(1)
            .map(o -> AppendRequest.to(
                Set.of(LogTag.of("orders"), LogTag.of("orders", o.id())),
                OrderEvent.fulfilled(o).toBytes()))
            .toList();
    }
}
```

Register and start:

```java
ExecutorEngine engine = new ExecutorEngine(service);
engine.register(new FulfilmentExecutor());
engine.start();
```

### Example: workflow executor

A workflow executor chains two executors together so that the output of the first
becomes the input of the second, advancing items through a multi-stage pipeline.

**Stage 1** reads `workflow.commands` (entries like `"submit:<id>"`) and emits
`"processing:<id>"` entries to `workflow.events`:

```java
static class WorkflowSubmitExecutor implements Executor<List<String>> {

    private final Set<String> emitted = ConcurrentHashMap.newKeySet();

    @Override public LogTag getInputTag() { return LogTag.of("workflow.commands"); }
    @Override public String getName()     { return "WorkflowSubmit"; }

    @Override public List<String> initialState() { return List.of(); }

    @Override
    public List<String> apply(List<String> pending, LogEntry entry) {
        String msg = new String(entry.data());
        if (msg.startsWith("submit:")) {
            String id = msg.substring("submit:".length());
            List<String> next = new ArrayList<>(pending);
            next.add(id);
            return next;
        }
        return pending;
    }

    @Override
    public List<AppendRequest> execute(List<String> pending, ExecutorContext ctx) {
        return pending.stream()
            .filter(emitted::add)           // idempotency: only emit each id once
            .map(id -> AppendRequest.to(
                LogTag.of("workflow.events"),
                ("processing:" + id).getBytes()))
            .toList();
    }
}
```

**Stage 2** reads `workflow.events`, picks up items in `processing` state, and
emits `"complete:<id>"` back to the same tag:

```java
static class WorkflowProcessExecutor implements Executor<List<String>> {

    private final Set<String> emitted = ConcurrentHashMap.newKeySet();

    @Override public LogTag getInputTag() { return LogTag.of("workflow.events"); }
    @Override public String getName()     { return "WorkflowProcess"; }

    @Override public List<String> initialState() { return List.of(); }

    @Override
    public List<String> apply(List<String> processing, LogEntry entry) {
        String msg = new String(entry.data());
        if (msg.startsWith("processing:")) {
            String id = msg.substring("processing:".length());
            List<String> next = new ArrayList<>(processing);
            next.add(id);
            return next;
        }
        return processing;
    }

    @Override
    public List<AppendRequest> execute(List<String> processing, ExecutorContext ctx) {
        return processing.stream()
            .filter(emitted::add)
            .map(id -> AppendRequest.to(
                LogTag.of("workflow.events"),
                ("complete:" + id).getBytes()))
            .toList();
    }
}
```

Register both executors with a single engine — they run concurrently on separate
virtual threads:

```java
ExecutorEngine engine = new ExecutorEngine(service);
engine.register(new WorkflowSubmitExecutor());
engine.register(new WorkflowProcessExecutor());
engine.start();

// Trigger the pipeline
service.append(AppendRequest.to(LogTag.of("workflow.commands"), "submit:order-99".getBytes())).join();
// → WorkflowSubmitExecutor emits "processing:order-99" to workflow.events
// → WorkflowProcessExecutor picks it up and emits "complete:order-99"
```

### Cross-tag fan-out

Executors can read and write other tags via `ExecutorContext.openView(tag)`:

```java
@Override
public List<AppendRequest> execute(OrderState state, ExecutorContext ctx) {
    LogView auditLog = ctx.openView(LogTag.of("audit"));
    auditLog.append(("processed " + state.pending().size() + " orders").getBytes()).join();
    return List.of();
}
```

---

## Virtual threads

The `ExecutorEngine` spawns one `Thread.ofVirtual()` per registered executor. Each
runner parks on `LinkedBlockingQueue.poll(500ms, MILLISECONDS)` while waiting for new
entries — a blocking call that costs nothing on a virtual thread because the JVM
unmounts it from its carrier thread while parked.

```java
// From ExecutorEngine — each runner is one virtual thread
Thread.ofVirtual()
    .name("executor-" + executor.getName())
    .start(runner::run);
```

Subscription delivery (both backlog replay and live notifications) also uses
`Thread.ofVirtual()`, so slow consumers never block the write path or each other.

The default async thread pool in `SharedLogConfig` is:

```java
Executors.newVirtualThreadPerTaskExecutor()
```

All `CompletableFuture`-returning methods in `SharedLogService` dispatch onto this
pool, meaning `append().join()` inside an executor's `execute()` method is safe and
does not risk deadlock or thread starvation.

---

## Examples

Runnable end-to-end examples live in
[`src/test/java/com/cajunsystems/gumbo/examples/`](src/test/java/com/cajunsystems/gumbo/examples/).
They are compiled and executed as part of `mvn verify`, so they always stay in
sync with the library.

| Example | What it demonstrates |
|---|---|
| [`QuickStartExample.java`](src/test/java/com/cajunsystems/gumbo/examples/QuickStartExample.java) | Append, read, `readFromPosition`, subscribe, multi-tag entries, `LogView` |
| [`OrderFulfilmentExample.java`](src/test/java/com/cajunsystems/gumbo/examples/OrderFulfilmentExample.java) | Stateless executor: backlog replay on startup, then incremental processing |
| [`FilePersistedExample.java`](src/test/java/com/cajunsystems/gumbo/examples/FilePersistedExample.java) | File-backed WAL: durability, crash recovery, and sequencer reseeding across restarts |
| [`WorkflowExecutorExample.java`](src/test/java/com/cajunsystems/gumbo/examples/WorkflowExecutorExample.java) | Multi-stage workflow: chained executors advance items through SUBMITTED → PROCESSING → COMPLETE states |
| [`ActorCheckpointExample.java`](src/test/java/com/cajunsystems/gumbo/examples/ActorCheckpointExample.java) | Actor checkpoint pattern: resume from KV checkpoint, replay backlog, switch to live via `subscribeTail()` |

Run a single example directly:

```bash
mvn test -Dtest=QuickStartExample
mvn test -Dtest=OrderFulfilmentExample
mvn test -Dtest=FilePersistedExample
mvn test -Dtest=WorkflowExecutorExample
```

---

## Building

Requires Java 21 and Maven 3.9+.

```bash
mvn verify          # compile + test
mvn package         # produce gumbo-0.4.0.jar
```

GitHub Actions runs `mvn verify` on every push using Java 21 (Temurin).
Test reports are uploaded as workflow artifacts.

---

## Further reading

- [Failure semantics](docs/FAILURE_SEMANTICS.md) — what each mutating operation leaves behind when it fails
- [Open work](docs/OPEN-WORK.md) — the remaining backlog, with evidence

- **[Boki Comparison](docs/BOKI_COMPARISON.md)** — How Gumbo relates to and differs from Boki
- [Boki paper — SOSP 2021](https://www.cs.utexas.edu/~witchel/pubs/jia21sosp-boki.pdf)
- [Boki source code](https://github.com/ut-osa/boki)
- [Halfmoon — SOSP 2023, extends Boki with cross-log transactions](https://xinjin.github.io/files/SOSP23_Halfmoon.pdf)
- [Persistence Evolution](docs/PERSISTENCE_EVOLUTION.md) — Roadmap for persistence optimizations
