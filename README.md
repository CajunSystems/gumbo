# Gumbo

A Boki-inspired shared log library for Java 21. A single append-only, totally-ordered
log acts as the source of truth for an entire system. Stateless **executors** derive
all their state by replaying the log, process it, and write new entries back — making
every component crash-safe and independently restartable with no coordination.

---

## Table of contents

1. [Inspiration: Boki](#inspiration-boki)
2. [Core concepts](#core-concepts)
3. [Architecture](#architecture)
4. [Module map](#module-map)
5. [Data model](#data-model)
6. [Persistence adapters](#persistence-adapters)
7. [Sequencers](#sequencers)
8. [Typed log views (Kryo)](#typed-log-views-kryo)
9. [Executor model](#executor-model)
10. [Virtual threads](#virtual-threads)
11. [Quick start](#quick-start)
12. [Examples](#examples)
13. [Building](#building)

---

## Inspiration: Boki

[Boki](https://github.com/ut-osa/boki) (SOSP 2021, UT Austin) is a research FaaS
runtime that exposes a **shared log** as a first-class primitive. The central insight
is simple but powerful:

> If every function reads its state from a log and writes new facts back to the same
> log, then functions become truly stateless. Any invocation — on any node, after any
> crash — can replay the log and recover exactly where it left off.

Boki implements this as a distributed, sharded log with a lightweight **sequencer**
that assigns global sequence numbers via *metalog* entries (tiny cut vectors) without
touching data. Storage nodes receive raw bytes directly from engine nodes in parallel,
so throughput scales without the sequencer becoming a bottleneck. Each log entry
carries one or more **tags** (`uint64`) that act as virtual log-stream identifiers;
a single physical log therefore serves many logical streams simultaneously.

### What this library takes from Boki

| Boki concept | This library |
|---|---|
| Global `seqnum` (64-bit monotonic) | `LogEntry.seqnum()` — assigned by `Sequencer` |
| Per-engine `localid` | `LogEntry.localId()` — per-tag counter assigned at append time |
| `user_tags: repeated uint64` | `LogTag(namespace, key)` — typed tag objects instead of raw integers |
| `user_logspace` (application namespace) | `LogTag.namespace` |
| Per-object tag (e.g. `objectLogTag(hash)`) | `LogTag.of("orders", "order-42")` |
| `SharedLogReadNext(tag, minSeqnum)` | `DefaultLogView.readNextAfter(minSeqnum)` |
| `SharedLogReadPrev(tag, maxSeqnum)` | `DefaultLogView.readPrevBefore(maxSeqnum)` |
| `SharedLogCheckTail(tag)` | `DefaultLogView.checkTail()` |
| Stateless function → rebuild state from log | `Executor<S>` functional fold |
| Sequencer metalog (drives ordering) | `LocalSequencer` (single-node AtomicLong) or `FoundationDBSequencer` (distributed) |
| Storage node (durable WAL) | `FileBasedPersistenceAdapter` (local) or `FoundationDBPersistenceAdapter` (distributed) |
| Pluggable back-ends | `PersistenceAdapter` interface |
| FDB metalog consensus | `FoundationDBSequencer` — read-modify-write transaction over a single FDB counter key; OCC handles multi-node contention |

### What this library takes from Boki (and where it differs)

Boki is a distributed system with engine nodes, storage nodes, sequencer nodes,
ZooKeeper-backed view management, and io_uring async I/O throughout. This library
takes the core insight — stateless executors + shared log — and delivers it in two
deployment tiers:

**Single-node / local** (default): `LocalSequencer` (AtomicLong) +
`FileBasedPersistenceAdapter` (local WAL). Zero external dependencies, sub-millisecond
append latency on NVMe.

**Multi-node / production** (recommended): `FoundationDBSequencer` +
`FoundationDBPersistenceAdapter`. FoundationDB handles replication, crash recovery,
and distributed sequence assignment — the same role FDB plays in Boki's metalog.
`SharedLog.appendBatch(requests)` claims all seqnums in a single FDB transaction
(Boki's batch-reservation optimisation), reducing sequencer round-trips from N to 1
per N-entry batch.

The `Sequencer` and `PersistenceAdapter` interfaces are the seams; switching tiers
is a one-line configuration change.

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

### Sequence numbers and local IDs

Every entry has two identifiers:

- **`seqnum`** — globally unique, monotonically increasing across all tags. Provides
  total ordering. Assigned by the `Sequencer`.
- **`localId`** — monotonically increasing *within* a tag's stream. Useful as a
  per-entity cursor (equivalent to Boki's `localid`).

In Boki, `seqnum = [logspace_id:32 | position:32]` encodes the physical log shard.
Here it is a plain `long` from an `AtomicLong`; a distributed implementation would
encode the node and term in the high bits.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                        SharedLogService                             │
│                                                                     │
│  append(AppendRequest)                                              │
│    │                                                                │
│    ├── writeLock.lock()                                             │
│    ├── sequencer.next()          ←── LocalSequencer (AtomicLong)   │
│    ├── localIdCounters[tag]++                                       │
│    ├── new LogEntry(seqnum, localId, tags, data, now)               │
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
│    └── checkTail()               ← Boki SharedLogCheckTail         │
└─────────────────────────────────────────────────────────────────────┘
           │                              │
           ▼                              ▼
┌──────────────────────┐      ┌──────────────────────────────────────┐
│  PersistenceAdapter  │      │           ExecutorEngine             │
│  (pluggable)         │      │                                      │
│                      │      │  register(Executor<S>)               │
│  ┌────────────────┐  │      │  start() → one virtual thread each   │
│  │  InMemory      │  │      │                                      │
│  │  (test/dev)    │  │      │  ┌──────────────────────────────┐   │
│  └────────────────┘  │      │  │  ExecutorRunner<S>           │   │
│                      │      │  │                              │   │
│  ┌────────────────┐  │      │  │  1. buildFullState()         │   │
│  │  FileBased     │  │      │  │     readAll(inputTag)        │   │
│  │  (production)  │  │      │  │     fold: apply(state, e)*   │   │
│  │                │  │      │  │                              │   │
│  │  log.dat       │  │      │  │  2. subscribe(inputTag)      │   │
│  │  index.dat     │  │      │  │     park on inbox.poll()     │   │
│  │  trim.dat      │  │      │  │                              │   │
│  └────────────────┘  │      │  │  3. on new entries:          │   │
└──────────────────────┘      │  │     apply(state, entry)*     │   │
                              │  │     execute(state, ctx)      │   │
                              │  │     → List<AppendRequest>    │   │
                              │  │     append each to log       │   │
                              │  └──────────────────────────────┘   │
                              └──────────────────────────────────────┘
```

### Write path

1. Caller invokes `sharedLog.append(request)` — returns a `CompletableFuture`.
2. The async pool submits the work to a virtual-thread executor.
3. Inside the write lock: `sequencer.next()` issues the next seqnum; the per-tag
   `localId` counter increments; a `LogEntry` is constructed.
4. `persistenceAdapter.append(entry)` persists durably (fsync for file adapter).
5. Active subscribers for the entry's tags are notified on individual virtual threads.
6. `AppendResult(seqnum, localId, primaryTag, timestamp)` is returned.

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
│   ├── LogEntry.java           Immutable log record (seqnum, localId, tags, data)
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
│ MAGIC    │ seqnum   │ timestamp    │ localId  │ tags (variable)          │ dataLen  │ data      │ CRC32    │
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
`ConcurrentSkipListMap<seqnum, localId>` indices. Fully concurrent reads; writes are
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
{root} / "tag"  / namespace / key / seqnum        → localId (8 bytes)
{root} / "meta" / "latest"                        → latestSeqnum (8 bytes)
{root} / "meta" / "trim"                          → trimSeqnum (8 bytes)
{root} / "meta" / "tagcount" / namespace / key    → localIdCount (8 bytes)
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
    System.out.println("seqnum=" + r.seqnum() + " localId=" + r.localId());

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
mvn package         # produce sharedlog-1.0.0-SNAPSHOT.jar
```

GitHub Actions runs `mvn verify` on every push using Java 21 (Temurin).
Test reports are uploaded as workflow artifacts.

---

## Further reading

- [Boki paper — SOSP 2021](https://www.cs.utexas.edu/~witchel/pubs/jia21sosp-boki.pdf)
- [Boki source code](https://github.com/ut-osa/boki)
- [Halfmoon — SOSP 2023, extends Boki with cross-log transactions](https://xinjin.github.io/files/SOSP23_Halfmoon.pdf)
