# Gumbo and Boki: Inspiration and Differences

This document explains how Gumbo is inspired by Boki and where the two systems differ.

---

## What is Boki?

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

---

## What Gumbo takes from Boki

| Boki concept | Gumbo |
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

---

## Key differences between Gumbo and Boki

### 1. Deployment model

**Boki** is a distributed system with:
- Engine nodes (run functions)
- Storage nodes (durable WAL)
- Sequencer nodes (assign sequence numbers)
- ZooKeeper-backed view management
- io_uring async I/O throughout

**Gumbo** takes the core insight — stateless executors + shared log — and delivers it in two
deployment tiers:

**Single-node / local** (default): `LocalSequencer` (AtomicLong) +
`FileBasedPersistenceAdapter` (local WAL). Zero external dependencies, sub-millisecond
append latency on NVMe.

**Multi-node / production** (recommended): `FoundationDBSequencer` +
`FoundationDBPersistenceAdapter`. FoundationDB handles replication, crash recovery,
and distributed sequence assignment — the same role FDB plays in Boki's metalog.

The `Sequencer` and `PersistenceAdapter` interfaces are the seams; switching tiers
is a one-line configuration change.

### 2. Language and runtime

- **Boki**: C++, custom runtime with io_uring
- **Gumbo**: Java 21, leverages virtual threads for lightweight concurrency

### 3. Tag representation

- **Boki**: Tags are raw `uint64` values
- **Gumbo**: Tags are typed objects with `namespace` and optional `key` fields (`LogTag.of("orders", "order-42")`)

### 4. Sequence number encoding

- **Boki**: `seqnum = [logspace_id:32 | position:32]` encodes the physical log shard
- **Gumbo**: Plain `long` from an `AtomicLong`; distributed implementations could encode node/term in high bits

### 5. Batch optimization

**Boki**: Batch-reservation optimization in metalog reduces sequencer round-trips

**Gumbo**: `SharedLog.appendBatch(requests)` claims all seqnums in a single FDB transaction
(when using `FoundationDBSequencer`), reducing sequencer round-trips from N to 1
per N-entry batch — the same optimization Boki uses.

### 6. Serialization

- **Boki**: Works with raw bytes
- **Gumbo**: Provides `TypedLogView<T>` with pluggable `LogSerializer<T>` (Kryo implementation included) for type-safe operations

### 7. Executor model

- **Boki**: FaaS functions are invoked on demand
- **Gumbo**: `Executor<S>` pattern with continuous fold-execute cycle, running on virtual threads

### 8. Persistence options

**Boki**: Custom distributed storage layer

**Gumbo**: Pluggable persistence adapters:
- `InMemoryPersistenceAdapter` (testing)
- `FileBasedPersistenceAdapter` (single-node production)
- `BatchingPersistenceAdapter` (wraps any adapter with group commit)
- `FoundationDBPersistenceAdapter` (multi-node production)

### 9. Operational complexity

- **Boki**: Requires managing multiple node types, ZooKeeper, and custom infrastructure
- **Gumbo**: Single-node mode has zero dependencies; multi-node mode only requires FoundationDB cluster

---

## When to use Gumbo vs Boki

### Use Gumbo when:

- You want a shared log library for Java applications
- You need flexible deployment (single-node to multi-node)
- You want to leverage Java 21 virtual threads
- You prefer pluggable persistence backends
- You need type-safe log operations
- You want simpler operational overhead

### Use Boki when:

- You're building a FaaS platform from scratch
- You need the absolute highest throughput (Boki's C++ + io_uring implementation)
- You're doing research on distributed systems
- You need the full distributed architecture out of the box

---

## Further reading

- [Boki paper — SOSP 2021](https://www.cs.utexas.edu/~witchel/pubs/jia21sosp-boki.pdf)
- [Boki source code](https://github.com/ut-osa/boki)
- [Halfmoon — SOSP 2023, extends Boki with cross-log transactions](https://xinjin.github.io/files/SOSP23_Halfmoon.pdf)
