# Architecture

## What Is This System?
Gumbo is a **shared-log library** inspired by Boki (SOSP 2021). The core insight: make a single append-only, totally-ordered log the source of truth. Stateless **executors** derive state by replaying the log, then write new entries back. This eliminates complex state management — any component can restart and replay from scratch.

## Three-Tier Abstraction

1. **Shared Log** (`SharedLog`) — central append-only ledger; concurrent readers, serialized writes
2. **Log Views** (`LogView`) — tag-scoped windows that filter entries and allow append-back
3. **Executors** (`Executor<S>`) — stateless workers that fold log entries into state, then emit new entries

## Key Abstractions

| Abstraction | Role |
|-------------|------|
| `LogEntry` | Immutable record: seqnum, localId, tags, data[], timestamp |
| `LogTag` | Stream identifier: namespace + optional key |
| `AppendRequest/Result` | Write request/response |
| `LogPosition` | Read cursor (seqnum); constants `BEGINNING`, `END` |
| `Sequencer` | Assigns monotonic sequence numbers |
| `PersistenceAdapter` | Pluggable storage backend |
| `LogSerializer<T>` | Type-safe byte[] conversion |
| `ExecutorEngine` | Orchestrates executor virtual threads |
| `SharedLogService` | Main `SharedLog` implementation |

## Design Patterns
- **Event Sourcing** — all state changes are immutable log entries; state derived by replay
- **Functional Fold** — `initialState()` + `apply(state, entry)` chain
- **Adapter Pattern** — swap persistence and sequencer implementations without changing consumers
- **Builder Pattern** — `SharedLogConfig.Builder` configures all components
- **Decorator Pattern** — `BatchingPersistenceAdapter` wraps any `PersistenceAdapter`
- **Tag-Based Filtering** — entries carry multiple tags; reads are tag-scoped projections
- **Batch Optimization** — `Sequencer.nextBatch(n)` claims N seqnums in one atomic operation

## Write Path
```
Client → append(AppendRequest)
  → CompletableFuture (async pool)
  → acquire writeLock
  → Sequencer.next() → seqnum
  → increment per-tag localId counter
  → construct LogEntry(seqnum, localId, tags, data, now)
  → PersistenceAdapter.append(entry) [fsync]
  → release writeLock
  → notify subscribers on individual virtual threads
  → return AppendResult(seqnum, localId, primaryTag, timestamp)
```

## Read Path
```
Client → read(tag, from, maxEntries)
  → CompletableFuture (async pool)
  → PersistenceAdapter.readByTag(tag, fromSeqnum) [lock-free]
  → in-memory skip-list index lookup O(log n)
  → return filtered entries in seqnum order
```

## Executor Cycle (virtual thread)
```
1. Full state rebuild: read(tag) → fold all entries via apply()
2. Subscribe to new entries for tag
3. Loop:
   a. Park on inbox.poll() [cost-free on virtual thread]
   b. New entry → apply() to cached state
   c. execute(state, ctx) → List<AppendRequest>
   d. Append each request (flows back through log)
   e. Advance checkpoint seqnum
```

## Key Architectural Decisions

| Decision | Rationale |
|----------|-----------|
| Seqnum is global, localId is per-tag | Total ordering + per-entity cursors (Boki's insight) |
| Write lock is minimal scope | Only held during seqnum assignment + persist; reads are lock-free |
| Virtual threads for all async work | Blocking I/O is safe; no thread pool starvation |
| Pluggable persistence + sequencer | Same codebase for single-node and distributed deployments |
| Stateless executors with log replay | No separate checkpoint store; restart = replay |
| Batch seqnum reservation | Reduces distributed sequencer RTTs from O(N) to O(1) |

## Package Responsibility Map

```
core/        → Pure data types (no logic, no dependencies)
api/         → Interfaces (depend only on core/)
persistence/ → Storage backend implementations
sequencer/   → Sequence number generation
serialization/ → Object ↔ byte[] conversion
service/     → Wiring, orchestration, main implementations
```
