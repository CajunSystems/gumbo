# Gumbo — Actor System Support Features

## Summary
Gumbo library enhancements to support an actor system where each actor has a dedicated `LogTag` inbox within a shared global log. Actors need efficient positional reads (`readFrom`), instant offset queries (`latestSeqnum`), and optionally a push-tail subscribe and mutable key-value store for durable offset tracking.

The core constraint: new features must be **additive** (no breaking API changes) and **performance-correct** — `readFrom(n)` must scan from `n`, not from the beginning.

---

## Requirements

### Validated

- ✓ Append-only shared log with global seqnum ordering — existing
- ✓ Per-tag `LogView` scoped reads and subscriptions — existing
- ✓ `TypedLogView<T>` for type-safe log access — existing
- ✓ Pluggable `PersistenceAdapter` (InMemory, FileBased, Batching, FoundationDB) — existing
- ✓ Pluggable `Sequencer` (Local, FoundationDB) — existing
- ✓ Stateless `Executor<S>` pattern with log replay — existing
- ✓ Virtual-thread-based subscriptions with backlog delivery — existing
- ✓ Batch append with group-commit optimization — existing

### Active

- [ ] `logView.readFrom(long startSeqnum)` — reads tag entries starting at or after `startSeqnum`; must be O(from-N) not O(total); implemented across all adapters
- [ ] `logView.latestSeqnum()` — returns the latest seqnum for a tag without reading all entries; instant O(1) or O(log n) query
- [ ] `logView.subscribe(Consumer<LogEntry>)` push-tail — push-based subscription starting from tail (live entries only, no backlog); enables live+replay handoff without polling
- [ ] Mutable key-value store on `SharedLog` or `LogView` — `setValue(key, value)` / `getValue(key)` for durable offset tracking; eliminates append-per-offset noise

### Out of Scope

- Actor system framework code — this project is gumbo library enhancements only; actor system is a separate project
- New persistence backends — no new adapters; enhancements must work across existing four
- Breaking changes to `SharedLog`, `LogView`, `TypedLogView`, or `PersistenceAdapter` interfaces — additive only

---

## Key Decisions

| Decision | Rationale | Outcome |
|----------|-----------|---------|
| Additive API only | Actor system depends on existing gumbo API; breaking changes would require coordinated updates | — Pending |
| readFrom must be O(from-N) | Actor replay from a checkpoint must not degrade with log size; in-memory index must be used, not linear scan | — Pending |
| Must-haves ship first | `readFrom` + `latestSeqnum` unblock Phase 14 actor replay; nice-to-haves follow | — Pending |
| Implement across all 4 adapters | Actor system must work with any persistence backend; no partial implementations | — Pending |

---

## Context

**Use case**: Actor system where each actor maps to a `LogTag` inbox. On restart, actors replay their log from a saved checkpoint seqnum — today this requires reading the full log and filtering, which is O(total). With `readFrom(checkpointSeqnum)`, replay is O(from-checkpoint).

**`latestSeqnum()` use case**: Actor system equivalent of Kafka's `topic.latestOffset()` — needed to detect how far behind an actor is without materializing all entries.

**Push-tail subscribe**: Enables a catching-up subscriber to stream backlog, then seamlessly transition to live entries without a poll loop.

**Mutable key-value**: Durable storage for per-actor offsets/checkpoints. Current workaround appends a new log entry per offset update — works but creates noise in the log.

---

*Last updated: 2026-04-17 after initialization*
