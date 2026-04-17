# Roadmap: Gumbo — Actor System Support Features

## Overview

Four library enhancements to support an actor system built on gumbo's shared-log model. Each actor maps to a `LogTag` inbox within the global log. The must-haves (`readFrom`, `latestSeqnum`) unblock efficient actor replay from checkpoints. The nice-to-haves (`push-tail subscribe`, `key-value store`) clean up the live+replay handoff and eliminate checkpoint noise. All features are additive and implemented across all four persistence adapters.

## Domain Expertise

None

## Phases

- [ ] **Phase 1: Efficient readFrom** — Fix `readByTag()` in all adapters for O(from-N) positional reads
- [ ] **Phase 2: Per-tag latestSeqnum** — Add/fix `latestSeqnum()` on `LogView` using maintained metadata, not readAll()
- [ ] **Phase 3: Push-tail subscribe** — Add tail-only subscription mode (live entries only, no backlog)
- [ ] **Phase 4: Key-value API design** — Design `setValue`/`getValue` interface with persistence semantics
- [ ] **Phase 5: Key-value implementation** — Implement key-value across all 4 adapters
- [ ] **Phase 6: TypedLogView + integration** — Surface new APIs through `TypedLogView<T>`, update docs and examples

## Phase Details

### Phase 1: Efficient readFrom
**Goal**: Fix `readByTag(tag, fromSeqnum)` in all 4 adapters to use index-based seek; actor replay from a checkpoint seqnum must be O(from-N) not O(total log size)
**Depends on**: Nothing (first phase)
**Research**: Unlikely (internal optimization of existing adapter code using existing indices)
**Plans**: TBD

Plans:
- [x] 01-01: Fix InMemory readByTag linear scan → tailMap; add gap-seqnum regression tests (ae190ab, 3e939a9)
- [ ] 01-02: Verify FileBased/FDB/Batching readByTag correctness end-to-end; close out Phase 1

### Phase 2: Per-tag latestSeqnum
**Goal**: `logView.latestSeqnum()` returns the latest seqnum for a tag in O(1)/O(log n) without materializing entries; uses maintained metadata
**Depends on**: Phase 1
**Research**: Unlikely (internal; pattern follows existing per-tag localId counter maintenance)
**Plans**: TBD

Plans:
- [x] 02-01: Add getLatestSeqnumForTag() to interface; implement in InMemory/FileBased; fix DefaultLogView (d913dbe, 574c9b1)
- [x] 02-02: Implement in Batching/FDB adapters; subscribeTail e2e test confirming future-only delivery (275fe1b, bfc22bf)

### Phase 3: Push-tail subscribe
**Goal**: `logView.subscribe(listener)` from `TAIL` delivers only live (future) entries with no backlog; enables catching-up → live handoff without polling
**Depends on**: Phase 2
**Research**: Unlikely (builds on existing virtual-thread subscription infrastructure)
**Plans**: TBD

Plans:
- [x] 03-01: Close-out — subscribeTail() was pre-existing and fully correct; zero implementation needed (cbd7d3a)

### Phase 4: Key-value API design
**Goal**: Design the `setValue(key, value)` / `getValue(key)` interface — decide scope (global vs. per-tag), persistence semantics (in-log entry vs. sidecar structure), durability guarantees per adapter
**Depends on**: Phase 3
**Research**: Likely (design decision with persistence implications; trade-offs differ across adapters)
**Research topics**: In-log KV (append sentinel entries) vs. sidecar (separate file/FDB subspace); per-tag vs. global scope; read-your-writes guarantee under batching; FDB key-value atomic ops

Plans:
- [x] 04-01: KV API on PersistenceAdapter + LogView + DefaultLogView; UnsupportedOperation defaults (da654ef, c9bd6df)

### Phase 5: Key-value implementation
**Goal**: Implement `setValue`/`getValue` across all 4 adapters with appropriate durability; match guarantees documented in Phase 4
**Depends on**: Phase 4
**Research**: Unlikely (implementation follows confirmed design from Phase 4)
**Plans**: TBD

Plans:
- [ ] 05-01: Implement key-value in InMemory and FileBased adapters
- [ ] 05-02: Implement in Batching and FoundationDB adapters; durability and recovery tests

### Phase 6: TypedLogView + integration
**Goal**: Surface `readFrom`, `latestSeqnum`, push-tail subscribe, and key-value through `TypedLogView<T>`; update Javadoc, examples, and README
**Depends on**: Phase 5
**Research**: Unlikely (internal propagation and documentation)
**Plans**: TBD

Plans:
- [ ] 06-01: Propagate new APIs through `TypedLogView<T>` interface and `DefaultTypedLogView`
- [ ] 06-02: Update examples (OrderFulfilmentExample, WorkflowExecutorExample) to show actor replay pattern; update README

## Progress

| Phase | Plans Complete | Status | Completed |
|-------|----------------|--------|-----------|
| 1. Efficient readFrom | 1/2 | In progress | — |
| 2. Per-tag latestSeqnum | 2/2 | Complete | 2026-04-17 |
| 3. Push-tail subscribe | 1/1 | Complete | 2026-04-17 |
| 4. Key-value API design | 1/1 | Complete | 2026-04-17 |
| 5. Key-value implementation | 0/2 | Not started | — |
| 6. TypedLogView + integration | 0/2 | Not started | — |
