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
- [ ] 01-01: Audit current readByTag/readFrom implementation in all adapters; implement O(from-N) in InMemory and FileBased
- [ ] 01-02: Implement O(from-N) in Batching and FoundationDB adapters; add correctness + performance tests

### Phase 2: Per-tag latestSeqnum
**Goal**: `logView.latestSeqnum()` returns the latest seqnum for a tag in O(1)/O(log n) without materializing entries; uses maintained metadata
**Depends on**: Phase 1
**Research**: Unlikely (internal; pattern follows existing per-tag localId counter maintenance)
**Plans**: TBD

Plans:
- [ ] 02-01: Add `latestSeqnum()` to `LogView` interface; implement in InMemory and FileBased adapters
- [ ] 02-02: Implement in Batching and FoundationDB adapters; test that no readAll() is triggered

### Phase 3: Push-tail subscribe
**Goal**: `logView.subscribe(listener)` from `TAIL` delivers only live (future) entries with no backlog; enables catching-up → live handoff without polling
**Depends on**: Phase 2
**Research**: Unlikely (builds on existing virtual-thread subscription infrastructure)
**Plans**: TBD

Plans:
- [ ] 03-01: Add tail-subscribe mode to `LogView` interface; implement in `SharedLogService` subscription infrastructure
- [ ] 03-02: Validate across all adapters; test live+replay handoff, no spurious backlog delivery

### Phase 4: Key-value API design
**Goal**: Design the `setValue(key, value)` / `getValue(key)` interface — decide scope (global vs. per-tag), persistence semantics (in-log entry vs. sidecar structure), durability guarantees per adapter
**Depends on**: Phase 3
**Research**: Likely (design decision with persistence implications; trade-offs differ across adapters)
**Research topics**: In-log KV (append sentinel entries) vs. sidecar (separate file/FDB subspace); per-tag vs. global scope; read-your-writes guarantee under batching; FDB key-value atomic ops

Plans:
- [ ] 04-01: Investigate persistence options; document trade-offs; propose and confirm interface design

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
| 1. Efficient readFrom | 0/2 | Not started | — |
| 2. Per-tag latestSeqnum | 0/2 | Not started | — |
| 3. Push-tail subscribe | 0/2 | Not started | — |
| 4. Key-value API design | 0/1 | Not started | — |
| 5. Key-value implementation | 0/2 | Not started | — |
| 6. TypedLogView + integration | 0/2 | Not started | — |
