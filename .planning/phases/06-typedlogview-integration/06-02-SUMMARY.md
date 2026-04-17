# Phase 6 Plan 2: Examples + README Summary

**Created `ActorCheckpointExample.java` demonstrating the full checkpoint → replay → live handoff pattern, and added an "Actor checkpoints" README section documenting all 5 new APIs; full test suite passes (129 tests, 0 failures).**

## Accomplishments

- `ActorCheckpointExample.java`: runnable JUnit 5 test demonstrating:
  1. Pre-populate 3 messages
  2. Persist checkpoint after msg-1 using `setValue()`
  3. On "restart": read checkpoint via `getValue()`, replay msg-2 + msg-3 via `readFrom(checkpoint+1, 100)`
  4. Switch to live via `subscribeTail()`, receive msg-4 + msg-5 only
  - Deviations from plan template: uses `SharedLogService.open(config)` static factory (not constructor); reads actual seqnum from first entry rather than hardcoding `0L` (makes test robust to global sequencer state); uses `e.data()` instead of `dataUnsafe()`.
- README "Actor checkpoints" section inserted after "Typed log views (Kryo)": API comparison table + working Java code snippet + link to the example.
- README Examples table expanded from 4 → 5 entries.
- Full `mvn test` passes: 129 tests, 0 failures, 18 skipped (FDB — no cluster available).

## Files Created/Modified

- `src/test/java/com/cajunsystems/gumbo/examples/ActorCheckpointExample.java` — new runnable example
- `README.md` — new "Actor checkpoints" section + 5th example table entry

## Decisions Made

Used actual seqnum from first entry (not hardcoded `0L`) in the example to be robust against global sequencer state where first entry may not have seqnum=0.

## Issues Encountered

None.

## Git Commits

- `93b0467` — `feat(06-02): add ActorCheckpointExample demonstrating checkpoint-replay-live pattern`
- `0a5f3a2` — `docs(06-02): add actor checkpoint documentation and example to README`

## Next Step

Phase 6 complete. All 6 phases done. Project objective achieved:
- Phase 1: Efficient readFrom (O(log N) positional seeks)
- Phase 2: Per-tag latestSeqnum (O(1) across all 4 adapters)
- Phase 3: Push-tail subscribe (pre-existing, verified)
- Phase 4: KV API design (sidecar per-tag approach)
- Phase 5: KV implementation (all 4 adapters)
- Phase 6: TypedLogView integration + actor checkpoint example + README
