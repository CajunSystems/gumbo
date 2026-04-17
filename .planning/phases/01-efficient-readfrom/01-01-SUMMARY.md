# Phase 1 Plan 1: Efficient readFrom Summary

**Fixed `InMemoryPersistenceAdapter.readByTag()` from O(N) linear scan to O(log N) skip-list seek via `tailMap`, corrected stale Javadoc, and added gap-seqnum regression tests to both `InMemoryPersistenceAdapterTest` and `BatchingPersistenceAdapterTest`.**

## Accomplishments

- Replaced the O(N) `for`-loop-with-if-filter in `readByTag()` with an `idx.tailMap(fromSeqnum, true)` seek on the `ConcurrentSkipListMap`, reducing positional read complexity from O(N) to O(log N + K) where K is the number of results returned.
- Removed the redundant post-loop `sort()` call — `tailMap` on a `ConcurrentSkipListMap` guarantees ascending key order by contract.
- Corrected the Javadoc on the `tagIndex` field: the old comment claimed "localId → seqnum" but the index actually stores seqnum as both key and value ("seqnum → seqnum"), enabling the `tailMap` seek.
- Added `readByTag_fromSeqnum_skipsEntriesBelowBoundary` to `InMemoryPersistenceAdapterTest` using non-contiguous seqnums (0, 5, 10) to guard against future regressions on positional reads.
- Added two tests to `BatchingPersistenceAdapterTest`: one verifying boundary filtering on fully-pending (unflushed) entries, and one verifying that entries flushed to the delegate and entries still pending are both correctly included/excluded relative to `fromSeqnum`.

## Files Created/Modified

- `src/main/java/com/cajunsystems/gumbo/persistence/InMemoryPersistenceAdapter.java` — replaced O(N) linear scan with `tailMap` seek in `readByTag()`; corrected Javadoc on `tagIndex` field from "localId → seqnum" to "seqnum → seqnum"
- `src/test/java/com/cajunsystems/gumbo/persistence/InMemoryPersistenceAdapterTest.java` — added `readByTag_fromSeqnum_skipsEntriesBelowBoundary` gap-seqnum regression test
- `src/test/java/com/cajunsystems/gumbo/persistence/BatchingPersistenceAdapterTest.java` — added `readByTag_fromSeqnum_skipsEntriesBelowBoundary` and `readByTag_fromSeqnum_includesPendingEntriesAboveBoundary` tests
- `.planning/phases/01-efficient-readfrom/01-01-SUMMARY.md` — this file

## Decisions Made

None — implementation was fully determined by research. The `tailMap(fromSeqnum, true)` approach is the canonical O(log N) seek on a `NavigableMap`; the existing `NavigableMap` import was already present so no new imports were required.

## Issues Encountered

- `SharedLogServiceTest.subscribeFromMidPoint` produced a flaky failure during the first full `mvn test` run (timing-sensitive subscription test). It passed when run in isolation and passed on the second full run. This failure is pre-existing and unrelated to the changes in this plan.

## Git Commits

- `ae190ab` — `perf(01-01): fix InMemoryPersistenceAdapter.readByTag O(N) linear scan`
- `3e939a9` — `test(01-01): add gap-seqnum regression tests for readByTag positional reads`

## Next Step

Phase 1 complete — ready for Phase 2: Per-tag latestSeqnum
