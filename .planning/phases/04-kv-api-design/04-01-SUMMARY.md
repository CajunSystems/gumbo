# Phase 4 Plan 1: Key-Value API Design Summary

**Established the KV API contract across all layers — `setTagValue`/`getTagValue`/`deleteTagValue` on `PersistenceAdapter` (with `UnsupportedOperationException` defaults), `setValue`/`getValue`/`deleteValue` on `LogView`, and `DefaultLogView` delegation — leaving adapter implementations for Phase 5.**

## Accomplishments

- Added 3 KV default methods to `PersistenceAdapter` under a `// ── Key-Value ──` section; all throw `UnsupportedOperationException` with the adapter class name for clear error messaging.
- Added 3 abstract KV methods to `LogView` interface; `CompletableFuture` was already imported.
- Implemented all 3 in `DefaultLogView` via `CompletableFuture.runAsync/supplyAsync` delegating to `service.adapter()`. `UnsupportedOperationException` (a `RuntimeException`) propagates uncaught through the async wrapper and lands as the cause of the `CompletionException` from `.join()`.
- Added `kvApi_throwsUnsupportedBeforeImplementation` test to `SharedLogServiceTest` documenting the exception chain — this will become a real behavior test in Phase 5.
- Full `mvn test` suite passes; the new test confirms the UnsupportedOperation propagation chain exactly.

## Files Created/Modified

- `src/main/java/com/cajunsystems/gumbo/persistence/PersistenceAdapter.java` — 3 KV default methods with `UnsupportedOperationException`
- `src/main/java/com/cajunsystems/gumbo/api/LogView.java` — 3 abstract KV methods
- `src/main/java/com/cajunsystems/gumbo/service/DefaultLogView.java` — 3 KV implementations delegating to adapter
- `src/test/java/com/cajunsystems/gumbo/service/SharedLogServiceTest.java` — API shape + UnsupportedOperation test

## Decisions Made

None — all decisions were resolved by research (sidecar per-tag, write-through for Batching, CompletableFuture async pattern matching existing API).

## Issues Encountered

None.

## Git Commits

- `da654ef` — `feat(04-01): add KV methods to PersistenceAdapter, LogView, and DefaultLogView`
- `c9bd6df` — `test(04-01): add KV API shape test documenting UnsupportedOperation before Phase 5`

## Next Step

Phase 4 complete — ready for Phase 5: Key-value implementation across all 4 adapters (InMemory, FileBased, FDB, Batching)
