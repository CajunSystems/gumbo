# Concerns

## Critical / High

### Tag index semantic mismatch in InMemoryPersistenceAdapter
- **File**: `persistence/InMemoryPersistenceAdapter.java:71-73`
- **Issue**: Class comment says index maps `localId → seqnum` but implementation stores `seqnum → seqnum`. The readByTag() path works but carries a misleading invariant; confusing to maintainers.
- **Impact**: Logic bug (semantic), not a runtime failure — but creates incorrect mental model.

### Unchecked iterator.next() without empty-set guard
- **Files**: `core/LogEntry.java:65`, `service/SharedLogService.java:138,165`
- **Issue**: Calls `.iterator().next()` on tag sets without verifying non-empty. Constructor guards exist but aren't verified defensively.

## Medium

### Suppressed exceptions in FileBasedPersistenceAdapter.closeQuietly()
- **File**: `persistence/FileBasedPersistenceAdapter.java:552`
- **Issue**: All exceptions during channel close silently swallowed without logging. Disk errors or final-fsync failures hidden.

### Unbounded reads (Integer.MAX_VALUE default)
- **Files**: `api/LogView.java:48,56`, `api/SharedLog.java:95`, `service/ExecutorEngine.java:220`
- **Issue**: `readAll()` / `readAfter()` default to reading up to `Integer.MAX_VALUE` entries. Large logs risk OOM. FoundationDB adapter documents 5 MB transaction read limit — no enforcement in API.

### FDB tag index deferred cleanup
- **File**: `persistence/FoundationDBPersistenceAdapter.java:400-409`
- **Issue**: Tag-index entries below the trim boundary accumulate indefinitely. Range scans in `readByTag()` degrade over time.

### BatchingPersistenceAdapter data loss window
- **File**: `persistence/BatchingPersistenceAdapter.java:31-35`
- **Issue**: Entries added to `pendingBatch` are visible to reads but not yet flushed. JVM crash during `maxDelayMs` window (default 10 ms) loses those entries. Documented but requires careful tuning awareness.

### BatchingPersistenceAdapter flusher thread interrupt handling
- **File**: `persistence/BatchingPersistenceAdapter.java:112-134`
- **Issue**: Background flusher thread interrupt handling is fragile; if blocked inside `flushQuietly()` (swallows IOException), the interrupt may not terminate the loop cleanly.

### Unchecked exception wrapping (LogWriteException / LogReadException)
- **File**: `service/SharedLogService.java:374-380`
- **Issue**: IOException wrapped in unchecked RuntimeException. Callers can accidentally ignore I/O errors if they don't handle CompletableFuture exceptions carefully.

### Hardcoded constants not exposed as configuration
- **Files**: Multiple
  - `POLL_TIMEOUT_MS = 500` — `service/ExecutorEngine.java:72`
  - `DEFAULT_MAX_BATCH_SIZE = 64` — `persistence/BatchingPersistenceAdapter.java:59`
  - `POOL_CAPACITY = 8` — `serialization/KryoLogSerializer.java:40`
  - `FDB_API_VERSION = 730` — `persistence/FoundationDBPersistenceAdapter.java:86`
- **Issue**: Production tuning requires code changes.

### No metrics or observability hooks
- **Issue**: No built-in way to monitor write/read latency, subscriber lag, executor cycle duration, or FDB cache hit rates. Production diagnosis requires external instrumentation.

### No input validation on LogTag names
- **File**: `core/LogTag.java:34-38`
- **Issue**: Namespace/key validated as non-blank but no length limits. Very long values could cause memory pressure or hit FDB key size limits (1 MB).

## Low

### POISON_PILL uses Instant.EPOCH
- **File**: `service/ExecutorEngine.java:316-318`
- **Issue**: Sentinel entry uses `Instant.EPOCH` as timestamp. Never persisted, but `Instant.MAX` would be a clearer sentinel.

### volatile flag without explicit synchronization comment (SharedLogService.closed)
- **File**: `service/SharedLogService.java:71`
- **Issue**: Safe by volatile semantics but intent is implicit; no comment explaining the concurrent close protocol.

### ByteBuffer pattern without explicit flip
- **Files**: `persistence/FoundationDBPersistenceAdapter.java:529`, `sequencer/FoundationDBSequencer.java:155,182`
- **Issue**: `ByteBuffer.allocate(8).putLong(v).array()` works (`.array()` returns backing array unaffected by position) but is a code smell; `.flip()` is the canonical pattern.

### ExecutorEngine anti-drift poll timeout is hardcoded
- **File**: `service/ExecutorEngine.java:215-223`
- **Issue**: 500 ms safety-net poll on inbox timeout accumulates latency under load; not configurable.

## Test Coverage Gaps
- Error path tests: partial writes, listener exceptions, I/O failures, FDB connection failures
- Concurrent executor register/deregister
- Trim conflicts with active readers
- Chaos/fault injection: `execute()` throws, intermediate batch failures, virtual thread starvation
- No formal "Safety & Failure Modes" documentation for operators
