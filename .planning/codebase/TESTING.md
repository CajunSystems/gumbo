# Testing

## Frameworks
- **JUnit 5** (junit-jupiter 5.10.2) — test runner
- **AssertJ** 3.25.3 — fluent assertions (`assertThat().isEqualTo()`, `.hasSize()`)
- **Awaitility** 4.2.1 — async/concurrent assertion (`await().atMost(3, SECONDS).until(...)`)
- **No mocking framework** — all tests use real implementations with in-memory adapters

## Test Structure
Tests mirror main source structure: `src/test/java/com/cajunsystems/gumbo/{persistence,sequencer,serialization,service,examples}`.

### Class Layout Convention
1. `// ---- Fixtures ----` section: `@BeforeEach` / `@AfterEach` + field declarations
2. Test method sections grouped by feature
3. Private static helpers at the bottom (e.g., `entry()` factory methods)

### Lifecycle
- `@BeforeEach`: initialize service/adapter, call `.open()`
- `@AfterEach`: call `.close()` for resource cleanup
- `@TempDir Path tempDir` for file system tests

## Test Types

### Unit Tests
Single-component isolation. Each adapter tested against `PersistenceAdapter` contract:
- `InMemoryPersistenceAdapterTest` (12 tests)
- `FileBasedPersistenceAdapterTest` (9 tests including recovery scenarios)
- `BatchingPersistenceAdapterTest` (11 tests on batching behavior)

### Integration Tests
Multi-component interaction:
- `SharedLogServiceTest` (service + adapter + sequencer)
- `ExecutorEngineTest` (executor registration and execution)
- `AppendBatchTest` (batch seqnum optimization)
- `TypedLogViewTest` (type-safe views with serialization)

### Example Tests (runnable usage docs)
- `QuickStartExample` — append/read/subscribe/multi-tag
- `OrderFulfilmentExample` — stateless executor with backlog replay
- `FilePersistedExample` — file-based persistence setup
- `WorkflowExecutorExample` — multi-stage chained executors

## Coverage Areas

### Well-Covered
- Core append/read semantics (dozens of tests)
- Subscription delivery (8+ tests: from-beginning, mid-point, tail, closure)
- Multi-tag indexing and filtering
- Persistence adapter contract compliance (each adapter 8–12 tests)
- Batch operation correctness
- State recovery and durability
- Serialization round-trips

### Moderately Covered
- Executor engine patterns (4 core tests + examples)
- FoundationDB integration (optional, minimal tests — requires live FDB)

### Coverage Gaps
- Error path testing (partial writes, listener exceptions, I/O failures)
- Concurrent executor register/deregister
- Trim conflicts with active readers
- Chaos/fault injection (execute() throws, batch intermediate failures)

## Test Helpers

### Factory Methods
```java
private static LogEntry entry(long seqnum, long localId, LogTag tag, String data)
private static LogEntry entry(long seqnum, long localId, LogTag tag, byte[] data)
```

### Test Doubles
- `CountingAdapter` in `BatchingPersistenceAdapterTest` — wraps `InMemoryPersistenceAdapter`, counts batch flush calls
- `CountingExecutor` in `ExecutorEngineTest` — concrete `Executor<Integer>` for engine pattern tests
- `OrderEvent` / `Metric` in `TypedLogViewTest` — domain objects for serialization tests

## Synchronization in Tests
- `CountDownLatch` — synchronize on async events
- `CopyOnWriteArrayList` — thread-safe result collection
- `CompletableFuture.join()` — block until async operations complete

## Running Tests
```bash
mvn test
mvn test -Dtest=SharedLogServiceTest
```

## Assertion Style
```java
assertThat(entries).hasSize(3).contains(expected);
assertThatThrownBy(() -> adapter.append(null)).isInstanceOf(IllegalArgumentException.class);
await().atMost(3, SECONDS).until(() -> !received.isEmpty());
```
