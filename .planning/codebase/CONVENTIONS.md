# Conventions

## Naming

| Element | Convention | Examples |
|---------|-----------|---------|
| Classes/Interfaces | PascalCase | `SharedLogService`, `PersistenceAdapter` |
| Suffix: Adapter | Pluggable implementations | `FileBasedPersistenceAdapter` |
| Suffix: Executor | Processing components | `ExecutorEngine` |
| Suffix: View | Filtered access patterns | `DefaultLogView`, `TypedLogView` |
| Methods/Variables | camelCase | `appendBatch()`, `localId` |
| Accessors | No `get` prefix | `seqnum()`, `tags()`, `localId()` |
| Boolean accessors | `is` prefix | `isActive()`, `isClosed()` |
| Constants | UPPER_SNAKE_CASE | `ORDERS`, `POLL_TIMEOUT_MS` |
| Packages | lowercase, hierarchical by role | `persistence`, `sequencer`, `serialization` |

## Class Organization
1. Static logger (`private static final Logger logger`)
2. Constants and static fields
3. Instance fields grouped by concern; volatile flags for visibility
4. Factory/static methods at top (`open()`, `of()`, `create()`)
5. Constructor (often private; use static factory)
6. Methods grouped by section with comments: `// Append`, `// Read`, `// Subscribe`, `// Lifecycle`
7. Package-private helpers at bottom
8. Static nested classes (e.g., `SubscriptionImpl`, `CountingExecutor`)

## Immutability
- Data classes are `final class` with `private final` fields
- Records for simple carriers (`AppendResult`)
- Defensive copying: `Arrays.copyOf()` on byte arrays in constructors
- `Collections.unmodifiableSet()` / `Set.copyOf()` / `List.copyOf()` on returned collections
- `dataUnsafe()` escape hatch when defensive copy overhead is unacceptable (documented)

## Error Handling
- **Checked exceptions** (`throws IOException`) for I/O: `open()`, `close()`, `append()`, all persistence methods
- **Unchecked exceptions** for logic errors:
  - `IllegalArgumentException` — precondition violations (empty tags, invalid seqnum)
  - `IllegalStateException` — postcondition violations (closed adapter, closed service)
  - `LogWriteException` / `LogReadException` — unchecked wrappers for IOException from async paths
- Validation pattern: `if (seqnum < 0) throw new IllegalArgumentException("seqnum must be >= 0")`
- `Objects.requireNonNull()` for null guards
- Try-finally for lock cleanup; try-with-resources for I/O streams

## Concurrency
- Single `ReentrantLock` (`writeLock`) serializes all appends
- Lock-free reads via direct adapter queries
- `ConcurrentHashMap` for per-tag state; `CopyOnWriteArrayList` for subscription lists
- `volatile boolean` for close/active flags
- Named virtual threads for debugging: `"sharedlog-backlog-" + tag`
- `CompletableFuture` for all async operations

## Builder Pattern
- `SharedLogConfig.builder()` returns inner `Builder`
- Fluent methods return `this`
- Terminal `build()` returns immutable config

## Factory Pattern
- Static `open()` as primary factory for services
- Static `of()` / `to()` for data types: `LogTag.of("orders")`, `AppendRequest.to(tag, data)`

## Logging
- `private static final Logger logger = LoggerFactory.getLogger(ClassName.class)`
- INFO: lifecycle events (startup, shutdown)
- WARN: recoverable errors (listener exceptions, backlog delivery failures)
- No DEBUG/TRACE in production code paths

## Documentation
- Comprehensive class-level Javadoc with HTML markup, design intent, and examples
- Interface methods documented for contract details
- No redundant method comments; code should be self-documenting
