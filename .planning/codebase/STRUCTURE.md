# Structure

## Directory Layout

```
gumbo/
├── src/
│   ├── main/java/com/cajunsystems/gumbo/
│   │   ├── api/
│   │   │   ├── Executor.java              Stateless worker contract
│   │   │   ├── ExecutorContext.java        Runtime context for execute()
│   │   │   ├── LogView.java               Tag-scoped read/write/subscribe view
│   │   │   ├── SharedLog.java             Main log interface
│   │   │   └── TypedLogView.java          Type-safe LogView<T> wrapper
│   │   ├── core/
│   │   │   ├── AppendRequest.java         Payload + target tags for append
│   │   │   ├── AppendResult.java          Seqnum + localId returned from append (record)
│   │   │   ├── LogEntry.java              Immutable record (seqnum, localId, tags, data, timestamp)
│   │   │   ├── LogPosition.java           Cursor/seqnum wrapper; BEGINNING/END constants
│   │   │   └── LogTag.java                Stream identifier (namespace + optional key)
│   │   ├── persistence/
│   │   │   ├── PersistenceAdapter.java    Interface: open/close/append/read/trim/metadata
│   │   │   ├── InMemoryPersistenceAdapter.java   ConcurrentSkipListMap, no durability
│   │   │   ├── FileBasedPersistenceAdapter.java  WAL (log.dat) + index (index.dat) + trim.dat
│   │   │   ├── BatchingPersistenceAdapter.java   Group-commit decorator; configurable batch size
│   │   │   └── FoundationDBPersistenceAdapter.java Distributed storage via FDB
│   │   ├── sequencer/
│   │   │   ├── Sequencer.java             Interface: next(), current(), nextBatch(count)
│   │   │   ├── LocalSequencer.java        AtomicLong; single-node
│   │   │   └── FoundationDBSequencer.java FDB read-modify-write; multi-node
│   │   ├── serialization/
│   │   │   ├── LogSerializer.java         Interface: serialize(T), deserialize(byte[])
│   │   │   └── KryoLogSerializer.java     Kryo 5 backend; pooled instances (8 capacity)
│   │   └── service/
│   │       ├── SharedLogService.java      Main SharedLog impl; write lock, subscriptions
│   │       ├── SharedLogConfig.java       Fluent builder for all configuration
│   │       ├── DefaultLogView.java        LogView impl; readNextAfter/readPrevBefore
│   │       ├── DefaultTypedLogView.java   TypedLogView impl with serialization
│   │       └── ExecutorEngine.java        Virtual-thread executor orchestration
│   └── test/java/com/cajunsystems/gumbo/
│       ├── examples/
│       │   ├── QuickStartExample.java      Core API patterns
│       │   ├── OrderFulfilmentExample.java Executor with backlog replay
│       │   ├── FilePersistedExample.java   Durability and crash recovery
│       │   └── WorkflowExecutorExample.java Multi-stage chained executors
│       ├── persistence/
│       │   ├── InMemoryPersistenceAdapterTest.java
│       │   ├── FileBasedPersistenceAdapterTest.java
│       │   ├── BatchingPersistenceAdapterTest.java
│       │   └── FoundationDBPersistenceAdapterTest.java
│       ├── sequencer/
│       │   └── SequencerBatchTest.java
│       ├── serialization/
│       │   └── KryoLogSerializerTest.java
│       └── service/
│           ├── SharedLogServiceTest.java
│           ├── ExecutorEngineTest.java
│           ├── AppendBatchTest.java
│           └── TypedLogViewTest.java
├── docs/
│   └── PERSISTENCE_EVOLUTION.md           WAL, index, memory-mapping, io_uring roadmap
├── pom.xml
├── README.md
└── LICENSE
```

## Module Organization
Single monolithic JAR — no Maven submodules. All packages in `com.cajunsystems.gumbo.*`.

## Package Dependency Direction
```
service/ → api/ → core/
service/ → persistence/
service/ → sequencer/
service/ → serialization/
persistence/ → core/
sequencer/ (no deps on other packages)
serialization/ → core/
```

## Configuration
No properties/YAML files. All configuration via `SharedLogConfig.builder()` (programmatic only).

## Defaults (SharedLogConfig)
- Persistence: `InMemoryPersistenceAdapter`
- Sequencer: `LocalSequencer`
- Async pool: `Executors.newVirtualThreadPerTaskExecutor()`
- Read batch size: 1024 entries
