# Stack

## Language
- **Java 21** (minimum required)
- Uses Java 21 features: virtual threads (Project Loom), records

## Build System
- **Apache Maven**
- maven-compiler-plugin 3.12.1
- maven-surefire-plugin 3.2.5
- Project version: 1.0.0-SNAPSHOT
- Packaging: JAR (library)

## Core Dependencies

| Library | Version | Purpose |
|---------|---------|---------|
| Kryo | 5.6.0 | Binary serialization for TypedLogView |
| SLF4J API | 2.0.12 | Logging facade |
| Logback Classic | 1.5.3 | Logging implementation (runtime) |
| FoundationDB Java Client | 7.3.43 | Optional distributed storage/sequencing |

## Test Dependencies

| Library | Version | Purpose |
|---------|---------|---------|
| JUnit Jupiter | 5.10.2 | Test framework |
| AssertJ | 3.25.3 | Fluent assertions |
| Awaitility | 4.2.1 | Async/concurrent assertion helpers |

## Runtime Requirements
- JVM: Java 21+
- Encoding: UTF-8 (enforced in pom.xml)
- Virtual threads: used throughout for async and subscription delivery
- FoundationDB 7.3.x cluster (optional, only for distributed deployments)

## Concurrency Model
- Virtual threads (`Thread.ofVirtual()`, `Executors.newVirtualThreadPerTaskExecutor()`)
- Single `ReentrantLock` for write serialization; lock-free reads
- `ConcurrentHashMap`, `CopyOnWriteArrayList`, `ConcurrentSkipListMap` for shared state

## CI/CD
- GitHub Actions (Ubuntu latest)
- Matrix: Java 21 only
- Maven cache via actions/setup-java@v4
- Surefire test report artifacts uploaded on each run
