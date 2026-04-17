# Integrations

## Storage Backends

### In-Memory (`InMemoryPersistenceAdapter`)
- `ConcurrentSkipListMap<seqnum, LogEntry>` for global log
- Per-tag indices in `ConcurrentHashMap`
- No durability — for testing and local development only

### File-Based (`FileBasedPersistenceAdapter`)
- **Files**: `log.dat` (WAL), `index.dat` ([seqnum:8][offset:8] per entry), `trim.dat` (8-byte marker)
- **Format**: custom binary with CRC32 checksums; magic `0xC0FFEE42`
- **Entry layout**: seqnum(8) + timestamp(8) + localId(8) + numTags(4) + tags + dataLen(4) + data + CRC32(4)
- **Durability**: `FileChannel.force(false)` (fdatasync) per append
- **Recovery**: index rebuilt from WAL on startup; falls back to full log scan if index missing
- **Concurrency**: `ConcurrentSkipListMap` for lock-free in-memory reads

### FoundationDB (`FoundationDBPersistenceAdapter`)
- **Client**: `org.foundationdb:fdb-java:7.3.43`, FDB API version 730
- **Subspace layout**:
  - `{root}/log/{seqnum}` → entry bytes
  - `{root}/tag/{namespace}/{key}/{seqnum}` → localId (8-byte big-endian)
  - `{root}/meta/trim` → trim seqnum
  - `{root}/meta/latest` → latest seqnum
  - `{root}/meta/tagcount/{namespace}/{key}` → localIdCount
- **Default root subspace**: `"gumbo"`
- **Batch limit**: 8 MB per FDB transaction (hard limit 16 MB with 2 MB margin)
- **Read pattern**: two-phase (range scan + parallel point reads)
- **Connection**: default cluster file or custom path via constructor

### Batching Wrapper (`BatchingPersistenceAdapter`)
- Decorator over any `PersistenceAdapter`
- Default batch size: 64 entries; default flush delay: 10 ms
- Group-commit: reduces fdatasync calls from 2N to 2
- **Durability trade-off**: up to `maxDelayMs` of data loss on JVM crash (documented)

## Sequencers

### Local (`LocalSequencer`)
- `AtomicLong` counter; single-JVM only
- `advanceTo(minNext)` for post-crash reseeding from latest persisted seqnum

### FoundationDB (`FoundationDBSequencer`)
- FDB read-modify-write transaction per `next()` or per `nextBatch(count)`
- Key path: `{rootPrefix}/seq`; default root: `"gumbo_seq"`
- OCC handles multi-node contention
- `currentGlobal()` for cross-node visibility

## Serialization

### Kryo (`KryoLogSerializer<T>`)
- Kryo 5.6.0, pooled instances (8 capacity)
- Thread-safe via `Pool<Kryo>`
- Optional class registration callback
- Java records supported natively (Kryo 5.x)
- 256-byte initial output buffer (grows automatically)

## No Other External Integrations
- No HTTP/REST server (library-only)
- No cloud provider SDKs
- No message queues (subscriptions are in-process)
- No Spring or dependency injection
- No metrics/tracing (SLF4J logging only)
- No authentication/authorization (application responsibility)
- No environment variable configuration (programmatic builder only)
