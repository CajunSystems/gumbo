# Phase 5 Plan 1: KV Implementation (InMemory + FileBased) Summary

**Implemented sidecar KV storage in InMemoryPersistenceAdapter (nested ConcurrentHashMap) and FileBasedPersistenceAdapter (append-only kv.dat with tombstones, survives close/reopen), and replaced the Phase 4 placeholder test with 3 real KV behavior tests in SharedLogServiceTest.**

## Accomplishments

- `InMemoryPersistenceAdapter`: three KV methods using `ConcurrentHashMap<LogTag, ConcurrentHashMap<String, byte[]>> kvStore`; no durability (in-memory only by contract); no `throws IOException` on overrides.
- `FileBasedPersistenceAdapter`: append-only `kv.dat` file using the same ByteBuffer/FileChannel pattern as existing log files:
  - `open()` calls `loadKvFile()` if `kv.dat` exists — replays all records, last-write-wins
  - `setTagValue` updates in-memory + appends record
  - `deleteTagValue` removes from in-memory + appends tombstone (valLen = -1)
  - `getTagValue` reads from in-memory (O(1))
  - `close()` closes kvChannel
- Binary format: `[nsLen:2B][ns][tagKeyLen:2B][tagKey][kvKeyLen:2B][kvKey][valLen:4B signed][value?]`
- `kvPersistsAcrossReopen` test confirmed that values written to `kv.dat` survive adapter close/reopen.
- Replaced `kvApi_throwsUnsupportedBeforeImplementation` placeholder with 3 real behavior tests (`setAndGetValue`, `deleteValue`, `isolatedPerTag`); removed the now-unused `assertThatThrownBy` import.

## Files Created/Modified

- `src/main/java/com/cajunsystems/gumbo/persistence/InMemoryPersistenceAdapter.java` — KV via nested ConcurrentHashMap
- `src/main/java/com/cajunsystems/gumbo/persistence/FileBasedPersistenceAdapter.java` — KV via append-only kv.dat with tombstone support
- `src/test/java/com/cajunsystems/gumbo/persistence/InMemoryPersistenceAdapterTest.java` — 4 new KV tests
- `src/test/java/com/cajunsystems/gumbo/persistence/FileBasedPersistenceAdapterTest.java` — 4 new KV tests including persistence test
- `src/test/java/com/cajunsystems/gumbo/service/SharedLogServiceTest.java` — placeholder → 3 real KV tests

## Decisions Made

None beyond research decisions. Binary format chosen to match existing adapter style (ByteBuffer/FileChannel). Tombstone approach (append negative valLen record) avoids random-access rewriting.

## Issues Encountered

None.

## Git Commits

- `929d0d6` — `feat(05-01): implement KV in InMemoryPersistenceAdapter and add tests`
- `d4fa4c4` — `feat(05-01): implement KV in FileBasedPersistenceAdapter and update tests`

## Next Step

Ready for 05-02: Batching and FDB KV implementations + full test suite verification
