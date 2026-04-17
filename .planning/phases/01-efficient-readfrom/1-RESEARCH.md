# Research: Phase 1 — Efficient readFrom

## Summary

Deep codebase analysis reveals that the Phase 1 scope is narrower than originally estimated. Three of four adapters already implement `readByTag` efficiently. One adapter has a linear scan bug. Additionally, `DefaultLogView.getLatestSeqnum()` has a separate O(N) bug, and `subscribeTail()` already exists but is inefficient as a consequence of that bug.

Confidence: **HIGH** — findings are based on reading the actual source, not inference.

---

## What Phase 1 Actually Covers

The roadmap described Phase 1 as "Fix readByTag() in all 4 adapters for O(from-N) positional reads." After audit, the actual state is:

| Adapter | readByTag(tag, fromSeqnum) | Status |
|---------|---------------------------|--------|
| InMemoryPersistenceAdapter | Linear scan over all tag index values, filter by `if (seqnum >= fromSeqnum)` | **NEEDS FIX** |
| FileBasedPersistenceAdapter | `idx.tailMap(effectiveFrom, true)` — skip-list seek | ✓ Already O(from-N) |
| BatchingPersistenceAdapter | Delegates to underlying adapter, merges pending buffer | ✓ Correct if delegate is correct |
| FoundationDBPersistenceAdapter | FDB range scan starting at `fromSeqnum` key | ✓ Already O(from-N) |

---

## Bug 1: InMemoryPersistenceAdapter.readByTag (Lines 95–112)

**File**: `src/main/java/com/cajunsystems/gumbo/persistence/InMemoryPersistenceAdapter.java`

**Current (broken):**
```java
for (long seqnum : idx.values()) {          // iterates ALL entries
    if (seqnum >= fromSeqnum) {              // filters in-loop — O(N)
        LogEntry e = log.get(seqnum);
        if (e != null) result.add(e);
    }
}
result.sort((a, b) -> Long.compare(a.seqnum(), b.seqnum()));
```

**Root cause**: Tag index is `ConcurrentSkipListMap<Long, Long>` (seqnum → seqnum). The code iterates `.values()` (all values) instead of using the skip-list's `tailMap()` to seek directly to `fromSeqnum`.

**Fix:**
```java
NavigableMap<Long, Long> range = idx.tailMap(fromSeqnum, true);  // O(log N) seek
List<LogEntry> result = new ArrayList<>(range.size());
for (long seqnum : range.keySet()) {                              // O(K) iteration
    LogEntry e = log.get(seqnum);
    if (e != null) result.add(e);
}
// No sort needed — tailMap preserves skip-list order (ascending seqnum)
return Collections.unmodifiableList(result);
```

**Note**: The comment on line 99 says "The tag index maps localId → seqnum" but the actual implementation (line 73) stores `seqnum → seqnum`. The comment is wrong. The fix above uses key-based tailMap correctly given the actual structure.

---

## Bug 2: DefaultLogView.getLatestSeqnum (Lines 49–57) — Phase 2 Scope

**File**: `src/main/java/com/cajunsystems/gumbo/service/DefaultLogView.java`

**Current (broken):**
```java
public long getLatestSeqnum() {
    List<LogEntry> entries = service.adapter().readByTag(tag, 0L);  // reads ALL entries
    if (entries.isEmpty()) return -1L;
    return entries.get(entries.size() - 1).seqnum();                // just to get last seqnum
}
```

This reads the entire tag history just to return the last seqnum. O(N) where N = tag entry count.

**Required fix**: Add `getLatestSeqnumForTag(LogTag tag)` to `PersistenceAdapter` interface, implemented cheaply in each adapter:

| Adapter | Available Data | Efficient Implementation |
|---------|---------------|--------------------------|
| InMemory | `tagIndex.get(tag)` is a ConcurrentSkipListMap | `tagIndex.get(tag).lastKey()` — O(log N) |
| FileBased | `tagSeqnums.get(tag)` is a ConcurrentSkipListMap | `tagSeqnums.get(tag).lastKey()` — O(log N) |
| Batching | Delegates + pending buffer | `max(delegate.getLatestSeqnumForTag(tag), max pending for tag)` |
| FDB | In-memory `tagLocalIdCount` map doesn't track seqnum | Needs a new `tagLatestSeqnum` map maintained on append, OR a `getRange` FDB query |

**Note**: This is primarily Phase 2 work. Including here because the root cause is relevant to Phase 1 planning.

---

## Discovery: subscribeTail() Already Exists

**File**: `src/main/java/com/cajunsystems/gumbo/api/LogView.java` (lines 91–95)

```java
default SharedLog.Subscription subscribeTail(Consumer<LogEntry> listener) {
    long s = getLatestSeqnum();
    return subscribe(new LogPosition(Math.max(0, s + 1)), listener);
}
```

`subscribeTail()` already exists as a default interface method. It works correctly but is **inefficient** because it calls `getLatestSeqnum()` which reads all entries (Bug 2). Once Phase 2 fixes `getLatestSeqnum()`, `subscribeTail()` becomes efficient automatically.

**Implication for Phase 3**: Phase 3 (Push-tail subscribe) may reduce to:
1. Verify `subscribeTail()` works correctly end-to-end after Phase 2
2. Potentially add a `subscribe(Consumer<LogEntry>)` convenience overload that calls `subscribeTail`

---

## Subscription Live-Delivery Protocol

Understanding this is important for correctness testing.

**How subscriptions work** (from `SharedLogService.java`):

1. `subscribe(tag, from, listener)` creates a `SubscriptionImpl` with `backlogDone = false`
2. A virtual thread starts and delivers entries from `from.seqnum()` (backlog)
3. After backlog completes, calls `markBacklogDone()` → sets `backlogDone = true`
4. `notifySubscribers()` ONLY delivers to subscribers where `isBacklogDone() = true`

**For subscribeTail (from = latest + 1)**:
- Backlog reads from `latest + 1` — returns empty if no entries exist above `latest`
- `markBacklogDone()` called immediately
- Live entries received immediately after

**Race condition handled correctly**: If an entry is appended between `subscribeTail()` computing `from` and the backlog thread starting, it will have seqnum ≥ `from` and will be included in the backlog read. The `notifySubscribers` path skips it because `backlogDone = false`. The backlog thread catches it.

---

## Adapter Index Structure Reference

### InMemoryPersistenceAdapter
```
log: ConcurrentSkipListMap<seqnum, LogEntry>
tagIndex: ConcurrentHashMap<LogTag, ConcurrentSkipListMap<seqnum, seqnum>>  ← key=seqnum, value=seqnum (comment is wrong)
tagLocalIdCount: ConcurrentHashMap<LogTag, AtomicLong>
```

### FileBasedPersistenceAdapter
```
globalIndex: ConcurrentSkipListMap<seqnum, fileOffset>
tagSeqnums: ConcurrentHashMap<LogTag, ConcurrentSkipListMap<seqnum, localId>>  ← key=seqnum, value=localId
tagLocalIdCount: ConcurrentHashMap<LogTag, AtomicLong>
```

### BatchingPersistenceAdapter
```
pendingBatch: List<LogEntry> (guarded by ReentrantLock)
delegate: PersistenceAdapter (any of the above)
```

### FoundationDBPersistenceAdapter
```
latestSeqnum: volatile long (in-memory cache, updated on every commit)
trimSeqnum: volatile long
tagLocalIdCount: ConcurrentHashMap<LogTag, AtomicLong>
FDB subspaces: log/{seqnum}, tag/{ns}/{key}/{seqnum}, meta/trim, meta/latest, meta/tagcount/{ns}/{key}
```
Note: FDB adapter has NO per-tag latest seqnum cache. Needs one added for Phase 2.

---

## Phase 1 Execution Plan (Revised)

**What needs to change:**

1. **`InMemoryPersistenceAdapter.readByTag()`** — replace linear scan with `tailMap(fromSeqnum, true)` 
2. **Test**: verify that `readByTag(tag, N)` does not return entries with seqnum < N in any adapter

**What does NOT need to change for Phase 1:**
- FileBasedPersistenceAdapter — already correct
- BatchingPersistenceAdapter — correct (delegates)
- FoundationDBPersistenceAdapter — already correct
- The `LogView.readFrom()` interface — already correct (passes `from.seqnum()` to `readByTag`)
- The `SharedLogService.read()` path — already correct

**Phase 1 is smaller than planned.** The 2-plan split in the roadmap (01-01: InMemory+FileBased, 01-02: Batching+FDB) can likely merge into a single plan since only InMemory needs fixing.

---

## Relevant Tests to Write/Update

Existing tests to reference:
- `InMemoryPersistenceAdapterTest` — add `readByTag_returnsOnlyEntriesFromPosition()` 
- `FileBasedPersistenceAdapterTest` — add same test for parity
- `SharedLogServiceTest` — add `readFrom_startsMidLog()` to verify LogView behavior

Test pattern for verifying O(from-N):
```java
// Append 5 entries, read from seqnum 3 — should get entries 3, 4, 5 only
for (int i = 0; i < 5; i++) adapter.append(entry(i, ...));
List<LogEntry> result = adapter.readByTag(tag, 3L);
assertThat(result).hasSize(3);
assertThat(result.get(0).seqnum()).isEqualTo(3L);
```

---

## What NOT to Change

- Do not add `maxEntries` parameter to `PersistenceAdapter.readByTag()` — the `SharedLogService.read()` method trims the result at the service layer (`raw.subList(0, maxEntries)`)
- Do not change the `LogView` or `PersistenceAdapter` interfaces in Phase 1 — Phase 2 adds `getLatestSeqnumForTag`
- Do not change `readPrevBefore()` — it reads from seqnum 0 intentionally to scan the full history backwards; it's a different concern from readFrom efficiency
