# Phase 4 Research: Key-Value API Design

## Context

The goal is to add `setValue(key, value)` / `getValue(key)` to Gumbo for use by actor systems — actors need a fast, durable place to write state checkpoints without polluting the message log. The question is where and how to store it.

---

## Candidate Approaches

### Option A: In-Log KV (sentinel entries)

KV mutations are appended as `LogEntry` objects. `data` encodes the operation (e.g., `{"_kv": "set", "key": "ck", "value": "...base64..."}`). `getValue(key)` replays the tag's log to find the latest set-operation for the given key.

**Analysis:**
- `getValue` is O(N) unless you maintain a materialized view
- Log grows with every `setValue` call (checkpoint on every message = 2× entries)
- Trim will silently delete KV state when it removes old log entries — breaks reads after trim
- Requires a discriminator in `LogEntry.data` (encoding convention) or a new `kind` field — LogEntry currently has no type field
- Replay-friendly: if you replay the log, KV state comes along for free
- Not suitable for the actor use case: actors want O(1) `getValue`, not O(N) replay

**Verdict:** Not viable without materialization. Trim incompatibility is a hard blocker.

---

### Option B: Sidecar KV (separate per-tag structure)

KV state lives in a dedicated structure alongside the log, not in it. Each adapter maintains its own KV storage that is separate from the log subspace:

| Adapter | Storage |
|---------|---------|
| InMemory | `ConcurrentHashMap<LogTag, ConcurrentHashMap<String, byte[]>>` |
| FileBased | `{dataDir}/kv.dat` — line-delimited or binary per-tag KV file |
| FDB | `{root}/kv/{ns}/{tag-key}/{user-key}` → value (new subspace) |
| Batching | In-memory KV + write-through to delegate |

**Analysis:**
- O(1) `getValue` — direct map lookup or FDB point read
- No log pollution — KV ops don't appear as entries
- No trim interaction — KV is independent of the log
- Exactly mirrors the existing `tagLocalIdCount` / `tagLatestSeqnum` pattern — proven, understood
- Per-tag scope is natural for actors (each actor's KV namespace isolated)
- Simple recovery: FDB is always consistent; FileBased loads kv.dat on open; InMemory starts empty (in-memory only, by contract)

**Verdict:** Correct fit. O(1) reads, no log bloat, no trim hazard, mirrors existing patterns.

---

### Option C: Hybrid (in-log mutations + materialized sidecar)

Log the mutation AND maintain a materialized view. Replay-consistent, fast reads.

**Analysis:**
- 2× write path complexity
- Materialized view must be maintained transactionally with the log commit (especially hard for FDB chunked commits)
- Adds complexity with no benefit over Option B for the actor use case — actors don't need KV replay, they need fast checkpoint reads

**Verdict:** Over-engineered. Skip.

---

## Decision: Sidecar KV per-tag (Option B)

**Rationale:**
1. O(1) reads — direct requirement for actor state checkpoints
2. Exact structural analogy to `tagLocalIdCount` — three adapters can copy the pattern mechanically
3. No trim interaction — KV state persists across log trim (actors checkpoint to avoid replaying trimmed entries; killing KV on trim defeats the purpose)
4. Per-tag scope: actors map to LogTags; their KV namespace should be isolated per-tag

---

## API Design

### PersistenceAdapter (new methods)

```java
// Write KV value for a tag. Durable on return (or on next flush for Batching).
void setTagValue(LogTag tag, String key, byte[] value) throws IOException;

// Read KV value for a tag. Returns null if key not present.
byte[] getTagValue(LogTag tag, String key) throws IOException;

// Delete a KV entry. No-op if key not present.
void deleteTagValue(LogTag tag, String key) throws IOException;
```

No default fallback needed — all adapters must implement. These are not optional.

### LogView (new methods)

```java
// Write actor-scoped KV entry. CompletableFuture mirrors existing append() pattern.
CompletableFuture<Void> setValue(String key, byte[] value);

// Read actor-scoped KV entry. Returns null if absent.
CompletableFuture<byte[]> getValue(String key);

// Delete actor-scoped KV entry.
CompletableFuture<Void> deleteValue(String key);
```

DefaultLogView implements each by delegating to `service.adapter()`.

---

## Per-Adapter Implementation Sketch

### InMemoryPersistenceAdapter

New field:
```java
ConcurrentHashMap<LogTag, ConcurrentHashMap<String, byte[]>> kvStore = new ConcurrentHashMap<>();
```

All three methods: get/put/remove on `kvStore.computeIfAbsent(tag, k -> new ConcurrentHashMap<>())`.

### FileBasedPersistenceAdapter

New file `{dataDir}/kv.dat` — simple binary format:
```
per-entry:
  [nsLen  : 2B unsigned]
  [ns     : UTF-8]
  [tagLen : 2B unsigned]
  [tag    : UTF-8]
  [keyLen : 2B unsigned]
  [key    : UTF-8]
  [valLen : 4B big-endian]
  [value  : valLen bytes]
```
Loaded fully into a `ConcurrentHashMap<LogTag, ConcurrentHashMap<String, byte[]>>` on `open()`.
`setTagValue` appends to `kv.dat` and updates the in-memory map.
`deleteTagValue` writes a tombstone entry (valLen = -1) and removes from in-memory map.
Full rewrite of `kv.dat` on close (to compact tombstones). Or rewrite on trim.

Alternative: JSON/properties file for simplicity. Binary is consistent with existing `log.dat` format.

### FoundationDBPersistenceAdapter

New subspace constant and field:
```java
private static final String KV_NS = "kv";
private Subspace kvSubspace;  // {root}/kv
```

Initialized in `initSubspaces()`:
```java
kvSubspace = rootSubspace.subspace(Tuple.from(KV_NS));
```

`setTagValue(tag, key, value)`:
```java
db.run(tr -> {
    tr.set(kvSubspace.pack(Tuple.from(tag.namespace(), tag.key(), userKey)), value);
    return null;
});
```

`getTagValue(tag, key)`:
```java
return db.run(tr ->
    tr.get(kvSubspace.pack(Tuple.from(tag.namespace(), tag.key(), userKey))).join()
);
```

`deleteTagValue(tag, key)`:
```java
db.run(tr -> {
    tr.clear(kvSubspace.pack(Tuple.from(tag.namespace(), tag.key(), userKey)));
    return null;
});
```

No in-memory cache needed for KV — FDB point reads are already O(1) and low-latency.

### BatchingPersistenceAdapter

Write-through pattern: `setValue` writes immediately to delegate (no buffering — KV writes are low-frequency; latency is acceptable). This avoids the complexity of merging pending + flushed views for KV reads.

```java
@Override
public void setTagValue(LogTag tag, String key, byte[] value) throws IOException {
    delegate.setTagValue(tag, key, value);
}
```

Same for `getTagValue` and `deleteTagValue` — all delegate directly.

---

## Read-Your-Writes Under Batching

If log entries and KV writes are independent, a pattern like:
```
adapter.append(entry);         // goes to pending buffer (not yet flushed)
adapter.setTagValue(tag, k, v); // goes directly to delegate (write-through)
```
…is fine. KV reads always go to delegate; they are always immediately consistent.

The actor use case writes KV *before* or *after* appending messages — rarely in atomic conjunction. Write-through for KV is correct.

---

## FDB Atomic Ops Assessment

FDB's native atomic operations (`MutationType.ADD`, `MutationType.COMPARE_AND_CLEAR`, etc.) are available but not needed here. KV values are set by one actor at a time — the actor is single-threaded by design. No CAS needed. Simple `tr.set()` is sufficient.

---

## Summary

| Question | Answer |
|----------|--------|
| In-log vs. sidecar? | **Sidecar** — O(1) reads, no trim interaction |
| Per-tag vs. global? | **Per-tag** — natural for actor model |
| Read-your-writes under Batching? | **Write-through** — KV bypasses pending buffer |
| FDB atomic ops needed? | **No** — single-actor writes; `tr.set()` is sufficient |
| LogEntry changes needed? | **No** — KV is separate, no sentinel entries |
| New `kind` discriminator on LogEntry? | **No** |

---

## Next Step

Plan 04-01: Define `PersistenceAdapter` KV methods + `LogView` KV API + `DefaultLogView` delegation. This is purely additive; no breaking changes.
