# Changelog

All notable changes to gumbo are documented here.
Format: [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

---

## [Unreleased]

### Added

**Storage-owned stream versions + conditional append** (D1 / A1)
- `SharedLog.append(AppendRequest, long expectedVersion)` — appends only if the primary tag
  is still at that version, else fails with the new `VersionConflictException`
- `PersistenceAdapter.append(PendingAppend, long expectedVersion)` returns the persisted
  `LogEntry`; the **adapter** assigns `streamVersion`, not the caller.
  `PersistenceAdapter.ANY_VERSION` appends unconditionally
- `PersistenceAdapter.appendBatchAssigningVersions(List<PendingAppend>)` — batch form;
  versions are assigned in list order so repeated appends to one tag stay consecutive
- New `PendingAppend` record: an append whose version has not been assigned yet, which
  `LogEntry` cannot express since it requires one at construction
- `SharedLogService` no longer keeps per-tag version counters. They were seeded once from
  storage and then diverged silently from every other writer's copy — two processes on one
  log both handing out `0, 1, 2` with nothing to reconcile them (D1)
- The SPI default assigns from `getNextStreamVersion` and writes, which is what callers did
  before and no weaker, but **rejects** a conditional append with
  `UnsupportedOperationException` rather than performing a non-atomic check. An adapter that
  appeared to fence while doing nothing of the sort would surface as corruption, not an error
- On FoundationDB the read, compare and write happen in **one transaction**, so the fence is
  enforced by the store and holds across processes. The file and in-memory adapters assign
  under their own lock, which is atomic within the single-writer configuration they support
  (enforced by the directory lock added earlier)

### Changed — breaking

**`localId` renamed to `streamVersion`**
- `LogEntry.localId()` → `LogEntry.streamVersion()`; `AppendResult.localId()` →
  `AppendResult.streamVersion()`. Both old accessors remain as
  `@Deprecated(forRemoval = true)` delegates, so 0.2.0 code compiles with a warning
  rather than an error
- `PersistenceAdapter.getLocalIdCountForTag(LogTag)` → `getNextStreamVersion(LogTag)`,
  deliberately with **no** default: an adapter that silently inherited one would hand out
  versions colliding with those already on disk, so third-party adapters get a compile
  error instead
- The name was a fossil of Boki's per-*engine* `localid` — a write-path id superseded once
  the sequencer assigns a `seqnum`. What this field holds is a per-*tag*, externally
  visible, permanent position in a stream: a different quantity that happened to share a
  name. `streamVersion` also names what conditional append will condition on and what
  version-keyed reads are keyed by
- **No log migration.** The on-disk and FDB layouts are byte-identical — same field, same
  offset, same width — and the assigned values are unchanged, so every existing log reads
  back as before and every persisted cursor stays valid

### Added

**Single-writer enforcement for `FileBasedPersistenceAdapter`**
- `open()` now takes an exclusive `FileLock` on `{dataDir}/lock` and throws the new
  `LogAlreadyOpenException` when another adapter already holds the directory — whether it
  is in this JVM or another process
- Previously a second writer was accepted silently and corrupted the log two ways: both
  processes assigned the same `localId` values for a tag (process-local counters, never
  reconciled), and the last one to close overwrote the other's `index.dat`, leaving
  entries on disk that no reader could see
- The lock is released on `close()` and by process exit, so restart-after-crash is
  unaffected and a leftover `lock` file never needs removing by hand

**Version-keyed reads** — read a tag's stream by its own position instead of the global seqnum
- `SharedLog.readFromVersion(LogTag, long)` / `readAfterVersion(LogTag, long)`
- `LogView.readFromVersion(long)` / `readAfterVersion(long)` / `getLatestVersion()`
- `TypedLogView.readFromVersion(long)` / `readAfterVersion(long)` / `getLatestVersion()`
- `PersistenceAdapter.readFromVersion(LogTag, long)` / `readAfterVersion(LogTag, long)`, with a
  correct filtering default for third-party adapters and an efficient override in all four
  shipped adapters
- Every existing read is keyed on the global `seqnum`, which coincides with a tag's own
  numbering only while the log holds a single tag. A consumer resuming from a cursor into
  one stream — an executor from a checkpoint, a workflow replaying its history — previously
  had to pass its version into a seqnum-keyed API, silently re-reading entries it had already
  processed as soon as a second tag shared the log

### Fixed

- `BatchingPersistenceAdapter.getNextStreamVersion` (then `getLocalIdCountForTag`) ignored the pending buffer, on the
  assumption that its only caller asked once per brand-new tag. It now counts pending entries,
  so a tag's version tip is correct between flushes

### Known limitation

- An entry carries one `streamVersion`, drawn from its primary tag, so a tag carried only as a
  *secondary* tag on an atomic multi-tag append inherits the primary's numbering instead of
  counting its own — and which tag is primary is `tags.iterator().next()` over a
  `Set.copyOf`, whose order Java salts per JVM run. Version-keyed reads are therefore defined
  for a tag's own primary stream; use the seqnum-keyed reads for a shared fan-out tag.
  Resolving it needs a version per tag per entry (storage-owned per-tag versions), and
  `VersionKeyedReadTest.anAtomicMultiTagAppendLeavesOneStreamMisNumbered` pins the current
  behaviour until then

---

## [0.2.0] — 2026-04-17

### Added

**Efficient positional reads**
- `LogView.readFrom(LogPosition, int)` — read entries starting from a specific seqnum; O(log N) in InMemory (fixed from linear scan), O(seek) in FileBased/FDB/Batching
- `LogPosition` constructor accepting a seqnum for checkpoint-based replay

**Per-tag latest seqnum**
- `LogView.getLatestSeqnum()` — returns the highest seqnum written to a tag's log in O(1) via maintained metadata; available across all 4 persistence adapters
- `PersistenceAdapter.getLatestSeqnumForTag(LogTag)` — backing method with O(log N) in InMemory/FileBased, O(1) via AtomicLong cache in FDB, merged-scan in Batching

**Push-tail subscribe**
- `LogView.subscribeTail(listener)` — delivers only future (live) entries with no backlog; enables seamless catching-up → live handoff without polling

**Per-tag key-value store**
- `LogView.setValue(String key, byte[] value)` — persist a durable KV checkpoint scoped to the tag
- `LogView.getValue(String key)` — read a KV checkpoint
- `LogView.deleteValue(String key)` — remove a KV entry
- Durability per adapter: FileBased uses an append-only `kv.dat` sidecar file (survives reopen); FDB uses a dedicated `kv` subspace; Batching is write-through to delegate; InMemory is in-memory only

**TypedLogView additions**
- `TypedLogView.getLatestSeqnum()` — delegating wrapper of `LogView.getLatestSeqnum()`
- `TypedLogView.currentPosition()` — default method returning `LogPosition.BEGINNING` when the view is empty
- `TypedLogView.setValue/getValue/deleteValue` — KV API delegated to underlying `LogView`

**Examples and documentation**
- `ActorCheckpointExample` — runnable JUnit 5 example demonstrating: read KV checkpoint → replay backlog via `readFrom` → switch to live via `subscribeTail` → persist new checkpoint on each entry
- README "Actor checkpoints" section documenting all new APIs with working code snippet

### Changed

- `pom.xml` version bumped from `1.0.0-SNAPSHOT` to `0.2.0`
- `DefaultLogView.getLatestSeqnum()` reimplemented to call `PersistenceAdapter.getLatestSeqnumForTag()` instead of materialising all entries (O(N) → O(1))

---
