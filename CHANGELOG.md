# Changelog

All notable changes to gumbo are documented here.
Format: [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

---

## [0.4.0] — 2026-07-26

Continues the [Catalyst requirements report](https://github.com/CajunSystems/catalyst/blob/main/docs/gumbo-requirements.md)
at its item **A3**: with storage-owned versions and conditional append in place (0.3.0),
compare-and-set on the tag KV is what turns the KV from a place to keep checkpoints into a
coordination substrate — leases, ownership records, work claims — with no new subsystem.

**Why 0.4.0 and not 0.3.1.** The KV additions are additive for callers but not for
implementors: `LogView` and `TypedLogView` each gain two abstract methods, so anything
implementing those interfaces stops compiling until they are added. That is the same class of
change as 0.2.0 → 0.3.0, and it gets the same treatment. Adapters are unaffected — every new
`PersistenceAdapter` method has a default.

### Added

**Conditional mutation on the tag key-value store**
- `PersistenceAdapter.compareAndSetTagValue(tag, key, expected, value)` — writes only if the
  key still holds `expected`, comparing by content. `expected == null` means *the key must be
  absent*; `value == null` removes it, so a conditional release is the same operation as a
  conditional claim
- Derived forms, defined in terms of it so overriding one supplies all four:
  `setTagValueIfAbsent` (claim), `deleteTagValueIf` (release),
  `incrementTagValue(tag, key, delta)` (counter, absent reads as `0`)
- The same four on `LogView` and `TypedLogView`, scoped to the view's tag:
  `compareAndSetValue`, `setValueIfAbsent`, `deleteValueIf`, `incrementValue`
- New `CounterValues` — the counter encoding, eight bytes big-endian, stated once so a
  client decoding the key through `getValue` and an adapter incrementing it agree. FDB does
  **not** use its native `MutationType.ADD` here: that op is little-endian, so a counter it
  maintained would disagree byte-for-byte with every other adapter
- On FoundationDB the read, comparison and write are one transaction, so a claim is arbitrated
  across processes. The file and in-memory adapters decide it under their own lock, atomic
  within the single writer the file adapter enforces
- No working default, matching `append(PendingAppend, expectedVersion)`: an adapter that
  compared non-atomically would hand every contender a `true` and report two owners as
  success, so the base implementation throws `UnsupportedOperationException`

The two new abstract methods are `compareAndSetValue` and `incrementValue`; the other two are
defaults over them.

**Why a lease still needs the append fence.** Expiry needs a clock, and a clock is the part
that can be wrong. With the append conditioned on the version, skew that lets two nodes both
decide a lease is free costs duplicated effort rather than a corrupted stream: only one wins
the swap, and only one passes the fence. `TagValueCoordinationTest` pins that pair.

### Fixed

- **KV writes were not synced.** `setTagValue` returned once the bytes reached the channel,
  so an acknowledged checkpoint could be lost while the log entries written either side of it
  survived — and the KV is what a consumer resumes *from*. `kv.dat` is now synced before the
  new value is published to the in-memory map, so visibility follows durability as it does for
  the log
- **KV values were aliased to the caller's array**, in and out, on both adapters that answer
  reads from memory. A caller reusing a buffer could change a value nobody wrote — including
  one another caller was comparing against. Worse on `FileBasedPersistenceAdapter`, where
  `kv.dat` holds the bytes as they were at write time but `kvStore` is the read path: a
  mutation after a successful swap changed what every reader and the next compare saw while
  the committed value stayed put, so the divergence was invisible until a reopen silently
  reverted it. FoundationDB is exempt — the bytes leave the process when the transaction sets
  them

### Build and CI

- **Mutation ratchet raised from 76 to 77**, measured at 462 of 592 killed on CI and 460
  locally (test strength 83-84%, up from 82%). Both halves of the fraction moved when the
  conditional KV landed, so the floor is recomputed with it: 453 reports 77 and passes, 452
  reports 76 and fails. Not 78, which the CI number alone would justify — its floor of 459 sits
  one mutant under the observed low of 460, on a score that varies by two between runs, so the
  gate would fail on timing rather than on a regression
- **GitHub Actions pinned to commit SHAs** — `checkout`, `setup-java` and `upload-artifact`,
  across both jobs. A tag is mutable, so an upstream repoint changed what CI executed with no
  change here and no review; pinning one job and not the other would have left the same path
  open. Taken to the current majors while pinning, which also clears the Node 20 deprecation
  warning the runner had started emitting
- `assertj` 3.25.3 → 3.27.7 and `maven-compiler-plugin` 3.12.1 → 3.15.0. The other open
  dependency bumps are deliberately not here: `fdb-java` 7.3.43 → 7.4.6 cannot be exercised
  without a live cluster and the API version is pinned at 730 in code, and `logback`
  1.5.3 → 1.6.0 is a runtime logging change that should not ride along with a release cut

### Documentation

- The README now says which Maven coordinate to use and why there are two. JitPack rewrites
  the groupId to `com.github.{owner}` when it publishes, so the same jar is
  `com.cajunsystems:gumbo` when built locally and `com.github.CajunSystems:gumbo` when
  fetched — a difference that had a downstream build depending on a coordinate only resolvable
  on a machine where gumbo had been installed by hand
- `docs/OPEN-WORK.md` refreshed against what has actually landed

---

## [0.3.0] — 2026-07-26

Correctness work on the log layer, prompted by a
[requirements report from Catalyst](https://github.com/CajunSystems/catalyst/blob/main/docs/gumbo-requirements.md).
Several defects here were silent — data lost or duplicated with no error — so the notes say
what was wrong as well as what changed.

### Changed — breaking

**`localId` renamed to `streamVersion`**
- `LogEntry.localId()` → `LogEntry.streamVersion()`; `AppendResult.localId()` →
  `AppendResult.streamVersion()`. Both old accessors remain as
  `@Deprecated(forRemoval = true)` delegates, so 0.2.0 code compiles with a warning rather
  than an error
- `PersistenceAdapter.getLocalIdCountForTag(LogTag)` → `getNextStreamVersion(LogTag)`,
  deliberately with **no** default: an adapter that silently inherited one would hand out
  versions colliding with those already on disk, so third-party adapters get a compile
  error instead
- The name was a fossil of Boki's per-*engine* `localid` — a write-path id superseded once
  the sequencer assigns a `seqnum`. What the field holds is a per-*tag*, externally visible,
  permanent position in a stream
- **No log migration.** The on-disk and FDB layouts are byte-identical — same field, same
  offset, same width — so every existing log reads back as before and every persisted
  cursor stays valid

**Subscription delivery is ordered and serialised**
- Each subscription now owns one virtual thread delivering its entries in seqnum order.
  Previously a thread was spawned per entry, leaving order to the scheduler
- A listener is never called concurrently with itself, so it needs no synchronisation of
  its own. The trade is that a slow listener delays its own subscription rather than
  running deliveries in parallel — the only way ordered delivery is achievable
- `Subscription.close()` now **waits** for an in-flight listener call to return, so once it
  returns the listener is not running and will not run again. The wait is bounded and not
  shortened by an interrupt on the calling thread

**Conditional append names its tag**
- `SharedLog.append(request, expectedVersion)` requires a single-tag request;
  `append(request, fencedTag, expectedVersion)` is the multi-tag form. The primary tag of a
  multi-tag request is `tags.iterator().next()` over a `Set`, whose iteration order Java
  salts per JVM run, so an implicit fence would apply to a different stream between runs

### Added

**Storage-owned stream versions + conditional append**
- `SharedLog.append(AppendRequest, long expectedVersion)` — appends only if the tag is
  still at that version, else fails with the new `VersionConflictException`
- `PersistenceAdapter.append(PendingAppend, long expectedVersion)` returns the persisted
  `LogEntry`; the **adapter** assigns `streamVersion`, not the caller.
  `PersistenceAdapter.ANY_VERSION` appends unconditionally
- `PersistenceAdapter.appendBatchAssigningVersions(List<PendingAppend>)` — batch form
- New `PendingAppend` record: an append whose version has not been assigned yet, which
  `LogEntry` cannot express since it requires one at construction
- `SharedLogService` no longer keeps per-tag version counters. Seeded once from storage,
  they then diverged silently from every other writer's copy — two processes on one log
  both handing out `0, 1, 2` with nothing to reconcile them
- On FoundationDB the read, compare and write happen in **one transaction**, so the fence
  holds across processes. The file and in-memory adapters assign under their own lock,
  atomic within the single-writer configuration they enforce

**Version-keyed reads** — read a tag's stream by its own position instead of the global seqnum
- `SharedLog.readFromVersion` / `readAfterVersion`; the same pair plus `getLatestVersion()`
  on `LogView` and `TypedLogView`; `readFromVersion` / `readAfterVersion` on
  `PersistenceAdapter`, with a correct filtering default and an efficient override in all
  four shipped adapters
- Every existing read is keyed on the global `seqnum`, which coincides with a tag's own
  numbering only while the log holds a single tag. A consumer resuming from a cursor into
  one stream previously had to pass its version into a seqnum-keyed API, silently
  re-reading entries it had already processed as soon as a second tag shared the log

**Single-writer enforcement for `FileBasedPersistenceAdapter`**
- `open()` takes an exclusive `FileLock` on `{dataDir}/lock` and throws the new
  `LogAlreadyOpenException` when another adapter already holds the directory
- Previously a second writer was accepted silently and corrupted the log two ways: both
  processes assigned the same versions for a tag, and the last to close overwrote the
  other's `index.dat`, leaving entries on disk that no reader could see

**Documented failure semantics** — `docs/FAILURE_SEMANTICS.md`
- What every mutating operation leaves behind when it fails: `NOTHING`, `PREFIX`, or
  `UNKNOWN`. `appendBatch` is `PREFIX` on both durable adapters, so callers must not assume
  all-or-nothing
- `getLatestSeqnum()` is defined as a **durability** boundary, not a visibility one

**Mutation testing** — PIT over `persistence` and `service`, gated in CI at the measured
score so it cannot drift down

### Fixed

- **Entries appended during a subscriber's backlog delivery were silently dropped.** The
  backlog read had already happened and the live path skipped not-yet-ready subscribers, so
  the entry arrived by neither route — no error, no retry, nothing logged
- **A failed flush in `BatchingPersistenceAdapter` discarded its entries.** The pending
  buffer was cleared before the delegate write returned. It now drops only what the delegate
  confirms it holds, so a retry neither loses nor duplicates
- **The file adapter published entries before their fsync**, so `getLatestSeqnum()` reported
  writes that were not durable and a failed sync was indistinguishable from a successful one
- **FoundationDB rewound a secondary tag's version count** on a multi-tag append, which
  under the new conditional append would let a stale writer pass the fence. Counts are now
  raised, never lowered
- **A read racing a flush could return an entry twice** in `BatchingPersistenceAdapter`;
  all four read paths now deduplicate by seqnum
- **`readAfterVersion(Long.MAX_VALUE)` returned the entire stream** instead of nothing, via
  integer overflow in the exclusive-to-inclusive conversion
- **A failed `open()` leaked file descriptors** — the unwind released the directory lock but
  not the channels already opened
- A listener throwing an `Error` no longer stops delivery for every later entry
- `BatchingPersistenceAdapter` forwards `trim` to its delegate; a truncated `index.dat` is
  now rejected and rebuilt rather than partially trusted

### Known limitation

- An entry carries one `streamVersion`, drawn from its primary tag, so a tag carried only as
  a *secondary* tag on a multi-tag append inherits the primary's numbering instead of
  counting its own. Version-keyed reads are defined for a tag's own primary stream; use the
  seqnum-keyed reads for a shared fan-out tag. Resolving it needs a version per tag per
  entry, which changes the record layout and forces a log migration.
  `VersionKeyedReadTest.anAtomicMultiTagAppendLeavesOneStreamMisNumbered` pins the current
  behaviour until then
- `BatchingPersistenceAdapter`'s pending buffer is unbounded, and `flushQuietly` swallows
  the error, so a persistently failing delegate grows memory without telling anyone

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
