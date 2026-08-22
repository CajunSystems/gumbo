# Changelog

All notable changes to gumbo are documented here.
Format: [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

---

## [Unreleased]

Closes the last item on the [Catalyst requirements report](https://github.com/CajunSystems/catalyst/blob/main/docs/gumbo-requirements.md)'s
backlog that carried a data-format cost, and the one a consumer's design had come to depend
on: **an entry now carries a position per tag, not one position borrowed from its primary
tag.**

### Fixed

**Multi-tag entries mis-numbered every stream but one**

An entry held a single `streamVersion`, drawn from its primary tag, and every tag it touched
was told that number — written into that tag's index *and* used to drag that tag's counter
forward. A tag carried only as a **secondary** tag therefore inherited another stream's
numbering: not dense, not starting at zero, and — the part that actually loses data — able
to go *backwards* relative to entries already delivered.

That last property is the defect rather than an inelegance. A worker cursoring a shared work
queue holds a position and asks for what came after it. Under the old numbering an item
enqueued by a workflow whose history sat at version 4 was numbered 4, so a worker that had
already advanced to 8 behind a busier workflow was handed nothing: the item is below where
the cursor already is, a version-keyed tail read skips it, and **nothing in the log looks
wrong**. This is exactly the shape a workflow engine uses when one atomic append records
history and enqueues the work item.

- `LogEntry` now carries `Map<LogTag, Long>` and answers `streamVersion(LogTag)`. Every
  adapter claims the next position in *every* tag an append touches, and stores each tag's
  own number in that tag's index
- A multi-tag append therefore advances each of its tags by one. It always did belong to
  both streams; only the numbering pretended otherwise
- `AppendResult` reports the position of the tag the append was **addressed to**, rather
  than whichever the entry's `Set` iterated first — that order is salted per JVM run

**No migration, and no rewrite of anything already written.** The record's own marker says
which layout it is:

| Adapter | Marker |
|---|---|
| file | magic `0xC0FFEE43` (was `0xC0FFEE42`), with a version after each tag |
| FoundationDB | a leading `-1L`, which the old layout's first field — a seqnum, always `>= 0` — could never be |

So a log may hold both, which is what an upgrade actually produces: existing records stay
put and new ones land on the end. An older record holds one number and **cannot** say which
tag it counted — the primary was `tags.iterator().next()` over a `Set`, salted per run — so
it answers every tag with that number, precisely as it always did, and reports
`hasPerTagVersions() == false`. A consumer can ask instead of being quietly guessed at.

**Why the version is stored rather than derived.** Deriving each tag's position as its rank
in the per-tag index needs no format change at all, and is wrong: `trim` purges the in-memory
index and `rebuildTagIndices` skips entries below the trim point, so a trim would renumber
every survivor and silently invalidate every cursor a consumer had stored. Surviving a trim
is the whole reason the number is written down.

### Tests

- `VersionKeyedReadTest.anAtomicMultiTagAppendLeavesOneStreamMisNumbered` **asserted this
  defect** as a property (*both streams cannot be dense from 0*). It is now
  `anAtomicMultiTagAppendNumbersBothStreamsFromZero` and asserts both are — and it can be
  exact where the old one could not, because neither stream's numbering depends any more on
  which tag won the iteration order
- `aWorkerCursoringAFanOutTagSeesEveryItemExactlyOnce` pins the consequence, with the worker
  consuming *between* enqueues. Draining everything first and advancing once hides the bug
  entirely — the first version of this test did exactly that and passed against a faithful
  reproduction of the defect, which is the trap this repo has now walked into three times
- `RecordFormatCompatibilityTest` hand-assembles records in the old layout, the only honest
  way to test bytes the current code can no longer produce. Covers an old log, an old
  multi-tag record's admitted ambiguity, a mixed log, and recovery by full scan across both
  layouts — where a mis-sized cursor lands mid-record and truncates the log

---

## [0.5.0] — 2026-08-22

Continues the [Catalyst requirements report](https://github.com/CajunSystems/catalyst/blob/main/docs/gumbo-requirements.md)
at its item **A4**, promoted well above its original rank. The report put capabilities
seventh, as polish, reasoning that there was no real variation between adapters to
declare. 0.3.0 and 0.4.0 created that variation: `append(request, expectedVersion)` and
`compareAndSetTagValue` are arbitrated *across processes* on FoundationDB and *within a
single writer* on the file adapter. Both implement the method. They do not make the same
promise, and until now the difference existed only in prose — which is exactly how D4
happened, a client assuming a guarantee its adapter did not provide.

**Why 0.5.0, and why for a different reason than last time.** 0.4.0 was a minor because it
was *source-breaking for implementors* — `LogView` and `TypedLogView` each gained two
abstract methods. Nothing here breaks: every method added in this release has a default
(`PersistenceAdapter.capabilities()`, `SharedLog.capabilities()`, `Sequencer.distributed()`),
so existing adapters, log implementations and sequencers all keep compiling untouched. It is
a minor because it adds capability, not because it costs anyone a change. **Upgrading from
0.4.0 requires nothing** — which is the point worth stating, since the value of this release
is a question you can now ask rather than an answer you have to migrate to.

**Release note for downstreams, learned the hard way.** 0.4.0 was cut and merged on 26 July
and its tag was not pushed until 22 August, so everything in it — including the conditional
KV that the lease and claim story rests on — was unreachable from any build resolving through
JitPack for four weeks. A merged release is not a released one. Tag it.

### Added

**Declared capabilities, per adapter**
- New `LogCapabilities` record — `conditionalAppend`, `compareAndSet`, `versionedReads`,
  `pushSubscriptions`, `atomicMultiTagAppend`, `multiWriter` — built by name rather than
  from six positional booleans, and composable from another log's answer via
  `LogCapabilities.builder(from)`
- `PersistenceAdapter.capabilities()` and `SharedLog.capabilities()`. Both have defaults,
  both conservative: the adapter default declares only what *the interface itself*
  provides (`versionedReads`, which has a working filtering default) and nothing else, and
  `SharedLog`'s declares nothing at all
- The reach of a fence is carried by the **pair** `conditionalAppend` + `multiWriter`
  rather than by a third "scope" concept: the first says the compare and the increment are
  indivisible, the second says that holds against writers in other processes. A runtime
  that distributes execution needs both and can now refuse to start without them
- `pushSubscriptions` is never set by an adapter. Delivery is implemented above storage, so
  `SharedLogService` adds it when it answers for the log as a whole — an adapter asked
  about it would be answering for code it does not contain

**What each shipped adapter declares**

| | conditional append | compare-and-set | versioned reads | atomic multi-tag | multi-writer |
|---|---|---|---|---|---|
| `InMemoryPersistenceAdapter` | ✓ | ✓ | ✓ | ✓ | — |
| `FileBasedPersistenceAdapter` | ✓ | ✓ | ✓ | ✓ | — |
| `FoundationDBPersistenceAdapter` | ✓ | ✓ | ✓ | ✓ | ✓ |
| `BatchingPersistenceAdapter` | delegate's | delegate's | delegate's | delegate's | — |

- The file adapter's `multiWriter: false` is false by **enforcement**, not omission — the
  exclusive directory lock from 0.3.0 refuses a second process — which is what makes its
  single-writer fence sufficient rather than merely convenient
- **`BatchingPersistenceAdapter` narrows its delegate**, and this is the declaration worth
  knowing: it passes everything through except `multiWriter`, which it forces off however
  capable the delegate is. A version is claimed when `append` returns but the entry lands
  at flush time, and across two processes those two moments admit a third party between
  them. Wrapping a FoundationDB adapter therefore downgrades it, silently, unless someone
  asks

**Honesty is tested, not documented** — `LogCapabilityHonestyTest` is the report's test #5:
for every adapter, each capability it declares is exercised, and each one it disclaims is
asserted to *throw* rather than silently no-op. Every test branches on the declaration
rather than on a hard-coded expectation per adapter, so flipping a flag without changing
behaviour fails the build — which is the mutation that matters and the one a fixed
expectation would miss. Verified by injecting the lies and watching it fail (a file adapter
claiming `multiWriter`, an in-memory adapter disclaiming its own fence: four failures,
including the decorator case). One test covers the inherited default itself, via an adapter
implementing only the abstract methods — the case that decides whether having a default is
safe at all.

### Fixed

- **`SharedLogService` reported `multiWriter` from storage alone**, ignoring the sequencer it
  is configured with. The first cut of `capabilities()` passed everything but subscriptions
  through, reasoning that the service adds no fencing and weakens none. The second half was
  wrong. `multiWriter` is a property of the whole log and needs *two* independent things:
  storage that assigns per-tag versions across processes, and a `Sequencer` whose global
  `seqnum` spans them. This layer owns the second, and its default is `LocalSequencer` — a
  per-process `AtomicLong`.

  So a FoundationDB adapter behind the default sequencer was a configuration where every
  stream version is arbitrated correctly by storage and two processes still hand out the same
  seqnums. Nothing downstream catches that: the seqnum never passes through the adapter's
  fence, and both durable adapters index *by* seqnum (`globalIndex`, `tagSeqnums`), so a
  collision overwrites an index entry and the earlier record stops being readable while its
  bytes stay on disk — the view lost rather than the data, which is this layer's signature
  failure. A distributed caller checking the flag before starting would have been told yes.

  `Sequencer.distributed()` now states whether a sequencer's uniqueness guarantee survives
  leaving the process (default `false`, `FoundationDBSequencer` overrides it), and the service
  requires both halves. Pinned by `LogCapabilityCompositionTest`, including that a distributed
  sequencer does not rescue single-writer storage either. Caught in review on #28 — the
  over-report this interface exists to prevent, in the interface itself.

### Build and CI

- **Mutation score 473–474 of 599 killed (79%); threshold unchanged at 77.** Test strength
  84%. Both halves moved: the new mutants are the adapters' declarations and the service's
  composition, and the capability tests kill them because they assert behaviour *against* the
  declaration rather than against a fixed expectation per adapter. The floor recomputes to
  462 at the new denominator, leaving eleven mutants of headroom.

  The range is not CI versus local — **CI produced both numbers on identical code**, one run
  either side, which is worth recording because the last two releases described the spread as
  a machine difference. It is run-to-run variance in what times out, and a timed-out mutant
  counts as killed. That is precisely why the ratchet is not raised to 79: the floor there is
  474, which half these runs would fail on nothing but timing.

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
