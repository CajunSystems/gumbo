# Open work

Written as a handover. Everything below is either not started or deliberately deferred,
with enough evidence attached that it can be picked up cold.

Context for all of it: [Catalyst's requirements
report](https://github.com/CajunSystems/catalyst/blob/main/docs/gumbo-requirements.md),
which catalogued defects and missing primitives in the log layer. Items are referenced by
its numbering (D1–D4, A1–A6).

## Where things stand

**0.3.0 is cut, merged and tagged** (`47db570`). Shipped: D3 (directory lock), A2
(version-keyed reads), A5 (`localId` → `streamVersion`), D1 + A1 (storage-owned versions +
conditional append), plus a subscription-delivery rewrite and a mutation-testing gate that
were not on the report's list.

**0.4.0 carries A3** (KV compare-and-set), the release-hygiene work listed in §0 and §4, and
two KV defects found while implementing it — see the CHANGELOG.

Report items now outstanding: **A4**, **A6** (half), **D2**, and the multi-tag version
defect. Everything else below is testing, process or cross-cutting.

See [`FAILURE_SEMANTICS.md`](FAILURE_SEMANTICS.md) for the write-path contracts, which
several items below depend on.

---

## 0. ~~Release: the 0.3.0 tag is not pushed~~ — done

The tag is pushed (`0.3.0` → `47db570`, lightweight, matching the `0.2.0` convention) and
JitPack has built it: `jitpack.io/api/builds/com.github.CajunSystems/gumbo` reports
`"0.3.0" : "ok"`, with the jar, sources jar and pom published.

`0.4.0` follows it, carrying A3 — see the CHANGELOG. It is a minor bump rather than a patch
because `LogView` and `TypedLogView` each gained two abstract methods, which is source-
breaking for implementors.

### ~~Coordinates need checking~~ — settled, and measured

The two coordinates are both real and name the same jar:

| Obtained by | Coordinate |
|---|---|
| JitPack | `com.github.CajunSystems:gumbo` (JitPack rewrites the groupId to `com.github.{owner}`) |
| `mvn install` | `com.cajunsystems:gumbo` — what this repo's `pom.xml` declares |

Verified against the published artifact: `gumbo-0.3.0.pom` on JitPack carries
`<groupId>com.github.CajunSystems</groupId>`.

**Use the JitPack one.** It was tested end to end from a machine with normal egress, and the
control matters as much as the result:

```
before: Could not find artifact com.cajunsystems:gumbo:jar:0.3.0 in central
after:  Downloaded from jitpack.io: .../com/github/CajunSystems/gumbo/0.3.0/gumbo-0.3.0.jar
        Catalyst reactor: 11 modules SUCCESS, catalyst-gumbo tests 9/9
```

So the earlier note in Catalyst's pom — that jitpack.io is blocked and gumbo must come from a
local `mvn install` — was true of one sandboxed environment, not of the delivery mechanism.
The README now documents which coordinate to use and why there are two.

**Still to do on this:** Catalyst's pom change (groupId + the `jitpack.io` repository) is
written but not committed. It is what unblocks §1 on CI rather than only on a machine where
someone ran the install.

---

## 1. Catalyst: D4 is still live

The bug that prompted the whole report, still unfixed, now unblocked by 0.3.0.

`catalyst-gumbo/GumboEventLog` passes a per-execution cursor into a seqnum-keyed read. The
two number spaces coincide only when the log holds one stream — true in every test, false
in production. Measured end-to-end through Catalyst's snapshot warm path:

```
PROBE SOLE execution             cold steps=43  warm steps=43  OK
PROBE SHARED log (2 executions)  cold steps=43  warm steps=51  *** CORRUPTED ***
```

The reducer re-applies events already folded into the snapshot, so timeline steps, token
counts, cost and attempt counters all double-count.

**Fix:** bump Catalyst to gumbo 0.3.0, switch the tail read to `readFromVersion` /
`readAfterVersion`, and add a regression test with two executions in one log — the
configuration that exposes it. Catalyst's `seq` is gumbo's `streamVersion`, so the rename
also applies (`localId()` still works, deprecated for removal).

---

## 2. Report items not started

### A4 — declared capabilities

**Promote this above its original rank.** The report put it 7th as polish, on the grounds
that there was no real variation between adapters to declare. **0.3.0 created that
variation**: the same `append(request, expectedVersion)` call is *fenced across processes*
on FoundationDB and *fenced within a single writer* on the file adapter. That difference
currently exists only in prose, which is exactly how D4 happened — a client assuming a
guarantee the adapter did not provide.

```java
interface LogCapabilities {
    boolean conditionalAppend();
    boolean compareAndSet();
    boolean versionedReads();
    boolean pushSubscriptions();
    boolean atomicMultiTagAppend();
    boolean multiWriter();
}
```

Per-adapter, not per-Gumbo. The report's test #5 goes with it: for each adapter, assert
every capability reported `true` actually works and every `false` throws rather than
silently no-ops.

### D2 — non-clobbering index

`index.dat` is written per process, so the last to close overwrites the other's view. After
the two-writer probe the log reported 3 of 6 appends while `strings log.dat` showed all six
present — nothing lost, the *view* was.

Less urgent than it was: D3's directory lock now makes a second writer fail loudly rather
than silently corrupt. So this is required for multi-writer to become *usable*, not to stop
it being *dangerous*.

**Do the fault-injection harness (§4) first** — D2 is squarely a partial-write problem and
would be its first real customer.

### ~~A3 — KV compare-and-set~~ — done, unreleased

Shipped ahead of its rank here (this doc put it 7th; it was taken next because the report's
own order puts it 6th, above A4). See the CHANGELOG's `[Unreleased]`.

`compareAndSetTagValue` on `PersistenceAdapter`, with `setTagValueIfAbsent` /
`deleteTagValueIf` / `incrementTagValue` defined over it, and the four mirrored on `LogView`
and `TypedLogView`. Named `setTagValueIfAbsent` rather than the sketch's
`putTagValueIfAbsent`, to match the existing `setTagValue`.

Two things it changed that were not on any list, both in the methods it had to touch:
`kv.dat` was never fsynced, so an acknowledged checkpoint could be lost while the log entries
around it survived; and KV values were aliased to the caller's array in both directions on
both adapters that answer reads from memory, which a comparison protocol cannot survive.

The aliasing is worth remembering for its shape rather than its size. The fix was written for
`InMemoryPersistenceAdapter` on the reasoning that *durable adapters serialise, so the caller
cannot reach what they hold* — true of the bytes in `kv.dat`, and irrelevant, because
`FileBasedPersistenceAdapter` also keeps a `kvStore` cache and **the cache is the read path**.
So the durable adapter had the same bug with a worse failure mode (invisible until a reopen
reverted the value), and the review caught it. A durable adapter is not immune to an in-memory
defect just because it also writes to disk.

It also adds one more `removed call to FileChannel::force` mutant — same deliberately-
unkillable class as the others (§4), now covering `kv.dat` as well as the log.

Still open on this subject: **A3 gave the KV a fence, not a clock.** A lease's `expiresAt` is
compared by claimants, which is sufficient precisely because A1's version fence makes skew an
*efficiency* problem rather than a correctness one — the log rejects the loser. If a native
TTL is ever wanted, that is a new decision, not a gap in this.

### A6 — multi-tag ergonomics (half done)

The design question is **answered**: 0.3.0's `append(request, fencedTag, expectedVersion)`
conditions on the named tag only, and the caller names it. What remains is ergonomics:

```java
AppendResult appendAtomically(byte[] data, LogTag primary, LogTag... additional);
```

Note the explicit-fence overload with `ANY_VERSION` already gives deterministic primary-tag
selection for a multi-tag append, which is the only way to get it.

---

## 3. Defects found during the work, not fixed

These were discovered while implementing the report, are not in it, and are all documented
in code and pinned by tests where possible.

### Multi-tag entries carry one version — needs a log migration

An entry has a single `streamVersion`, drawn from its primary tag, so a tag carried only as
a *secondary* tag inherits another stream's numbering. Measured:

```
multi-tag append: localId=3 primary=history:wf-1
--- QUEUE stream: the shared entry is its FIRST, so its version should be 0 ---
   work seqnum=3 localId=3
next QUEUE-only append got localId=4 (expected 1)
```

This is exactly the Boudin shape (per-instance history tag + shared work-queue tag) that the
report lists under "what already works".

**Not fixed by D1/A1** — storage-owned assignment changes *who* assigns the number, not how
many the record can hold. It needs a version per tag per entry, which changes the on-disk
record layout and forces a log migration: the one property the `streamVersion` rename was
careful to preserve. **This is the only backlog item with a data-format cost.**

Pinned by `VersionKeyedReadTest.anAtomicMultiTagAppendLeavesOneStreamMisNumbered`, written
as a property (*both streams cannot be dense from 0*) because which tag is primary is
`tags.iterator().next()` over a `Set.copyOf`, whose iteration order Java salts per JVM run —
verified across six JVMs.

### `BatchingPersistenceAdapter` under sustained write failure

Two related gaps, both needing one decision rather than two patches: **what does this
adapter promise when the delegate keeps failing?**

- The pending buffer is **unbounded**. A failed flush now retains its entries (correctly —
  discarding them lost data), so a persistently failing delegate grows memory without limit.
- `flushQuietly` **logs and swallows**. A caller holding an `AppendResult` — with a version
  assigned and consumed — is never told its entry has not landed.

Options: a bounded buffer with a defined overflow policy; a failure state surfaced on the
next `append`; or an explicit "durability is best-effort until flush" contract. Four of the
sixteen defects found in review were in this class, so it deserves the decision.

### FoundationDB read amplification

`writeEntry` now reads each tag's count and latest-seqnum before writing, to raise-not-
overwrite (a fence bypass otherwise — see the 0.3.0 changelog). Correct, but it widens the
transaction's conflict range on multi-tag appends. An FDB atomic `MAX` mutation would remove
the read entirely. **Measure before optimising**; multi-tag appends are the minority path.

---

## 4. Testing and process work

### Fault-injection harness — do this next

Every fix in the D1/A1 round shipped with a bespoke failing delegate: four hand-written
fakes asking the same question. That should be one harness with one oracle:

> after any injected failure and recovery, the durable log is a prefix of what was
> acknowledged — no duplicates, no gaps

Injection points that found real defects: fail on the Nth write, fail after persisting K
entries, fail the fsync specifically, fail a read. That single property covers four of the
findings on #13 and is what would have caught them before review.

It is also the only way to kill the `removed call to FileChannel::force` mutants, which no
in-process test can distinguish — a clean reopen reads through the page cache either way.
That needs real crash injection (`kill -9` between write and read).

### Mutation-testing backlog

`mvn test-compile org.pitest:pitest-maven:mutationCoverage`. Currently **462 of 592 killed**
on CI and **460** locally (test strength 83–84%), threshold ratcheted at 77, floor 453. Was
421–422 of 553 at 76; A3 moved both halves of the fraction, which is why the floor was
recomputed rather than carried over.

The gap between those two runs is the useful part. The score varies by about two, so a
threshold set to the best observed run leaves the gate failing on timing rather than on a
regression: 78 would have had one mutant of headroom, 77 has seven. The same reasoning applies
next time it is raised — take the *low* run, not the flattering one.

The distribution is the useful part: survivors cluster in code that only runs *after
something has already gone wrong*, which is the same distribution as the defects found in
review, arrived at independently. Still uncovered:

- **`loadKvFile`** — every partial-record boundary check survives. The torn-KV-record path
  is entirely unexercised, and this got *more* load-bearing in 0.4.0: the KV now carries
  claims and counters, not just checkpoints, so a torn record silently reverting a key is no
  longer only a lost checkpoint. Reachable by the fault-injection harness below.
- **`decodeAt`** — cursor arithmetic in the binary decoder; `Replaced long addition with
  subtraction` survives.
- **`tryLoadGlobalIndex`** — partly covered now by `RecoveryAndTrimTest`, some survivors
  remain.

Two classes are **deliberately not worth killing** and are documented in
`FAILURE_SEMANTICS.md` so nobody burns time on them — the `force` mutants above, and:

- **`trim`'s in-memory index purge in `FileBasedPersistenceAdapter`** (`removed call to
  ConcurrentNavigableMap::clear`). That adapter clamps reads to
  `max(fromSeqnum, trimSeqnum)`, so the purge is unobservable *through reads* and no
  read-based test can kill the mutant.

  It is **not** dead code, though, and should not be deleted: it bounds the in-memory
  index, which is a listed cross-cutting concern. The mutant survives because it guards an
  invariant the tests do not assert — memory — rather than because the code does nothing.

  This does **not** generalise to `InMemoryPersistenceAdapter`, which has no trim point at
  all (no `trimSeqnum`, no read-time clamp): there the removal *is* the enforcement, and
  deleting it would make reads return trimmed entries. Its own surviving mutant on `trim`
  is a different one (a negated conditional).

**When raising the threshold, recompute the floor** — it moves with the denominator. At 553
mutations and a threshold of 76 the floor is 418 (418 → 76 passes, 417 → 75 fails). Stale
arithmetic here already caused one bad comment.

### Batching decorator — design spike

`BatchingPersistenceAdapter` produced 4 of 16 defects. It re-implements read-merging,
version assignment, flush retry and dedup — but group commit is already a property the file
adapter *has*: it owns the WAL and the fsync. Worth spiking whether group commit folds into
the durable adapters and the decorator disappears. That deletes a whole category rather than
fixing four things. Bigger call than it sounds — it removes a public class.

### ~~GitHub Actions are pinned to mutable tags~~ — done in 0.4.0

All three are pinned to commit SHAs across both jobs, taken to the current majors while
pinning (`checkout` v7.0.1, `setup-java` v5.6.0, `upload-artifact` v7.0.1), which also clears
the Node 20 deprecation warning the runner had begun emitting. Dependabot rewrites the
trailing version comment, so the pins will not rot.

The three Dependabot PRs proposing those bumps (#15, #16, #17) are superseded and can be
closed.

---

## 5. Cross-cutting, from the report's Part 3

Untouched, and all still true.

- **In-memory index growth.** `globalIndex`, `tagSeqnums` and `kvStore` are fully in-memory
  with an entry per record. For a log meant to live forever this is unbounded heap
  proportional to total history. Needs a plan — spillable index, or a bounded cache over an
  on-disk one — before any large deployment.
- **Trim versus permanent history.** `trim(upToSeqnum)` is honoured by `readByTag` via
  `effectiveFrom = max(fromSeqnum, trimSeqnum)`. **This is dangerous for Catalyst**: replay
  requires the whole stream from version 0, so trimming a tag silently destroys
  replayability for those executions. Either trim needs to be tag-aware with an opt-out, or
  clients need a way to mark a tag non-trimmable.
- **Snapshot interaction.** Trimming is safe when a snapshot covers the trimmed prefix and
  unsafe without one. That relationship should be explicit in the API rather than left to
  each client to get right.

---

## Suggested order

1. **Tag 0.4.0** once it merges, and check JitPack builds it before pointing Catalyst at it
2. **Commit Catalyst's coordinate change**, then **fix its D4** (§0, §1) — the live
   corruption that started all of this, and the reason the coordinate had to be settled first
3. **A4 capabilities** (§2) — cheap, and stops the next caller assuming the wrong guarantee
4. **Fault-injection harness** (§4) — before D2, which is its first customer
5. **D2** non-clobbering index
6. **Batching decision** (§3) and/or the decorator spike (§4) — same subject, take together
7. **A6** ergonomics (~~A3~~ shipped in 0.4.0, out of order — see §2)
8. **Multi-tag versions** last — the only item with a log-migration cost

~~Push the 0.3.0 tag~~ and ~~pin the GitHub Actions~~ are done; both were §0/§4 items that
blocked or shadowed everything else.

---

## Process notes

Three hazards that cost real time here, recorded so they are not rediscovered.

- **A stacked PR merged after its base has already gone to `main` leaves its content
  behind**, while GitHub reports it as merged. This happened to the `streamVersion` rename
  (#9 merged into #8's branch after #8 had already merged to `main`) and needed #12 to
  recover. Land the base first and **retarget** the stacked PR to `main` rather than merging
  it into its original base.
- **Review comments arrive after replies.** Re-read open threads before declaring a PR
  clear; a fifth P1 on #13 sat unnoticed because the previous fetch was treated as final.
- **A green test is not evidence the test works.** Two tests here passed against the very
  defect they were written for — most memorably one asserting on `readAll()`, which could
  not see duplicate records because both durable adapters deduplicate by seqnum on read.
  Revert the fix and re-run before trusting a new test. The mutation gate now automates part
  of this, but only for code inside its scope.
