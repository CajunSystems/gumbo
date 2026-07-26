# Open work

Written as a handover. Everything below is either not started or deliberately deferred,
with enough evidence attached that it can be picked up cold.

Context for all of it: [Catalyst's requirements
report](https://github.com/CajunSystems/catalyst/blob/main/docs/gumbo-requirements.md),
which catalogued defects and missing primitives in the log layer. Items are referenced by
its numbering (D1–D4, A1–A6).

## Where things stand

**0.3.0 is cut and merged** (`47db570`). Shipped: D3 (directory lock), A2 (version-keyed
reads), A5 (`localId` → `streamVersion`), D1 + A1 (storage-owned versions + conditional
append), plus a subscription-delivery rewrite and a mutation-testing gate that were not on
the report's list.

See [`FAILURE_SEMANTICS.md`](FAILURE_SEMANTICS.md) for the write-path contracts, which
several items below depend on.

---

## 0. Release: the 0.3.0 tag is not pushed

**Blocks everything downstream.** JitPack builds from tags, and the tag does not exist.

```
git tag 0.3.0 47db570
git push origin 0.3.0
```

Lightweight, matching the `0.2.0` convention (`git cat-file -t 0.2.0` → `commit`).

This could not be done from the session that cut the release: `git push` of a tag ref is
refused by that environment's git proxy (`--dry-run` reports `[new tag] 0.3.0 -> 0.3.0`,
the real push disconnects), the GitHub MCP surface has no `create_tag`/`create_release`,
and the REST API returns `403 Write access to this GitHub API path is not permitted`.
Branch pushes worked throughout, so it is tag refs specifically.

Verified before tagging: `mvn install` at `47db570` produces `gumbo-0.3.0.jar`, 212 tests
pass, and the PIT plugin has **no `<executions>` binding** so it will not run during
JitPack's build.

After pushing, force a build before pointing anything at it — the first fetch triggers it,
and a failure means the tag is already public and you need a `0.3.1`:

```
curl -s https://jitpack.io/com/github/CajunSystems/gumbo/0.3.0/build.log | tail -20
```

### Coordinates need checking

`pom.xml` declares `com.cajunsystems:gumbo`; the README install snippets say
`com.github.CajunSystems:gumbo` (JitPack rewrites coordinates). Catalyst currently depends
on `com.cajunsystems:gumbo:0.2.0`, which therefore is **not** resolving from JitPack — most
likely a local `mvn install`. Settle which coordinate Catalyst should use before bumping it,
or the D4 fix will appear to land and then fail to resolve on a clean machine.

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

### A3 — KV compare-and-set

```java
boolean compareAndSetTagValue(LogTag tag, String key, byte[] expected, byte[] value);
boolean putTagValueIfAbsent(LogTag tag, String key, byte[] value);
boolean deleteTagValueIf(LogTag tag, String key, byte[] expected);
long incrementTagValue(LogTag tag, String key, long delta);
```

The KV already exists and is load-bearing (Catalyst's idempotency index and snapshots). CAS
turns it into a coordination substrate — leases, ownership records, work claims — with no
new subsystem. With A1 shipped, clock skew on lease expiry is now an *efficiency* problem
(two nodes briefly duplicate work) rather than a *correctness* one (the log rejects the
loser), so a stored `expiresAt` compared by claimants is sufficient; native TTL is not
required.

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

`mvn test-compile org.pitest:pitest-maven:mutationCoverage`. Currently **421–422 of 553
killed**, threshold ratcheted at 76.

The distribution is the useful part: survivors cluster in code that only runs *after
something has already gone wrong*, which is the same distribution as the defects found in
review, arrived at independently. Still uncovered:

- **`loadKvFile`** — every partial-record boundary check survives. The torn-KV-record path
  is entirely unexercised.
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

### GitHub Actions are pinned to mutable tags

`.github/workflows/ci.yml` uses `actions/checkout@v4`, `setup-java@v4`, `upload-artifact@v4`.
An upstream tag repoint changes what CI executes with no change here. Pin all three to
commit SHAs — **across both jobs**, since pinning only one is security theatre.

This could not be done from the session that raised it: resolving the SHAs needs read access
to `actions/*`, outside that session's repo scope. `.github/dependabot.yml` is already in
place so the pins will not rot once applied — and Dependabot has already opened PRs bumping
these actions, which is the natural moment to pin them.

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

1. **Push the 0.3.0 tag** and verify JitPack builds (§0) — blocks everything downstream
2. **Fix Catalyst's D4** (§1) — the live corruption that started this
3. **A4 capabilities** (§2) — cheap, and stops the next caller assuming the wrong guarantee
4. **Fault-injection harness** (§4) — before D2, which is its first customer
5. **D2** non-clobbering index
6. **Batching decision** (§3) and/or the decorator spike (§4) — same subject, take together
7. **A3**, then **A6** ergonomics
8. **Multi-tag versions** last — the only item with a log-migration cost

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
