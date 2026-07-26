# Failure semantics

What every mutating operation leaves behind when it fails.

## Why this document exists

Gumbo's write paths were correct on success and unspecified on failure. That gap produced
a run of defects that all had the same shape — an operation that can partially complete,
with no stated answer for what a partial completion leaves behind:

| Seam | What went wrong |
|---|---|
| `open()` part-way | Channels opened before the failure leaked; `close()` returned early because `open` was still false |
| backlog → live subscription | Entries appended during the handover reached the subscriber by neither route |
| pending → durable (flush) | First the entries were discarded on failure; then, once retained, the persisted prefix was rewritten |
| claim version → write | A version was consumed for an entry that never landed |
| `close()` → stopped | The listener kept running after `close()` returned |

In each case the invariant held on both sides of the seam and broke in the middle, and the
behaviour was decided by which statement happened to come first rather than by a decision
anyone made. `pendingBatch.clear()` before or after `delegate.appendBatch()` is the whole
difference between losing data and duplicating it, and nothing recorded which was intended.

This document records the intent, so the next change to a write path has something to be
consistent with.

## The failure axis

Every mutating operation answers one question: **what is durable if this throws?**

| Outcome | Meaning | Caller's obligation |
|---|---|---|
| `NOTHING` | The operation had no durable effect | Retry is safe |
| `PREFIX` | An unspecified prefix landed; the boundary is discoverable | Reconcile before retrying, or retry only the remainder |
| `UNKNOWN` | Cannot be determined from here | Treat as in-doubt; read back before deciding |

`PREFIX` is not a weaker `NOTHING` — it is a different contract, and a caller written for
one is wrong against the other. The batching adapter assumed `NOTHING` and rewrote data
that had already landed.

## `PersistenceAdapter`

| Operation | On failure | Notes |
|---|---|---|
| `open()` | `NOTHING` | Unwinds fully: channels closed, directory lock released. A failed open leaves nothing to clean up and the directory reopenable. |
| `close()` | — | Idempotent. Releases what it holds even if flushing fails. |
| `append(LogEntry)` | `NOTHING` \| `PREFIX` | File: a torn record may reach the tail of `log.dat`; the recovery scan stops at it, so it is invisible to readers but occupies space. FDB: `NOTHING` — the transaction either commits or does not. |
| `appendBatch(List<LogEntry>)` | **`PREFIX`** | File writes entry by entry with no rollback. FDB chunks large batches and can commit an earlier chunk before a later one fails. **Callers must not assume all-or-nothing.** The boundary is `getLatestSeqnum()`: at or below it is durable. A failed *sync* on the file adapter is `NOTHING` — none of the batch is published, so the whole batch is retried. |
| `append(PendingAppend, expectedVersion)` | `NOTHING` | The version is assigned in the same operation as the write, so a failure consumes no version — except on the batching adapter, where see below. |
| `appendBatchAssigningVersions(...)` | `PREFIX` | As `appendBatch`. Versions are assigned in list order. |
| `trim(upToSeqnum)` | `NOTHING` | The trim point is written to a temp file and atomically renamed, so a crash mid-trim leaves the old point intact. |
| `setTagValue` / `deleteTagValue` | `NOTHING` \| `PREFIX` | Appended to a sidecar; a torn record is skipped on replay, so the key keeps its previous value. |

### `getLatestSeqnum()` is a durability boundary

Not a visibility one. It reports the highest seqnum that is **durable**, and an adapter
must not advance it for an entry whose write has not been made durable yet.

This is load-bearing rather than pedantic: it is how a caller works out how much of a
failed batch actually landed. The file adapter used to publish entries to its in-memory
index at write time, before the fsync — so a failed sync was indistinguishable from a
successful write, and the entries were dropped from the retry that was their last chance.
Visibility now follows durability there.

A third-party adapter that advertises written-but-not-durable entries here will silently
lose data on the retry path, and nothing will report it.

## `BatchingPersistenceAdapter`

The decorator defers durability, not identity. Two consequences worth stating plainly:

- **An `AppendResult` is not a durability receipt.** A version is assigned and returned at
  append time; the entry reaches storage at the next flush. A crash in between loses it.
  This is the documented trade-off of the adapter, not a defect — but it means a caller
  that treats the returned version as proof of durability is wrong.
- **A failed flush retains what it could not write, and drops what it could.** The
  delegate's `getLatestSeqnum()` names the boundary, since entries flush in seqnum order.
  If the delegate cannot answer, nothing is dropped: a duplicate is recoverable by
  inspection, a discarded entry is not.

Still open, and deliberately so — these need a decision about what the adapter promises
under sustained write failure, not a patch:

- The pending buffer is unbounded, so a persistently failing delegate grows it without
  limit.
- `flushQuietly` logs and swallows, so a caller holding an `AppendResult` is never told
  its entry has not landed.

## `SharedLog`

| Operation | On failure | Notes |
|---|---|---|
| `append(request)` | As the adapter | Wrapped in `LogWriteException`. |
| `append(request, expectedVersion)` | `NOTHING` | A `VersionConflictException` means the append was rejected, not attempted-and-failed. No version is consumed; the tag is at `actualVersion()`. |
| `subscribe(...)` | — | Nothing is lost at the backlog→live handover: the subscription is registered before the backlog is read, so live entries queue rather than being dropped. |
| `Subscription.close()` | — | Returns once the listener has stopped, or after the timeout having interrupted it. A listener that neither returns nor honours interruption cannot be stopped — that is the JVM's limit, not this API's. |

## Where the tests are weakest

Mutation testing (`mvn test-compile org.pitest:pitest-maven:mutationCoverage`) puts the
surviving mutants overwhelmingly in the paths described above — the ones that only run
after something has already gone wrong:

- `tryLoadGlobalIndex` — the torn-index check survived every mutation, including replacing
  the modulus with a multiplication. Nothing verified that a partially written index is
  noticed rather than partially trusted.
- `loadKvFile` — every partial-record boundary check survived. The torn-KV-record path was
  entirely unexercised.
- `trim` — the delegate call could be deleted from the batching adapter with every test
  still passing. Trim is the one operation that destroys data.

The first and third are now covered by `RecoveryAndTrimTest`. The rest stand as the
backlog, and the mutation threshold in `pom.xml` stops the number going backwards: it is
set to the measured score, not below it, so a decline fails the build.

Two classes of mutant here are **not** worth killing with unit tests, and are recorded so
nobody wastes effort on them:

- **`removed call to FileChannel::force`.** A clean close-and-reopen sees the data via the
  page cache whether or not it was fsynced, so no in-process test can distinguish them.
  Catching this needs real crash injection — kill `-9` between write and read — which is
  the fault-injection harness, not a unit test.
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
