# Gumbo — notes for Claude

Gumbo is a **shared log for the JVM** (Java 21, single Maven module, `com.cajunsystems.gumbo`),
modelled on Boki (SOSP 2021): one physical log, many logical streams addressed by `LogTag`, with
pluggable persistence adapters and a pluggable sequencer. It is the storage layer under
[Catalyst](https://github.com/CajunSystems/catalyst), a durable AI execution runtime, and most
of the work since 0.3.0 has been driven by Catalyst's
[requirements report](https://github.com/CajunSystems/catalyst/blob/main/docs/gumbo-requirements.md)
(items `D1`–`D4`, `A1`–`A6`). Read that report before touching the write path; it is the
reasoning behind most of what is below.

## Build & test

```bash
mvn verify                                   # build + full suite (no FoundationDB needed)
mvn -Dtest=ConditionalAppendTest test        # one class
mvn test-compile org.pitest:pitest-maven:mutationCoverage   # mutation gate (persistence + service)
scripts/fdb-tests.sh                         # the FoundationDB suite against a real cluster, in Docker
```

**FoundationDB tests skip without a cluster.** The `FoundationDB*Test` classes probe for one
in `@BeforeAll` (default cluster file, or `FDB_CLUSTER_FILE`) and skip when it is unreachable,
so a plain `mvn verify` never runs them. CI's `fdb` job runs them for real against a service
container with `GUMBO_REQUIRE_FDB=true`, under which an unreachable cluster is a failure, not
a skip (`FdbTestSupport`). `scripts/fdb-tests.sh` does the same locally in Docker. **A green
run that skipped them proves nothing about FDB** — check the surefire report says
`Skipped: 0` before believing it.

**Release = a PR that bumps the pom version, dates the CHANGELOG entry
(`## [X.Y.Z] — YYYY-MM-DD`) and updates the README coordinates.** When it merges,
`.github/workflows/release.yml` tags main (lightweight, the convention), publishes a GitHub
Release with that CHANGELOG section and the jars, and waits for JitPack to build the tag.
JitPack builds from tags, under the groupId `com.github.CajunSystems` (not the
`com.cajunsystems` this pom declares). The workflow exists because 0.4.0 sat merged and
untagged for four weeks, unreachable from every downstream build; a merged release is not a
released one. Check the Release run went green before telling a downstream to bump — and
never push a tag by hand for a version whose CHANGELOG entry is still `[Unreleased]`.
`main` is a locked branch: merges need the admin bypass.

## Architecture (where things live)

- `api/` — `SharedLog` (the log), `LogView` (a tag-scoped window: version-keyed reads, the
  tag KV, subscribe), `TypedLogView` (Kryo-serialised `LogView`), `Executor`.
- `core/` — value types. `LogEntry` (seqnum + a version *per tag* + tags + bytes),
  `AppendRequest`/`AppendResult`/`PendingAppend`, `LogCapabilities`, `LogTag`,
  `VersionConflictException`, `StreamVersions`.
- `service/` — `SharedLogService`: one `ReentrantLock` serialises *seqnum claim + adapter
  write*; reads are lock-free; each subscription owns one virtual thread that drains its own
  queue in seqnum order. `DefaultLogView`, `ExecutorEngine`, `SharedLogConfig`.
- `persistence/` — `PersistenceAdapter` and the four shipped adapters: `InMemory`, `FileBased`
  (WAL + index + `kv.dat`, exclusive directory lock), `Batching` (group-commit decorator),
  `FoundationDB` (every write is one serialisable FDB transaction; the only adapter that
  declares `multiWriter`).
- `sequencer/` — `Sequencer`: `LocalSequencer` (per-process `AtomicLong`, reseeded from
  `getLatestSeqnum()` on open) and `FoundationDBSequencer` (one counter key in FDB;
  `distributed() == true`).
- `serialization/` — `LogSerializer`, `KryoLogSerializer`.

Docs: `README.md` is the reference (concepts, API, each adapter, examples — the examples under
`src/test/java/.../examples` are what the README quotes); `docs/FAILURE_SEMANTICS.md` is the
write-path contract per operation (`NOTHING` / `PREFIX` / `UNKNOWN` on failure);
`docs/OPEN-WORK.md` is the handover: what is deferred, why, and the process hazards already
paid for; `CHANGELOG.md` carries the reasoning for every release. `.planning/` is a historical
planning directory from the 0.2.0 era and is not maintained.

## Key invariants

- **Two number spaces.** `seqnum` is global, assigned by the `Sequencer` before the adapter is
  called. `streamVersion` is **per tag, dense from 0, and storage-owned**: the adapter assigns
  it inside the same operation that persists the entry, never a counter in the service. The
  two coincide only while a log holds one tag — which is why every test with one tag in a
  fresh log is blind to the defects that matter (D1, D2, D4 all hid there). **Put a second tag
  in the log.**
- **Reads say which space they key on, and order follows the key.** `readByTag` /
  `LogView.readFrom` / `readAll` / `readAfter` are seqnum-keyed and return seqnum order.
  `readFromVersion` / `readAfterVersion` are version-keyed, return version order, and are
  what a per-stream cursor wants. Don't blur them; D4 was exactly that blur, and #42 was a
  `readAll()` fold that assumed list order was stream order under `multiWriter`.
- **The fence is decided by storage, or not offered.** `append(pending, expectedVersion)` must
  compare and increment atomically in the store. An adapter that cannot throws
  `UnsupportedOperationException` for a non-`ANY_VERSION` expectation; it never falls back to a
  non-atomic check. `compareAndSetTagValue` follows the same rule, and the other conditional
  KV forms are defined over it. A conditional append on a multi-tag request must **name** the
  fenced tag (`append(request, fencedTag, expectedVersion)`); the fence and the reported
  version follow that tag only.
- **Every tag an entry carries gets its own position** (0.6.0). A multi-tag append advances
  each of its tags by one. `AppendResult` reports the version of the tag the append was
  *addressed to*, never `tags.iterator().next()` — `Set` iteration order is salted per JVM.
  Records written before 0.6.0 hold one version and answer every tag with it
  (`hasPerTagVersions() == false`); both layouts coexist in one log via a per-record marker
  (file magic `0xC0FFEE43`; FDB leading `-1L`). **No migrations:** the next layout change
  gets a marker too.
- **Versions are stored, not derived**, because `trim` purges index entries and a derived
  rank would renumber every survivor and invalidate every stored cursor.
- **A failed append keeps the position it claimed.** A failed fsync does not mean the record
  is gone (`FailedWriteKeepsItsPositionTest` measures it), so releasing the position risks two
  entries at one version. A hole is the smaller loss and heals on reopen; a duplicate is
  silent. The reasoning is on `claimVersions` and in the 0.6.0 CHANGELOG entry.
- **`getLatestSeqnum()` is a durability boundary**, not a visibility one: at or below it is
  durable. Never advance it for a write that has not been made durable. The tag KV follows
  the same rule (`kv.dat` is synced before the in-memory value is published). **Under
  `multiWriter` it is a high-water mark of committed seqnums, not a durable prefix** — another
  writer's lower seqnum may still be in flight — so a `PREFIX` reconcile there reads its own
  seqnums back, or reasons in versions, which are always a dense prefix per tag.
- **Capabilities under-report, and every `true` is backed by a test.** `LogCapabilities` is
  answered per adapter; `SharedLogService` composes it — adding `pushSubscriptions` (delivery
  is the service's) and narrowing `multiWriter` to `storage.multiWriter() &&
  sequencer.distributed()`, because a cross-process version fence cannot rescue a colliding
  seqnum. `BatchingPersistenceAdapter` forces `multiWriter` off however capable its delegate
  is (the version is claimed at `append`, the write lands at flush). A capability flipped
  without a behaviour change must fail `LogCapabilityHonestyTest`.
- **Single-writer adapters refuse a second writer loudly.** The file adapter takes an
  exclusive directory lock (`LogAlreadyOpenException`). Silent co-writers are the failure
  mode everything above exists to prevent.
- **Subscriptions never lose or reorder.** Entries are enqueued under the write lock, drained
  by one virtual thread per subscription, and a listener is never called concurrently with
  itself. `close()` waits for an in-flight listener, escalating to an interrupt, and gives up
  after 5 s rather than hang the log. Delivery is **per process**: on a `multiWriter` log,
  another process's appends are not pushed to this one's subscribers.
- **Version-keyed reads come back in version order.** Under one writer that is seqnum order;
  under two it is not, because a writer claims its seqnum before its transaction and gets its
  version inside it. A seqnum-keyed tail cursor can skip an entry under `multiWriter`; a
  version-keyed one cannot, since versions are assigned in commit order.
- **The FoundationDB adapter caches nothing per process.** `getLatestSeqnum()`,
  `getNextStreamVersion(tag)` and `getLatestSeqnumForTag(tag)` are point reads;
  `meta/latest` is raised with an atomic `BYTE_MAX` inside the write transaction. A local
  high-water mark was wrong under two writers and could move the stored value backwards.
- **A refused conditional append has already consumed its seqnum.** The sequencer is asked
  before the adapter is. Seqnums are an order, not a count; only `streamVersion` is dense.
- **Every FDB read-modify-write must be idempotent.** `db.run` retries on
  `commit_unknown_result` with the commit possibly already applied; run twice, a
  version-assigning append fails its own fence or writes a second record one version up, a
  compare-and-set tells its winner it lost, an increment applies twice, the sequencer skips a
  number. Appends check for their own record at the pending's seqnum (`alreadyCommitted`); a
  different record there is a sequencer collision and is refused, never overwritten. The KV
  conditional writes and `FoundationDBSequencer.claim` record an `OperationToken` and the
  result under `readVersion/token` in the same transaction — one record per attempt, never a
  slot per key, since another writer in between would overwrite a slot — and a retry looks
  up its earlier attempts. A new FDB transaction that reads then writes gets the same
  treatment.

## Tests (keep green if you touch the engine)

- `ConditionalAppendTest`, `VersionKeyedReadTest`, `TagValueCompareAndSetTest`,
  `TagValueCoordinationTest` — the D1/A1/A2/A3/A6 contracts, parameterised over the in-memory,
  file and batching adapters.
- `LogCapabilityHonestyTest` — each capability exercised in both directions, branching on the
  declaration rather than hard-coding a per-adapter expectation. `LogCapabilityCompositionTest`
  — the service's `multiWriter` composition.
- `RecordFormatCompatibilityTest` — pre-0.6.0 records still read. `FailedWriteKeepsItsPositionTest`
  — the premise behind keeping a failed append's position. `RecoveryAndTrimTest`,
  `FileBasedPersistenceAdapterLockTest`, the `Batching*` tests, `SubscriptionDeliveryTest`.
- **FoundationDB** (needs a cluster; see above): `FoundationDBPersistenceAdapterTest` (one
  adapter), `FoundationDBMultiInstanceTest` (two adapters on one root, in one JVM, interleaved
  by hand: read-through metadata, the `meta/latest` raise, the cross-instance fence, D2 does
  not apply) and `FoundationDBMultiWriterTest` — **two JVMs**, each its own `SharedLogService`
  + `FoundationDBPersistenceAdapter` + `FoundationDBSequencer` on one cluster and root,
  racing through `MultiWriterPeer`'s script. It is the only place `multiWriter` is measured
  rather than declared. Its first run found two defects (the conflict exception never
  surfaced on FDB; version-keyed reads were not in version order), so keep it honest.
- **Mutation gate** (`pom.xml`, `mutationThreshold`): a ratchet set to the *low* observed run,
  not the best one, because what times out varies run to run and a timed-out mutant counts
  as killed. When the score rises durably, raise it and recompute the floor — it moves with the
  denominator. Two survivor classes are deliberately not worth killing (`FileChannel::force`,
  the file adapter's trim purge); `docs/OPEN-WORK.md` §4 says why.
- **A green test is not evidence the test works.** Two tests here passed against the very
  defect they were written for. Revert the fix and re-run before trusting a new test.

## Process notes (each cost real time once)

- **Stacked PRs:** land the base first and retarget the stacked PR to `main`. Merging a stacked
  PR into a base that has already merged leaves its content behind while GitHub reports it
  merged (#9 into #8).
- **Review comments arrive after replies.** Re-read open threads before declaring a PR clear.
- **A dependency bump is not a fix until the caller moves.** 0.3.0 *added* `readAfterVersion`;
  Catalyst stayed wrong until it called it. When a release fixes a downstream defect, say in
  the CHANGELOG what the downstream has to change.
- **Measure, don't infer.** Every defect in the requirements report was found by a probe, and
  several of this repo's tests exist because reading the code gave the wrong answer. If a
  property matters across processes, test it across processes.
- Commit messages here carry the reasoning, not just the change — read `git log` for the
  shape. The CHANGELOG entry for a release is written with the same care.
