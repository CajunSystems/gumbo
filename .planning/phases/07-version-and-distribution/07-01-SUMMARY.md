# Phase 7 Plan 1: Version + Distribution Summary

**Bumped pom.xml to 0.2.0, created jitpack.yml (openjdk21), added README Installation section with Maven/Gradle snippets, and created CHANGELOG.md documenting all Phase 1–6 changes; full suite passes (129 tests, 0 failures).**

## Accomplishments

- `pom.xml`: `1.0.0-SNAPSHOT` → `0.2.0`
- `jitpack.yml`: created at repo root with `jdk: [openjdk21]` — required because JitPack defaults to Java 8/11
- `README.md`:
  - TOC updated from 13 → 14 entries; new entry `13. [Installation](#installation)`
  - New `## Installation` section before `## Building` with Maven repo + dependency XML and Gradle snippets using `com.github.CajunSystems:gumbo:0.2.0`
  - Package command fixed: `sharedlog-1.0.0-SNAPSHOT.jar` → `gumbo-0.2.0.jar`
- `CHANGELOG.md`: new file at repo root following Keep a Changelog format; `[0.2.0]` entry documents all additions from Phases 1–6 (readFrom, getLatestSeqnum, subscribeTail, KV API, TypedLogView, ActorCheckpointExample) and the version bump/getLatestSeqnum optimization

## Files Created/Modified

- `pom.xml` — version bumped to 0.2.0
- `jitpack.yml` — Java 21 build spec (new)
- `README.md` — Installation section + TOC update + package cmd fix
- `CHANGELOG.md` — new file documenting all 0.2.0 changes

## Decisions Made

No deviations from plan.

## Issues Encountered

None.

## Git Commits

- `931d8d5` — `chore(07-01): bump version to 0.2.0 and fix README package cmd`
- `f8d2b64` — `feat(07-01): add JitPack distribution config and Installation section to README`
- `89167d7` — `docs(07-01): add CHANGELOG.md documenting all 0.2.0 changes`

## Next Step

Phase 7 complete. All 7 phases done.

To publish: create a `0.2.0` git tag and push — JitPack will build automatically.
```bash
git tag 0.2.0
git push origin 0.2.0
```
