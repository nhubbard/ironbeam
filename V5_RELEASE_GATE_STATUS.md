# Ironbeam 5.0.0 release gates

**Decision: hold the release.** Reviewed 2026-10-11 on
`codex/ironbeam-v5-runner-core`.

The root source runs v5, but the root package is still `4.1.1` and the v5
crates are `0.1.0`. Publication remains staged locally. Local Prism is the
portable runner proof for this release; hosted Dataflow is a later extension.

## Release snapshot

| Area | Where it stands | Next proof |
| --- | --- | --- |
| Workspace | V5-only root source, including with no default features; 28 local packages verify after legacy cloud retirement | Finish public API audit |
| Direct | V4 reference corpus and v5 helper tests pass | Close public API and edge-case parity |
| Portable | 18 live local Prism tests pass | Expand shared Direct/Prism corpus |
| Streaming | A split and accepted batches survive separate worker process restarts in the Fn Data fixture; the production reader also resumes through join and co-group | Prove live Prism worker restart and output recovery |
| Quality | Coverage floors, formatting, and package checks pass | Set performance limits and run final clean matrix |

Passing a current check does not close a release gate unless its full proof
below is complete.

## Open gates

### Behavior and API

| Gate | Done when |
| --- | --- |
| ☐ Public API and v4 behavior | The root API and promised behaviors are audited against the [helper tracker](V5_HELPER_PARITY_TRACKER.md) and [migration guide](V4_TO_V5_MIGRATION.md). |
| ☐ Portable operation coverage | Every advertised operation has shared Direct/Prism success and failure cases, including multi-input and output paths. |
| ☐ Runner agreement | Shared cases match on values, grouping, event times, windows, typed failures, and committed output. |

### Streaming and durability

| Gate | Done when |
| --- | --- |
| ☐ Unbounded recovery | Extend the passing Fn Data process restart proof to a supervised live Prism restart and cover the remaining graph shapes. |
| ☐ Output durability | The runner-wide commit contract passes successful retry and restart proofs, all production publishers require the journal manifest, and v4 checkpoint-policy replacements are decided. A live failed bundle already proves abort cleanup. |

### Packaging and release quality

| Gate | Done when |
| --- | --- |
| ☐ Coverage and verification | Uncovered contract paths are reviewed; clean tests, coverage, lint, docs, and package checks pass. |
| ☐ Performance | Acceptable latency and memory limits are set and pass on the paired v4/v5 corpus. |
| ☐ Versioning | The packages receive their intended release versions after the other gates pass. |

## Completed gates

| Gate | Evidence |
| --- | --- |
| Formatting and current suites | Both workspaces pass formatting, workspace tests, and the live Prism suite from their last full runs. |
| Cloud package cleanup | Contracts, fakes, operations, objects, provider clients, and JSONL readers have separate owners. The unused legacy cloud crate is removed; all 28 packages verify locally. |

## Latest measured results

| Check | Result |
| --- | --- |
| Root workspace | Tests and doctests passed after the CoGroupByKey event-time fix. |
| Local release script | Dependency and feature checks, tests, docs, and verification of 28 packages passed with Mr Boxington after legacy cloud retirement. |
| Cloud operation coverage | **186/261 source locations** and **34/42 branches** after bounding parallel helper concurrency; both CI ratchets pass. Uncovered utility paths remain in the quality review. |
| JSONL coverage | All-feature package: **407/435 source locations** and **38/40 branches** after eager cloud helper migration. |
| Local Prism | 19 live tests passed across the prior suite and a new failed-bundle proof. A production reader resumed from a residual cursor; join and co-group committed five exact rows across two windows. The multi-source final-result job binds all ten finalized format publishers to completed journal bundle identities and rejects any missing or extra accepted attempt. A separate job failed after staging and left no fragment, journal completion, or published output. |
| Portable worker restart | Fn Data test passes across three OS processes: pending split, first accepted batch, final batch; the full harness suite and warning-free lint pass. |
| Separate Dataflow workspace | Tests and doctests passed; changed proof package passed warning-free lint. |
| Clean coverage | Core **96.13% locations / 88.64% branches**; Direct **93.46% / 90.19%**; transforms **97.81% / 93.10%**. Existing floors pass. |
| Postcard coder | 13/13 source locations covered; LLVM reports no instrumented branches. |
| Formatting | Both workspaces pass `cargo fmt --all --check`. |

The last full paired benchmark matched output counts and checksums in all
seven cases. It is a measurement, **not a performance gate pass**:

| 100,000-row workload | V5/v4 median latency | Peak memory, v4 → v5 |
| --- | ---: | ---: |
| Map/filter | 3.12× | 3.83 → 3.55 MiB |
| JSONL round trip | 2.63× | 4.33 → 5.05 MiB |
| Duplicate-key join | 2.18× | 5.53 → 31.66 MiB |

After a small Direct allocation change, a targeted duplicate-key rerun peaked
at **29.91 MiB** in v5. The [full seven-case measurement](V5_PHASE0_BASELINE.md)
and repeatable comparison command are in the baseline record. A fresh full
run is needed before performance can be judged against release limits.

## Contract decisions

| Topic | Decision |
| --- | --- |
| Cross-runner parity | The same advertised graph and input must give the same values, grouping, event times, windows, typed failure category, and committed output. |
| Execution differences | Bundle size, scheduling, and retry timing may differ. Unsupported operations fail validation before a source or sink opens. |
| Ordered folds | Portable folds require an explicit order key. V4 encounter-order methods remain Direct compatibility behavior. See the [API similarity contract](V5_REPLACEMENT_ROADMAP.md#api-similarity-contract). |

## Detail and history

| Document | Use it for |
| --- | --- |
| [Evidence archive](V5_RELEASE_GATE_EVIDENCE.md) | Implementation history, fixtures, measurements, and known limits |
| [Replacement roadmap](V5_REPLACEMENT_ROADMAP.md) | API and runtime scope |
| [API inventory](V5_PHASE0_API_INVENTORY.md) | V4 surface and feature mapping still used for the parity audit |
| [Baseline](V5_PHASE0_BASELINE.md) | Repeatable behavior and performance comparisons |

Publishing the packages remains a separate decision after these gates close.
