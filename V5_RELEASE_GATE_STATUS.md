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
| Portable | 19 live local Prism tests and the separate native Arrow proof pass with completed-attempt manifests | Expand shared Direct/Prism corpus |
| Streaming | A split and accepted batches survive separate worker process restarts in the Fn Data fixture; the production reader also resumes through join and co-group | Prove live Prism worker restart and output recovery |
| Quality | Corrected branch accounting passes the core, Direct, and transforms floors after a paired-stream progress test; focused tests and strict lint pass | Audit uncovered contracts, reconcile the earlier coverage denominator, and set performance limits |

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
| ☐ Output durability | The runner-wide commit contract passes successful retry and restart proofs, and checkpoint retention/recovery limits are resolved. V5 batch/progress checkpoints are the approved replacement for v4's node, barrier, and time schedules. Shared final-result publication requires an exact attempt manifest from the completed journal. A live failed bundle already proves abort cleanup. |

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
| Root workspace | Tests and doctests passed after the eager JSONL and shared file-source changes. |
| Local release script | Dependency and feature checks, workspace and feature tests, docs, and verification of all 28 packages passed with Mr Boxington on 2026-10-11 after mandatory final-result manifest binding. Nothing was published. |
| Cloud operation coverage | **186/261 source locations** and **34/42 branches** after bounding parallel helper concurrency; both CI ratchets pass. Uncovered utility paths remain in the quality review. |
| JSONL coverage | All-feature package: **407/435 source locations** and **38/40 branches** at the last coverage run. The latest eager reader change has passing default/all-feature tests and strict lint; coverage has not been rerun. |
| Local Prism | 19 live tests passed across the prior suite and a new failed-bundle proof. A production reader resumed from a residual cursor; join and co-group committed five exact rows across two windows. The provider-neutral harness coordinator requires `DONE`, validates the graph's ten final sinks against the whole-job journal and committed row metrics, then supplies exact attempt IDs and counts to all ten publishers. A separate job failed after staging and left no fragment, journal completion, or published output. |
| Fresh Prism regression | The full 19-test live corpus and separate native Arrow proof passed again on 2026-10-11 against Prism built from the local Beam clone, after the publication-plan guard was added. |
| Final-result manifest binding | All shared final-result publishers reject an unbound store, including empty output. The live Prism corpus and Arrow proof passed after their stores were bound to the completed job journal's exact attempts. Local format tests and strict lint passed. |
| Portable worker restart | Fn Data test passes across three OS processes: pending split, first accepted batch, final batch. A separate final-result proof now checks that the journal refuses publication after staging and a wrong-build restart, then supplies the exact completed attempt after recovery and a duplicate callback. The full harness suite and warning-free lint pass. |
| Side-input runner agreement | The same bounded, windowed side-input graph now runs on Direct and live Prism. Their sorted output values match; Prism also matches the fixture's exact three rows. |
| Checkpoint policy decision | The [migration guide](V4_TO_V5_MIGRATION.md#checkpoint-policy-migration) maps v4 settings to current v5 Direct behavior. The user chose v5 batch/progress checkpoints as the 5.0.0 replacement for node-count, elapsed-time, and graph-barrier scheduling. Configurable history and fallback remain open. |
| Direct checkpoint publication | Bounded checkpoint data and the replacement manifest each sync their directory entry before old checkpoint data can be removed. Recovery now classifies a missing data file as corrupt checkpoint state. Focused Direct checkpoint/recovery tests and strict lint pass. Power-loss fault injection remains part of the durability gate. |
| Parallel I/O facade | The root `parallel-io` flag now forwards to each optional format crate only when that format is enabled. An isolated Avro facade test writes and reads a parallel container; the parallel flag alone does not pull in optional formats. |
| Separate Dataflow workspace | The full harness suite and doctests pass after publication-plan validation rejects a Beam transform that requests finalization but is absent from the declared sink set. The changed harness passes warning-free lint. |
| Current coverage | A clean isolated 2026-10-11 run with untouched branch sites counted reports core **3,146/3,258 locations (96.56%) / 487/562 branches (86.65%)**, Direct **3,936/4,209 (93.51%) / 841/932 (90.24%)**, and transforms **1,163/1,189 (97.81%) / 108/116 (93.10%)**. All three packages pass their existing floors. |
| Paired streaming progress | A Direct unbounded source emits an empty progress poll, a data poll, and an empty completion poll to two idempotent sinks. Both sinks receive exactly one row; the final checkpoint retains the source cursor, event time, and completion. |
| Core aggregate contracts | Core and Direct tests and doctests pass; strict core lint passes. New cases cover malformed join inputs, corrupt portable shuffle groups, fold-session output and emitter failure, and rejection of extra runner inputs. |
| Postcard coder | 13/13 source locations covered; LLVM reports no instrumented branches. |
| Formatting | Both workspaces pass `cargo fmt --all --check`. |

The coverage checker previously dropped branch sites whose two outcomes had
zero hits, making the branch ratios look better than they were. It now counts
those sites as uncovered. The earlier reused build target reported **3,657**
core source locations and **4,644** Direct locations, with both line floors
failing. A second clean isolated run reproduced the smaller counts above.
The denominator difference
needs investigation before treating this as a stable CI result; the coverage
gate still requires an uncovered-contract review and a clean full CI run.

The last full paired benchmark matched output counts and checksums in all
seven cases. It is a measurement, **not a performance gate pass**:

| 100,000-row workload | V5/v4 median latency | Peak memory, v4 → v5 |
| --- | ---: | ---: |
| Map/filter | 2.94× | 3.83 → 3.58 MiB |
| JSONL round trip | 2.01× | 4.33 → 4.45 MiB |
| Duplicate-key join | 2.09× | 5.53 → 21.42 MiB |

An adjacent comparison measured JSONL round trip at **10.227 → 8.177 ms**
and **5.06 → 4.44 MiB** in v5 after the eager reader and source-byte changes.
Counts and checksums matched. The [full seven-case measurement](V5_PHASE0_BASELINE.md)
and repeatable command are in the baseline record. Acceptable release limits
are still unset.

A subsequent compact inner-join path reduced equal-size join memory from
**45.97 to 39.54 MiB** and latency from **12.810 to 11.385 ms** in an adjacent
12-sample v5 comparison. The skewed duplicate-key case stayed near baseline
at **22.20 → 21.52 MiB** and **5.666 → 5.671 ms**. Output matched throughout.
Join memory remains an open gate against v4.

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
