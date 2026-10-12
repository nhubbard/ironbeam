# Ironbeam 5.0.0 release gates

**Decision: hold the release.** Reviewed 2026-10-11 on
`codex/ironbeam-v5-runner-core`.

The root source runs v5, but the root package is still `4.1.1` and the v5
crates are `0.1.0`. Publication remains staged locally. Local Prism is the
portable runner proof for this release; hosted Dataflow is a later extension.

## Release snapshot

| Area | Where it stands | Next proof |
| --- | --- | --- |
| Workspace | V5-only root source, including with no default features; the root facade and 28 workspace crates verify as local packages | Finish public API audit |
| Direct | V4 reference corpus and v5 helper tests pass | Close public API and edge-case parity |
| Portable | 25 live local Prism tests and the separate native Arrow proof pass with completed-attempt manifests | Expand shared Direct/Prism corpus |
| Streaming | A split and accepted batches survive separate worker process restarts in the Fn Data fixture; the production reader also resumes through join and co-group | Prove live Prism worker restart and output recovery |
| Quality | The exact CI coverage package set passes the core, Direct, and transforms floors after checkpoint recovery and planner tests | Audit uncovered contracts, reconcile the earlier coverage denominator, and set performance limits |

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
| ☐ Unbounded recovery | Extend the passing Fn Data process restart proof to a successful live Prism restart and cover the remaining graph shapes. A supervised process kill during the residual source open currently makes Prism fail the active job. |
| ☐ Output durability | The runner-wide commit contract passes successful retry and restart proofs and power-loss fault injection. Direct bounded and streaming checkpoints now have configurable retention and verified fallback. V5 batch/progress checkpoints replace v4's node, barrier, and time schedules. Shared final-result publication requires an exact attempt manifest from the completed journal. A live failed bundle already proves abort cleanup. |

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
| Root workspace | Tests and doctests passed with the current Direct inner-join implementation. |
| Root API composition | A new facade test builds two keyed sums, joins them, and collects the exact result through `ironbeam::v5::prelude` without the compatibility crate. It passes with default and no default features; strict lint passes. This exercises one public composition path, not the complete API ledger. |
| Local release script | On 2026-10-11, the complete script passed with Mr Boxington after the Direct join changes: dependency and feature checks, workspace and feature tests, docs, and local package verification for all 29 packages including the root facade. Nothing was published. |
| Cloud operation coverage | **186/261 source locations** and **34/42 branches** after bounding parallel helper concurrency; both CI ratchets pass. Uncovered utility paths remain in the quality review. |
| JSONL coverage | All-feature package: **407/435 source locations** and **38/40 branches** at the last coverage run. The latest eager reader change has passing default/all-feature tests and strict lint; coverage has not been rerun. |
| Local Prism | All 25 live tests pass, including bounded and unbounded per-window side-input cases, wait-on success/failure cases, and the supervised worker-loss failure case. A production reader resumed from a residual cursor; join and co-group committed five exact rows across two windows. The provider-neutral harness coordinator requires `DONE`, validates the graph's ten final sinks against the whole-job journal and committed row metrics, then supplies exact attempt IDs and counts to all ten publishers. A separate job failed after staging and left no fragment, journal completion, or published output. |
| Fresh Prism regression | The full 19-test live corpus and separate native Arrow proof passed again on 2026-10-11 against Prism built from the local Beam clone, after the publication-plan guard was added. |
| Final-result manifest binding | All shared final-result publishers reject an unbound store, including empty output. The live Prism corpus and Arrow proof passed after their stores were bound to the completed job journal's exact attempts. Local format tests and strict lint passed. |
| Portable worker restart | Fn Data test passes across three OS processes: pending split, first accepted batch, final batch. A separate final-result proof now checks that the journal refuses publication after staging and a wrong-build restart, then supplies the exact completed attempt after recovery and a duplicate callback. The full harness suite and warning-free lint pass. |
| Live worker loss | A process wrapper killed the worker as the production unbounded source reopened its residual cursor. Prism reported `FAILED` with active bundles, and the final-result coordinator refused publication. The existing production success fixture still passes. All 20 live Prism tests and strict fixture lint pass. This is a fail-closed result, not a successful in-job restart. |
| Side-input runner agreement | The existing `map_with_side_named` gives one global side view across fixed-window main and side branches on Direct and live Prism. The new `map_with_windowed_side_named` composes a per-window co-group; both runners match on four rows, including an empty side window, and preserve each main timestamp and window. The co-group may buffer a full window; a bounded side-view memory limit remains open. |
| Streaming per-window side view | Local Prism completes a TestStream main plus bounded side collection through three fixed windows, including an empty side window, with exact output values and main event times. Direct's streaming runner rejects the co-group route during planning because stateful streaming routes are not yet supported; runner agreement for this graph is open. |
| Wait-on runner agreement | A shared graph requires the bounded signal source to finish before fixed-window main rows leave `wait_on`. Direct and live Prism both observe completion and produce `0..2000000:1` and `2000000..4000000:2`. A second graph fails the signal after one value: Direct returns an execution error, Prism job messages report that execution error, and neither runner commits main output. |
| Checkpoint policy decision | The [migration guide](V4_TO_V5_MIGRATION.md#checkpoint-policy-migration) maps v4 settings to current v5 Direct behavior. The user chose v5 batch/progress checkpoints as the 5.0.0 replacement for node-count, elapsed-time, and graph-barrier scheduling. Bounded and streaming history and fallback are implemented. |
| Direct checkpoint publication | Bounded checkpoint data and the replacement manifest each sync their directory entry before old checkpoint data can be removed. Recovery classifies a missing data file as corrupt state. Child-process fault injection exits after data, history, and latest-manifest publication boundaries; bounded and streaming recovery find a verified old or new snapshot. The test also removes the just-renamed latest manifest to model loss of an unsynced directory entry and verifies fallback. Actual power-loss behavior still needs a filesystem-level proof. |
| Direct checkpoint history | `max_checkpoints` retains 1–128 bounded or streaming snapshots. Tests cover rotation, pruning after a lower limit, bounded output/fold fallback, streaming progress fallback, and refusal to cross execution identities. A paired streaming sink test replays from older progress without duplicate commits after the latest manifest is corrupted. |
| Parallel I/O facade | The root `parallel-io` flag now forwards to each optional format crate only when that format is enabled. An isolated Avro facade test writes and reads a parallel container; the parallel flag alone does not pull in optional formats. |
| Separate Dataflow workspace | The full harness suite and doctests pass after publication-plan validation rejects a Beam transform that requests finalization but is absent from the declared sink set. The changed harness passes warning-free lint. |
| Current coverage | A 2026-10-11 run of the exact CI package set with untouched branch sites counted reports core **3,177/3,290 locations (96.57%) / 493/570 branches (86.49%)**, Direct **4,451/4,727 (94.16%) / 902/1,000 (90.20%)**, and transforms **1,163/1,189 (97.81%) / 108/116 (93.10%)**. All three packages pass their existing floors. |
| Exhausted checkpoint recovery | New bounded and streaming tests corrupt both retained manifests and require a typed recovery error; a source-only planner test verifies that its explanation has no physical decisions. All 50 Direct library tests pass. |
| Paired streaming progress | A Direct unbounded source emits an empty progress poll, a data poll, and an empty completion poll to two idempotent sinks. Both sinks receive exactly one row; the final checkpoint retains the source cursor, event time, and completion. |
| Direct plan explanation | `PlanDecision` now exposes selected-plan pruning, materialization barriers, and fused stateless routes through the Direct package and root facade. The `explain_pipeline` example prints these decisions; focused planner tests, the full Direct suite, and strict Direct/root lint pass. This reports v5 choices and does not claim unimplemented v4 optimizer passes. |
| Core aggregate contracts | Core and Direct tests and doctests pass; strict core lint passes. New cases cover malformed join inputs, corrupt portable shuffle groups, fold-session output and emitter failure, and rejection of extra runner inputs. |
| Postcard coder | 13/13 source locations covered; LLVM reports no instrumented branches. |
| Formatting | Both workspaces pass `cargo fmt --all --check`. |

The coverage checker previously dropped branch sites whose two outcomes had
zero hits, making the branch ratios look better than they were. It now counts
those sites as uncovered. The earlier reused build target reported **3,657**
core source locations and **4,644** Direct locations, with both line floors
failing. A second clean isolated run reproduced the smaller counts above.
The exact CI package set produced identical denominators in two consecutive
runs against one isolated build target: core **3,258**, Direct **4,682** before
the latest tests. The earlier reused-target difference remains unexplained.
The coverage gate still requires an uncovered-contract review and a clean full
CI run.

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
Reusing the probe input's envelope buffer and removing outer-join bookkeeping
from smaller-right inner joins reduced peak memory in a 64-sample adjacent
comparison: equal-size join **39.48 → 36.22 MiB**, duplicate-key join
**23.00 → 16.11 MiB**. Median latency moved **10.971 → 11.332 ms** and
**4.985 → 5.019 ms**, respectively. A fresh v4/v5 comparison measured
equal-size join at **10.973/11.334 ms** and **21.50/36.22 MiB**;
duplicate-key join at **2.785/5.069 ms** and **5.53/16.12 MiB**. Output
counts and checksums matched. Join memory and duplicate-key latency remain
open against v4.

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
