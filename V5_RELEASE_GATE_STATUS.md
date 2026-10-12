# Ironbeam 5.0.0 release gates

**Decision: hold the release.** Reviewed 2026-10-12 on
`codex/ironbeam-v5-runner-core`.

The root source runs v5, but the root package is still `4.1.1` and the v5
crates are `0.1.0`. Publication remains staged locally. Local Prism is the
portable runner proof for this release; hosted Dataflow is a later extension.

## Release snapshot

| Area | Where it stands | Next proof |
| --- | --- | --- |
| Workspace | V5-only root source, including with no default features; the root facade and 28 workspace crates verify as local packages | Finish public API audit |
| Direct | V4 reference corpus and v5 helper tests pass | Close public API and edge-case parity |
| Portable | All 31 live local Prism tests pass; the production unbounded job executes versioned map, flat-map, and filter handlers and publishes object-backed JSONL | Expand shared Direct/Prism corpus |
| Streaming | A split and accepted batches survive separate worker process restarts in the Fn Data fixture; the production reader also resumes through join and co-group | Prove live Prism worker restart and output recovery |
| Quality | The exact CI coverage package set passes the core, Direct, and transforms floors after checkpoint recovery and planner tests | Audit uncovered contracts, reconcile the earlier coverage denominator, and set performance limits |

Passing a current check does not close a release gate unless its full proof
below is complete.

## Open gates

### Behavior and API

| Gate | Done when |
| --- | --- |
| ☐ Public API and v4 behavior | The root API and promised behaviors are audited against the [helper tracker](V5_HELPER_PARITY_TRACKER.md) and [migration guide](V4_TO_V5_MIGRATION.md). |
| ☐ Custom transform execution | Versioned map, filter, flat-map, and batch handlers execute on Direct and local Prism. Typed and reusable `DynOp` partition handlers run with an explicit batch size and semantic version on both runners; size one also runs incrementally on unbounded inputs. Size-one row and keyed value batch helpers run incrementally on unbounded Direct and Prism input. Complete v4 implicit partition and planner-hint semantics; `custom_named` remains a marker rejected by Direct. |
| ☐ Portable operation coverage | Every advertised operation has shared Direct/Prism success and failure cases, including multi-input and output paths. Ordered keyed batches now have bounded and unbounded value/time proofs and shared duplicate-order failure; other operations remain to be audited. |
| ☐ Runner agreement | Shared cases match on values, grouping, event times, windows, typed failures, and committed output. Stateful unbounded grouped routes, including ordered keyed batches, still need Direct streaming execution to match Prism. |

### Streaming and durability

| Gate | Done when |
| --- | --- |
| ☐ Unbounded recovery | Extend the passing Fn Data process restart proof to a successful live portable job restart and cover the remaining graph shapes. A supervised process kill during the residual source open makes Prism fail the active job. The local Beam Prism process environment launches one worker command, and its control service fails every active instruction when that worker disconnects; an in-job restart requires runner support beyond the current Ironbeam worker. |
| ☐ Output durability | The runner-wide commit contract passes successful retry and restart proofs and power-loss fault injection. Direct bounded and streaming checkpoints now have configurable retention and verified fallback. V5 batch/progress checkpoints replace v4's node, barrier, and time schedules. Shared final-result publication requires an exact attempt manifest from the completed journal. A live failed bundle already proves abort cleanup. |

### Packaging and release quality

| Gate | Done when |
| --- | --- |
| ☐ Coverage and verification | Uncovered contract paths are reviewed; clean tests, coverage, lint, docs, and package checks pass. |
| ☐ Performance | Acceptable latency and memory limits are set and pass on the paired v4/v5 corpus. |
| ☐ Versioning | The packages receive their intended release versions after the other gates pass. |

### Current paired performance measurement

On 2026-10-11, the release binaries ran one million rows per case, one warmup
pair and five alternating sample pairs. The two join rows were refreshed from
the committed typed path with two warmup pairs and eight alternating sample
pairs. V5 used an explicit 1 GiB Direct job budget; its 64 MiB default rejects
the million-row materialized grouping
case. Peak memory is process RSS from macOS `/usr/bin/time -l`. Each pair had
the same output count and checksum.

| Case | v4 ms | v5 ms | v5/v4 | v4 MiB | v5 MiB |
| --- | ---: | ---: | ---: | ---: | ---: |
| Source copy | 0.561 | 1.083 | 1.93× | 10.28 | 16.30 |
| Map/filter | 0.949 | 2.832 | 2.98× | 14.17 | 10.52 |
| Group by key | 5.364 | 10.582 | 1.97× | 29.83 | 21.64 |
| Latest per key | 6.370 | 10.949 | 1.72× | 48.50 | 10.75 |
| Inner join | 214.979 | 105.175 | 0.49× | 264.17 | 273.56 |
| Duplicate-key join | 28.474 | 10.156 | 0.36× | 42.09 | 42.73 |
| JSONL round trip | 41.308 | 49.882 | 1.21× | 16.72 | 19.47 |

Map/filter latency and join shapes outside the typed Direct path need work
before setting acceptance limits.
The million-row JSONL ratio is much smaller than the 100,000-row ratio, so its
fixed setup and durable publication costs need separate measurement.

A terminal streaming inner-join prototype was measured against the previous
v5 binary on the same million-row inputs and then removed. It made equal-size
joins 22% slower without reducing peak RSS; the duplicate-key case used 11%
less memory but ran 8% slower. The next join-memory change needs a compact
typed path rather than an envelope-at-a-time callback.

The compact typed Direct inner join is now the candidate for two global vector
sources collected directly, with no checkpoint. On 2026-10-11, separate
release binaries built from the previous commit and the candidate ran two
warmup pairs and eight alternating sample pairs at one million rows with the
same 1 GiB budget. Counts and checksums matched in every pair. Other join
shapes continue through the existing materialized path.

| Join case | Previous v5 ms | Candidate v5 ms | Candidate/previous | Previous MiB | Candidate MiB |
| --- | ---: | ---: | ---: | ---: | ---: |
| Equal-size inner | 211.893 | 106.399 | 0.50× | 418.95 | 273.58 |
| Duplicate-key inner | 43.560 | 9.963 | 0.23× | 119.16 | 42.75 |

This is an implementation comparison, not a new v4/v5 release comparison.
The broader corpus, especially map/filter latency and join variants outside
this fast path, still needs acceptance limits and fresh paired measurements.

## Completed gates

| Gate | Evidence |
| --- | --- |
| Formatting and current suites | Both workspaces pass formatting, workspace tests, and the live Prism suite from their last full runs. |
| Cloud package cleanup | Contracts, fakes, operations, objects, provider clients, and JSONL readers have separate owners. The unused legacy cloud crate is removed; all 28 packages verify locally. |
| Local object conditional create | Nested object parents are created component by component and each new directory entry is synced. A symlink escape is rejected before an outside child is created. Reopening a nested root returns the immutable object; the cloud-object package tests, strict lint, and the separate Dataflow object-journal tests pass. |

## Latest measured results

| Check | Result |
| --- | --- |
| Core, Direct, and compatibility packages | Tests and doctests pass with the dynamic partition operator, current Direct join implementation, and ordered keyed batches. The complete local release script passed again on 2026-10-12. |
| Root API composition | A new facade test builds two keyed sums, joins them, and collects the exact result through `ironbeam::v5::prelude` without the compatibility crate. It passes with default and no default features; strict lint passes. This exercises one public composition path, not the complete API ledger. |
| Versioned semantic handlers | Map, filter, and flat-map handlers record an application ID and version in the graph fingerprint. Core rejects invalid identity/version and reserved builtin IDs; Direct tests collect exact values. A v4/v5 differential test matches all three published custom-op examples. The production unbounded Prism fixture uses a nontrivial map and expands each value into positive/negative rows before the versioned filter removes the negatives. Core/Direct suites, strict lint, and all 31 live Prism tests pass. General custom operation registration remains open. |
| Versioned batch handler | `apply_transform_with_semantic_named` accepts owned rows with explicit batch size and records an application ID and version on the handler node; `map_batches_with_semantic_named` is the borrowed-slice form. Direct differential tests match v4 `DynOp` examples that reorder/expand rows and filter while changing row type. The shared bounded Direct/Prism fixture now runs the dynamic form before map/filter/flat-map and matches the output multiset. Batch encounter order after Beam shuffle is not yet a cross-runner promise; general partition-level parity remains open. |
| Reusable dynamic partition operator | `DynOp` accepts an owned type-erased partition, and `apply_dyn_transform_with_semantic_named` binds it to an explicit batch size and application version. V4/v5 differential tests match reversing/expanding and filtering/type-changing operators; a wrong output type returns a typed execution error. The root facade test passes with default and no default features. The bounded Direct/Prism fixture runs a type-changing operator; a one-row operator expands unbounded input on both runners without global aggregation. A shared invalid-output fixture fails with the same operator and error on Direct and live Prism, and neither runner commits a sink file. All 31 live Prism tests pass. V4 implicit partition boundaries and planner hints remain open. |
| Semantic version and batch identity | On 2026-10-11, the production Prism worker rebuilt the same unbounded graph with only the semantic map's minor version changed. The live job failed with `driver/worker execution identity mismatch`; the final-result coordinator refused completion and no object result was published. Core regressions prove that explicit row, byte, and keyed batch limits change graph fingerprints. A shared byte/keyed batch job matches Direct and live Prism values; all 30 live Prism cases pass. Successful in-job worker recovery remains open. |
| Local release script | On 2026-10-12, the complete script passed with Mr Boxington after the ordered keyed batch addition: dependency and feature checks, workspace and feature tests, docs, and local package verification for all 29 packages including the root facade. All 22 published v4 feature names are present in the v5 facade. Nothing was published. |
| Cloud operation coverage | **186/261 source locations** and **34/42 branches** after bounding parallel helper concurrency; both CI ratchets pass. Uncovered utility paths remain in the quality review. |
| JSONL coverage | All-feature package: **407/435 source locations** and **38/40 branches** at the last coverage run. The latest eager reader change has passing default/all-feature tests and strict lint; coverage has not been rerun. |
| Local Prism | All 31 live tests pass, including bounded and unbounded per-window side-input cases, an unbounded ordered keyed fold across two windows, wait-on success/failure cases, dynamic output type rejection with no sink commit, semantic-version rejection, and the supervised worker-loss failure case. A production reader resumed from a residual cursor; join and co-group committed five exact rows across two windows. The provider-neutral harness coordinator requires `DONE`, validates the graph's ten final sinks against the whole-job journal and committed row metrics, then supplies exact attempt IDs and counts to all ten publishers. A separate job failed after staging and left no fragment, journal completion, or published output. |
| Fresh Prism regression | The full 19-test live corpus and separate native Arrow proof passed again on 2026-10-11 against Prism built from the local Beam clone, after the publication-plan guard was added. |
| Final-result manifest binding | All shared final-result publishers reject an unbound store, including empty output. The live Prism corpus and Arrow proof passed after their stores were bound to the completed job journal's exact attempts. Local format tests and strict lint passed. |
| Object-backed JSONL final result | Shared I/O now has immutable object fragments with scoped checksums, conditional creation, accepted-attempt markers, exact completed-attempt matching, and idempotent final publication. The JSONL crate stages one fragment per portable bundle, accepts it through `FinalizeBundle`, validates each decoded fragment, and publishes uncompressed `.jsonl`. Local object lifecycle and all-feature I/O tests pass. The production unbounded Prism job finalizes object-backed JSONL bundles, publishes only after `DONE`, and reopens `[7, 8, 9]`; the existing worker-loss case still fails closed. Provider-specific consistency proofs and bounded-memory publication remain open. |
| Remote object adapter contract | The `object_store` adapter uses conditional `Create` and a byte-prefix-filtered listing. An in-memory adapter test verifies immutable create conflicts, unchanged bytes, sorted prefix listing, and bucket isolation; its package tests and strict lint pass. The S3, GCS, and Azure configurations still need provider-specific consistency proofs. |
| Final-result retry durability | The shared local publisher accepts an identical staged fragment after restart, rejects changed or corrupt retries, and syncs its directory on repeated acceptance or byte-identical publication. The separate Dataflow file journal resyncs identical record and completion-marker retries after reopen, and now syncs newly created nested journal-directory entries. The shared I/O and full harness suites pass with formatting and strict lint. This verifies retry behavior; it does not yet prove physical power-loss recovery. |
| Local final-result file sink | The ordinary Direct file sink syncs the published directory entry after replacing the target. Shared I/O also syncs each newly created ancestor entry for nested staging and output directories. Reopen tests cover both nested paths; the all-feature JSONL and shared I/O suites, cloud-only feature check, formatting, and strict lint pass. Physical power-loss recovery remains open. |
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
| Current coverage | A fresh 2026-10-11 run of the exact CI package set with untouched branch sites counted reports core **3,219/3,337 locations (96.46%) / 505/580 branches (87.07%)**, Direct **4,451/4,727 (94.16%) / 902/1,000 (90.20%)**, and transforms **1,163/1,189 (97.81%) / 108/116 (93.10%)**. All three packages pass their existing floors. Core tests now exercise windowed side-view construction failures and malformed unbounded resume progress. |
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
failing. A second clean isolated run reproduced smaller counts.
The exact CI package set produced identical denominators in two consecutive
runs against one isolated build target: core **3,258**, Direct **4,682** before
the latest API and test additions. The earlier reused-target difference remains unexplained.
The coverage gate still requires an uncovered-contract review and a clean full
CI run.

The latest paired 100,000-row release run used two warmup pairs and eight
measured pairs per case. Output counts and checksums matched throughout.
These are measurements, **not a performance gate pass**:

| Workload | v4 ms | v5 ms | v5/v4 | Peak MiB, v4 → v5 |
| --- | ---: | ---: | ---: | ---: |
| Source copy | 0.062 | 0.127 | 2.05× | 3.41 → 4.00 |
| Map/filter | 0.139 | 0.426 | 3.05× | 3.81 → 3.69 |
| GroupByKey | 0.647 | 1.100 | 1.70× | 5.06 → 4.59 |
| Latest per key | 0.733 | 1.210 | 1.65× | 7.30 → 3.94 |
| Equal-size inner join | 11.245 | 10.955 | 0.97× | 21.48 → 37.74 |
| Duplicate-key join | 2.800 | 4.403 | 1.57× | 5.51 → 16.11 |
| JSONL round trip | 4.396 | 11.746 | 2.67× | 4.39 → 4.58 |

The JSONL fixture now measures source construction, sink graph construction,
write, read graph construction, and read separately. V5's local final-result
sink syncs both the temporary file and published directory entry; v4's write
does not make that durability promise. The measured v5 writer still takes
most of its round-trip time. Earlier optimization samples and the repeatable
command are in the [baseline record](V5_PHASE0_BASELINE.md). Acceptable release
limits remain unset.

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
