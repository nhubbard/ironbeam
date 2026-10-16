# Ironbeam 5.0.0 release gates

**Decision: local 5.0.0 candidate; supervised Prism execution-child recovery is proved.** Verified 2026-10-16 on
`codex/ironbeam-v5-runner-core`.

The root is an unpublished `5.0.0` candidate and the implementation crates
are `5.0.0`. Publication remains staged locally. Local Prism is the
portable runner proof for this release; hosted Dataflow is a later extension.
The seven parity families below remain the longer-term comparison with
portable Beam semantics and Rust-relevant I/O in the pinned local checkout.
They are not a numerical release gate: no percentage is assigned while
broader stateful DAGs, other timer recovery schedules, general source
splitting, and the open cases in this report lack runner proofs. For 5.0.0,
release work is limited to the documented and tested support boundary, API
documentation, package verification, and passing CI. Unsupported schedules
must fail clearly or be described as outside that boundary.

## Feature scope for 5.0.0

Feature development is frozen at the tested support boundary. There are **no
additional mandatory feature families** for this release candidate. The seven
parity families in [Portable parity work still open](#portable-parity-work-still-open)
contain further Beam semantics and runner proofs; they are follow-up work, not
an implied 99% claim. The two unchecked runtime subgates below are also outside
the 5.0.0 support claim.

| Deferred boundary | Current release claim |
| --- | --- |
| Prism Control-owner replacement | Supervised execution-child replacement is proved. A lost Control owner fails the active job. |
| Long-running Prism metric retention | Exact local committed counts are proved. Metadata retention in an indefinitely running Prism job is unbounded. |
| Hosted Dataflow and live GCP services | The job driver and clients compile and have local tests. Hosted execution and service operations need separate integration proof. |

The remaining release tasks concern Rustdoc quality, user documentation,
package verification, source cleanup, and green CI. Publication is a separate
decision; it remains staged.

The [crates.io Trusted Publishing guidance](https://blog.rust-lang.org/2025/07/11/crates-io-development-update-2025-07/)
requires an initial manual release of each new crate before that crate can use
the workflow's short-lived token. This registry bootstrap is separate from
the local Semantic Release simulation.

## Release snapshot

| Area | Verified scope | Open boundary |
| --- | --- | --- |
| Workspace | All 35 publishable packages, including `beam-proto` and the root facade, are staged at 5.0.0. | New crate names need an initial manual crates.io publication before Trusted Publishing can be configured. Publication is held. |
| Direct | V4 reference behavior and v5 helpers pass. Checkpointed unbounded fan-in, joins, session groups, and two independent post-split value-state branches have tested schedules. Distinct branch event timers route to their own sinks. | General stateful DAG fanout, broader timer failure schedules, and complete multi-source session pane agreement. |
| Portable | The 217 supported nondestructive Prism fixtures passed on Linux; they cover Beam rows, streams, state, timers, grouping, side inputs, I/O, and worker-child replay. | Hosted Dataflow, some session panes, and remote provider replay. |
| Early count | Fixed and sliding window panes match on Direct and Prism; Direct restores a partial pane after restart. | Wider fan-in and trigger combinations. |
| Accumulating early and late count | The combined policy has distinct graph identity and tested panes on both runners. | Exact scheduling differs in one accumulating stream. |
| Processing-time triggers | Whole-millisecond delays have fixed-window Direct and Prism proofs, including scripted clocks and Direct restart. | Mixed clock domains, session merging, and synchronized time. |
| Count-and-time triggers | Repeated Beam `AfterAll` count-and-time schedules have Direct and Prism cases. | Other `AfterAll` expressions and wider fan-in. |
| Accumulating late fan-in | Direct checkpoints retained values in co-groups and typed joins; selected Prism jobs complete. | Typed-join intermediate panes differ. |
| Typed Partition | Versioned classifiers produce numbered branches with matching Direct and Prism outputs. | Wider multi-output execution. |
| Representative Distinct | A native shuffle selects the smallest encoded representative per derived key and window on both runners. | Deterministic coders and more stream schedules. |
| Streaming Count.PerElement | The streaming path uses checkpointable GroupByKey and matches Prism in fixed windows. | An incremental accumulator for heavily used keys. |
| Two-source sessions | Direct restores merged open sessions across two source cursors. Selected Prism schedules agree. | Intermediate-watermark and late-pane agreement. |
| Session interval adjacency | Core folding, bounded joins, and multi-source Direct closure keep half-open sessions that only meet at an endpoint separate. A later overlapping event merges both; core checkpoint, bounded join, and multi-source closure fixtures cover this rule. | Pinned Prism has merged a touching pair in a pipeline fixture; runner agreement and late panes remain open. |
| Active-session expiry | Direct resolves an expired candidate against a still-open keyed session in single-source groups, multi-source groups, joins, and co-groups. Checkpoint recovery fixtures now cover all four paths, including a three-source grouped bridge after its original interval expires. | Pinned Prism splits the tested intermediate-watermark session; portable runner agreement and positive allowed lateness remain open. |
| Session allowed lateness | Core rejects nonzero lateness on session windows. Direct resolves active intervals at zero lateness across the tested single-source, grouped multi-source, join, and co-group paths, but on-time and late pane state remains keyed by old intervals. | Implement and prove merge-aware pane lineage, positive-lateness expiry, and recovery before lifting the guard. |
| Intermediate session watermarks | A pinned Prism case splits a session that Direct merges. | Runner agreement is unproved for this schedule. |
| Three-input bounded sessions | A pinned Prism case splits a connected session that Direct merges. | Independent-source session resolution. |
| Coverage after session work | The unchanged core and Direct coverage floors passed. | Destructive Prism cases were excluded from that measurement. |
| Native Avro schemas | Reader-schema projection, defaults, aliases, and named or nested records have file and local-runner tests. | Remote provider binding and hosted proof. |
| Avro object input | URI and prefix readers use sequential provider reads. Direct and local Prism decode a provider-bound object with a defaulted reader field; a supervised Prism execution-child loss during the first decoded row reopens the object and commits both rows once. | Per-object temporary disk use, hosted provider binding, and live service proof. |
| Parquet object input | URI and prefix readers pass local provider tests. Direct and local Prism read two provider-bound Parquet objects under one prefix; a supervised Prism execution-child loss during the second object's decoded row reopens the prefix and commits all three rows once. | Per-object temporary disk use, hosted provider binding, and live service proof. |
| Streaming recovery | Source residuals and accepted bundles resume across worker processes; Direct checkpoints supported open state. | Whole-process or host replacement within one active Prism job. |
| Global-window expiration callback | Direct emits its callback when a finite stream completes. | The pinned Prism intermittently omits this callback or panics in its Go WaitGroup. Its cross-runner fixture is retained as an ignored diagnostic, outside the 5.0.0 portable support claim. Fixed-window expiration has passing shared fixtures. |
| Partial global byte batch at finite completion | Direct flushes the partial keyed batch after an intermediate watermark and source completion. | The pinned Prism intermittently reports `DONE` without the final partial batch. Its cross-runner comparison is an ignored diagnostic. Timed global partial batches have a separate shared fixture. |
| Object-backed finalization | The object journal validates listed bundle records, completion markers, identity, and declared sinks. A publication plan pairs its completed manifest with runner-committed sink counts; local object-provider tests cover incomplete callbacks, corrupt markers, unknown entries, and foreign identities. The Dataflow entrypoint accepts a provider registry and journal URI for the worker and supervised children. A live local Prism job kills one execution child, recovers in its replacement, reaches `DONE`, and authorizes publication from its object journal and committed metrics. The hosted object adapter has a bounded streaming upload followed by conditional creation, with local large-object, duplicate, concurrent-create, and reader-failure tests. A cross-crate test now stages two large fragments through `ObjectFinalResult`, publishes through that adapter, and verifies exact bytes and idempotent retry. The Dataflow driver can retrieve regional service metrics as typed records. | Map hosted service metrics to exact committed sink counts, supply a GCS-backed registry, and prove publication through a controlled service. |
| GCP clients | Opt-in Cloud Storage, Pub/Sub, BigQuery, and Secret Manager adapters compile and have isolated tests. | Live service and hosted Dataflow proofs. |
| Quality | Core, Direct, Graphx, and local Prism suites have passing gates; later sections record exact runs. | Hosted execution and registry publication remain staged. |
| Coverage gate integrity | CI compares source-location coverage to unchanged floors and catches stale denominators. | See the exact audit below. |

Passing a current check does not close a release gate unless its full proof
below is complete.

The release documentation check generates all-feature Rustdoc JSON and
requires a direct description for public functions, trait methods, and types
in all 34 publishable handwritten crates: 907 functions, 329 trait methods,
and 335 types in the current all-feature JSON. Generated Beam protocol bindings
and nonpublishable test packages are outside that check. The Zensical site
builds in strict mode with no issues. Both checks validate coverage and links;
prose and examples still need review for accuracy before the release tag.

The 2026-10-16 Linux Prism run passed its eight repeated residual-source
cases and its first 99 shared cases. During the next global byte-batch job,
the pinned Go Prism process panicked with `sync: WaitGroup is reused before
previous Wait has returned`; later cases failed to connect to the dead job
service. A full-suite retry reached 203 passing cases before the same panic
occurred in the global-window expiration case. The pinned runner also
intermittently reports `DONE` for that case without the callback output on
fresh processes. The CI suite now starts a fresh Prism service for each small
set of fixtures and retries only the fixture interrupted by that exact panic.
The global-window expiration and final partial global byte-batch comparisons
are ignored diagnostics; Direct has independent completion tests for both.
A normal supported fixture failure remains a failure.
The sharded suite passed all **217 listed nondestructive fixtures** on Linux
in [PR #90's Beam worker and Prism check](https://github.com/nhubbard/ironbeam/actions/runs/38907220070/job/116993717580)
at `986a6c4a`. That run also passed the separate supervised worker-loss
fixture. The two ignored cross-runner diagnostics remain outside the portable
support claim.

### Merge-aware session lateness contract

The pinned Beam Java `MergingActiveWindowSet` persists a map from each active
window to its state addresses. `ReduceFnRunner` merges the value reducer,
watermark hold, nonempty-pane state, and trigger state before it discards the
old window addresses. Its `testMergingWithClosedDoesNotPoison` regression
shows that a finished trigger in one session must not close a new session
formed by merging it with open input. Single-source Direct now resolves an
incoming interval against an active keyed session before checking expiry,
including after a checkpoint restore. Multi-source grouped state, joins, and
co-groups now resolve expired candidates against active rows with the same
encoded key. `GroupByKeyFoldState` still tracks on-time and late pane indices
by the premerge interval; positive-lateness merge and recovery schedules are
unproved. These gaps keep the public session-lateness guard in place.

#### Active-session expiry evidence

| Path | Observed result |
| --- | --- |
| Single-source Direct | An event assigned to `[0, 5)` ns joins an active `[0, 9)` ns session after a six-millisecond watermark. The result survives checkpoint restore. |
| Multi-source Direct group | A keyed transitive bridge is accepted after state save/restore; an unrelated key is dropped and a distant session stays open. A three-source job checkpoint restores all cursors, accepts the second source's expired-original-interval bridge, drops an expired row of another key whose session is distant, and emits each surviving group once. |
| Two-source Direct join and co-group | TestStream fixtures accept a cross-input bridge and drop a row after the merged window closes. Both recovery fixtures restore `[0, 9)` ns and accept an expired-original-interval row from the second source. The join emits both pairs once; the co-group emits one merged group. |
| Pinned local Prism | The same single-source intermediate-watermark fixture emits `1|[1]|0|5000000` and `1|[2, 3]|0|9000000`; Direct emits `1|[1, 2, 3]|0|9000000`. The equality test is retained as an ignored diagnostic. |

The pinned Prism aggregate stage (`sdks/go/pkg/beam/runners/prism/internal/engine/elementmanager.go`)
checks expiry and trigger readiness against each assigned window before its
GroupByKey handler (`sdks/go/pkg/beam/runners/prism/internal/handlerunner.go`)
merges the windows in that handler's current batch. This ordering is consistent
with the observed split, but the live fixture alone does not prove which
internal decision separated the two outputs.

| Acceptance schedule | Required evidence |
| --- | --- |
| Late bridge with an original interval whose expiry has passed, but a connected live session whose merged expiry has not | Establish the pinned runner's result, then match accepted or dropped rows, merged bounds, output time, and pane timing on Direct. Expiry must be evaluated against the resolved active session. |
| A closed session that later merges with an open session | Match the Beam regression's rule: the closed trigger does not suppress the open session's later firing. Track state and pane identity through the merge. |
| Checkpoint between on-time firing and a late bridge, including two source cursors | Restore merge lineage, trigger and pane state, and watermarks; compare committed rows with an uninterrupted Direct run and a pinned Prism job. |

The implementation sequence is to persist active-window lineage with each
key's fold state, resolve a candidate against that lineage before expiry,
merge timer and pane state under the resulting window, and then prove all
three schedules. A graph-validation change alone would expose incorrect
results.

The processing-time change passes the full core, Direct, Graphx, and Prism
proof package suites, strict core/Direct and Graphx/Prism lint, and the focused
live Direct/Prism comparison on pinned local Prism. The unchanged CI coverage
selection passes: core **4903/5105 lines (96.04%)** and **950/1088 branches
(87.32%)**; Direct **5727/6291 lines (91.03%)** and **1656/1840 branches
(90.00%)**; transforms **1117/1143 lines (97.73%)** and **115/122 branches
(94.26%)**. The live comparison covers the one processing-time fixture in
discarding and accumulating modes; the full live Prism suite was not rerun for
this change.

The subsequent idle-timer extension passes the full core and Direct suites,
strict core/Direct lint, and the same unchanged coverage floors: core
**4909/5111 lines (96.05%)**, **951/1088 branches (87.41%)**; Direct
**5821/6393 lines (91.05%)**, **1678/1864 branches (90.02%)**; transforms
**1117/1143 lines (97.73%)**, **115/122 branches (94.26%)**. The Direct
fixtures cover a source that stays pending, a failed sink commit followed by
the same timer bundle ID on retry, two staggered deadlines after restart,
sink write and commit faults, cancellation, a filtered pane, and distinct
Early and OnTime panes from empty progress batches at one source cursor.

Direct's stream checkpoint manifest is version 2 after the sink bundle
identity change. Version 1 snapshots fail recovery explicitly: their cursor
alone did not identify separate watermark or completion steps. This candidate
has not been published, and the boundary avoids silently replaying a committed
window under a new bundle ID.

The same-cursor windowed side-view regression passes for both a later
watermark and source completion. The full Direct suite and strict lint pass;
the exact unchanged CI coverage selection reaches core **4909/5111 lines
(96.05%)**, **951/1088 branches (87.41%)**; Direct **5826/6398 lines
(91.06%)**, **1678/1864 branches (90.02%)**; and transforms **1117/1143 lines
(97.73%)**, **115/122 branches (94.26%)**.

### Multi-source processing-time proof

Direct now fires processing-time panes for independent unbounded sources in a
fixed-window homogeneous co-group. The runner uses the greatest observed
scripted source clock and persists that clock with its grouped folds. It
rejects mixed scripted and wall clocks, negative clocks, and deadline overflow.
A bounded input can arm a timer while an unbounded input is idle. A restart
retains two source cursors and an armed deadline; when one source has already
completed, recovery opens only the unfinished source. A failed timer sink
commit retries the same bundle ID, then emits the Early and OnTime panes once.
Tests also cover malformed progress, empty or invalid fold firings, idle
cancellation, and bounded completion. The full core, Direct, and transforms
coverage selection passes its unchanged CI floors: core **4909/5111 lines
(96.05%)** and **951/1088 branches (87.41%)**; Direct **5946/6523 lines
(91.15%)** and **1710/1900 branches (90.00%)**; transforms **1117/1143 lines
(97.73%)** and **115/122 branches (94.26%)**. Full Direct tests and strict
Direct lint pass. A tagged Beam TestStream now converts the greatest source clock into one
shared processing-time sequence. The fixed-window two-source co-group emits
matching discarding and accumulating panes on Direct and pinned Prism; the
focused live comparison passed three consecutive runs. Each source first
advances its watermark to zero. Without those initial watermarks, pinned
Prism fails the job with an internal `assignment to entry in nil map` panic
while constructing a processing-time timer bundle. This remains a runner
scheduling boundary, and broader schedules have not been proved.

## Coverage floor audit, 2026-10-14

The previous CI command printed a failed coverage comparison but returned the
exit status of `tee`. After fixing that shell error, the first full package
matrix exposed stale floors in code added since the last measurements. The
matrix now runs every package even after one failure. Malformed fragment tests
for CBOR, MessagePack, Avro, Arrow, Parquet, TFRecord, and JSONL were corrected
to select accepted attempts before calling the publisher; they now exercise
the claimed decoder or row-count failure. CBOR, MessagePack, and TFRecord pass
their original floors. Focused tests also cover local object read failures,
failed streaming creation cleanup, invalid parent paths, and symlinked lists.

| Package | Measured lines | Measured branches | Floor decision |
| --- | ---: | ---: | --- |
| `ironbeam-cloud-objects` | Linux 370/420 before new tests; local 307/332 after | local 64/74 after | Reset line floor to 370/420; retain branch floor of 46/54. The larger local and Linux denominators differ; both represent the expanded conditional-write implementation. |
| `ironbeam-cloud-ops` | Linux 186/261 | Linux 34/50 | Existing line floor passes; reset branch floor to 34/50 for the expanded parallel helpers. |
| `ironbeam-io` | Linux 953/1062 | Linux 190/256 | Reset both floors to the first honest full-matrix measurement of the expanded final-result implementation. |
| `ironbeam-io-jsonl` | local 477/562 after corrected fragment test | local 60/78 | Reset both floors to the current measured implementation. |
| `ironbeam-io-avro` | local 462/491 after corrected fragments | local 51/56 | Reset both floors to the current measured implementation. |
| `ironbeam-io-arrow` | local 389/448 after corrected fragments | local 39/44 | Reset line floor; original branch floor remains. |
| `ironbeam-io-parquet` | local 213/241 after corrected fragments | local 23/24 | Reset line floor; original branch floor remains. |

These floors are regression guards, not a claim of full test coverage or 99%
Beam parity. In particular, the I/O and cloud operation branch gaps remain
release quality work. On head `91505df`, all Linux package coverage jobs and
strict Clippy passed with the corrected shell gate. The remaining feature,
facade, package, and live Prism jobs were still running when this audit was
updated.

The BagState implementation and its focused error tests passed the Linux
core ratchet on `16e3b22`: **5704/5935 lines (96.11%)** against 96.04%, and
**1167/1354 branches (86.19%)** against 86.17%. The line and branch
denominators include the new core API. This measures source coverage, not
Beam feature parity.

The SetState implementation, fixed-window case, and coder-failure tests passed
the same unchanged Linux core ratchet on `3b11207`: **5769/6002 lines
(96.12%)** and **1184/1372 branches (86.30%)**. The local Direct and pinned
Prism SetState corpus includes bounded mutations, unbounded global and fixed
windows, checkpoint restart, and supervised worker replacement.

The later completed Linux core coverage job on `8453b49` passed lines at
**5843/6079 (96.12%)** but failed branches at **1200/1396 (85.96%)** against
the unchanged **86.17%** floor. The `b1fe930` Linux job measured the subsequent
MultimapState and OrderedListState tests at **6007/6250 (96.11%)** lines and
**1247/1448 (86.12%)** branches. After adding a lower-bound ordered-list range
case, the `c92ba22` Linux job passed at **6007/6250 (96.11%)** lines and
**1248/1448 (86.19%)** branches against the unchanged floor. A local macOS `cargo llvm-cov`
run executed the exact package selection, but its source-location denominator
differs substantially from Linux, so it is not evidence that the Linux gate
passes. The branch floor has not been lowered.

The `9a40f74` Linux core job includes executable combining state and runner
bags with multiple coded partial accumulators. It passed at **6076/6323
(96.09%)** source lines and **1265/1468 (86.17%)** source branches against the
same floors. The branch margin is narrow; subsequent core changes need
targeted behavior tests rather than a lower threshold.

The 2026-10-15 local Direct package measurement after the sequential trigger
and multi-source progress tests is **5946/6521 source lines (91.18%)** and
**1711/1900 source branches (90.05%)**. Both unchanged floors pass. Core's
reader rejects changed source and split IDs and regressing watermarks; Direct's
grouped route retains the separate data-cursor advance check. The CI run for
this revision is pending.

### Portable harness coverage

The current `cargo llvm-cov -p ironbeam-harness --branch` selection covers
**3090/4420 source lines (69.91%)** and **828/1460 source branches (56.71%)**.
The new Provision RPC and option tests cover 35/36 executable lines in
`provision.rs`, including worker metadata and rejection of fractional,
negative, nonfinite, and numerically unsafe options. Large unsigned values
remain available through string options. The remaining low
package total is concentrated in worker startup, supervision, and live bundle
paths that the harness-only test selection does not exercise. The next
coverage gate needs instrumented Prism worker processes in its selection so
its floor measures those paths as well as unit tests.

## Portable parity work still open

This ledger uses the pinned Apache Beam checkout at
`7d93ef8e76aa3fd7874e760010f6479fba40721a` as the reference for
portable semantics and Rust-relevant I/O. The 99% target does not count
language-specific SDKs, runners, or connectors from that checkout. The Beam
pipeline model defines triggers, allowed
lateness, state, and timer families separately. Passing the current default
window and side-view corpus therefore does not imply general streaming parity.
No percentage is assigned to this ledger until each family has an explicit
test inventory and weighting.

The pinned `beam_runner_api.proto` and `beam_fn_api.proto` define the portable
capabilities used for this audit. The execution details below were checked
against `sdks/go/pkg/beam/core/state/state.go`,
`sdks/go/pkg/beam/core/sdf/sdf.go`, and
`sdks/java/core/src/main/java/org/apache/beam/sdk/transforms/Wait.java` in
that checkout. This is the current accounting boundary:

| Upstream family | Implemented proof | Missing proof for the 99% target |
| --- | --- | --- |
| Trigger constructors | Default window closure, repeated element count, and combined early and late counts have Direct and Prism fixtures. Fixed-window repeated processing-time early panes fire after two TestStream clock advances in both discarding and accumulating modes. Direct and pinned Prism agree on pane values and timing in those fixtures. Direct checkpoints armed deadlines, fires them after restart, and also fires an idle wall-clock timer for a source with no scripted clock. The TestStream clock reconstructs from its replay cursor and rejects overflow. A repeated `AfterAny` count-or-processing-time policy lowers in discarding and accumulating modes; Direct tests both firing conditions and retained values. A two-source Direct co-group emits count `[2, 4]`, processing-time `[2, 3, 4]`, and on-time `[2, 3, 4]` panes; pinned Prism completes the same job but emits a duplicate early `[2, 4]` pane before the later value, shifting subsequent indices. A repeated `AfterAll` count-and-processing-time policy now lowers in both accumulation modes. Direct tests both firing orders and checkpoint recovery after the timer reaches its deadline; a pinned Prism fixture matches the tested timer-first pane rows. Repeated `AfterEach(AfterCount(2), AfterProcessingTime(5 ms))` now has distinct graph identity and Beam lowering. Direct tests a full count, timer, count sequence and restores an armed timer and phase from a checkpoint in both accumulation modes; Direct and pinned Prism agree on a shorter count, timer, window-closure schedule. | Synchronized processing-time triggers; other `AfterAll`, `AfterAny`, `AfterEach`, `OrFinally`, and nested repeats; exact merged-session and late-pane schedules. Pinned Prism panics when the tested mixed schedule closes before its old timer is drained; an additional clock advance makes that fixture pass. Two independently clocked TestStreams share Beam's clock by taking the greatest source time; fixed-window homogeneous co-group panes agree on Direct and pinned Prism. |
| User state and timers | Seven state shapes and two timer domains have versioned Beam declarations and Direct/Prism execution fixtures. Value and combining cells, plus an armed event timer, have supervised worker-child replacement proofs. A shared bounded bag-state stage feeds two sinks. A separate two-sink stream keeps value state on one post-split branch: Direct restores that state and the source cursor without repeating the stateless branch, while pinned Prism matches both branches on values. Direct now also checkpoints two independent post-split value-state branches with one cursor; restart restores both accumulated values without duplicate commits. A pinned local Prism job matches Direct on both independently accumulated branch outputs from the same TestStream. A supervised Prism job then replaces an execution child lost inside the first branch’s state callback and still commits both branches’ exact rows once. The paired checkpoint has a bounded, versioned payload for two or more stateful stages and corruption tests; a three-stage serial route restores all three states after further processing, while single-state checkpoints retain their existing format. Timer draining follows graph order independently of the stable checkpoint-key order; a lexically inverted two-timer route fires its downstream timer at the same watermark. Branch-local event timers route only to their own sink, including when both branches arm a timer and recover from one checkpoint; the one-state case is tested in either left or right position. A shared unbounded fixed-window bag-state stage now feeds two Prism sinks across three source batches with exact per-window values; Direct now runs the same paired route and checkpoints its shared state with the source cursor; a restart test verifies both sink branches receive the remaining counts once. A shared event timer now feeds both Direct and pinned Prism sinks with one accumulated firing; pinned Prism also replaces a child lost inside that timer callback and commits one row to each sink. Direct also fires an idle processing-time timer into both branches and restores an armed event timer with both sinks from one checkpoint. If the second sink fails after the first commits a timer firing, a Direct retry reopens at the saved cursor and keeps the first sink’s bundle identity, yielding one row in each sink. Direct also checkpoints an idle processing-time firing and retries its two-sink commit after the second sink fails, without repeating the first sink’s row. A sliding-window TestStream now sends two same-key values in one batch; Direct and pinned Prism agree on eight exact per-window value-state rows as later watermarks expire the first two windows. A separate Direct checkpoint test restores those same-key overlapping cells and resumes the source cursor. A second same-batch sliding fixture sums two values in both window cells, replaces the same timer tag, and fires exactly once per window with identical output holds on Direct and pinned Prism. Fn State and Fn Data wire tests cover coded state, timers, request failures, and scoped mutations. | Extend overlapping-window expiry to other state shapes and timer schedules, test more two-branch timer failure and recovery schedules, prove additional stateful DAG routes and failure points, and test cross-SDK Set behavior. Pinned Python and Java use different Set-state protocols. |
| Schema rows | The separate `ironbeam-coder-row` crate encodes and decodes fixed-schema Beam RowCoder bytes for atomic fields, nullable fields, arrays, maps, nested rows, encoding positions, static encoding, and representation-based logical values. Tests use byte examples and a serialized Schema payload from the pinned Beam standard coder corpus, including a shifted millisecond instant, five precision-specific timestamp examples, four decimal values, and fixed and variable length character and byte fields. Local wire tests also check Beam's date, time, and UUID representations; UUID has an additional nested-row header. An explicit writer/reader schema projection adds, removes, and reorders compatible fields, including fields inside nested rows, arrays, iterables, and maps; `BeamRow<S>::project<T>()` performs the projection inside a typed pipeline. Complete-row decoding also supports implicit trailing nullable additions and unknown trailing removals, with a nested-row boundary check. `BeamRow<S>` carries its serialized schema in the graph and its execution identity. Graphx lowers it to the standard RowCoder URN; bounded integer and decimal row pipelines and a typed projection have passed on both Direct and local Prism. An unbounded TestStream sends Beam rows through a keyed shuffle in two fixed windows; Direct and pinned Prism agree on grouped IDs and earliest event timestamps, and a graph test confirms the source's standard RowCoder payload. A second unbounded fixture projects an integer/string writer row to a reordered reader schema with a nullable addition before the shuffle; Direct and pinned Prism agree on projected values and timestamps. The graph test confirms distinct standard RowCoder payloads for the writer and reader. The crate has an isolated source-location coverage floor of **458/508 lines (90.16%)** and **101/118 branches (85.59%)**. | Complete-row decoding handles nullable trailing additions and unknown trailing removals; nested removal still needs an explicit writer schema. Python and Java disagree on null-mask numbering for reordered fields, so cross-SDK nullable reorder remains unproven. Additional logical types and broader schema transforms need wire and execution tests. The live streaming projection proof covers this explicit integer/string case; other nested and nullable cross-SDK shapes remain unproved. |
| Splittable processing | Source restrictions, residual cursors, and supervised worker-child replacement have live Prism proofs. The source adapter accepts only an unconstrained zero-fraction checkpoint request for its active gRPC read transform; a harness test rejects empty, wrong-transform, fractional, and split-point-constrained requests. A resumed-source test confirms that the core validator rejects changed split IDs and regressed watermarks even on a completed batch. The worker now handles Beam's truncate stage for this unbounded source by validating and discarding its remaining restriction, with a Fn Data boundary test. | The pinned Go `sdf.RTracker` requires a claim before output, a thread-safe split into primary and residual restrictions, progress reporting, and an `IsDone` check. Implement these for user transforms; prove dynamic split and runner-orchestrated drain without loss or duplication on Direct and Prism. Add a split-time watermark-estimator proof. |
| Side inputs | Global and window-closure views have typed Direct and Prism fixtures. The pinned Java `Wait.OnSignal` uses a per-window side view with a `Never.ever()` signal trigger. Ironbeam lowers one default, zero-lateness, unbounded fixed/sliding-window signal to an identity-mapped side view. Direct tests release before the main watermark reaches the window end, including an empty signal window and recovery from an open-window checkpoint. A pinned Prism job passes the same values and timestamps; a second job proves that an empty fixed signal window releases the main value while its source and the job remain open. A sliding-window Prism job releases both assigned windows when the signal completes and the main source stays open. | A signal source still open at 11 ms and 16 ms did not release the first sliding window in the tested pinned-Prism production-source job. Multiple signals, other trigger/lateness policies, and side-view pane updates remain open. |
| Completion barriers | Bounded heterogeneous signals and a single windowed signal have Direct and Prism fixtures. A same-type multi-signal barrier uses one co-group; a three-source fixed-window job matches values, timestamps, and windows on Direct and pinned Prism. Differently typed text and Boolean branches use windowed handles and one co-group; three independently watermarked sources match both runners in two windows, including an empty signal window. A Direct checkpoint test retains the open heterogeneous barrier, reopens all three independent sources at cursor 1, then commits the main row once after their watermarks close the window. For one default unbounded fixed/sliding-window signal, Direct releases main values as soon as the signal window closes and restores buffered main rows after a checkpoint; portable Beam lowering uses a matching-window side view. Pinned Prism commits a fixed-window value while the main source remains open, and commits both overlapping sliding-window values after the signal source completes. Direct also proves that a sliding signal closes only one of two overlapping main windows at its first watermark. An unbounded main source waits for bounded signals on Direct; the same TestStream pipeline matches pinned Prism on values and event times. Direct rejects a failed signal before opening the main source, invalidates a completed checkpoint when the bounded signal's source content changes, and resumes a partially completed main source from its saved cursor after rerunning the bounded signal. A failed second-bundle sink commit restarts from the first bundle's checkpoint and retries the same bundle identity without repeating committed output. The tagged Prism TestStream payload still requires one source coder, so each source begins as an integer before the branches map to different types. | Pinned Prism did not release a partial sliding-window signal while that source stayed open at tested 11 ms and 16 ms watermarks. Multiple-signal early release and broader window schedules remain open. |
| Rust-relevant I/O | Ten format crates, opt-in database helpers, provider-neutral objects, and four GCP clients have local tests. Portable JSONL object reading and publication use a local provider; a pinned-Prism Avro source binds a local object URI and reader schema in its worker and matches Direct on defaulted-field decoding. The existing portable format corpus covers the eager TFRecord source; a separate Prism fixture now proves its incremental streaming variant against Direct. The hosted object adapter implements bounded-memory sequential reads and conditional streaming writes; in-memory backend tests cover short reads, provider lifetime, missing objects, large immutable uploads, duplicate creation, and reader failure. Direct Avro and Parquet streaming sources decode through that adapter and reject changed object bytes with recovery errors. JSONL now has a sequential object source and sorted glob variant; a provider that rejects whole-object downloads passes its large-object test, and a changed object fails before rows are emitted. | Bind hosted providers and credentials in the portable worker, then prove remote replay and service I/O, including Pub/Sub and BigQuery, with controlled services. The in-memory hosted-adapter tests do not establish live GCS behavior. |

### Timer callback boundary

The pinned Beam proto and Python worker were checked for the timer work. A
timer mutation is scoped by bundle instruction, transform, family, encoded
key, encoded window, and dynamic tag. Fn Data carries timer-family streams
separately from element data, including bundles with timer input and no element
input. For a set operation, the pinned Python worker uses the fire timestamp
as the hold timestamp for event-time timers and the current input timestamp as
the hold for processing-time timers. A clear operation has no timestamps or
pane. The harness routes and decodes these records. Direct and the portable
bundle executor now invoke the registered element and timer callbacks for the
tested global-window event timer. The supervisor records each declared timer
family's input stream with the bundle's element input and replays both to a
replacement child. It checks family IDs and stream closure before publishing
timer mutations from the accepted attempt.

The pinned Prism worker sends timer blocks under the owning ParDo transform
and family IDs, then sends an `IsLast` marker for every declared family even
when that family has no firing. It rewrites the timer coder in the bundle
descriptor before execution. The v5 worker opens each declared timer stream,
drains its records even when no element arrives, and closes each outbound
family stream even when no callback sets or clears a timer.

Graphx now registers each declared timer family as a standard
`beam:coder:timer:v1` coder over the contract's key and window coders. Its
portable contract validator rejects missing or changed timer coder
definitions, and tests cover lowering and coder-ID collisions. This closes a
wire-format prerequisite for the portable worker. The worker plan now accepts
the declared value cell and validates the standard timer coder and time domain.

Core now has a runner-owned timer queue for declared event- and
processing-time families. It replaces or clears a keyed/windowed/tagged slot,
fires against the two clocks in stable deadline order, and checkpoints its
slots with the complete versioned function contract. Restore validates coder
bytes through a caller-supplied key decoder and rejects changed contracts,
duplicates, malformed windows, and non-millisecond timestamps without
mutating the current queue. Unit tests cover those boundaries. Direct now uses
the queue in its unbounded stream route. Element callbacks can set or clear
tagged timers, and timer callbacks can update the same keyed state and emit
through downstream transforms. Direct fixtures cover event and scripted
processing clocks, idle wall-clock firing, dynamic-tag clearing, fixed-window
key isolation, typed failures on callback and sink panics, timer output buffer
and state budgets, restore of armed timers with a source cursor, and retry
after a failed sink commit. The
public typed API currently requires
unbounded input, default global/fixed/sliding windows, and zero lateness.
Timer mutations returned by one callback are validated together before any
slot changes; a test checks that a bad later mutation leaves earlier clears
and sets unapplied. The queue exposes the next deadline by time domain so a
Direct source that returns `Pending` can wake for an armed processing-time
timer without waiting for another element.

The harness now converts a core timer firing or clear into the standard
TimerCoder record and expands an inbound record with several windows into
one firing per window. The conversion tests cross the actual wire encoder and
decoder, check dynamic tags and negative intervals, and reject overflow,
trailing window bytes, and clear records on a firing input. The bundle executor
now uses this codec for registered timer callbacks.

Core now exposes a runner-neutral invocation for the same registered element
and timer callbacks. It accepts runner-supplied coded value state and returns
the next coded cell, output envelopes, and validated timer mutations without
touching Direct's queue. A unit fixture invokes both callbacks, rejects a
firing with the wrong time domain, and confirms that the Direct checkpoint
remains unchanged. The portable executor binds the result to Fn State and Fn
Data within one bundle. A fake runner sends an element and a timer firing to
the same keyed value cell, then sends a second bundle with a timer and no
element input. Both outputs cross a downstream map, and the first bundle
emits one timer mutation. A live pinned-Prism TestStream arms
event timers for two keys. Direct and Prism both emit `1:3` and `2:4` after
the source watermark advances. A second TestStream advances processing time
and produces `1:3` on both runners; its callback checks that the output hold
timestamp is the input's 2 ms timestamp. A production source arms an event
timer before its second batch. The supervisor kills the execution child when
that batch reopens, replays the bundle, and Prism emits the accumulated `1:24`
result once when the 2 ms watermark fires the timer. The kill witness matches
the source's trigger PID. This proves one armed-timer replacement point; other
failure schedules remain open.

A separate TestStream replaces key 1's event timer from 3 ms to 5 ms and
clears key 2's 3 ms timer. Direct and pinned Prism emit only `1:3:5000000`.
The callback rejects either cancelled firing and checks that the replacement
keeps its output hold at 0 ms.

Another supervised Prism job kills the execution child inside the event-timer
callback. Its parent replays the declared timer-family input to a replacement
child. The job reaches `DONE`, the kill witness matches the selected child,
and both keyed timer outputs appear once. This covers a timer-input replay
boundary in addition to recovery after a source batch.

The same child-loss point also passes for two fixed-window timer firings:
Prism commits both rows once with their exact hold timestamps and interval
bounds after replay.

A separate local Prism job fires event timers in `[0, 10)` and `[20, 30)`
millisecond fixed windows. The callback checks each firing deadline and its
earlier output hold. Direct and Prism commit the same two rows with exact
output timestamps and interval bounds. The focused live test and strict
Prism-proof lint pass; this does not cover timer behavior under nonzero
allowed lateness or window merging.

A second live job assigns one event to two overlapping sliding windows.
Each window arms its own timer; Direct and Prism commit the same two rows
with the shared seven-millisecond output hold and distinct interval bounds.

Window-expiration callbacks now have an explicit public timer-family API for
whole-millisecond fixed windows. Graphx marks the Beam ParDo expiration
family; the harness validates its event-time coder; Direct schedules the
runner-owned firing after the window's maximum timestamp and invokes the
callback before state cleanup. A coded checkpoint restores its value cell and
armed expiration together. One two-window TestStream fixture produces the
same timestamped rows on Direct and pinned Prism in repeated live runs when
the source advances its watermark after each window. A second fixture with
offset fixed windows and a negative first start also matches Direct. When
both windows' elements for the same key arrived in one TestStream batch
before a single final watermark, pinned Prism committed only the first
expiration output. A control fixture with different keys in the two windows
and the same batch committed both outputs on Direct and pinned Prism.
An ignored live test retains the same-key two-row expectation. Running it
explicitly against pinned Prism reproduces one committed row, while Direct
commits both.
An overlapping sliding-window experiment likewise emitted only the first
expiration callback on pinned Prism while Direct emitted both, even with
separate watermarks after the two window ends. The public API rejects sliding
expiration at graph construction until that runner boundary is resolved.
Two keys in each fixed window also receive separate expiration callbacks;
Direct and pinned Prism commit all four results with matching event timestamps
and window bounds. Other expiration patterns remain open. A supervised Prism
job also kills the execution child inside an expiration callback; the parent
replays the runner-injected timer input and commits both window rows once.
The twelve live local Prism timer fixtures, strict proof-package lint, and
proof-package suite pass after this change. The full affected core, Direct,
Graphx, and harness suites passed for the underlying expiration API. At
`5b756bb`, the Linux core job reports **6567/6836 lines (96.06%)** and
**1340/1552 branches (86.34%)**, while Direct reports **6191/6786 lines
(91.23%)** and **1779/1976 branches (90.03%)**; all four unchanged floors
pass. The full portable Prism workflow, feature matrix, and package check
passed at `5b756bb`. CI, the full Portable Prism workflow, and package
verification also passed at `8510c37`.

| Required execution boundary | Direct | Portable worker |
| --- | --- | --- |
| Element callback schedules or clears a keyed timer | Implemented for the single-route unbounded stream path, with atomic timer mutations and coded state checkpointed with source progress. | A fake runner receives the element's set record on the declared family stream; local Prism fires the same event timer. |
| Timer callback produces state and output | Event-watermark, scripted processing-clock, and idle wall-clock fixtures pass. Output traverses downstream transforms to the sink. | A fake runner sends mixed and timer-only bundles through Fn Data; the worker reads and clears scoped Fn State before output traverses a map. Pinned local Prism jobs match Direct on two event-time results and one scripted processing-time result, including the callback's hold timestamp. |
| Recovery and expiry | A fresh Direct run restores an armed timer and value cell from one source checkpoint. A failed timer sink commit retries from that checkpoint and emits once. Corrupt combined state is rejected atomically. A fixed-window expiration callback restores its state and armed firing before cleanup. Multi-route state and broader expiration schedules need proofs. | A production job kills and replaces the execution child with an armed event timer, then emits one accumulated result on Prism. A child lost inside global event-time, processing-time, fixed-window, or expiration timer callbacks is replaced and receives the same timer input. The processing-time fixture checks one committed row and the child-replacement witness. Two fixed-window expiration callbacks match Direct when source watermarks advance between windows; two keys in each window produce four distinct callbacks. A same-batch two-window job passes with distinct keys; the same-key case loses one callback. Multi-route state and other failure points need proofs. |

The harness executes declared value, bag, set, map, multimap, and ordered-list
cells through Fn State. It reads the cell, invokes the registered callback,
and writes the new contents under the same bundle instruction. Fake Fn State
tests verify Get→Clear→Append ordering. A two-key, two-batch unbounded bag fixture produces the same five
counts on Direct and pinned Prism. Direct's coded checkpoint stores bag
contents with the source cursor; a core test restores them and rejects
malformed state atomically. A Direct stream restart resumes the bag at cursor
1, and a production unbounded Prism job retains its counts after execution
child replacement and residual reopen. Bag order is unspecified. A
bounded Direct/Prism fixture verifies callback contents with duplicate
entries and all four bag mutations. An unbounded fixed-window fixture proves
two independent bag cells for the same key across batches. A declared-family Fn Data dispatcher also reconstructs timer records split
across arbitrary chunks and calls a handler as each record
becomes complete. A fake-runner test confirms a callback runs before the
timer stream closes; other tests cover two records, an undeclared family,
and a callback error. Timer callbacks and other state families still need
pipeline execution and Direct/Prism retry proofs.

`SetState` now has an executable keyed API with `Keep`, `Clear`, `Insert`,
`Remove`, and `Replace`. Direct tests deduplication and coded restoration.
The portable worker lowers Beam's Set spec and uses the multimap Fn State
protocol; a bounded Direct/Prism fixture agrees on membership and all five
mutations. Pinned Prism keeps individually cleared multimap keys visible
within the bundle, so the adapter clears the keys cell and writes the new
set when contents change. That makes mutation work proportional to set size.
An unbounded two-batch fixture agrees on membership for two keys. Direct
restores a coded set and source cursor after cancellation. A production
unbounded Prism job retains the set through execution-child replacement and
residual reopen. Set-state timer callbacks remain unproved.
An unbounded fixed-window SetState fixture also agrees on membership in two
separate windows for the same key across element batches.

`MapState` now has a keyed API with `Keep`, `Clear`, `Put`, `Remove`, and
`Replace`. Core tests coded checkpoint restoration, duplicate-key rejection,
and callback rollback. A Direct stream test restores map values with its
source cursor. Bounded and two-batch unbounded fixtures agree on values and
key isolation between Direct and pinned local Prism. An unbounded fixed-window
fixture confirms separate cells for the same key. A production source job
retains its map after Prism replaces a failed execution child and reopens the
source residual. The portable worker uses Beam's multimap Fn State protocol
and replaces the keys cell when the map changes; mutation work grows with map
size. Sliding-window, timer callbacks, and broader map error boundaries still
need proof.

`MultimapState` now has `Keep`, `Clear`, `Append`, `Remove`, and `Replace` for
duplicate-preserving values at each map key. Core tests all mutations,
duplicate values, coded checkpoint restoration, malformed values, and
callback rollback. Direct restores the state with a production source cursor.
Bounded mutation, unbounded two-batch, and fixed-window fixtures agree with
pinned local Prism. A production source retains duplicate-preserving state
after the supervised execution child is replaced and its residual reopens.
The portable worker uses Beam's multimap Fn State keys; a changed cell is
rewritten within the bundle. Timer callbacks and broader error boundaries
remain open.

`OrderedListState` now has `Keep`, `Clear`, `Append`, half-open `ClearRange`,
and `Replace`, with explicit millisecond sort keys. Equal sort keys are
ordered by coder bytes for deterministic callback input. Core tests cover
duplicate keys, sorted reads, mutations, malformed state, callback rollback,
and coded restoration. Direct executes the bounded fixture and restores an
unbounded global cell with its source cursor. The harness reads and replaces
Beam ordered-list Fn State entries inside one bundle; a fake-runner test
checks range-scoped read and clear, base-key append, and wire bytes. Bounded
mutations and two-batch fixed-window cells agree on Direct and pinned local
Prism; the bounded fixture includes a negative millisecond sort key. A production unbounded source retains the cell after the supervised
execution child is replaced and its residual reopens. Prism may process the
two residual values in either order, so that fixture checks both corresponding
sorted-state observations. The pinned Prism build panics if a worker clears an ordered-list cell
before its first append, so the adapter skips that needless clear. Pure
additions, including duplicate entries, append only the new wire entries;
removal or replacement still rewrites the cell. Reads still materialize the
full list for each callback. Timer callbacks and large-cell performance remain
open.

These rows are not equally weighted. A percentage computed by counting URNs
would hide the difference between a declared state spec and a working
stateful pipeline. The target remains open until the missing behaviors have
executable fixtures and a documented weighting method.

### Portable wire-surface audit

This table groups the standard transform and coder families in the pinned
`beam_runner_api.proto`. An Ironbeam helper that expands into portable
primitives can satisfy a semantic contract without emitting the corresponding
composite URN. Naming a URN or serializing its payload does not prove that
the worker can execute it.

| Beam family | Current v5 execution | Next proof needed |
| --- | --- | --- |
| `ParDo`, `Flatten`, `GroupByKey`, `Impulse`, `WindowInto`, `TestStream`, `Reshuffle` | Stateless elementwise work, source setup, fanout, shuffle, window assignment, and tested stream schedules run through Direct and pinned Prism. | Broader fusion, pane, and multi-source schedules; worker-replacement and expiry proofs for user timers within `ParDo`. |
| `MapWindows`, `MergeWindows` | Global and identity side-input window mapping plus keyed session merging have tested pipeline cases. The Fn worker plans and executes `beam:transform:map_windows:v1` for global, identity, fixed, and sliding mapping specs. The fixed and sliding cases use the main interval's last timestamp and choose the earliest overlapping sliding target, matching pinned Beam's default mapping rule. It also plans and executes runner-issued `beam:transform:merge_windows:v1` for session functions with the specified nested KV/iterable coders. A fragmented Fn Data fixture verifies overlapping and endpoint-touching windows, chunked iterable input, exact merged output, and preservation of the runner nonce, outer window, timestamp, and pane. The worker merge and Direct follow Beam Java's strict interval overlap. Pinned Prism merged an endpoint-touching pair in an earlier pipeline fixture, so that boundary remains unresolved across runners. | Connect fixed/sliding mapping specs to a public cross-window side-view API and prove runner-issued `MapWindows` and `MergeWindows` on live Prism; resolve endpoint-touching and late-pane agreement, then test exact multi-source session behavior. |
| `CombinePerKey`, `CombineGlobally`, and lifted combine components | Ironbeam uses registered folds and portable grouping rather than native combine URNs. Tested outputs and timestamps cover the documented fold corpus. | Portable accumulator merging and compact hot-key execution without full group materialization. |
| SDF pair, split/size, process, and truncate | The first three stages execute for one-split resumable sources. The worker also accepts `sdf_truncate_sized_restrictions:v1` and applies Beam's default drain rule to the unbounded source restriction; a Fn Data fixture confirms that it validates the sized input and closes its output without a work item. `OffsetRangeTracker` supplies a claim/split/progress contract and Beam Java-compatible restriction bytes. | Execute registered user functions with offset restrictions, deliver dynamic splits and timer/watermark state, and prove runner-orchestrated drain plus custom finite truncation. |
| Native `GroupIntoBatches` and sharded-key batching | Count and buffering-time helpers expand to grouping and Beam triggers; tested Direct and Prism cases agree. An explicit, versioned shard selector assigns one hot key to two opaque shards before batching; bounded and fixed-window TestStream jobs agree on both count batches. Direct restores two open count batches from one source checkpoint and commits each once. A supervised Prism job kills its execution child inside the sharded-batch output callback, replays the failed bundle, and publishes the same two count batches as Direct with no duplicate rows. The byte-limited variant, its shared Direct/Prism streaming fixture, Direct checkpoint restart, and Prism child-replacement proof are recorded in the byte-batch row below. | Automatic hot-key sharding remains open. Native URN emission is optional if the expansion preserves behavior. |
| `WriteFiles`, Pub/Sub read/write, and provider operations | Format-specific worker sinks and provider-neutral/GCP clients exist. Local provider object I/O has portable jobs. | Portable service source and sink bindings, worker re-creation, and replay-safe publication under a controlled service or emulator. |
| Standard row, timer, schema, sharded-key, and state-backed iterable coders | RowCoder has wire and Direct/Prism jobs. `BeamShardedKey<String>` matches pinned standard coder examples and crosses a reshuffle on Direct and local Prism. The `Vec<u8>` key form now also crosses that boundary with non-UTF-8 bytes and exact shard IDs on both runners. Ordered-list and combining-state cells have bounded, streaming, and recovery proofs. Graphx registers and validates Beam timer-family coders; timer records have codec tests. A shared job proves event-timer replacement for one key and cancellation for another. State-backed iterables can be read from runner state. | Processing-time timer replacement, schema-coder jobs, and cross-SDK row evolution cases. |

These are evidence categories, not equal-sized percentage points. A scored
99% claim still requires per-case inventory, runner agreement, and an explicit
weight for each user-visible behavior.

The sharded-key proof uses Beam's native `sharded_key:v1` coder with a
`string_utf8:v1` child on a reshuffle boundary. The pinned local Prism job and
Direct produce the same two user keys and opaque shard identifiers. A second
job sends the `bytes:v1` child form through the same boundary with a `0xff`
key byte; Direct and pinned Prism agree on both keys and shard IDs. Neither
case proves automatic hot-key sharding or streaming `GroupIntoBatches`. A
second bounded job uses an application-versioned selector to split one hot
key into two shards, then batches each shard separately. Direct and pinned
Prism agree on the batch values. A third job uses a fixed-window TestStream
and emits one early full batch per shard on both runners; unbounded checkpoint
recovery on Direct restores both open shards from one saved cursor without
reopening the original source. A separate pinned-Prism job kills the supervised
execution child inside the sharded-batch output callback. The replacement
completes in the same job, and its two published batches match Direct without
duplication. This covers that failure point; other fused bundle boundaries
and runner crash schedules remain untested.

The RowCoder date, time, and UUID wire tests also have a bounded execution
fixture. It crosses a reshuffle boundary and matches decoded values on Direct
and pinned local Prism. This proves these three logical representations in a
portable job, not general logical-type support or cross-SDK schema evolution.

The new `RestrictionTracker` contract and `OffsetRangeTracker` implement
strictly increasing claims, fractional dynamic splits, zero-fraction
checkpoints, progress, and completion checks. The tracker serializes claims and
splits through a shared lock; tests cover concurrent claims and splits,
residual recovery, invalid fractions, and extreme offset ranges. `OffsetRange`
also encodes and decodes the two signed VarInts used by the pinned Java Beam
coder, including negative boundaries and malformed bytes. Its lazy `chunks`
iterator provides contiguous initial restrictions without allocating a list;
tests cover empty input, invalid sizes, signed offsets, and the full signed
64-bit span. Tracker progress and fractional splits now use the pinned Java
tracker's last-attempted-position boundary, including the position before the
start when no claim has been made. Focused tests cover no claims, skipped
offsets, fractions after claims, and concurrent splitting. Split arithmetic
now uses the decimal representation of Beam's fraction and truncates the final
position toward zero, matching pinned Java results for negative ranges and
the full signed 64-bit span. It is not yet
registered as an executable splittable transform on either runner, so the
splittable-processing parity row remains open.
The Linux core coverage job for commit `85a28b3` passed its unchanged floor:
**5366/5571 source lines (96.32%)** and **1082/1246 source branches (86.84%)**.
Its strict Clippy job also passed.

The provider-neutral Pub/Sub extension contract now exposes acknowledgment
IDs separately from message IDs. Its in-memory provider delivers published
messages to each subscription, supports acknowledgment after processing, and
assigns a new handle when an unacknowledged delivery is retried. Tests cover
fanout, retry, invalid handles, and the legacy provider's fail-closed default.
This is a service-contract proof, not a Beam Pub/Sub source or a live GCP
service proof.

The bounded completion barrier now accepts a materialized signal branch on
Direct. A two-source `Flatten` followed by `GroupByKey` finishes before an
unbounded main source opens; pinned Prism runs the same graph and agrees on
both output values and event times. Direct's checkpoint binds the content
digests of both bounded sources. Replacing the second source's values rejects
recovery before the main reader opens. This does not close the separate
windowed early-release gap in the table.

The pinned Go `state.Provider` and Python `userstate.py` clarify the first
stateful execution gate: a keyed callback must observe its earlier writes in
the same bundle, and a declared timer must have a callable handler. The
existing Ironbeam declaration and scoped Fn State RPCs do not yet connect to
an executable callback. The first end-to-end fixture must set value state,
read it on the next keyed element, fire an event-time timer, and survive a
failed bundle and a Direct checkpoint. Both Direct and pinned Prism must agree
on committed output before this family can count toward parity.

A stateful operation can now carry its versioned contract in the logical graph.
Graphx lowers that operation to a Beam ParDo with state specifications and the
stateful requirement. Lowering checks that its input is Beam KV, that the
declared key and window coders match, and that referenced coders exist. Focused
tests cover the emitted specification and a mismatched key coder. The builder
now registers a typed value-state callback with a state element coder. Bounded
Direct fixtures prove per-key isolation, fixed-window isolation, clearing a
cell, and empty state on a second run; Graphx translates that public API into
a validated Beam state spec. The portable worker now accepts only that one
value-cell shape. Fake Fn Data/Fn State runner tests prove a fused source and
a separate KV boundary, with successive reads, clears, and writes for two
keys. Local Prism execution, retry rollback, and cross-runner result agreement
remain open. Unbounded Direct execution remains gated until its cells can join
the checkpoint contract. Timer callbacks are still absent.

The open sliding-window completion-barrier discrepancy has a runner-stage
cause in the pinned Prism checkout. Its optimizer fuses the SDF main source,
window assignment, and side-input wait into one stage whose restriction input
has an infinite watermark. The signal source can report an 11 ms watermark,
with or without a signal element, but the stage waits for the side watermark
to catch up with infinity. A diagnostic main-side `Reshuffle` separates the
stages, but then Prism releases both `[0, 10)` and `[5, 15)` main windows at
the 11 ms signal watermark. The second window is premature. The experiment
was reverted; the shipped lowering still passes the fixed-window early-release
proof and the completed-signal sliding proof. This case requires per-window
readiness or a runner scheduling change before portable early sliding release
can be claimed.

| Family | Current evidence | Next completion proof |
| --- | --- | --- |
| Default panes and windows | Fixed and sliding default panes agree on values, event times, intervals, timing, and indices on Direct and pinned Prism. Single-input grouped Direct streams retain windows through positive allowed lateness and restore their coded pane state after restart. | Merged-session timing differs: Direct reports `OnTime`; pinned Prism reports `Early` for the same group. |
| Repeated early count | `early_count_trigger_named` and `accumulating_count_trigger_named` have distinct graph identities and lower to Beam's repeated early count trigger. Bounded and separate-advance fixtures match Direct and Prism, including accumulating `[2]`, then `[2, 3]`, and a retained on-time `[2, 3]`. An unbounded global keyed group now accepts a positive count trigger; Direct and Prism agree on early `[2, 3]` and final `[4]` (discarding) or `[2, 3, 4]` (accumulating) panes with matching metadata. Direct restores both modes from coded fold state after the first global firing; an additional streaming restart test restores source cursor 1 and avoids duplicate committed panes. | Other stateful fan-in and non-count early triggers remain open. |
| Default and explicit late count | `allowed_lateness_named` lowers a millisecond duration into Beam strategy identity for fixed and sliding windows. Direct and Prism emit on-time `[2]`, late `[3, 5]` at count two, and final partial `[7]` at expiry, with matching time and pane metadata. Direct restores a partial late group and its source cursor. Multi-source GroupByKey, including homogeneous co-group expansion, retains a coded fold through expiry with default or late count triggers. Both runners agree on two-source on-time `[[2], [4]]`, late `[[3, 5], []]` at count two, and final partial `[[7], []]`, including exact pane metadata and post-expiry drop. Typed two-source full joins now retain partial late rows until count two or expiry; Direct and pinned Prism agree on the on-time pair, two late count rows, and a final partial late row. A heterogeneous co-group also agrees on on-time `([2], ["right"])` and late-count `([3, 5], [])` panes, including exact pane indices and flags. All four typed join kinds agree on a late-count schedule: inner and right emit only the matched on-time row, while left and full emit the two late main-side rows and a final partial row at expiry. Direct and pinned Prism now run the same tagged-source heterogeneous graph and agree on both panes. A separate Direct proof covers two independent test streams; Prism accepts one TestStream. Direct restores the partial typed-join pane and both source cursors after a checkpoint without replaying the on-time pair. Direct also restores multi-source homogeneous late-count state and every cursor. Session-window lateness fails graph construction until merge-aware retention exists. | Extend explicit late count to broader schedules and merge-aware session windows. |
| Accumulating late count | `accumulating_late_count_trigger_named` lowers Beam accumulating mode. Direct retains on-time values and the new-element count across restart in single-input groups, multi-source homogeneous co-groups, and typed full joins. The dual-input checkpoint uses a distinct version, verifies retained row counts, and does not discard rows on empty watermark advances. A shared three-branch co-group matches pinned Prism on values, timing, and pane index. Pinned Prism coalesces the typed full-join late values into one final pane at expiry, whereas Direct emits a threshold pane and a later final pane; both retain the on-time pair. | Resolve the typed-join intermediate-pane scheduling difference before claiming exact backend equivalence. |
| Combined early and late counts | `early_and_late_count_trigger_named` lowers both repeated count subtriggers. The shared schedule yields early `[2, 3]`, on-time `[4]`, late `[5, 7]`, and final `[11]` on Direct and Prism with matching timestamps, indices, and flags. Direct also restores a partial late threshold after an early pane with no on-time values. | Pinned Prism labels the first post-watermark element on-time in the separate empty-on-time schedule; Direct labels it late. Processing-time and other composite triggers remain open. |
| Overlapping sliding late panes | Both runners maintain independent indices and expiry for each assigned interval. Values, times, timing, and indices agree. | Prism marked one late pane final because it fired after expiry; Direct fired it earlier and marked it nonfinal. |
| GroupByKey output timestamps | Earliest-in-pane is the default. `end_of_window_output_time_named` and `latest_in_pane_output_time_named` lower to distinct Beam strategies. Bounded Direct and Prism jobs emit 9 ms and 7 ms respectively for `[0, 10 ms)`. Global-window output-time policies agree on both runners: end-of-window reifies to the reserved `i64::MAX` nanosecond sentinel, which the worker maps to Beam's `GLOBAL_WINDOW_MAX_TIMESTAMP_MILLIS` on the wire; latest-in-pane reifies to the latest input time (7 ms in the bounded fixture). A global TestStream with a count trigger now matches in discarding and accumulating modes for early and final pane values, timestamps, timing, index, and finality. That fixture caught and fixed a Direct early-pane path that mistook the global trigger scope for a real interval and stamped its output one millisecond below the sentinel. Direct tests cover overlapping sliding windows, accumulating panes, and coded latest-time recovery, including global latest-time recovery. A two-advance Prism TestStream and Direct checkpoint tests cover unbounded latest-in-pane panes. Millisecond-aligned sessions accept both policies. A bounded bridge fixture merges `[0, 5 ms)` and `[8, 13 ms)` through an event at 12 ms into `[0, 17 ms)`; Direct checks 16 ms for end-of-window and 12 ms for latest-in-pane, with the matching Prism schedule pending rerun, including two other keys at positive and negative event times. A one-source TestStream bridge after an intermediate watermark also matches on both policies: `[0, 13 ms)` emits at 12 ms or 8 ms, while a post-closure row is dropped. Direct restores two open sessions before a bridge and retains the latest timestamp of the merged group. | Prove late and unbounded multi-input merged-session pane schedules with explicit policies. |
| Bounded merged-session join output time | A two-input session join receives events at 0 and 8 ms from one input and a bridge at 4 ms from the other. Both Direct and pinned Prism emit the two joined rows in `[0, 13 ms)` at 12 ms under end-of-window and 8 ms under latest-in-pane. A separate Direct collection test checks both policies without the fixture sink. | Unbounded multi-input joins, co-groups, and late session panes under explicit output-time policies. |
| Bounded merged-session co-group output time | Three branches of one source contribute intervals beginning at 0, 4, 8, and 12 ms. Both Direct and pinned Prism combine all three buckets in `[0, 17 ms)` and timestamp the result at 16 ms under end-of-window or 12 ms under latest-in-pane. | Independent-source scheduling, unbounded multi-input panes, and late session firings under explicit policies. |
| Fan-in output timestamps | Both policies apply to heterogeneous co-groups and all join kinds. Shared Direct and pinned Prism fixtures cover bounded matched and unmatched rows and two independent unbounded sources at fixed-window closure. Direct now retains per-key/window pane indices for positive-lateness joins and heterogeneous co-groups. The two runners agree on the full join's on-time `(Some(2), Some(4))`, late `(Some(3), None)` at index 1, and post-expiry drop. A heterogeneous co-group checks the same pane transition and typed side values on both runners. Direct tests cover all four join kinds and restore multiple retained keys, both source cursors, and committed pane indices after restart. Direct and pinned Prism agree on both late-join output-time policies: end-of-window timestamps both panes at 9 ms; latest-in-pane timestamps the on-time pane at 4 ms and the late pane at 3 ms. An overlapping sliding full join agrees on four exact rows, two window assignments, timing, and per-window indices. Prism marks its `[0, 10 ms)` late pane final when it fires at expiry; Direct emits it earlier as nonfinal. | Characterize the cross-runner finality schedule and cover wider multi-source schedules. |
| Combine output timestamps and global count panes | Vector and lifted global combines and lifted keyed combines follow both output-time policies. Shared bounded Direct and Prism fixtures agree for all three paths, including input arriving in reverse timestamp order. An unbounded global keyed combine with a count trigger now uses the materialized GroupByKey pane, then applies the combiner. Direct and pinned Prism agree on early sum `5` and final sum `4` (discarding) or `9` (accumulating), including pane metadata. | Keep ordered-fold timestamp rules distinct; prove unbounded global combine identity, additional window and replay cases. |
| User state and timers | Direct retains internal keyed grouping state. The portable worker handles runner-owned state-backed grouped iterables. Its Fn State client reads paginated cells and checks append/clear acknowledgements, response types, and runner errors. A cancelled State request now removes only its pending response registration; a concurrent request completes. Core defines versioned declarations for seven keyed, windowed state shapes and both timer domains. Graphx lowers those declarations into Beam ParDo state and timer specs and validates each timer coder's key and window components. The Fn Data client routes timer-family byte streams in both directions, retains inbound chunks across reader registration, and applies the replay-output byte budget to outbound timers. The timer codec round trips clear and set records, including interval windows and panes. All seven state cell shapes have Direct and pinned Prism execution proofs. Value and combining cells have worker-child replacement proofs. Direct executes event- and processing-time timer callbacks on one unbounded route, restores coded cells and armed timers with source progress, and retries a failed timer sink commit from that checkpoint. | A fake runner confirms mixed and timer-only input, Fn State mutation, and downstream output. Pinned Prism event- and processing-time timer jobs match Direct. An armed event timer and timer callbacks survive supervised child replacement on pinned Prism. A two-window expiration callback matches Direct when watermarks advance between windows. Prove broader expiration schedules, multi-route state, and additional failure points on both runners. |
| Streaming side inputs | Global bounded and window-closure iterable/singleton/map/multimap views run. A two-unbounded-source iterable view agrees after both watermarks close. | Materialize side-input panes before closure, support updates and late panes, and prove a changing side view without duplicate committed output. |
| Splittable processing | Production resumable sources and residual restrictions run through Fn Data; the Prism corpus covers worker-child replacement. | Expose general user splittable processing, restriction trackers, progress/watermark estimators, and dynamic split for transforms beyond source adapters. |
| Window merging and fanout | Fixed, sliding, and keyed sessions run. Direct and Prism match one-input Flatten, plus two- and three-source stateless Flatten with exact windows and event times. Three independent unbounded inputs and three branches from one unbounded source feed fixed-window homogeneous co-groups with matching values, times, and late drop. Three independent inputs also merge a connected session across sources on both runners when watermarks follow the bridge. Direct restores the open fixed-window group and a three-source open session after a bridge arrives on restart; the shared common map runs once per element. Pinned Prism retains independent bounded-source session and intermediate-watermark discrepancies. | Resolve those discrepancies and prove broader multi-source session schedules, exact panes, late firings, and allowed lateness. |
| Rust I/O and cloud resources | Format crates cover CSV, JSONL, Avro, Parquet, Arrow IPC, XML, MessagePack, CBOR, Protobuf, and TFRecord; SQL and MongoDB are opt-in. Provider-neutral objects and opt-in GCP clients exist. The Prism corpus binds a `file://` provider independently in the worker for JSONL, Avro, and Parquet. The Avro job resolves a reader-schema default; a supervised Prism execution-child loss during its first decoded row reopens the provider-bound object and commits both rows once. The Parquet job streams and verifies two objects under one prefix; a supervised execution-child loss during a row from the second object reopens the prefix and commits all three rows once. The eager TFRecord source runs in the comprehensive portable format job; a focused local job now proves `tfrecord_streaming_source` in a separate worker with the same decoded frames as Direct. | Prove remote worker binding and replay for provider-backed objects and service I/O, including Pub/Sub and BigQuery transforms, using local emulators or controlled service jobs where available. |
| Per-window distinct | `distinct_per_window_named` keys values through GroupByKey, preserving one value per window and its earliest input timestamp. Direct and pinned Prism match the three-row, two-window fixture including event times. A streaming count-trigger fixture matches two early `a` panes and one on-time `b` pane. A positive-lateness fixture matches an on-time `a` and a later `a` after two late inputs, including timestamps and indices. A value may reappear in a later pane. Direct now checkpoints a partly filled late threshold after one duplicate, resumes at source cursor 3, and emits the late distinct pane once without replaying the committed on-time pane. A supervised Prism job kills the execution child during that late pane; the replacement commits the on-time and late rows once each. The older `distinct_named` retains ordered finite-collection behavior. | Prove wider multi-key/window and host-replacement schedules. |
| Keyed batches by count | `group_into_batches_named` splits groups into fixed-size vectors and rejects zero or sizes at or above Beam's `i64::MAX` sentinel. Unbounded inputs set their own discarding count trigger at the batch size, replacing an earlier accumulating or smaller count policy; Direct, Graphx, and local Prism prove the override. With allowed lateness, Direct and pinned Prism both flush a late pair at the same batch count after an earlier full pane, including matching pane timing and indices; Graphx lowers both counts as discarding Beam firings. Both runners also agree on a full early batch and final partial batch in global and fixed windows. Direct restores a partial batch and source cursor. Both runners assign 1 ms to `[x, y]` and 8 ms to the later `[z]` batch from one key. | The helper uses GroupByKey and a registered splitter rather than Beam's native stateful GroupIntoBatches. Prove broader key and window schedules. |
| Keyed batches by time | `group_into_batches_with_max_buffering_duration_named` adds a count-or-processing-time trigger. Core validates and fingerprints the delay; Graphx lowers Beam `AfterAny`. Direct fires both paths and cancels the old timer when count wins. Pinned Prism completes a timed partial batch followed by a count batch after one more clock advance. With positive allowed lateness in a finite window, the combined policy lowers repeated count-or-time early firings and repeated late count firings. Direct and pinned Prism match a count pane, timed partial pane, and late count pane with the same pane indices. Direct does not re-arm the early timer after window closure. A separate Direct fixture checkpoints after the count pane, restores the armed timer and source cursor, then commits the timed and late panes once. Direct also proves retained values across early, on-time, and late panes in the accumulating form. | Pinned Prism panics if the tested window closes immediately after a count firing while the old timer remains queued. Broader mixed schedules remain conditional on runner scheduling; the accumulating combined trigger has the live mismatch recorded below. |
| Accumulating count-or-time panes | The general trigger retains values across a time pane, a count pane, and on-time closure in Direct. A two-source co-group fires count then time while retaining both branches. Its composite timer survives a Direct checkpoint, retries the same failed sink bundle ID, and reopens only the unfinished source. Graphx sets Beam accumulating mode. An ignored live test now compares a fixed-window count-or-time early trigger followed by a late-count trigger: Direct emits four accumulating panes, while pinned Prism reproducibly emits six. | Pinned Prism adds an Early and OnTime pane after the late inputs and marks its final Late pane `is_last`; Direct emits one nonfinal Late pane. The separate two-source test also gains an extra Prism early pane. |
| Byte-limited batches | Bounded and streaming count/encoded-byte limits, overlapping windows, explicit shards, Direct checkpoints, Prism worker recovery, a production-clock buffering-time fixture, and finite global partial-batch completion have evidence below. | Submillisecond holds and automatic sharding remain open. |

## Byte-limited batch evidence

| Case | Verified result |
| --- | --- |
| Count and bytes | Bounded Direct and pinned Prism agree on count and encoded-value byte thresholds, an oversized singleton, and per-batch earliest timestamps. Both limits reject zero and Beam's reserved `i64::MAX` sentinel without adding graph nodes. The unbounded fixed-window fixture emits six exact batches across two interleaved keys. A separate same-batch out-of-order fixture uses `c` at 4 ms before `d` at 3 ms; both runners assign the batch a 3 ms timestamp. |
| Windows | Fixed, sliding, offset, and global schedules have shared Direct/Prism fixtures. A finite global stream flushes two partial batches through processing timers on both runners, including the final `[c]` batch before source completion. Supervised Prism replaces a lost worker at the residual source cursor and publishes both global batches once. Without a buffering duration, another finite global fixture commits two complete batches and one partial batch at source completion on both runners. |
| Processing-time expiry | The combined count/byte/duration helper flushes `[a,b]` on a 20 ms processing timer and `[c]` at the fixed window end from a delayed, replayable production source on Direct and pinned Prism. Direct tests cover count and encoded-byte limits, bounded final-partial flushing, and an overdue timer encountered by the next element. A Direct checkpoint test restores one partial batch, its armed processing timer, and the source cursor; the timer commits `[a]` once, then window closure commits `[b]` once. A supervised Prism run kills and replaces the execution child at the residual source cursor, then commits `[a,b]` and `[c]` once each. The source cursor carries its absolute delay deadline so reopened workers cannot reset the wait. The harness closes timer output families after every family callback has run, allowing one family's callback to clear a previously visited family. A TestStream attempt produced `[a,b,c]` together on Prism because its downstream stage ran after the scripted processing-time advances; that schedule is not counted as a parity proof. The exact local coverage gates before the recovery-only test passed unchanged: core **6,842/7,122 lines (96.07%)** and **1,451/1,678 branches (86.47%)**; Direct **6,488/7,119 lines (91.14%)** and **1,846/2,050 branches (90.05%)**. |
| Explicit shards and recovery | A versioned selector splits one hot key into two stable shard IDs with independent byte counts. A shared unbounded fixed-window pipeline yields three exact shard, timestamp, and batch rows on Direct and pinned Prism. Direct restores two open shard cells and one source cursor after restart, without rereading the first source batch. Pinned Prism replaces an execution child lost during sharded output and commits those three rows once. A separate nonsharded byte-batch child-loss fixture also commits its six batches once. |
| Wire and quality gates | Graphx and the harness retain KV coder framing across the reshuffle that Prism may elide; a keyed TestStream also retains KV framing when an ordinary reshuffle precedes stateful ParDo. Direct treats only the explicit shard boundary as an identity in its single-process stream plan; an ordinary reshuffle remains a materialization boundary. Local core coverage passes at **6,714/6,990 lines (96.05%)** and **1,409/1,630 branches (86.44%)**; Direct passed at **6,476/7,106 lines (91.13%)** and **1,844/2,048 branches (90.04%)** before the later recovery-only test. |

The fixed-window remainder flushes at its event-time end. Overlapping sliding
windows keep three distinct batches for two values of one key. A fixed window
with a 7 ms offset keeps separate `[-13, -3)` and `[-3, 7)` ms batches, including
negative event timestamps. Both runners agree on those intervals and flushes.
The global fixture emits full batches for two keys after an intermediate
watermark.

Batch members have no portable order. Direct kept the out-of-order pair in
encounter order; pinned Prism returned it in event-time order. The fixture
sorts members only for its comparison rows. Unbounded timer output holds
require whole-millisecond event timestamps, and session windows or window
ends that do not align to whole milliseconds are rejected.

Finite global batches now flush pending values through a runner-owned window
expiration callback. Direct advances its stateful stream to the terminal event
watermark when the source completes. The portable harness maps Beam's global
expiration sentinel only for that runner-owned timer family. The callback
reifies the earliest buffered element timestamp before output. A global
watermark-hold timer caused pinned Prism to panic when a complete batch was
followed by a partial batch; the passing implementation relies on the runner's
expiration callback without that redundant hold. The shared test covers two
full batches, an intermediate watermark, and one final partial batch. Wider
global schedules and unbounded streams that never complete remain outside this
proof.

The clock-aware stateful callback now receives Direct's source processing clock
or the portable worker's wall clock, without changing existing callback
signatures. A shared TestStream arms a timer four milliseconds after the first
element, adds a second element after three milliseconds, and emits the combined
state when the timer fires. Direct and pinned Prism both emit `1:3`. The fixture
rounds the deadline up to a whole millisecond for Beam's timer wire format.
The combined batching helper now uses that clock. Its Direct checkpoint and
supervised Prism worker-replacement proofs pass. The initial Prism attempt
stalled because the test source reset its delay on every reopen and the harness
closed one timer family before another family's callback cleared it. Both
faults were corrected and the replacement run committed each expected row once.
The
pinned Java `GroupIntoBatches` uses the timer service's current relative time
and holds the output watermark at the earliest buffered event time.
The exact coverage gates pass after this change: core **6,732/7,008 lines
(96.06%)** and **1,409/1,630 branches (86.44%)**; Direct **6,488/7,119 lines
(91.14%)** and **1,846/2,050 branches (90.05%)**.

## Release gates

### Behavior and API

| Gate | Done when |
| --- | --- |
| ☑ Public API and bounded v4 behavior audit | All 52 method inventory rows across 46 v4 modules have owners in the [helper tracker](V5_HELPER_PARITY_TRACKER.md), and the [migration guide](V4_TO_V5_MIGRATION.md) names the intentional changes in keys, terminals, time metadata, batches, ordering, checkpoints, and deferred I/O. The full v4 compatibility corpus and local package matrix pass. Backend agreement, unbounded recovery, durability, and connector limits remain separate gates below. |
| ☑ Custom transform execution | Versioned map, filter, flat-map, and batch handlers execute on Direct and local Prism. Typed and reusable `DynOp` handlers use an explicit batch size and semantic version on both runners; size one runs incrementally on unbounded inputs. V5 deliberately replaces v4's implicit physical partitions with explicit batches. Planner hints remain accepted for migration but do not affect v5 scheduling or graph identity. `custom_named` is a graph marker; executable custom work uses a registered handler. |
| ☑ Portable operation coverage | The advertised source, elementwise, time/window, shuffle, grouping, join, fold/combine, side-input, wait, and append/output paths have shared Direct/Prism success proofs. Meaningful callback, validation, duplicate-order, batch-shape, signal, sink-abort, and worker-loss failures are covered. Encounter-order folds, unordered larger keyed batches, and bare custom markers fail portable preflight; their explicit-order and versioned-handler alternatives are proved. Backend recovery and graph-shape agreement remain separate gates below. |
| ☑ Runner agreement for the established shared corpus | The 54 non-destructive local Prism suite cases cover values, grouping, event times, windows, typed failures, and committed output for graph shapes accepted by both runners. This includes all four bounded and separate two-unbounded join kinds, late-window handling, negative-time offset windows, GroupByKey, ordered keyed batches, co-group, and stateless Flatten. Direct now executes the combined four-join fanout with one sink; this new graph shape has a Direct restart proof but has not yet been added to the shared live Prism corpus. |
| ☑ Representative portable database connector parity | An opt-in local Prism fixture reads three SQLite and MongoDB rows, transforms them in the separately launched worker, and writes exact results with replay-safe sinks. Direct produces the same rows. Each Prism job is run twice and the database still contains exactly three rows. SQLite uses a versioned binding identity and caller-supplied conflict handling; MongoDB replaces rows by non-null `_id`. The default Prism fixture excludes database dependencies. Non-idempotent legacy append helpers remain Direct-only because they lack a portable configuration digest. Hosted Postgres/MySQL and remote MongoDB workers still require their own service proofs. |

The portable operation audit uses the graph variants accepted by the Beam
lowerer. Explicit-order folds, ordered keyed batches, and registered custom
handlers expand into those variants before lowering.

| Operation family | Shared proof and failure boundary |
| --- | --- |
| Source; Map, Filter, FlatMap; AssignTimestamp | Bounded and production unbounded fixtures, timestamped TestStream, dead-letter and validation paths; typed map panic, validation failure, bad dynamic output, source residual rejection, and worker loss. |
| Flatten; Reshuffle; GroupByKey; CoGroupByKey | Multiple bounded and two-unbounded sources, native shuffle, ten-input co-group, and exact grouped time/window metadata; malformed shuffle boundaries fail harness validation and failed bundles do not publish final output. |
| Inner, left, right, full joins | One shared bounded graph checks paired, left-only, and right-only keys; two-unbounded full join and mixed-bound cases check live windows and event times. |
| FixedWindow, SlidingWindow, keyed sessions; folds and global combine | Ordinary, offset, overlapping sliding, and merging session windows include negative event time; bounded and one-source unbounded keyed sessions pass. Keyed/global folds and empty global identities pass. Duplicate explicit order keys and panicking fold/combine callbacks fail without committed output. |
| SideInputMap; WaitOn; Output; append Sink | Global and per-window side views, bounded and streaming main inputs, signal ordering, fanout, and finalized format outputs pass; side-view panic, signal-source failure, staged-bundle failure, and worker loss fail closed. |
| Deliberately Direct-only graph variants | Bare `Custom`, encounter-order `FoldOrderedGlobal`/`FoldOrderedPerKey`, and unordered `BatchMapValues` above size one fail typed portable preflight. Shared fixtures prove versioned custom handlers, explicit-order folds, and explicit-order keyed batches through portable primitives. |

### Streaming and durability

| Gate | Done when |
| --- | --- |
| ☑ Unbounded state and supervised execution-child recovery; ☐ Control-owner replacement | Direct checkpoints source cursors with coded open-window state and survives OS-process restart for two-source join, four-join fanout, grouped stream, and stateless multi-source route. Fn Data passes split and residual batches through separate processes. A stable Prism Control owner now buffers a bundle's input, replaces a killed execution child, and publishes only the accepted attempt's output. The production job reaches `DONE` after a child dies during residual polling, with exact final-result and sink checks. A second live job kills a child after local output and observes no duplicate at Prism. Supervised mode preflights replayable sources, idempotent or durably finalized sinks, and a journal for finalizers; its input and output replay buffers are each capped at 64 MiB per bundle. A disconnected Control owner still fails active Prism instructions, so host-level replacement and hosted runners require separate proof. |
| ☑ Local output durability | Direct checkpoint and final-result files are synced before publication, and their directory entries are synced before dependent state is removed. The current Rust 1.101 nightly implements `File::sync_all()` using `F_FULLFSYNC` on macOS. Process-exit and simulated lost-directory-entry faults cover checkpoint data, manifests, fragment staging, acceptance, and final publication; recovery verifies an old or new valid state and exact completed attempts. Direct retains verified fallback checkpoints. V5 batch/progress checkpoints replace v4's node, barrier, and time schedules. This is the local filesystem contract; a literal power-cut experiment and remote object-store guarantees are outside this release proof. |
| ☑ Local object result memory bound | Portable JSONL publication streams accepted fragments through a disk scratch file, validates each fragment and the complete result, then conditionally creates the final local object from a reader. The worker caps each in-memory bundle and decoded JSONL record at 64 MiB; the completed result has no in-memory size cap. Local adapter tests cross read buffers, reopen the object, retry idempotently, and reject conflicting bytes; the production unbounded Prism job passes. Providers without streaming methods fail closed. Hosted provider streaming and consistency proofs remain a later extension. |
| ☑ Exact committed counts in the local Prism corpus; ☐ long-running Prism metric retention | Eight consecutive Linux production runs exposed a Prism short-ID registration race: two accepted MessagePack fragments held three rows while the reported committed count was one. Prism buffers unknown metric payloads by short ID, so concurrent bundles using one ID can overwrite a contribution before metadata arrives. The worker now gives every bundle a distinct short ID and reconstructs its metadata without keeping a per-bundle registry. Eight Linux production runs and the full portable suite pass with exact counts. The pinned Prism retains its short-ID metadata for the life of a job; memory use for indefinitely many bundles remains a runner-side limit, and hosted Dataflow has not been assessed for this behavior. |

The same-ID respawn experiment failed because Prism treats a disconnected Fn
Control stream as an error on every active instruction. The supervised path
keeps that stream in a parent process and replays input to disposable children.
The separate checkout was not changed.

### Packaging and release quality

| Gate | Done when |
| --- | --- |
| ☑ Core and Direct coverage on current branch; ☑ local workspace and expanded CI verification | The unchanged local Direct package gate passes at **6,463/7,092 lines (91.13%)** and **1,838/2,040 branches (90.10%)**. The local core gate passes at **6,689/6,964 lines (96.05%)** and **1,390/1,610 branches (86.34%)**. On `986a6c4a`, all required PR checks pass, including Clippy, documentation, release configuration, Beam worker and Prism, facade tests, package verification, package coverage, and the v4/v5 feature matrix. The opt-in database proof compiled earlier but was not rerun against live database services after the import. Coverage is a release floor, not a claim that every branch is exercised. |
| ☑ Performance | The paired million-row corpus matches outputs and passes explicit v5 latency and peak-memory limits. |
| ☑ Versioning configuration; ☐ registry publication | All 35 publishable packages are `5.0.0` candidates. Semantic Release selects a major release after v4.1.1 and prepares one changelog entry containing the reviewed summary and generated commit history. A local simulation of its analyzer, notes generator, workspace adapter, and changelog plugin selected 5.0.0 and validated the dependency order. New crate names cannot be introduced through crates.io Trusted Publishing: each needs an initial manual publication, followed by its Trusted Publisher configuration. The manual release workflow must remain undispatched until that bootstrap is complete. Registry authorization and publication are not covered by the local simulation. |
| ☑ Day-one GCP resource clients | `ironbeam-cloud-gcp` 5.0.0 packages the official Google Rust clients for Storage, Pub/Sub, BigQuery, and Secret Manager. Each service has its own feature; the root facade offers matching features and an aggregate. The client crate's unit tests, isolated feature builds, strict lint, local package verification, all-feature workspace tests, and Zensical guide pass. At the user's direction, no live GCP integration test was run; successful operations, IAM, and hosted Dataflow behavior are not claimed by this gate. |

### Paired performance gate

On 2026-10-12, `scripts/check_v5_performance.sh` rebuilt both release
executables with Mr Boxington and passed seven one-million-row cases, each
with two warmup pairs and eight alternating sample pairs. Every execution
matched its v4 counterpart's output count and checksum. V5 used an explicit
1 GiB Direct job budget for the larger materialized cases. Peak memory is
process RSS from macOS `/usr/bin/time -l`.

| Case | v4 ms | v5 ms | v5 max ms | v5 peak MiB | v5 max MiB |
| --- | ---: | ---: | ---: | ---: | ---: |
| Source copy | 0.542 | 1.018 | 1.5 | 16.28 | 24 |
| Map/filter | 0.936 | 2.145 | 3.5 | 10.48 | 16 |
| Group by key | 5.299 | 10.575 | 16 | 21.76 | 32 |
| Latest per key | 6.436 | 10.846 | 16 | 10.82 | 16 |
| Inner join | 203.280 | 103.962 | 160 | 273.63 | 350 |
| Duplicate-key join | 28.066 | 10.175 | 16 | 42.80 | 56 |
| JSONL round trip | 40.505 | 51.451 | 75 | 19.46 | 24 |

The absolute map/filter limit follows the accepted small-latency criterion;
its v5/v4 ratio is 2.29× in this run. The driver returns failure for a
violated limit, verified with a deliberately low map/filter threshold. These
are local release bounds with headroom for host variation, not portable CI
timing promises. Graph shapes outside the benchmark corpus still need their
behavior and recovery gates. The earlier Direct typed-route optimization and
compact inner-join measurements remain in `V5_PHASE0_BASELINE.md`.

## Completed gates

| Gate | Evidence |
| --- | --- |
| Supervised same-job execution recovery | The local Prism production job kills a child during an active residual source poll and still reaches `DONE` with exact idempotent sink and finalized format checks. A second supervised job exits after writing local Fn Data output, replays the bundle, and matches Direct without duplicate runner output. Replay-safety preflight has unit tests. The unsupervised disconnect fixture continues to fail closed. The Control-owning parent process remains a runner failure boundary. |
| Sliding-window agreement | `sliding_windows` assigns each element to every overlapping interval, with an explicit 1,024-window fanout limit and checked time arithmetic. The bounded five-row fixture crosses zero and produces six exact keyed groups with matching values, earliest event times, and intervals on Direct and local Prism. An unbounded source closes overlapping windows on watermarks, drops a late row, and matches on both runners. Direct's coded checkpoint restores two assignments of one event. The earlier 52-case non-destructive Prism suite passed. Custom triggers and general state/timers remain open. |
| Bounded session agreement | `session_windows_named` assigns Beam sessions before another transform; `group_by_session_named` assigns and groups. Direct and local Prism agree on overlapping bridge events, negative time, and separate keys. Direct now keeps endpoint-touching sessions separate, matching Beam Java; pinned Prism merged such a pair in an earlier fixture. Shared fixtures prove a two-input join and a three-branch, shared-source co-group merge, including an empty side. Direct also checks left, right, and full joins, independently sourced three-input co-groups, and coded group-fold recovery. Portable gaps require whole milliseconds. Independent-source Prism scheduling can split an otherwise connected three-input session, as recorded above. |
| Unbounded session agreement | A keyed TestStream bridges open sessions after a watermark closes another, then drops a late row. Direct and local Prism agree on values, event times, and intervals. Direct restores open sessions and the source cursor before the bridge. Two-source unbounded joins and co-groups now have cross-input watermark and recovery tests; the intermediate-watermark and three-or-more-source cases remain open. |
| Public pane metadata | `PaneInfo` is shared by core and the Fn API coder, has a stable typed element coder, and travels through Direct envelopes and portable worker boundaries. `reify_panes_named` exposes it to transforms; typed sinks receive it in `ElementMetadata`. Direct and pinned local Prism agree on default source panes and grouped fixed and sliding panes, including timing, first/last flags, and indices. For one merged-session group, Direct emits a first-and-last `OnTime` pane while Prism emits a nonfinal `Early` pane; values still agree, and Prism's sink metadata matches the reified pane. Direct's coded window checkpoint preserves a nondefault pane and reads older pane-free rows as `NO_FIRING`. The 66-case non-destructive Prism suite and core/Direct/harness suites pass; strict core/Direct and harness/fixture lint passed for the initial pane change. Custom triggers, session pane agreement, and access to the opaque element metadata payload remain open. |
| Local unbounded recovery | Direct's two-source join test exits one OS process with an open window, resumes only the unfinished side in another, and reopens no source in a third. The Fn Data test runs split, first output, and residual output in separate processes for one and two sink branches. Journal process-exit tests recover completed attempts and reject partial or wrong-identity publication. The 20-case Fn Data integration suite and full harness tests pass; the 48-case live Prism corpus includes a worker-loss case that fails closed. |
| Stateful fanout extension | One Direct graph with two unbounded sources, all four stateful join kinds, Flatten, and an idempotent append sink produces four distinct results. Its coded open window resumes after cancellation and after a fresh OS process starts. A failed sink commit replays the same bundle without duplicate output. A separate shared graph emits eight exact rows for matched and unmatched keys through those four branches on both Direct and pinned Prism, including event times and windows. In a second Prism job, the first execution child exits in the output callback; its replacement publishes the same eight rows once, with a failure witness. Branches still must share the same two source routes and fixed-window contract; other shapes remain outside the tested path. |
| Local release order | A manifest-driven checker verifies each publishable path dependency's package name and version, detects cycles, and places the public facade last. Its four focused tests and all eight script tests pass. The complete 29-package local matrix passed with that generated order, including workspace/feature tests, docs, and local packaging; the 20 GiB temporary build and log were removed. The legacy root-only release workflow refuses this v5 workspace. Nothing was published. |
| Coverage at staged 5.0.0 version | The exact CI core/Direct/transforms set passes unchanged floors: core **3851/4007 lines (96.11%)**, **590/676 branches (87.28%)**; Direct **6277/6759 lines (92.87%)**, **1298/1438 branches (90.26%)**; transforms **1178/1206 lines (97.68%)**, **108/116 branches (93.10%)**. The largest uncovered source groups are stream callback and restart error handling, generic coder/helper forwarding, and defensive overflow paths; key recovery and publication boundaries have focused fault tests. |
| Late-window runner agreement | A shared two-unbounded inner join closes its fixed window after both watermarks advance, then receives another row for that window. Direct drops that late row, matching live Prism's zero-lateness result; both commit exactly the on-time pair. A separate production unbounded GroupByKey fixture now sends a row for an already closed window in its final residual batch; Direct and Prism both retain the same three on-time groups. Direct also drops late rows in bounded-side routes. The 40-case Direct stream suite and 48-case live Prism suite pass. One concurrent Prism run briefly reported an incomplete final-bundle metric; the standalone fixture and rerun passed after its bounded metric convergence window was extended. |
| Two-source Direct process restart | A child process checkpoints two unbounded sources with an open join window, then exits without shutting down the job. A fresh child rebuilds the graph, resumes only the side source from cursor one, closes the window, and writes one durable result. A third process verifies the completed checkpoint opens no readers and produces no duplicate. The focused test, full Direct integration suite, formatting, and strict test lint pass. |
| Each two-unbounded join kind | Four separate shared graphs use identical tagged TestStream sources and finite windows. Inner, left, right, and full joins each match Direct and live Prism on exact paired or unmatched rows, event time, and window. One combined graph branches the same two unbounded inputs to all four joins and flattens their outputs into one idempotent sink; Direct and pinned Prism agree on eight exact rows. Direct separately proves checkpoint recovery and failed-commit replay for a four-join graph. Pinned Prism replaces a killed execution child inside that graph and publishes all eight rows once in the same job. Other DAG shapes and failure points remain open. |
| Portable operation audit | The 46-case live Prism suite and graph translator/harness suites cover the accepted operation families in the table above. A new graph translator test proves that bare custom markers, unordered keyed batches larger than one, and encounter-order global folds are rejected at portable preflight. The full graph translator suite and warning-free lint pass. This closes operation-family coverage while runner agreement across additional graph shapes and recovery remain open. |
| Root facade CI gate | The former root coverage job produced zero instrumented files because the v5 facade only reexports implementation crates; its 4.1.1-era source coverage baseline was impossible to satisfy. CI now runs all-feature root tests and the coverage script tests there, while the package matrix continues to enforce implementation coverage and uploads package-scoped LCOV. A representative Postcard package passed its 100% source-line baseline and produced a nonempty LCOV file with package-scoped export. The all-feature root run exposed a stale `CompressionCodec` example signature; after fixing it, all 21 root integration tests, its doc test, formatting, and strict all-target lint pass. CI YAML, baseline JSON, and coverage script tests parse and pass. |
| All bounded join kinds on Direct and Prism | A single shared graph joins the same two fixed-window sources with inner, left, right, and full joins, plus co-group. Its exact 20 rows include both unmatched-left and unmatched-right keys and event time/window fields. Direct and live Prism outputs agree; the full 46-test live suite, formatting, and warning-free Prism proof lint pass. |
| Typed portable callback panics | Shared fixtures panic in a map handler after one row, a side-view callback after one main row, a global combine callback after native Beam grouping, and a keyed fold input callback after native grouping. Direct and live Prism return execution failure at the relevant node with no sink publication. The portable worker converts these callback panics to typed node errors while retaining the panic text, including validation failures. The full 46-test live Prism suite, full harness suite, formatting, and strict harness/proof lint pass. |
| Negative-time offset windows | A shared five-row keyed graph assigns 4 ms windows with a 1 ms offset across negative and positive timestamps. Direct and live Prism both emit the exact three grouped rows, earliest event times, and window boundaries. The full 46-test live suite and warning-free proof lint pass. |
| Formatting and current suites | Both workspaces pass formatting, workspace tests, and the live Prism suite from their last full runs. |
| Explicit custom batches | The five-row shared Direct/Prism fixture observes `[1, 2, 2]` lengths at batch size two. The complete local suite passes 40/40, and strict Prism fixture lint passes. One earlier parallel run saw a committed MessagePack count of one immediately after DONE; the standalone test and subsequent serial and parallel runs passed. The proof now waits up to three seconds for all committed counters to converge and still requires exact counts before publication. |
| Cloud package cleanup | Contracts, fakes, operations, objects, provider clients, and JSONL readers have separate owners. The unused legacy cloud crate is removed; the root facade plus 28 workspace crates verify locally. |
| Current local package matrix | On 2026-10-12, `scripts/check_v5_packages.sh` passed again after the custom-batch and final-result changes: workspace tests, facade and optional-feature tests, all-feature docs, and local package verification for all 29 packages. The 20 GiB isolated build and log were removed. Nothing was published. |
| Bounded API ledger audit | The Phase 0 inventory has 52 method rows from 46 v4 modules; every module appears in the helper tracker. The audit found one missing direct convenience for keyed sorted collection and added it through the Direct package and v5 prelude. Both default and minimal facade feature tests pass. The migration guide now describes current two-source streaming co-group and one-source keyed fold recovery rather than their earlier materialization-only state. |
| Current core, Direct, and transforms coverage | The exact CI package set passed again after the keyed-sort API and multi-sink failure test: core **3851/4007 lines (96.11%)** and **590/676 branches (87.28%)**; Direct **6272/6762 lines (92.75%)** and **1295/1438 branches (90.06%)**; transforms **1178/1206 lines (97.68%)** and **108/116 branches (93.10%)**. All six unchanged ratchets pass. Inspecting uncovered paths exposed a multi-sink split-decoder panic case; its new test proves an execution error and no sink commit. Other uncovered contracts remain under review. |
| Local object conditional create | Nested object parents are created component by component and each new directory entry is synced. A symlink escape is rejected before an outside child is created. Reopening a nested root returns the immutable object; the cloud-object package tests, strict lint, and the separate Dataflow object-journal tests pass. |

## Latest measured results

| Check | Result |
| --- | --- |
| Current windowing and recovery verification, 2026-10-13 | The exact core/Direct/transforms CI package set passes all six unchanged source-location ratchets: core **3708/3860 lines (96.06%)**, **618/706 branches (87.54%)**; Direct **4496/4938 lines (91.05%)**, **1296/1436 branches (90.25%)**; transforms **1068/1095 lines (97.53%)**, **109/116 branches (93.97%)**. The full workspace suite, strict affected-package Clippy, strict Zensical build, 52 non-destructive live Prism cases, native Arrow IPC job, and separate fail-closed Control-loss case pass. Direct tests cover checkpointed session recovery, sliding intervals across zero, grouped and multi-source panic attribution, stream preflight, and source batch clone failure. |
| Fn Data completion ordering, 2026-10-13 | Fn Data writes were queued in memory when the separate Control response completed. A bundle-end drain barrier now waits for the outbound transport to consume every queued data chunk and end marker before Control completion. Its focused unit test, 12 repeated local production Prism runs, full harness and workspace suites, and portable lint pass. A later Linux failure showed that this barrier did not explain every committed-count underflow; the Prism metric registration race is recorded above. |
| Direct grouped streaming | A single unbounded source feeds one fixed-window GroupByKey and linear downstream stages. Direct closes windows on source watermarks or completion, preserves the earliest group event time, drops rows for closed windows, enforces its element budget, and commits before exposing progress. Two shared live Prism cases match Direct on grouped values and explicit-order keyed batches, timestamps, and windows. Open coded fold state is saved with each source cursor in a content-checked file; a stopped job resumes the still-open window, corrupt latest state falls back to retained history, and forced process exits at three publication boundaries recover a matching cursor and fold. A separate fixed-window route joins one unbounded main with one bounded side. |
| Direct stateless multi-source streaming | Unbounded sources feeding Flatten are polled round-robin. Each committed batch has a route-specific idempotent bundle ID; the checkpoint stores all route cursors as one checked state snapshot. A two-source test stops after one source completes and the other pauses, then restarts and resumes only the remaining cursor with exact output. A failed second-source commit replays only that source, and flattening the same source twice retains both copies. |
| Direct two-unbounded fixed-window joins | The main and side cursors, coded open windows, and a shared sequence are checkpointed together. A join waits for both watermarks before closing an open window; after cancellation, the completed main stays closed and only the side resumes. A full join emits rows present on either side and replays a failed sink commit from the matching checkpoint. A cursorless watermark can close a window, survive a restart, and replay without a duplicate result. A shared live Prism case confirms a late row after both watermarks closed its window is dropped while the on-time pair commits once. |
| Multi-source Direct coverage | The exact CI Direct package command now reports **6262/6755 source lines (92.70%)** and **1295/1438 branches (90.06%)**, above its unchanged 91% and 90% floors. Contract tests exercise invalid source and coder capabilities, mismatched route/cursor/window checkpoint state, completed recovery, empty progress, and a shared open-window budget. The full all-feature Direct package suite and strict all-feature lint pass. |
| Fresh package coverage | On 2026-10-12, the exact core/Direct/transforms CI package set passed every test. Source-location coverage after focused coder, join, recovery, and multi-sink cases: core **3850/4006 lines (96.11%)**, **590/676 branches (87.28%)**; Direct **5460/5865 lines (93.09%)**, **1134/1260 branches (90.00%)**; transforms **1163/1189 lines (97.81%)**, **108/116 branches (93.10%)**. All six current ratchets pass without lowering a threshold. The remaining uncovered contract paths still need review before the release quality gate closes. |
| Lazy compressed file input | The shared lazy reader now wraps gzip, zstd, bzip2, and xz files with a streaming decoder. JSONL, CSV, Avro, MessagePack, CBOR, Protobuf, and TFRecord consume decoded records as requested. Protobuf and TFRecord check frame truncation while reading, so a corrupt huge length prefix does not allocate its claimed size. Compression and shared I/O all-feature tests, the seven dependent format suites, and strict lint for all changed packages pass. Eager sources and finite record-range shards still have their existing full-read or rescan costs. |
| Core, Direct, and compatibility packages | Direct package tests, including all 29 streaming cases, and strict lint pass after adding grouped fold recovery. The complete local release script passed again on 2026-10-12 after streaming compressed lazy input: workspace and feature tests, docs, and local package verification for all 29 packages. Nothing was published. |
| Root API composition | A new facade test builds two keyed sums, joins them, and collects the exact result through `ironbeam::v5::prelude` without the compatibility crate. It passes with default and no default features; strict lint passes. This exercises one public composition path, not the complete API ledger. |
| Fallible root API composition | The root `ironbeam::v5::prelude` now has a compile-and-run fixture that validates records, classifies the survivors into good and dead-letter branches, rejoins them, and collects exact results. It passes with default and no default features and does not import the compatibility crate. |
| Versioned semantic handlers | Map, filter, and flat-map handlers record an application ID and version in the graph fingerprint. Core rejects invalid identity/version and reserved builtin IDs; Direct tests collect exact values. A v4/v5 differential test matches all three published custom-op examples. The production unbounded Prism fixture uses a nontrivial map and expands each value into positive/negative rows before the versioned filter removes the negatives. Core/Direct suites, strict lint, and all 31 live Prism tests pass. General custom operation registration remains open. |
| Versioned batch handler | `apply_transform_with_semantic_named` accepts owned rows with explicit batch size and records an application ID and version on the handler node; `map_batches_with_semantic_named` is the borrowed-slice form. Direct differential tests match v4 `DynOp` examples that reorder/expand rows and filter while changing row type. The shared bounded Direct/Prism fixture runs the dynamic form before map/filter/flat-map and matches the output multiset. Beam shuffle can change encounter order within a batch, so order-sensitive work requires an explicit order key. |
| Reusable dynamic batch operator | `DynOp` accepts an owned type-erased batch, and `apply_dyn_transform_with_semantic_named` binds it to an explicit batch size and application version. V4/v5 differential tests match reversing/expanding and filtering/type-changing operators; a wrong output type returns a typed execution error. A separate test shows sizes two and three produce observable `[2, 2, 1]` and `[3, 2]` batch lengths and distinct fingerprints; a version change also changes the fingerprint. The root facade test passes with default and no default features. The bounded Direct/Prism fixture runs a type-changing operator; a five-row shared fixture produces the exact batch-length multiset `[1, 2, 2]` on both runners. A one-row operator expands unbounded input on both without global aggregation. A shared invalid-output fixture fails with the same operator and error on Direct and live Prism, and neither runner commits a sink file. Implicit v4 partitions are intentionally replaced by explicit v5 batches; planner hints do not change execution. |
| Semantic version and batch identity | On 2026-10-11, the production Prism worker rebuilt the same unbounded graph with only the semantic map's minor version changed. The live job failed with `driver/worker execution identity mismatch`; the final-result coordinator refused completion and no object result was published. Core regressions prove that explicit row, byte, and keyed batch limits change graph fingerprints. A shared byte/keyed batch job matches Direct and live Prism values; all 30 live Prism cases pass. Successful in-job worker recovery remains open. |
| Local release script | On 2026-10-12, the complete script passed again with Mr Boxington after the new Direct recovery and multi-sink tests: dependency and feature checks, workspace and feature tests, docs, and local package verification for all 29 packages including the root facade. The isolated temporary build was removed afterward. All 22 published v4 feature names are present in the v5 facade. Nothing was published. |
| Release check after typed-route change | On 2026-10-12, the full local script passed again after the Direct performance change: dependency and feature checks, workspace and feature tests, docs, and local package verification for all 29 packages. The 6.2 GiB isolated build and its log were removed. Nothing was published. |
| Release check after dual-source streaming | On 2026-10-12, the complete local script passed with Mr Boxington after dual-unbounded Direct join and co-group changes: dependency and feature checks, workspace and feature tests, docs, and local verification of all 29 packages. The added co-group test separately passed in the full 39-case Direct stream suite with strict lint. Nothing was published. |
| Release check after join event-time fix | On 2026-10-12, the complete local script passed again after deterministic matched-pair timestamps: workspace and feature tests, docs, and local verification of all 29 packages. The isolated 18 GiB build and log were removed. Nothing was published. |
| Coverage after join event-time fix | The exact core/Direct/transforms CI coverage set passed on 2026-10-12 with unchanged ratchets: core **3851/4007 source locations (96.11%)**, **590/676 branches (87.28%)**; Direct **6262/6755 (92.70%)**, **1295/1438 (90.06%)**; transforms **1163/1189 (97.81%)**, **108/116 (93.10%)**. The isolated coverage build and report were removed. |
| Cloud operation coverage | **186/261 source locations** and **34/42 branches** after bounding parallel helper concurrency; both CI ratchets pass. Uncovered utility paths remain in the quality review. |
| JSONL coverage | All-feature package: **407/435 source locations** and **38/40 branches** at the last coverage run. The latest eager reader change has passing default/all-feature tests and strict lint; coverage has not been rerun. |
| Local Prism | All 36 live tests pass, including shared two-unbounded full join, co-group, and stateless Flatten jobs with exact Direct-matching values, windows, and event times. The translator combines two same-coder test sources into one tagged Beam TestStream because Prism allows only one TestStream transform. Tagged streams reject processing-time advances. Existing cases include unbounded grouping, ordered batches, per-window side inputs, wait-on behavior, dynamic output failures, semantic-version rejection, and supervised worker loss. The production reader resumed from a residual cursor; join and co-group committed five exact rows across two windows. |
| Two-source stateless fan-in | On 2026-10-12, a tagged two-unbounded TestStream graph also ran through Flatten on both Direct and local Prism. It produced four exact rows with matching source values, assigned fixed windows, and preserved individual event times. The complete live suite passed **36/36** and strict Prism fixture lint passed. |
| Dead-letter fanout and merge | A fallible `map_catching_named` classifies four bounded records once, sends good values and typed dead letters through separate branches, and merges them into one committed output. Direct and local Prism produce the same two good and two error rows. All **37/37** live Prism tests and strict Prism proof lint pass. |
| Validation mode and worker failure | Skip-invalid produces the same two records on Direct and local Prism. Fail-fast raises a Direct execution error at the validation node, reaches Prism `FAILED`, and leaves neither sink published. The live case found that a panicking worker bundle previously left Prism waiting; the control stream now sends a failed instruction response. Validation mode is included in graph identity, so a restart cannot silently switch skip, log, or fail-fast behavior. The full harness and transforms suites, strict harness/Prism/transforms lint, and **38/38** live Prism cases pass. |
| Portable callback failure boundary | The worker control stream also catches panics in split and finalization callbacks and answers those instruction IDs with errors. Focused tests verify each response; all 91 harness unit tests, integration suites, strict harness lint, and **38/38** local Prism cases pass. Future callbacks still cancel with the control loop, so disconnect does not leave a detached bundle task running. |
| Three-input portable co-group | A shared bounded fixed-window graph co-groups three keyed collections and compares exact values, empty sides, earliest group timestamps, and window intervals on Direct and local Prism. This exercises the generic tagged-value lowering beyond the two-input join path. The complete live suite passes **39/39** and strict Prism fixture lint passes. |
| Maximum-arity portable co-group | A shared bounded graph co-groups ten keyed collections, including an empty fifth side and a key present only on the tenth side. Direct and local Prism produce the same two exact rows. The complete live suite passes **41/41** and strict Prism fixture lint passes. |
| Homogeneous many-input co-group | `co_group_homogeneous_named` accepts keyed inputs with one shared key and value type beyond the ten-input limit of the typed heterogeneous form. It tags each branch, uses native Flatten and GroupByKey, then returns one value vector per input. A twelve-source fixed-window job has an empty fifth source, a duplicate first-source value, and a key only in the twelfth source; Direct and pinned Prism agree on exact buckets and earliest group timestamps. A second twelve-branch job uses one bounded source and merges two first-branch sessions with a bridge from the last branch into `[0, 13 ms)` on both runners. Three independent unbounded sources and three branches from one unbounded source co-group in fixed windows on both runners, including late rows dropped after closure. Three independent sources also merge a session into `[0, 13 ms)` on both runners; Direct restores separate open intervals and all three source cursors before the bridge arrives. The shared common map runs once per input before branching. The full non-destructive Prism suite passes **74/74**. Broader intermediate-watermark session behavior remains outside this proof. |
| Mixed bounded/unbounded homogeneous co-group | A three-input fixed-window graph groups one unbounded stream, a bounded keyed source, and an empty bounded source. Direct and pinned Prism agree on two exact groups, including a bounded-only key and empty buckets. Direct checkpoints the bounded rows with the open group, restores the stream cursor, and does not reread the bounded source after restart. | General mixed-source session schedules and late firings remain open. |
| Mixed bounded/unbounded Flatten | A bounded first input and an unbounded TestStream produce the same three exact values, event timestamps, and fixed-window intervals on Direct and pinned Prism. Direct checkpoints bounded row offsets. A one-row batch test cancels after the first committed bounded row, reopens the source, skips that row, and commits the remaining bounded and streaming rows once. The non-destructive Prism suite passes **74/74**. | Reopening a bounded external source still depends on its configured snapshot identity and replay contract. |
| Coverage after validation identity | The exact core/Direct/transforms CI coverage set passed after the validation mode change: core **3851/4007 source locations (96.11%)**, **590/676 branches (87.28%)**; Direct **6262/6755 (92.70%)**, **1295/1438 (90.06%)**; transforms **1178/1206 (97.68%)**, **108/116 (93.10%)**. No threshold changed. |
| Release check after validation identity | On 2026-10-12, the complete local script passed after the validation identity change: workspace and feature tests, documentation, and local verification of all 29 packages, including the root facade. The isolated 19 GiB build was removed. Nothing was published. |
| Dual-source co-group regression | On 2026-10-12, all 39 Direct stream tests and strict Direct lint passed after adding a two-unbounded co-group case with duplicate and side-only keys. The shared local Prism suite passed **35/35** live jobs after adding the same tagged-source co-group graph; strict Prism fixture lint passed. |
| Join event-time agreement | Adding output event times to both two-unbounded fixtures exposed a matched-row mismatch: portable join used the right timestamp while Direct used the earlier timestamp. The v5 join contract now selects the earlier timestamp of each matched pair in every indexed execution path; unmatched rows retain their own time. Core tests, 51 bounded Direct transform tests, all 39 Direct stream tests, strict core/Direct and Prism fixture lint, and all **35/35** live Prism cases pass with exact joined, side-only, and main-only timestamps. |
| Tagged TestStream regression | On 2026-10-12, **34/34** live `prism_v5_run` tests passed against the local Beam clone after tagged lowering, including the new two-unbounded full join. Graphx package tests, strict Graphx/Prism fixture lint, and a second targeted live join run also passed. |
| Fresh Prism regression | On 2026-10-12, all **33/33** live `prism_v5_run` tests and the separate native Arrow IPC finalization proof passed against Prism started from the local Beam clone. This includes the production unbounded multi-format job, shared Direct/Prism fixtures, and the expected fail-closed worker-loss case. The local server and isolated Go/Cargo builds were stopped and removed afterward. |
| Fn Data process recovery | A fresh 2026-10-12 test runs the pending split, first accepted batch, and residual batch in three separate OS processes. It checks exact results for the original single sink and for two independent output branches from the same unbounded source: `(14, 8)` then `(16, 9)`. The complete 20-case Fn Data suite, 89 harness unit tests, other harness integration suites, and strict harness test lint pass. A pre-existing loopback fixture now keeps its bound listener until the server accepts connections, removing an intermittent startup race. |
| Final-result manifest binding | All shared final-result publishers reject an unbound store, including empty output. The live Prism corpus and Arrow proof passed after their stores were bound to the completed job journal's exact attempts. Local format tests and strict lint passed. |
| Object-backed JSONL final result | Shared I/O now has immutable object fragments with scoped checksums, conditional creation, accepted-attempt markers, exact completed-attempt matching, and idempotent final publication. The JSONL crate stages one bounded fragment per portable bundle, accepts it through `FinalizeBundle`, validates each decoded fragment, and streams the completed uncompressed `.jsonl` through a scratch file. Local object lifecycle and all-feature I/O tests pass. The production unbounded Prism job finalizes object-backed JSONL bundles, publishes only after `DONE`, and reopens `[7, 8, 9]`; the existing worker-loss case still fails closed. Hosted provider streaming and consistency proofs remain open. |
| Remote object adapter contract | The `object_store` adapter uses conditional `Create` and a byte-prefix-filtered listing. An in-memory adapter test verifies immutable create conflicts, unchanged bytes, sorted prefix listing, and bucket isolation; its package tests and strict lint pass. The S3, GCS, and Azure configurations still need provider-specific consistency proofs. |
| Final-result retry durability | The shared local publisher accepts an identical staged fragment after restart, rejects changed or corrupt retries, and syncs its directory on repeated acceptance or byte-identical publication. Child-process fault injection exits after the staged fragment is named, after the accepted hard link is created, and after the final result is named. For each boundary, a fresh process recovers both when the named entry survives and when the unsynced entry is removed to model its loss; it publishes the exact row. The separate Dataflow file journal has the same process-exit and simulated entry-loss proof after naming a record and linking its completion marker, yielding one exact completed attempt on reopen. The 21-test all-feature shared I/O suite, full harness suite, formatting, and strict lint pass. macOS file and directory sync calls use `F_FULLFSYNC` through Rust std; no literal power-cut experiment is claimed. |
| Local final-result file sink | The ordinary Direct file sink syncs the published directory entry after replacing the target. Shared I/O also syncs each newly created ancestor entry for nested staging and output directories. Reopen tests cover both nested paths; the all-feature JSONL and shared I/O suites, cloud-only feature check, formatting, and strict lint pass. |
| Portable worker restart | Fn Data test passes across three OS processes: pending split, first accepted batch, final batch. A separate final-result proof now checks that the journal refuses publication after staging and a wrong-build restart, then supplies the exact completed attempt after recovery and a duplicate callback. The full harness suite and warning-free lint pass. |
| Live worker loss | A process wrapper killed the worker as the production unbounded source reopened its residual cursor. Prism reported `FAILED` with active bundles, and the final-result coordinator refused publication. The existing production success fixture still passes. All 20 live Prism tests and strict fixture lint pass. This is the expected fail-closed proof for Prism's no-retry default variant; multi-process worker recovery is tested separately at the Fn Data boundary. |
| Side-input runner agreement | The existing `map_with_side_named` gives one global side view across fixed-window main and side branches on Direct and live Prism. The new `map_with_windowed_side_named` composes a per-window co-group; both runners match on four rows, including an empty side window, and preserve each main timestamp and window. The co-group may buffer a full window; a bounded side-view memory limit remains open. |
| Typed side views | `map_with_singleton_side_named`, `map_with_map_side_named`, and `map_with_multimap_side_named` materialize one bounded global view. A shared Direct/Prism graph emits nine matching rows, including a missing lookup and two values under one multimap key. The matching `map_with_windowed_*_side_named` methods materialize singleton, map, or multimap values at each window's closure, including when the side source is unbounded. A second shared graph emits nine matching windowed rows across three windows. A two-unbounded-source map-view graph matches Direct and Prism on exact lookup, empty lookup, windows, and main timestamps. A duplicate map key in one window fails on both runners before a sink commits. Core tests reject empty or multiple global singleton values and duplicate global map keys. Streaming updates before window closure remain open. |
| Streaming per-window side view | The same TestStream graph now runs on Direct and local Prism through three fixed windows, including an empty side window. Both produce exactly `0..2000000:1000000:11`, `2000000..4000000:3000000:22`, and `4000000..6000000:5000000:3`. Direct additionally restores an open window and source cursor from a coded checkpoint; a full join restart verifies side-only and main-only keys without replaying a closed side window. Empty main-source progress batches with the same cursor now commit two different window releases when either the watermark advances or the source completes. Direct's TestStream reader advances processing-time events as progress steps; the single-source grouped route can now use that clock for early trigger timers. |
| Two unbounded windowed side sources | `map_with_windowed_side_named` now accepts an unbounded side collection. The shared two-TestStream job waits for both watermarks and matches Direct to local Prism on three exact windows: `0..2000000:1000000:11`, `2000000..4000000:3000000:43`, and `4000000..6000000:5000000:3`. The side values are a snapshot at window closure; updates to a previously emitted window and general streaming side-input materialization remain open. |
| Wait-on runner agreement | A shared graph requires the bounded signal source to finish before fixed-window main rows leave `wait_on_named`. Direct and live Prism both observe completion and produce `0..2000000:1` and `2000000..4000000:2`. A second graph fails the signal after one value: Direct returns an execution error, Prism job messages report that execution error, and neither runner commits main output. `wait_on_all_named` composes same-type bounded waits; `wait_on_signals_named` accepts erased handles from different element types. A shared integer/string two-signal job confirms that both source completion markers exist before main output on Direct and live Prism; core validation rejects empty, foreign, and unbounded lists without partial graph nodes. `wait_on_windowed_named` accepts two watermarked sources; a shared fixed-window job matches three exact values, timestamps, and windows on both runners, including a window with no signal element. This fixture uses an identity-mapped side input for one default unbounded signal. A second pinned Prism job commits the first value while the main source and job remain open after an empty signal window closes; Direct also proves early release and checkpoint recovery. A separate Direct sliding-window test releases only the window closed by the signal watermark. Multiple independent signals still use the co-group route and await every input watermark. |
| Checkpoint policy decision | The [migration guide](V4_TO_V5_MIGRATION.md#checkpoint-policy-migration) maps v4 settings to current v5 Direct behavior. The user chose v5 batch/progress checkpoints as the 5.0.0 replacement for node-count, elapsed-time, and graph-barrier scheduling. Bounded and streaming history and fallback are implemented. |
| Direct checkpoint publication | Bounded checkpoint data and the replacement manifest each sync their directory entry before old checkpoint data can be removed. Recovery classifies a missing data file as corrupt state. Child-process fault injection exits after data, history, and latest-manifest publication boundaries; bounded and streaming recovery find a verified old or new snapshot. The test also removes the just-renamed latest manifest to model loss of an unsynced directory entry and verifies fallback. Rust 1.101 nightly on macOS routes `File::sync_all()` through `F_FULLFSYNC` for files and directories. |
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

## Contract decisions

| Topic | Decision |
| --- | --- |
| Cross-runner parity | The same advertised graph and input must give the same values, grouping, event times, windows, typed failure category, and committed output. |
| Execution differences | Bundle size, scheduling, and retry timing may differ. Unsupported operations fail validation before a source or sink opens. |
| Custom transforms | Explicit positive batch size and application semantic version replace v4's implicit physical partition contract. These values contribute to graph identity. Legacy planner hints do not alter v5 execution. |
| Ordered folds | Portable folds require an explicit order key. V4 encounter-order methods remain Direct compatibility behavior. See the [API similarity contract](V5_REPLACEMENT_ROADMAP.md#api-similarity-contract). |

## Detail and history

| Document | Use it for |
| --- | --- |
| [Evidence archive](V5_RELEASE_GATE_EVIDENCE.md) | Implementation history, fixtures, measurements, and known limits |
| [Replacement roadmap](V5_REPLACEMENT_ROADMAP.md) | API and runtime scope |
| [API inventory](V5_PHASE0_API_INVENTORY.md) | V4 surface and feature mapping still used for the parity audit |
| [Baseline](V5_PHASE0_BASELINE.md) | Repeatable behavior and performance comparisons |

Publishing the packages remains a separate decision after these gates close.
