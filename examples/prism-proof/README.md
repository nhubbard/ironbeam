# Local Prism execution proof

This unpublished package owns the format-heavy Prism fixture and its separate
Fn API worker. The `ironbeam-dataflow` runtime package does not directly depend
on the fixture's JSONL, Avro, CSV, XML, Parquet, Arrow IPC, TFRecord, or object-storage extensions.

The smaller `prism_arrow` test and `v5_arrow_worker` prove native Arrow IPC
row and record-batch input with both finalized output forms in a separate
local Prism job. Beam lowering gives sink transforms no output collection, so
Prism schedules these final-only graph branches and reports their committed
element counts.

With Prism serving its JobService endpoint locally:

```bash
IRONBEAM_PRISM_ENDPOINT=localhost:8073 cargo test -p ironbeam-prism-proof --test prism_v5_run
```

Database proofs are opt-in so the default fixture does not build SQL or
MongoDB clients. With a local MongoDB service available, run the corpus with
`--features database-proofs` and set
`IRONBEAM_PRISM_MONGODB_URI` to its connection URI. The SQLite and MongoDB
cases each read a real database source and write replay-safe results through
the separately launched worker twice, comparing the final rows with Direct.

The fixture asserts actual job completion and captured values, including
JSONL, Avro, CSV, XML, Parquet, Protobuf, and TFRecord input with terminal captures,
format-specific append output, object-backed JSONL, native Flatten, global
and fixed-window GroupByKey, keyed latest with restored event time, two-,
three- and ten-input typed CoGroupByKey, twelve-input homogeneous co-group
in fixed and merged-session windows, and two unbounded homogeneous co-groups:
one with three independent sources and one with three branches from a shared
source. Both cover window closure and late-row drop. A third unbounded job
merges one keyed session across three independent sources. A mixed-source
co-group combines one unbounded and two bounded inputs, including an empty
branch and a bounded-only key. A bounded-first Flatten also preserves three
values, timestamps, and fixed windows across a streaming input. The corpus also covers all
four joins with
duplicate and unmatched keys, a fixed-window join with preserved input
timestamps, a bounded side input fetched through Beam Fn State, native
Reshuffle, event-time assignment, and fixed-window metadata.

Two count-trigger tests exercise `early_count_trigger_named` from the public
collection API through the separately launched worker. With a repeated
count-one early trigger, pinned Prism emits one early pane for two values in
one bounded bundle. Values separated by TestStream watermark advances appear
in early panes numbered 0 and 1. Both tests passed five consecutive local
Prism runs during characterization. Direct's single-input keyed grouping path
emits the corresponding bounded and streaming panes and restores pane indices
from a checkpoint.

A separate small job runs versioned `DynOp` partition transforms, including a
row-type change, followed by map/filter/flat-map on Direct and the local Prism
worker. Both reach successful completion and emit the same values, independent
of bundle output order. The root runner-contract package also runs that
fixture against published v4 Direct. The shared batch-helper job compares
byte-limited and keyed batch outputs on Direct and Prism using repeated values,
so shuffle order cannot change the expected batch contents. The explicit
limits are included in graph identity.
An unbounded global keyed batching job checks that a full batch fires before
source completion and a smaller final batch is flushed. Direct and pinned
Prism agree on both values and pane metadata for this fixture.
A one-row `DynOp` also expands each row of an unbounded TestStream on Prism;
the Direct streaming suite exercises the same incremental contract.
The unbounded ordered-keyed job also applies a size-one keyed value batch
handler before folding. A separate Direct/Prism failure job verifies that a
handler returning the wrong number of values aborts without sink output.
Both the bounded and unbounded ordered-keyed jobs now apply larger value
batches in an explicit unique order, retaining each row's event time. A
shared Direct/Prism case rejects duplicate order keys without sink output.
Two additional shared jobs use an unbounded source with fixed windows: one
groups by key and one maps explicitly ordered keyed batches. Direct and Prism
match on values, event times, and window intervals. The same unbounded
TestStream graph with a bounded side view now runs on both runners and matches
three exact windowed rows. Direct closes these windows on source watermarks or
completion and checkpoints coded open-window state with the source cursor.
Two unbounded TestStream inputs now lower to one tagged Beam TestStream and
feed fixed-window full join and co-group fixtures plus stateless Flatten.
Live Prism jobs match Direct on values, windows, and event times in all three
operations. Prism still accepts only one TestStream transform per job; tagged
streams use one element coder and exclude processing time advances.
A fixed-window group fixture sends one key values out of timestamp order and
checks that Direct and Prism both emit the earliest input event timestamp and
the same window end. The production unbounded fixture now asserts that exact
group timestamp as well.
It also checks a postcommit `FinalizeBundle` callback: a probe sink writes a
pending fragment after a runner-managed Reshuffle and the callback marks it
accepted. Once Prism reports `DONE`, the driver compares its committed sink
count with the accepted fragments and atomically publishes the complete result.
Independent local Prism jobs now run explicit-key ordered per-key and global
folds through Beam GroupByKey. They compare values, event times, and fixed
windows with Direct, verify the empty global identity, and confirm that
duplicate global and keyed order keys fail before a sink commits.
Another local Prism job verifies explicit tie-key global and keyed latest
selection against Direct values, event times, and windows. Separate keyed and
global duplicate tie-key jobs and an empty global latest job fail before their
sinks commit.
The same job publishes and reopens CRC-validated TFRecord, native-schema
Avro, JSONL, CSV, XML, Parquet, MessagePack, CBOR, and Protobuf results from separate
finalized sinks. Repeated
publication with the same fragments yields the same files.
The fragment name is derived from the original bundle instruction ID, and a
conflicting replay of that ID fails without replacing accepted bytes.
The worker registry also retains progress per finalizing sink: if a later sink
fails, a retry resumes there without invoking earlier successful finalizers.
A two-bundle unit test covers independent callbacks and duplicate requests.
The fixture supplies a local finalization journal directory to the worker.
Before returning each finalizing bundle response, the worker durably records
its exact sink set and execution identity. The fixture checks that every
journal record has a completion marker after `DONE`. A unit test drops the
registry and recovers a pending Protobuf fragment from a new registry using
that record; unknown bundle IDs and missing fragments fail closed. The worker
also accepts a provider-neutral `FinalizationJournal` implementation through
`V5BundleExecutor::with_finalization_journal`. Another test exits the staging
process immediately after writing the record, then rebuilds the v5 executor
in a fresh process to finalize and publish its fragment through the worker
callback. An intervening process with a different build ID is rejected, and
another fresh process accepts a duplicate callback after completion.
`ObjectFinalizationJournal` uses the
provider-neutral object API with atomic create-if-absent; a local object
provider test verifies exact multi-sink recovery and conflict rejection.
The production SDF fixture resumes from a residual restriction and finalizes
at least two probe fragments in separate Prism bundles. It compares their
row total with Prism's committed sink count, publishes only after `DONE`, and
reopens all three values. The same unbounded result now also writes native
JSONL fragments. The fixture verifies at least two accepted JSONL bundles,
matches their row total to Prism's committed count, publishes the JSONL file
after `DONE`, reopens it through the v5 JSONL source, and checks all three
values. A separately coded user-defined Protobuf row now follows the same
production source across at least two finalized bundles; its native file is
published after `DONE` and reopened through the v5 Protobuf reader with the
same three values. A user-defined Avro record with an explicit native schema
also publishes from at least two finalized production-source bundles, then
reopens with the original IDs and labels. The production source also writes a
TFRecord branch containing user-defined Protobuf payloads. At least two
TFRecord fragments are accepted, the published
file passes native frame and CRC validation, and the v5 reader reopens all
three Protobuf values. MessagePack and CBOR each publish and reopen three
values from at least two accepted bundles. MessagePack has two consecutive
Reshuffles, while CBOR has three. Prism coalesces these boundaries and exposes
an earlier input collection to the worker; the worker now walks only adjacent
Reshuffle parents when validating that alias. A focused test rejects aliases
that would skip a user Map.
A live process wrapper now kills the production worker when Prism reopens
the residual source cursor. Prism reports `FAILED` while other bundles are
active and cancels the process environment. The fixture verifies the killed
worker ID and rejects final-result publication. Beam's Prism default variant
does not retry failed work, so this is its live fail-closed proof. Separate
multi-process Fn Data tests verify worker recovery across accepted batches
for both one output and a two-output fanout from the same source;
a runner that replaces workers needs its own live restart proof. Hosted object
storage integration remains a later gate.
The production source also runs a keyed lifted `CombineFn` over two fixed
windows. The v5 translator emits a fold-specific tag branch, native Beam
GroupByKey, and a typed fold ParDo; the parent also feeds an ordinary
GroupByKey branch. The worker preserves the shuffled input timestamps for the
fold. The live job checks both branches' sums, their window metadata, and
their committed sink counts. Global
mergeable combines now use a constant key and the same shuffle boundary. A
bounded global-window combine adds an Impulse seed so empty input still emits
the combiner identity. A separate live Prism job verifies `[2, 3]` yields 5
and empty input yields 0; the production unbounded fixture verifies two
fixed-window global sums. The encounter-order fold remains Direct-only;
explicit-key ordered folds have the separate Prism proofs above.
The worker's compact monitoring metadata is also exercised: Prism reports a
committed source count and a transform-scoped sink count. The latter matches
the accepted fragment row total.
Concurrent source bundles share one cached Fn Data stream per endpoint. The
reader registry and early-chunk buffer now use one lock, so a runner chunk
cannot arrive between the pending-buffer check and reader registration. A
concurrent registration regression and ten consecutive runs of the full
four-test local Prism suite passed after this fix. Before it, a ten-input
co-group occasionally omitted a value and an extra source occasionally had
no committed element-count metric despite `DONE`.
The fixture writes shared capture files under a lock so bundle commits preserve
complete records; the full live `prism_v5_run` suite passes with the
production multi-bundle proof and the large graph fixture together.
The bounded `wait_on` case uses a windowed completion signal and checks that
its sink sees every main value and window only after the signal finishes.
A smaller shared Direct/Prism case checks the same completion ordering and
compares both fixed-window output rows across runners.
A companion case makes the signal source fail after one value; Direct returns
an execution error, Prism reports that same error through its job messages,
and neither runner commits main output.
A separate shared side-input graph checks the new per-window co-group helper:
both runners use only side values in the main row's window and preserve each
main event timestamp, including a window with no side values.
An additional live Prism case uses an unbounded TestStream main with a bounded
side collection across three windows and checks the exact values and event
times. Direct streaming still rejects this stateful co-group route.
It also groups 512 distinct 4 KiB values under one key and verifies the
2 MiB iterable's count, total value length, and full-byte SHA-256 after a
real Prism shuffle. The local Prism build emits GBK iterables inline, so this
large-value proof does not exercise the worker's state-backed iterable path;
that path remains covered by a mock Fn State RPC test.
It uses a shared
local filesystem and does not prove remote artifact staging.
The full opt-in test passed against a local Prism JobService on 2026-10-11
with byte, TFRecord, native Avro, JSONL, CSV, XML, Parquet, MessagePack, CBOR, and Protobuf final-result sinks. Append output may
duplicate records on bundle replay; the finalized sinks use immutable bundle
fragments and whole-job publication after completion.
The separate Arrow proof passed after the ten-run graph corpus on the same
long-lived Prism service. An earlier service left two Arrow jobs running past
the fixture's ten-second deadline; restarting it cleared that stall before
the Fn Data registration fix. The fixture reports the last observed state on
timeout. The cause of the Arrow stall remains unproven, and the new result is
repeatability evidence rather than proof that every service race is closed.
