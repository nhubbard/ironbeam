# Apache Beam semantics in Ironbeam

Apache Beam defines a model for processing bounded data and streams. A pipeline
contains collections of timestamped elements and transforms between those
collections. A runner chooses where and when to execute the transforms. Ironbeam
5 can execute its logical graph with Direct or lower supported graphs to Beam
protobuf for a portable runner. The two paths share element and graph
descriptors, but they do not yet cover every part of the Beam model.

## Parity target and evidence

The reference is the local Apache Beam checkout at commit `7d93ef8e76aa3fd7874e760010f6479fba40721a`.
The target covers portable pipeline semantics and I/O useful to Rust
applications. It does not count Beam's Java, Python, Go, and TypeScript SDK
implementations as four copies of the same feature. Each supported semantic
must have a Direct test and a local Prism job with comparable output, time,
window, and failure behavior. A connector also needs a format or protocol
round trip and an execution test on each runner that supports its lifecycle.
This page does not assign a percentage while those tests are incomplete.

| Capability | Current evidence | Work needed for parity |
| --- | --- | --- |
| Graph identity, typed elements, and coders | Versioned graph and worker validation; custom types cross Fn Data in local Prism; pane and interval coder bytes match pinned Beam examples | Broader Beam coder compatibility corpus |
| Schema-aware rows | Avro reader schemas and typed Rust elements are available through their I/O and element coders. The separate `ironbeam-coder-row` crate supplies `BeamRow<S>`, which carries a fixed Beam schema through graph identity; Graphx lowers it to `beam:coder:row:v1` with the serialized schema payload. Bounded integer and decimal row pipelines, plus a typed writer-to-reader projection, have passed on Direct and local Prism. An unbounded TestStream of Beam rows also passes through a keyed shuffle and two fixed windows on both runners with matching grouped values and event timestamps. A second streaming fixture projects a writer row to a reordered reader schema with a nullable addition before the shuffle; Direct and Prism agree on the projected rows and timestamps. The RowCoder byte codec matches pinned Beam examples for atomic, nullable, array, map, nested, reordered, static, decimal, fixed and variable length character and byte fields, and pinned millisecond and precision-specific timestamp fields. Local wire tests also cover Beam date, time, and UUID representations. An explicit writer and reader schema can project compatible added, removed, and reordered fields, including nested rows and rows inside arrays, iterables, and maps. | Complete-row field-count evolution is tested; nested removal needs an explicit writer schema. Cross-SDK nullable reorder, additional logical types, and broader schema transforms remain open. Streaming schema evolution is proved for this explicit integer/string projection, not for every supported nested shape. |
| Bounded and unbounded sources | Vector, file, and resumable source fixtures; residuals cross processes | General source connector contracts and more split shapes |
| Elementwise transforms and branching | Map, filter, flat-map, Flatten, typed Partition, per-window Distinct, representative-based Distinct, and shared fanout jobs; bounded and unbounded Partition fixtures match Direct and local Prism values, timestamps, and fixed windows; stateless Flatten matches across two and three independent unbounded sources and a mixed bounded/unbounded pair; per-window Distinct matches values and earliest event times across two windows and has matching repeated early and late panes in streaming fixtures. Representative-based Distinct chooses the smallest encoded value in each derived-key/window group and matches across Direct and Prism. | Broader multi-output and fusion cases |
| Keyed shuffle and aggregation | GroupByKey, co-group, joins, lifted folds, and ordered folds in shared cases; homogeneous co-group is proved with twelve bounded inputs, three independent unbounded inputs, three branches from one unbounded source, and mixed bounded/unbounded inputs in fixed windows; three independent unbounded inputs also merge a keyed session after the bridge arrives | General stateful DAG fanout, broader multi-source session schedules, and hot-key behavior |
| Event time and watermarks | Fixed, offset, sliding, and keyed session cases; bounded and two- or three-source unbounded co-groups merge sessions across inputs when source watermarks advance after the connecting records; fixed and sliding windows have tested positive allowed lateness and zero-lateness drops | Independent bounded-source and intermediate-watermark agreement for cross-input sessions, session lateness, and broader merging cases |
| Triggers and panes | Default, repeated early count, processing-time, late count, and combined early/late count policies have Direct and Prism fixtures. Count-or-time (`AfterAny`), count-and-time (`AfterAll`), and count-then-time (`AfterEach`) policies lower to Beam and have shared tested schedules; Direct restores their armed timers and phases from checkpoints. Discarding late-count co-groups and joins match across the two runners. `reify_panes_named` and sink metadata expose pane timing and indices. | General nested trigger expressions, synchronized processing time, merged-session pane schedules, and runner agreement for all accumulating and multi-source schedules remain open. Pinned Prism emits extra early panes in a tested two-source count-or-time schedule. |
| User state and timers | Internal grouped checkpoints and Fn State reads serve existing operations. Versioned state and timer declarations lower to Beam ParDo fields. The harness routes timer-family streams and can encode or decode timer records. A scoped Fn State accessor checks declared state kinds before bag, multimap, or ordered-list RPCs. It replaces a value or one map entry by clearing the scoped cell before appending an encoded value; worker planning still rejects user state and timers without executable callbacks. | Public key/window state and event or processing-time timer callbacks, state replay, and runner agreement. Pinned Python and Java SDKs use different protocols for Set state, so cross-SDK Set interoperability is unproved. |
| Side inputs | Bounded iterable, singleton, map, and multimap global views; per-window iterable, singleton, map, and multimap views; two unbounded sources match Direct and Prism after both watermarks close each window, including a typed map lookup | Streaming side-input updates before closure |
| Completion signals | Bounded `wait_on_named`, `wait_on_all_named`, and heterogeneous `wait_on_signals_named`, plus windowed `wait_on_windowed_named`, `wait_on_all_windowed_named`, and heterogeneous `wait_on_windowed_signals_named`; fixed-window barriers match Direct and Prism on values, including empty signal windows. A single fixed-window signal releases a main value while both production sources and the Prism job remain open. A completed sliding-window signal releases both overlapping main windows while the main source remains open. Direct also releases one of two sliding windows when only the earlier signal window has closed. | Pinned Prism has not released the earlier sliding window while its signal source stays open at the tested partial watermarks. Multiple-signal early release and broader window schedules remain open. |
| Bundle retries and output commits | Final-result journals and supervised execution-child replay in one Prism job | Control-owner replacement, hosted runner proof, and indefinite metric retention |
| I/O formats | Separate CSV, JSONL, Avro, Parquet, Arrow IPC, XML, MessagePack, CBOR, Protobuf, and TFRecord crates. Native Avro reader schemas resolve defaults, projections, numeric promotion, and field aliases through nested and named schemas across local files, globs, shards, and provider-neutral object URIs. Avro and Parquet accept object URIs and prefixes through a registered provider; their streaming variants spool and verify each object before decoding records incrementally. A local Prism worker rebuilds `file://` providers for both formats and reads their rows. Both the eager and incremental TFRecord sources have separate-worker Prism jobs; the incremental job matches Direct on decoded local frames. | Expand schema evolution beyond the tested Avro cases and prove remote object providers in portable workers. |
| External services | SQL, MongoDB, object storage, and opt-in GCP clients | Provider emulator and hosted service behavior, authentication, and streaming commit guarantees |

Bounded keyed batching can also use encoded-value byte and count limits. The
byte-limited method rejects unbounded input until a stateful streaming
implementation exists.

A bounded RowCoder fixture carries Beam date, time, and UUID logical fields
through a reshuffle. Direct and the pinned local Prism job produce the same
decoded values after that wire boundary. The date uses epoch days, the time
uses nanoseconds since midnight, and the UUID uses a nested row containing
its most and least significant 64-bit halves.

For an unbounded fixed or sliding window with allowed lateness,
`group_into_batches_with_max_buffering_duration_named` uses a repeated
count-or-processing-time trigger before the watermark and a repeated count
trigger afterward. A late partial batch flushes when allowed lateness expires.
The same count, timer, and late-count fixture produces matching panes on the
Direct runner and local Prism. The portable graph carries both Beam trigger
branches. A Direct checkpoint test resumes after the count pane, fires the
pending timer, and then emits the late batch once from the restored cursor.
The accumulating variant retains the window's earlier values in its timed,
on-time, and late panes in a separate Direct fixture.

The [release gate report](https://github.com/nhubbard/ironbeam/blob/main/V5_RELEASE_GATE_STATUS.md)
records exact passing suites and remaining release constraints. The table is
an inventory of work, not a weighted feature score.

## Portable graph

The Ironbeam graph records each collection's element coder, boundedness,
windowing strategy, and producing transform. Its identity also includes the
semantic version of registered operations. Portable lowering turns supported
nodes into Beam pipeline protobuf: `WindowInto` assigns windows, native
`GroupByKey` supplies the shuffle, and executable Rust transforms run through
the Beam Fn API. A worker rebuilds the application graph and checks its
identity before accepting bundles. This prevents a runner from executing a
pipeline with a different callback or coder than the driver submitted.

A coder describes the bytes exchanged at a runner boundary. A type that works
in Direct memory may still be unusable in a portable job if it has no stable
coder. Portable preflight reports that mismatch before the worker reads input.

Beam also separates a logical transform from an execution stage. A runner may
fuse adjacent transforms into one stage, divide a stage into bundles, or execute
bundles in a different order from the source records. `reshuffle_named` adds a
shuffle boundary where downstream work must be independently schedulable. It
does not sort elements. Application callbacks should use the element value and
metadata, rather than a bundle number or the order in which workers happen to
receive bundles.

The interval-window byte coder accepts the zero-duration example in Beam's
standard coder corpus. The graph still requires a valid windowing strategy for
execution; accepting those bytes does not make an empty interval a useful
application window.

## Elements and time

An Ironbeam element has a value, an event timestamp in nanoseconds, and window
metadata. Event time describes when the represented event occurred. Processing
time describes when a runner handles it. A source can report event-time
progress separately from its elements. For an unbounded source, that progress
helps the runner decide when a window is ready to close.

A watermark is a runner's estimate of how far event time has advanced. It is
not the timestamp of the most recently delivered element. A source can emit
an element with a smaller timestamp after reporting progress; with the
current zero-lateness strategy, a grouped window already closed by the
watermark will not reopen. In the shared fixed-window fixture, Direct and
Prism both discard a row that arrives after the window closes. A watermark
does not impose an order on rows that are still in the same open window.

A source may advance its event-time watermark in a batch with no elements.
For example, a source can emit a value at one millisecond, advance its
watermark past a ten-millisecond fixed window, and then remain idle. Direct and
the pinned local Prism runner both close the grouped window and deliver its
result while the source and job remain open. The source cursor advances with
the empty progress batch, allowing a resumed worker to distinguish it from
the earlier data batch.

Ironbeam's fixed-window operation assigns each element to one interval. The
portable graph expresses that operation as Beam `WindowInto` and gives the
resulting collection an interval-window coder. Its Beam windowing strategy uses
the default trigger, discards accumulated values after a firing, emits at the
earliest timestamp in the pane, and allows no lateness. A late element for a
closed window is therefore absent from the tested grouped output. The
[windowing guide](../guides/windowing.md) shows timestamp assignment and a
fixed-window group.

Sliding windows can assign an element to several intervals. Ironbeam supports
them on bounded and unbounded sources on Direct and local Prism. The unbounded
proof checks watermark closure and a late row; Direct also restores its
open-window checkpoint.

Session windows start at each event's timestamp and extend by a configured
gap. Keyed sessions merge when their intervals overlap or touch. The arrival
order does not determine the final interval: a later bridge event can join two
existing groups. `session_windows_named` assigns session intervals, while
`group_by_session_named` also groups the keyed values. Bounded and
single-source unbounded grouping runs on Direct and local Prism. They agree on a
bridge, independent keys, the earliest group event time, and merged interval
bounds. An unbounded test advances the watermark, merges two still-open
sessions, closes them, and drops a row for a closed window. It also closes one
negative-time session while the others remain open. Direct restores
open sessions and the source cursor from one coded checkpoint before the
bridge arrives. Bounded joins and co-groups merge session intervals across
their keyed inputs before matching or grouping rows. Shared Direct and Prism
fixtures check bridges formed by other inputs, isolated sessions, empty sides,
values, and merged bounds. Two-source unbounded session joins and co-groups
also merge intervals across inputs. Direct waits for both watermarks before
closing a keyed session and restores the open rows with both source cursors
from a checkpoint. Shared Direct and local Prism jobs check the bridge,
merged bounds, and a row dropped after closure for a join and co-group. More
than two unbounded inputs and
nonzero allowed lateness remain outside this tested contract. The [windowing guide](../guides/windowing.md#keyed-sessions)
contains a working example.

An intermediate-watermark case remains unresolved. With an `a` event at zero
on the main input, an `a` event at four milliseconds on the side input, and a
second main `a` event at eight milliseconds, Direct forms `[0, 13)`
milliseconds when both source watermarks first reach six milliseconds and
later reach twenty. In the same local Prism job, the `a` event at zero is
emitted in `[0, 5)` before the other two are grouped in `[4, 13)`. The
tagged TestStream protobuf places the side event before either six-millisecond
watermark. This observation is limited to the pinned local Prism runner and
the tested portable graph; the cause has not been isolated. Applications
that need identical cross-runner session results should keep source
watermarks behind all records that may connect an open session until this
boundary is resolved.

The bounded three-input co-group fixture uses three branches from one source.
With three independent bounded sources, local Prism sometimes closes
`[0, 9)` milliseconds before the third branch's eight-millisecond record
joins it; Direct merges the complete connected interval. The shared-source
fixture proves the three-input merge without depending on that scheduling
choice. An independent-source Direct test continues to assert the complete
merge. The independent-source Prism result remains an open agreement boundary.

## Keys, grouping, and order

A grouping transform collects elements with the same key and window. A portable
runner performs the shuffle between the producing and consuming transforms.
Ironbeam lowers supported grouping, co-grouping, and joins through Beam's native
shuffle boundary, then runs typed Rust handlers on the grouped values.

The shuffle does not promise an encounter order. An ordered fold therefore
needs an explicit order key; equal keys are rejected where the operation
requires a total order. Direct can preserve encounter order for its local
variant, but that local behavior is not a portable ordering guarantee.

`distinct_by_per_window_named` groups values by a derived key and selects one
representative in each window. It chooses the value with the smallest encoded
bytes, so its result does not depend on bundle arrival order. The value and
derived-key types need registered coders, and the value coder must encode the
same value consistently. Shared bounded and watermarked Direct/Prism jobs
check the chosen values and group timestamps in two fixed windows. The older
`distinct_by_named` retains its encounter-order behavior for local use.

Grouping may retain all values for a key and window until the window closes.
For a large key, this can use substantially more memory than an incremental
combiner. The supported keyed fold and combine operations describe a reducer
that can process inputs incrementally; users should choose one when they do
not need the entire grouped value list. An explicit order key is still
required when the reducer's result depends on sequence.

`count_per_element_named` uses a lifted combiner on bounded collections. On
unbounded collections, it groups each value in its window so Direct can
checkpoint the open group and release it when the watermark closes the window.
A two-window TestStream job has matching counts and output timestamps on
Direct and local Prism. This streaming path retains the values for each open
key and window; a count with a very large key may reach the configured state
budget.

## Side inputs and completion signals

A side input supplies a view of another collection to a transform. Its values
must be available for the main element's window before that element can run.
Ironbeam supports global iterable, singleton, map, and multimap views. A
singleton requires exactly one value; a map rejects duplicate keys. A shared
Direct/Prism job checks all three typed views and an empty lookup. The same
four view shapes are available per window. A shared job checks the typed
windowed shapes on both runners and verifies that duplicate map keys in a
window fail before an output is committed. The per-window views
use a co-group, so an empty side window still has a defined result when the
main side has values. The live shared fixture checks both runners with an
unbounded main collection, including an empty side window. The windowed API
also accepts an unbounded side collection. A second shared job supplies both
inputs from watermarked streams and checks three exact windows on Direct and
local Prism. Another uses the typed map view with two independent watermarked
sources and checks a matching key and an empty lookup. Each job waits for its
window to close on both inputs, then applies the side values to every main
value in that window. This is a view at window
closure; it does not revise a result that was already emitted when the side
stream later changes.

`wait_on_named` delays main output until a bounded signal collection
completes, including when the main collection is unbounded. Direct drains the
bounded signal before opening the main reader. On checkpoint recovery it
repeats the signal and reopens the main reader at its saved cursor; the
checkpoint rejects a changed signal source. A shared unbounded-main fixture
produces the same values and event times on Direct and pinned Prism.
Another shared fixture joins two bounded signal branches with `Flatten`, groups
their values by key, and waits for that grouping before emitting main values.
`wait_on_all_named` composes several bounded signal barriers of one
element type. `wait_on_signals_named` accepts different signal element types
through `wait_signal` handles. `wait_on_windowed_named` accepts a watermarked
signal stream. For two unbounded inputs with default firing, zero allowed
lateness, and fixed or sliding windows, it represents the signal as a
matching-window Beam side input. Direct releases buffered main values when
the signal watermark closes their window, even if the main watermark remains
behind it. The portable lowering uses an identity window mapping, and a local
Prism job matches Direct's output values and timestamps. A second local Prism
job emits a main value, keeps that source open, and closes an empty signal
window. Its sink receives the value while the job is still running. The worker
accepts this barrier in an SDF source bundle when Prism fuses the two stages.
With sliding windows, a completed signal releases both overlapping windows
while the main source remains open. Direct can release the first sliding window
when the signal watermark passes its end and leave the second buffered. In the
pinned Prism version, a signal source that remains open did not release that
first window in the tested production-source schedule.
Other configurations use the co-group path and wait for both input watermarks.

`wait_on_all_windowed_named` accepts several signals of one element type. It
groups them with the main input in one shuffle and waits for every input
watermark. A shared three-source job checks the same windows and timestamps on
Direct and local Prism.

`wait_on_windowed_signals_named` accepts handles converted from signals of
different element types with `windowed_wait_signal_named`. Its shared job
matches Direct and Prism with three independently watermarked sources whose
signal branches carry text and Boolean values, including an empty signal
window. Each handle discards the
signal values but retains its windows and source progress. A window without
signal elements also closes when its watermark advances. This windowed barrier
uses a co-group, so it can
buffer a full window and does not release main values early when only the
signal watermark has advanced. These methods express a dependency between
collections, not an order among main elements. A failing signal prevents
dependent output from being committed. None provides mutable user
state.

## Triggers, panes, and late data

A trigger determines when a window produces a pane. A pane is one firing's
output for a window. Repeated firings can discard earlier contents or retain
them, depending on the accumulation mode. Allowed lateness controls how long
the runner keeps a window available after its watermark passes the window end.

Ironbeam emits the Beam default trigger with discarding accumulation and zero
allowed lateness unless a fixed- or sliding-window collection calls
`early_count_trigger_named` or `accumulating_count_trigger_named`. `reify_panes_named` exposes each element as
`(PaneInfo, value)`, and a typed sink receives the same pane through
`ElementMetadata`. A source element has `Unknown` timing before a firing;
the tested grouped output is a first-and-last `OnTime` pane with index zero.
Direct and local Prism agree on those values after native GroupByKey for fixed
and overlapping sliding windows. In a merged-session fixture, both runners
emit one group with the same values, but Direct marks it first-and-last
`OnTime` while the pinned local Prism marks it nonfinal `Early` with
non-speculative index −1. The fixture checks that Prism passes this pane to
both the reified value and the typed sink. Beam's default trigger fires when
the watermark reaches the window end, so this difference requires further
runner-level investigation before session pane parity can be claimed.
The repeated early count trigger is part of the graph descriptor and lowers
to Beam's `AfterEndOfWindow` trigger with repeated `ElementCount` early
firings. A bounded two-row bundle produced one early pane containing both
rows on pinned Prism; two TestStream advances produced panes with indices 0
and 1. Direct emits the same panes in the corresponding bounded and streaming
cases. Its keyed fold stores the next pane index with the source checkpoint,
so a resumed stream does not repeat an earlier firing. For sliding windows,
an identity transform carries the new trigger policy after window assignment;
another `WindowInto` would assign overlapping windows twice. A bounded
overlapping-window proof matches Direct and pinned Prism in both accumulation modes. The accumulating count
policy retains earlier values in later panes. In the tested two-advance stream,
Direct and pinned Prism both emit `[2]` in the first early pane, `[2, 3]` in
the second, and `[2, 3]` again in the on-time pane. Direct checkpoints the
retained group and count since the previous firing. Its accumulating fold
requires a value coder. Later sections describe the tested late-count and
composite trigger schedules; other combinations remain open.

`count_then_processing_time_trigger_named` repeats a two-step early trigger:
a pane fires after the specified number of new elements, then another fires
after a later element has waited for the processing-time delay. The delay is
given in nanoseconds and must be a positive whole number of milliseconds.
`accumulating_count_then_processing_time_trigger_named` retains earlier
values in subsequent panes. For a keyed stream with 10 ms fixed windows:

```rust
let groups = source
    .fixed_windows("windows", 10_000_000)?
    .count_then_processing_time_trigger_named("count-then-time", 2, 5_000_000)?
    .group_by_key_named("groups")?;
```

Direct emits a count pane, a later timer pane, and another count pane in the
tested schedule. The first count and timer cycle also agrees with the pinned
Prism runner in both accumulation modes. Direct restores the trigger phase
and armed timer after a checkpoint.

The default strategy fires once when a nonempty window is ready, uses the
earliest event timestamp in that firing for grouped output, and discards
accumulated values afterward. `end_of_window_output_time_named` changes a
fixed or sliding window's GroupByKey output timestamp to the final whole
millisecond in that window. A window ending at 10 ms therefore emits its
group at 9 ms. Direct and the pinned Prism runner agree on the bounded
fixed-window fixture; Direct also computes separate ends for overlapping
sliding windows. This policy requires millisecond-aligned window dimensions.
`latest_in_pane_output_time_named` instead stamps each group at the maximum
event timestamp in its pane. A bounded fixed-window fixture emits at 7 ms for
input at 5, 2, and 7 ms on both Direct and pinned Prism. Direct preserves the
maximum through coded checkpoint recovery and accumulating early and on-time
panes. A two-advance TestStream also exercises latest-in-pane output time on
pinned Prism; Direct restores the corresponding discarding and accumulating
unbounded folds after a checkpoint. Both policies currently apply to
single-input GroupByKey.

The portable graph can set nonzero allowed lateness for fixed and sliding
windows with `allowed_lateness_named`. Beam records the duration in whole
milliseconds. On pinned Prism, a TestStream job emitted an on-time pane at
2 ms, a late pane at 3 ms, and no output from an element sent after window
expiry. Direct emits the same values, timestamps, timing, and pane indices for
single-input GroupByKey. Its coded fold checkpoint preserves the on-time pane
index across restart, permits two further late firings, and drops an element
after expiry. Other stateful fan-in and explicit late trigger policies remain
unavailable on Direct.

The overlapping sliding-window fixture agrees on values, event times, window
intervals, pane timing, and indices. Its first late pane has different
`is_last` metadata: pinned Prism fires it after expiry and marks it final,
while Direct emits it before expiry and leaves it nonfinal. This difference
depends on when each runner schedules the firing.

Ironbeam's Fn Data coder decodes Beam pane
timing, first and last flags, and indices. When Beam sets the element metadata
flag, a length-prefixed protobuf payload follows the pane header; the worker
keeps that payload separate from the user element and carries it across its
supported transform boundaries. It writes both to outgoing Fn Data. The
public `PaneInfo` exposes the metadata-present flag but not the opaque
protobuf payload.

## State and timers

Beam's stateful processing stores data by key and window. Timers request a
callback when event time or processing time reaches a specified point. These
facilities can implement custom aggregation, buffering, and trigger behavior,
but their storage and callback lifecycle belong to the runner contract.

Ironbeam's Direct runner checkpoints coded open-window state for its supported
grouping and join operations. The portable worker uses the Beam Fn State API
for supported side inputs and state-backed grouped iterables. Ironbeam does not
yet offer a general user-facing state cell or timer callback API. Source
checkpoints and sink finalization journals solve different problems: they
record progress and publication decisions, not arbitrary per-key application
state.

The distinction matters for retries. A source cursor says which input can be
replayed. A grouped checkpoint contains values that have not yet formed a
completed result. A sink finalization journal records which bundle attempts
were accepted. None of those structures is a general key-and-window state
cell with a timer callback. The Beam Fn State API is used internally for
side inputs and state-backed grouped iterables. The worker also has a raw,
scoped user-state accessor for a validated declaration, but application
transforms cannot register or invoke a stateful function through the current
v5 builder.
Graph validation and worker planning reject a `ParDoPayload` that declares
user state or timers. This prevents a stateful transform from being executed
as an ordinary elementwise transform.

## Bundles and committed output

A Beam bundle is a runner-selected unit of work. A successful callback alone
does not make its output a completed job result. The worker sends element bytes
on Fn Data and reports bundle completion on Fn Control. Prism waits for each
declared Fn Data output to close before committing that bundle. A sink that
requires finalization stages fragments under the bundle attempt identity;
the runner later accepts an attempt through `FinalizeBundle`. Ironbeam's local
publication plan requires a `DONE` job, a complete finalization journal, and
committed sink counts before it exposes the final file or object.

Retries can execute a transform more than once. An append to an external
system can therefore duplicate an effect even if the final PCollection has
the expected values. Supervised execution accepts idempotent sinks or sinks
whose fragments are bound to a durable finalization journal. It rejects a
graph that cannot meet that replay contract before opening its sources or
sinks. A source checkpoint records where reading can resume; it does not
retroactively make an external append idempotent.

## Splitting and recovery

The portable unbounded-source path uses a splittable Beam transform. A source
split has a stable identity, a coded position, and event-time progress.
Residual work can resume in a new process after an accepted split. Final-result
sinks keep a durable record of accepted bundle attempts so publication can be
checked after worker restart.

The source adapter accepts a Fn API checkpoint request when it names the
active gRPC read transform, requests fraction zero, and supplies no allowed
split points. It returns the residual when the bundle finishes. A request for
another transform, an arbitrary fraction, or a constrained split point does
not checkpoint this source. Dynamic splitting of user transforms remains
unavailable.

Prism treats a disconnected Fn Control stream with active instructions as a
bundle failure. Starting another process under the same worker ID does not
resend the failed instruction. Ironbeam's optional supervised worker keeps the
Fn Control stream in a stable parent process. It replays buffered Fn Data input
to an isolated child and publishes that child's output only after the child
succeeds. The local Prism fixture kills a child during an active residual
source poll and completes the same job after replacement. Another fixture
kills a child after it has written local output and checks that the failed
attempt's output does not reach Prism. Graphs using this mode need replayable
sources and idempotent or durably finalized sinks. The Control-owning process
remains a failure boundary.

The supervisor buffers one bundle's input and output up to 64 MiB each and runs at most two
child attempts. It checks that every expected output stream closes before
forwarding the accepted attempt's chunks. The replay contract requires a
source whose input can be replayed and a sink that is idempotent or finalized
through a durable journal. This protects the observed local Prism jobs from
duplicate publication when an execution child dies. If the Control-owning
process or its host disappears, Prism fails the active instructions; the
current proof does not cover replacing that owner inside the same job.

For ordinary Fn Data output, Ironbeam waits until the outbound transport has
consumed a bundle's queued chunks and end markers before it acknowledges the
bundle on the separate Fn Control stream. The check closes an ordering gap
between data delivery and bundle completion. It does not replace the runner's
commit decision or the sink's durable finalization record.

## Current scope

| Beam facility | Ironbeam 5 status |
| --- | --- |
| Bounded and unbounded collections | Supported in the documented source contracts; the unbounded path requires stable split and progress behavior. |
| Event timestamps and watermarks | Timestamp assignment and source progress are available; Direct and Prism compare tested fixed-window results. |
| Global and fixed windows | Supported on the established Direct and local Prism corpus. |
| Sliding windows | Supported on bounded and unbounded collections on Direct and local Prism, including overlap, negative event time, watermark closure, and late-row discard. |
| Session windows | Bounded and single-source unbounded keyed grouping merge overlapping or touching intervals on Direct and local Prism. Bounded joins and co-groups, including a three-input shared-source co-group, merge keyed intervals. Two-source unbounded joins and co-groups also run on both. Direct checkpoints open unbounded sessions and source cursors. Independent-source and intermediate-watermark session cases can differ on local Prism; more than two unbounded inputs and nonzero allowed lateness remain unproved. |
| Default trigger | Used by portable lowering, with discarding accumulation. Direct and Prism agree on fixed-window on-time and late panes with nonzero allowed lateness for single-input GroupByKey. |
| Group output timestamp | Earliest-in-pane is the default. End-of-window and latest-in-pane are available for fixed and sliding windows on GroupByKey; bounded fixed-window jobs agree on Direct and pinned Prism. |
| Pane metadata | `reify_panes_named` makes `PaneInfo` available to elementwise callbacks, and typed sinks receive it in `ElementMetadata`. Direct and Prism agree for fixed and sliding grouped windows. The pinned Prism runner marks the tested merged-session group as `Early` while Direct marks it `OnTime`. |
| Early count trigger | Public fixed- and sliding-window repeated count policy lowers to Beam. Bounded overlapping sliding windows and fixed-window TestStream panes pass on local Prism. Direct supports single-input keyed grouping and fixed-window homogeneous co-groups with independent unbounded inputs. An unbounded homogeneous co-group may use a global window when it declares a positive early count. Discarding and accumulating co-group panes retain their indices and values across checkpoint recovery, including a global accumulating group. A bounded input can fire an early pane while a second unbounded input is idle; Direct commits the pane before checkpointing it. A shared two-branch source produces the same fixed- and global-window accumulating pane values, timing, and indices on Direct and Prism; sink delivery order can differ. Independent-source early scheduling has a Direct proof only. |
| Processing-time early trigger | A positive millisecond delay can be used for repeated early panes in discarding or accumulating mode. Fixed-window single-source keyed grouping has matching Direct and local Prism results after two TestStream clock advances. Direct uses wall time when a source does not supply a scripted clock and can fire while that source remains pending. A checkpoint retains the deadline; a failed timer-pane commit retries with the same sink bundle ID. Empty progress batches at one source cursor have separate sink IDs when their folded state changes, so an early pane does not suppress a later on-time pane. Two independent TestStream sources also produce matching fixed-window co-group panes on Direct and local Prism. Direct checkpoints their open timer and resumes an unfinished source without reopening a completed one. The portable TestStream uses the greatest source clock as its shared processing time. The pinned Prism runner requires each tag to report an initial watermark before the timer advances; broader schedules and session merging remain unproved. |
| Other triggers | Repeated count-or-processing-time (`AfterAny`) and count-and-processing-time (`AfterAll`) early triggers are available in discarding and accumulating modes; keyed batches use the discarding `AfterAny` form. Direct handles count-first and time-first `AfterAny` schedules, and its accumulating form retains values across both firings in one-source groups and a two-source co-group. Local Prism completes the tested single-source time-first count schedule when its clock advances before window closure. In a two-source accumulating co-group, it emits an extra early pane before the second value, shifting later indices. It panics if that window closes while the old timer remains queued. For `AfterAll`, Direct and pinned Prism match a single-source timer-first fixed-window schedule in both accumulation modes. They also match a two-source homogeneous co-group: an Early pane contains both inputs, followed by one retained OnTime pane in accumulating mode. Direct restores a checkpoint taken after the timer elapsed but before the count was reached. The repeated `AfterEach` count-then-processing-time trigger also runs in both accumulation modes: a short count and timer cycle matches Direct and pinned Prism, and Direct restores its armed timer and phase after a checkpoint. A longer count, timer, count TestStream schedule panics in the pinned Prism aggregation timer path, so that schedule has only a Direct proof. Synchronized processing-time and other general composite triggers remain pending. Default and repeated-count late panes with nonzero allowed lateness run on Direct and Prism for fixed and sliding windows. Accumulating repeated count firings run for fixed-window single-input grouping and tested multi-input fan-in. Accumulating combined-count and intermediate typed-join late-pane schedules differ across the tested runners. |
| General user state and timers | Pending; internal checkpoint and Fn State paths serve specific operations. |
| Splittable source residuals | Supported by local process-restart fixtures. |
| General splittable functions | `OffsetRangeTracker` can claim positions, divide an offset restriction, report progress, and check completion. The tracker is thread-safe. User splittable functions are not yet executable on Direct or Prism. |
| Active execution-child replacement in one Prism job | Supported for replay-safe graphs with supervised execution; a killed child and a failed partial-output attempt have live Prism proofs. |
| Control-owner process replacement | Pending; Prism fails active instructions when that connection disconnects. |

The [portable execution guide](../guides/portable.md) describes how to run the
local proof. Bounded keyed batching can also use encoded-value byte and count limits. The
byte-limited method rejects unbounded input until a stateful streaming
implementation exists.

The [release gate report](https://github.com/nhubbard/ironbeam/blob/main/V5_RELEASE_GATE_STATUS.md)
records the exact test corpus and unresolved gates.
