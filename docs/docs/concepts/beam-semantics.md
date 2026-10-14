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
| Graph identity, typed elements, and coders | Versioned graph and worker validation; custom types cross Fn Data in local Prism | Broader Beam coder compatibility corpus |
| Bounded and unbounded sources | Vector, file, and resumable source fixtures; residuals cross processes | General source connector contracts and more split shapes |
| Elementwise transforms and branching | Map, filter, flat-map, Flatten, and shared fanout jobs | Broader multi-output and fusion cases |
| Keyed shuffle and aggregation | GroupByKey, co-group, joins, lifted folds, and ordered folds in shared cases | General stateful fanout and hot-key behavior |
| Event time and watermarks | Fixed, offset, sliding, and keyed session cases; tested late rows drop at zero lateness | Session joins, nonzero allowed lateness, and broader merging cases |
| Triggers and panes | Default trigger only. The Fn API boundary now decodes and preserves pane timing and indices in the worker | Public pane metadata, early and late triggers, accumulation, and Direct/Prism agreement |
| User state and timers | Internal grouped checkpoints and Fn State reads | Public key/window state and event or processing-time timer callbacks |
| Side inputs | Bounded global and per-window side-view cases | Streaming side-input updates and broader view types |
| Bundle retries and output commits | Final-result journals and supervised execution-child replay in one Prism job | Control-owner replacement, hosted runner proof, and indefinite metric retention |
| I/O formats | Separate CSV, JSONL, Avro, Parquet, Arrow IPC, XML, MessagePack, CBOR, Protobuf, and TFRecord crates | Expand protocol fixtures, schema evolution, and portable production cases |
| External services | SQL, MongoDB, object storage, and opt-in GCP clients | Provider emulator and hosted service behavior, authentication, and streaming commit guarantees |

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
existing groups. `group_by_session_named` implements this behavior for bounded
and single-source unbounded keyed input. Direct and local Prism agree on a
bridge, independent keys, the earliest group event time, and merged interval
bounds. An unbounded test advances the watermark, merges two still-open
sessions, closes them, and drops a row for a closed window. It also closes one
negative-time session while the others remain open. Direct restores
open sessions and the source cursor from one coded checkpoint before the
bridge arrives. Session joins and co-groups need cross-input merge rules and
remain unsupported. The [windowing guide](../guides/windowing.md#keyed-sessions)
contains a working example.

## Keys, grouping, and order

A grouping transform collects elements with the same key and window. A portable
runner performs the shuffle between the producing and consuming transforms.
Ironbeam lowers supported grouping, co-grouping, and joins through Beam's native
shuffle boundary, then runs typed Rust handlers on the grouped values.

The shuffle does not promise an encounter order. An ordered fold therefore
needs an explicit order key; equal keys are rejected where the operation
requires a total order. Direct can preserve encounter order for its local
variant, but that local behavior is not a portable ordering guarantee.

Grouping may retain all values for a key and window until the window closes.
For a large key, this can use substantially more memory than an incremental
combiner. The supported keyed fold and combine operations describe a reducer
that can process inputs incrementally; users should choose one when they do
not need the entire grouped value list. An explicit order key is still
required when the reducer's result depends on sequence.

## Side inputs and completion signals

A side input supplies a view of another collection to a transform. Its values
must be available for the main element's window before that element can run.
Ironbeam supports a global side view and a per-window side view. The latter
uses a co-group, so an empty side window still has a defined result when the
main side has values. The live shared fixture checks both runners with an
unbounded main collection, including an empty side window.

`wait_on` delays main output until its required signal is available. It is a
dependency between collections, not a source of ordering among the main
elements. If the signal fails, the dependent output fails and is not
committed. Neither side inputs nor `wait_on` provide mutable user state.

## Triggers, panes, and late data

A trigger determines when a window produces a pane. A pane is one firing's
output for a window. Repeated firings can discard earlier contents or retain
them, depending on the accumulation mode. Allowed lateness controls how long
the runner keeps a window available after its watermark passes the window end.

Ironbeam currently emits the Beam default trigger with discarding accumulation
and zero allowed lateness. It has no public trigger builder, pane index or
timing metadata, accumulating mode, or configurable allowed lateness. A job
that needs early results, late corrections, or multiple panes must wait for
those contracts to be implemented and tested on both Direct and Prism. The
absence of an API is deliberate: treating repeated panes as ordinary duplicate
elements would make sinks and folds ambiguous.

The default strategy fires once when a nonempty window is ready, uses the
earliest event timestamp in that firing for grouped output, and discards
accumulated values afterward. Ironbeam's Fn Data coder decodes Beam pane
timing, first and last flags, and indices. The worker retains those fields
across its supported transform boundaries and writes them to outgoing Fn Data.
The public element metadata still does not expose them to application
callbacks or sinks. A trigger configured externally in a modified Beam graph
would therefore not give the application a complete pane contract. Such a
graph remains outside the supported portable API.

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
side inputs and state-backed grouped iterables; application transforms cannot
register a state specification or event-time timer through the current v5
builder.

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
| Session windows | Bounded and single-source unbounded keyed grouping merge overlapping or touching intervals on Direct and local Prism. Direct checkpoints open unbounded sessions. Session joins and co-groups remain unsupported. |
| Default trigger | Used by portable lowering, with discarding accumulation and zero allowed lateness. |
| Custom triggers and pane metadata | The worker preserves pane headers. A public pane API and custom trigger contract remain pending. |
| General user state and timers | Pending; internal checkpoint and Fn State paths serve specific operations. |
| Splittable source residuals | Supported by local process-restart fixtures. |
| Active execution-child replacement in one Prism job | Supported for replay-safe graphs with supervised execution; a killed child and a failed partial-output attempt have live Prism proofs. |
| Control-owner process replacement | Pending; Prism fails active instructions when that connection disconnects. |

The [portable execution guide](../guides/portable.md) describes how to run the
local proof. The [release gate report](https://github.com/nhubbard/ironbeam/blob/main/V5_RELEASE_GATE_STATUS.md)
records the exact test corpus and unresolved gates.
