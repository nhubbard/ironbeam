# Apache Beam semantics in Ironbeam

Apache Beam defines a model for processing bounded data and streams. A pipeline
contains collections of timestamped elements and transforms between those
collections. A runner chooses where and when to execute the transforms. Ironbeam
5 can execute its logical graph with Direct or lower supported graphs to Beam
protobuf for a portable runner. The two paths share element and graph
descriptors, but they do not yet cover every part of the Beam model.

## Elements and time

An Ironbeam element has a value, an event timestamp in nanoseconds, and window
metadata. Event time describes when the represented event occurred. Processing
time describes when a runner handles it. A source can report event-time
progress separately from its elements. For an unbounded source, that progress
helps the runner decide when a window is ready to close.

Ironbeam's fixed-window operation assigns each element to one interval. The
portable graph expresses that operation as Beam `WindowInto` and gives the
resulting collection an interval-window coder. Its Beam windowing strategy uses
the default trigger, discards accumulated values after a firing, emits at the
earliest timestamp in the pane, and allows no lateness. A late element for a
closed window is therefore absent from the tested grouped output. The
[windowing guide](../guides/windowing.md) shows timestamp assignment and a
fixed-window group.

Beam also defines sliding windows, which can assign an element to several
intervals, and session windows, which merge intervals when elements are close
enough in event time. Ironbeam supports sliding windows on bounded and
unbounded sources on Direct and local Prism. The graph translator encodes both
sliding and session window function payloads, but there is no public
session-window operation. Encoding the session payload does not by itself
implement session merging. The unbounded sliding proof checks watermark
closure and a late row; Direct also round-trips its open-window checkpoint.

## Keys, grouping, and order

A grouping transform collects elements with the same key and window. A portable
runner performs the shuffle between the producing and consuming transforms.
Ironbeam lowers supported grouping, co-grouping, and joins through Beam's native
shuffle boundary, then runs typed Rust handlers on the grouped values.

The shuffle does not promise an encounter order. An ordered fold therefore
needs an explicit order key; equal keys are rejected where the operation
requires a total order. Direct can preserve encounter order for its local
variant, but that local behavior is not a portable ordering guarantee.

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

## Splitting and recovery

The portable unbounded-source path uses a splittable Beam transform. A source
split has a stable identity, a coded position, and event-time progress.
Residual work can resume in a new process after an accepted split. Final-result
sinks keep a durable record of accepted bundle attempts so publication can be
checked after worker restart.

That durable state does not replace a failed worker inside an active Prism job.
Prism treats a disconnected Fn Control stream with active instructions as a
bundle failure. Starting another process under the same worker ID does not
resend the failed instruction. Same-job replacement requires the Control stream
to remain connected while execution is retried, with input replay and output
commit boundaries that prevent partial attempts from escaping. The current
worker-loss fixture verifies that publication fails closed; it is not a
successful same-job replacement proof.

## Current scope

| Beam facility | Ironbeam 5 status |
| --- | --- |
| Bounded and unbounded collections | Supported in the documented source contracts; the unbounded path requires stable split and progress behavior. |
| Event timestamps and watermarks | Timestamp assignment and source progress are available; Direct and Prism compare tested fixed-window results. |
| Global and fixed windows | Supported on the established Direct and local Prism corpus. |
| Sliding windows | Supported on bounded and unbounded collections on Direct and local Prism, including overlap, negative event time, watermark closure, and late-row discard. |
| Session windows | Beam payload encoding exists; keyed merge behavior and collection operations are pending. |
| Default trigger | Used by portable lowering, with discarding accumulation and zero allowed lateness. |
| Custom triggers and pane metadata | Pending. |
| General user state and timers | Pending; internal checkpoint and Fn State paths serve specific operations. |
| Splittable source residuals | Supported by local process-restart fixtures. |
| Active worker replacement in one Prism job | Pending; disconnected jobs fail closed. |

The [portable execution guide](../guides/portable.md) describes how to run the
local proof. The [release gate report](https://github.com/nhubbard/ironbeam/blob/main/V5_RELEASE_GATE_STATUS.md)
records the exact test corpus and unresolved gates.
