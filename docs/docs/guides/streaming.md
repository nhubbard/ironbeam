# Streaming pipelines

An unbounded source produces values over more than one polling cycle. Each
value carries an event timestamp. The source also reports a watermark: an
estimate that earlier event times are complete. A windowed group can emit a
pane when a trigger fires or the watermark closes its window.

## Build a windowed stream

The following graph uses a finite test stream. Timestamps and window sizes are
in nanoseconds. Its two values have the same key and belong to the same
ten-millisecond window.

```rust
use ironbeam::{Pipeline, TestStreamEvent};

fn main() -> Result<(), ironbeam::Error> {
    let pipeline = Pipeline::default();
    let grouped = pipeline
        .test_stream_source(
            "events",
            vec![
                TestStreamEvent::Elements(vec![
                    (("site-a".to_owned(), 2_i32), 1_000_000),
                    (("site-a".to_owned(), 3_i32), 2_000_000),
                ]),
                TestStreamEvent::AdvanceWatermark(10_000_000),
            ],
        )?
        .fixed_windows("windows", 10_000_000)?
        .group_by_key_named("group")?;
    let _output = grouped.output("result")?;
    Ok(())
}
```

This code builds a graph. A streaming job also needs a sink and a runner.
`DirectRunner::start_stream_sink` starts a Direct job for a registered sink;
the returned `StreamJob` exposes progress, cancellation, and `wait`. The
[Prism proof package](https://github.com/nhubbard/ironbeam/tree/main/examples/prism-proof)
contains complete Direct and portable examples with replay-safe sinks.

## Choose a firing policy

`group_by_key_named` uses the collection's window and trigger policy. The
default trigger emits when the event watermark passes the window end. A
continuing source in the global window may never reach that end. For regular
output, use finite windows or an early trigger before grouping:

```rust
let batches = input
    .early_count_trigger_named("every-two", 2)?
    .group_by_key_named("groups")?;
```

In discarding mode, values emitted in one early pane are absent from the next
pane. Accumulating policies retain earlier values. That choice affects output
counts and sink idempotency. [Event time and windows](windowing.md) describes
count, processing-time, and late-data policies with examples.

For keyed records that should form bounded vectors, use
`group_into_batches_named`, or `group_into_batches_with_limits_named` when
encoded byte size also matters. A full batch emits when its limit is met. A
partial fixed-window batch emits at window expiration. A partial global batch
emits when a finite source completes; a continuing global source needs a limit
or processing-time deadline to emit it.

## Record source progress

A production unbounded source must be replayable. Its reader returns a stable
split ID, a cursor, and event-time progress. Direct checkpoints that progress
with supported coded state. After a restart, it reopens the reader at the
saved cursor and validates the source configuration and watermark. A changed
split or regressed watermark is a recovery error.

Set a stable `binary_build_id` and use a dedicated checkpoint directory for
each Direct job. [Direct checkpoints](checkpoints.md) gives a configuration
example. A checkpoint can prevent rereading committed input; an external sink
still needs an idempotent or finalized write contract to prevent duplicates.

## Acknowledge a source after commit

`Pipeline::unbounded_source_with_finalizer` registers a callback for a
source position. Direct calls it after the output sinks and a per-batch
checkpoint commit. On restart, Direct calls it again for the last saved
position, including when the previous run completed. The callback must
therefore tolerate duplicate calls. A Direct job using this source requires
`JobConfig::checkpoint` with `interval_batches: 1`.
The source must attach a nonempty checkpoint position to every ready batch;
Ironbeam checks this before processing its elements.

A portable worker records the source position for Beam's `FinalizeBundle`.
The runner calls it after committing that bundle's output. When a source and
sink share a bundle, Ironbeam runs its sink finalizers before the source
callback. A shuffle or other stage boundary can place the downstream sink in
a later bundle, so source finalization does not wait for that sink's commit.
Bundle-finalized sinks require an immediate `Reshuffle`, which places them in
a separate stage from the source. A local Prism job verifies two source
callbacks and the accepted sink fragments across this boundary.
The runner must retain the committed output for downstream retries.
Supervised workers require a durable file or object journal so a replacement
child can recover the callback. Bundle finalization is best effort: if the
callback is lost, the external source must be able to redeliver the record,
even when its reader is reopened at the saved cursor. A provider should
acknowledge a delivery ID, not merely mark a message ID as seen. Ironbeam does
not yet include a Pub/Sub pipeline source; the [GCP guide](gcp.md#pubsub)
describes the separate client.

## Runner boundaries

Direct uses the source clock when a test stream supplies one and wall time for
ordinary processing-time timers. A portable runner controls its own bundle
and timer schedule. Values, window assignment, and supported pane policies
have shared Direct and local Prism proofs, but a shuffle does not preserve
input encounter order. Use an explicit order key whenever a result depends on
order.

The [5.0.0 support boundary](../reference/support.md) distinguishes tested
Direct and Prism behavior from hosted runner work. The [portable execution
guide](portable.md) explains how a worker runs the same logical graph through
Beam protobuf and the Fn API.
