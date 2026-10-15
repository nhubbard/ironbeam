# Transforms

A transform adds a node to a pipeline graph and returns a new `PCollection` handle. Named methods identify the operation and return `Result` so invalid graph construction can be reported before execution.

## Element operations

`map_named` changes each element. `filter_named` retains elements for which a predicate returns true. `flat_map_named` can emit zero or more elements for each input.

```rust
use ironbeam::{Pipeline, prelude::*};

fn main() -> Result<(), ironbeam::Error> {
    let pipeline = Pipeline::default();
    let values = pipeline
        .from_vec("source", vec![1_i32, 2, 3, 4])?
        .map_named("square", |value| value * value)?
        .filter_named("even-square", |value| value % 2 == 0)?
        .collect_seq()?;
    assert_eq!(values, vec![4, 16]);
    Ok(())
}
```

Elementwise operations can often be executed as a fused Direct route. Grouping and similar operations require a materialization boundary; `DirectRunner.explain` identifies those choices.

`flatten_named` accepts bounded and unbounded inputs when their element types
and time declarations match. A shared fixed-window fixture preserves each
input's value, event timestamp, and window on Direct and local Prism. With a
checkpointed Direct job, a bounded source resumes by skipping rows already
committed before continuing with the stream. Use a source whose configured
snapshot remains stable across that restart.

## Partitioning

`partition_named` routes each input element to one of a fixed number of
outputs. The classifier returns an index starting at zero. The resulting
collections have stable keys formed from the output prefix and index:

```rust
use ironbeam::{Pipeline, prelude::*};

fn main() -> Result<(), ironbeam::Error> {
    let pipeline = Pipeline::default();
    let source = pipeline.from_vec("numbers", vec![0_i32, 1, 2, 3, 4, 5])?;
    let parts = source.partition_named("classify", "part", 2, |value| {
        value.rem_euclid(2) as usize
    })?;
    assert_eq!(parts[0].collect_seq()?, vec![0, 2, 4]);
    assert_eq!(parts[1].collect_seq()?, vec![1, 3, 5]);
    Ok(())
}
```

An index outside the declared range fails execution. Direct may evaluate the
classifier separately for downstream branches, and any runner may retry an
element. The classifier must return the same index on each evaluation and
should have no observable side effects. Use
`partition_with_semantic_named` to include an application semantic ID and
version in graph identity; change that version when routing behavior changes.
The local Prism fixtures check all three bounded outputs and a two-branch
unbounded source against Direct, including event timestamps and fixed windows.

## Grouping by key

Pairs can be grouped by their first component:

```rust
use ironbeam::{Pipeline, prelude::*};

fn main() -> Result<(), ironbeam::Error> {
    let pipeline = Pipeline::default();
    let groups = pipeline
        .from_vec("pairs", vec![("a".to_string(), 1_i32), ("a".to_string(), 2)])?
        .group_by_key_named("group")?
        .collect_seq()?;
    assert_eq!(groups.len(), 1);
    Ok(())
}
```

A grouped result contains a key and its values. A shuffle may change value order, so an application that depends on order must provide an explicit order key through the corresponding ordered operation. The transform crates also include keyed reductions, joins, sampling, selection, validation, and side inputs. The [ETL example](https://github.com/nhubbard/ironbeam/blob/main/examples/etl_pipeline.rs) applies several of these operations in one pipeline.

`distinct_per_window_named` emits each distinct value once in each window. It
uses a keyed shuffle, so output order is unspecified. Its output timestamp
follows the earliest input for that value and window. Direct and local Prism
agree on a fixture containing duplicates in two fixed windows. The existing
`distinct_named` helper retains its ordered, finite-collection behavior.
With a repeated count trigger, a value can appear in more than one pane of
the same window. Direct and local Prism both emit two early `a` panes and one
on-time `b` pane in the tested streaming schedule.
When allowed lateness is set, the same value can also appear in a late pane.
Both runners emit an on-time `a` followed by a late `a` after two additional
rows of that value arrive before the window expires.

`distinct_by_per_window_named` derives a key from each value and keeps one
representative per key and window. It chooses the smallest encoded value, so
its result is stable when a runner changes bundle order:

```rust
use ironbeam::{Pipeline, prelude::*};

fn main() -> Result<(), ironbeam::Error> {
    let pipeline = Pipeline::default();
    let mut representatives = pipeline
        .from_vec("names", vec!["a2".to_owned(), "a1".to_owned(), "b2".to_owned()])?
        .distinct_by_per_window_named("by-prefix", |name| name[..1].to_owned())?
        .collect_seq()?;
    representatives.sort();
    assert_eq!(representatives, ["a1", "b2"]);
    Ok(())
}
```

The value and derived key require stable coders. The helper groups the values
for each open key and window; a large key can reach the configured state
budget. On unbounded input, use a finite window or an early trigger for the
global window. `count_per_element_named` also works on watermarked input. Its
streaming path groups each value and emits a count when the matching window
closes; the bounded path uses an incremental combiner.

For an unbounded keyed collection, call `early_processing_time_trigger_named("timer", 5_000_000)` before `group_by_key_named` to emit an early pane five milliseconds after the first row in a key and window. A later row can arm another firing. `accumulating_processing_time_trigger_named` retains earlier values in each new pane; the other method discards values after each firing. The delay is a positive whole number of milliseconds expressed in nanoseconds. The policy accepts global, fixed, and sliding windows; session windows are rejected. Direct uses the source's scripted clock when supplied and wall time otherwise, so an idle source can still fire. Direct and local Prism have matching fixed-window results for a single TestStream source with explicit processing-time advances.

`group_into_batches_named` emits vectors of values under each key. Its size
must be greater than zero and smaller than `i64::MAX`; invalid sizes fail
while the graph is built. For an unbounded input, Ironbeam sets a discarding
count trigger at the requested size. It replaces any earlier trigger on the
input so a smaller count or accumulating policy cannot emit partial or repeated
batches. When the input allows late data, the helper uses the same count for
late firings. A final pane can contain fewer values than the requested size.
The count-or-processing-time batching method rejects unbounded input with
allowed lateness until it can preserve a late firing policy as well.
Batch membership can depend on arrival and firing order after a shuffle.
Each output batch receives the earliest event timestamp among the values in
that batch. A later batch in the same key and window can therefore have a
later timestamp than the first batch.

`group_into_batches_with_max_buffering_duration_named("batches", 2, 5_000_000)`
also flushes a partial batch five milliseconds after its first pending value.
It uses a repeated count-or-processing-time trigger: a full batch fires as
soon as the count is met, while the time limit permits a smaller early batch.
The delay must be a positive whole number of milliseconds, expressed in
nanoseconds. The Direct runner tests both firing paths. A local Prism job
confirms a timed partial batch followed by a count batch when its clock
advances again before window closure. The pinned Prism version can panic if
the window closes while an old processing timer from that count firing is
still queued. That schedule remains unproved on Prism.

For a general keyed group, `count_or_processing_time_trigger_named` uses
discarding panes and `accumulating_count_or_processing_time_trigger_named`
retains earlier values in each pane. Apply either trigger before
`group_by_key_named`. The accumulating form is separate from batching because
repeating earlier batch values would duplicate application records.

For bounded input, `group_into_batches_with_limits_named("batches", 2, 4096)`
splits a key's values at two elements or 4,096 encoded value bytes, whichever
comes first. The byte count excludes the key and Beam's window envelope. A
single value larger than the limit forms a one-value batch. Direct and local
Prism produce the same batch values in the tested bounded job. The method
also assigns each batch its own earliest value timestamp. It rejects
unbounded input because byte-triggered streaming requires keyed
state and timers that this helper does not yet provide.

`co_group_homogeneous_named` groups collections whose keys and values have
the same types. The result is a key and one value vector
per input. The first vector belongs to the collection on which the method is
called; the remaining vectors follow the order of `others`. A missing key in
one input produces an empty vector. Inputs must share time and window
declarations; bounded and unbounded inputs may be mixed. Any unbounded input
requires fixed, sliding, or session windows. The method uses Flatten and
GroupByKey in the portable graph, so
value order within each vector is unspecified. A twelve-input
fixed-window job and a twelve-branch merged-session job pass on Direct and
local Prism. The session job derives its branches from one bounded source so
the bridge record precedes source completion on both runners. Three independent
unbounded sources and three branches from one unbounded source also pass on
both runners in fixed windows. Direct checkpoints their open groups and source
cursors; late records after window closure are dropped. A common transform
before the branches runs once per input element. Three independent unbounded
inputs also merge a keyed session when their watermarks advance after the
bridging record. Direct can restore its open intervals and source cursors
before that bridge arrives. Intermediate-watermark session schedules still
need runner agreement.

A shared three-input fixed-window job combines one unbounded input, one bounded
input, and an empty bounded input. Direct and local Prism produce the same two
keyed groups, including a key found only in the bounded input. Direct stores
the bounded rows with the open group; after a checkpoint restart, it resumes
the stream without reading those bounded rows again.

## Side views

A side view lets each main element consult a bounded collection. The global
`map_with_side_named` method supplies a slice of all side values. Three typed
helpers give that slice a specific shape:

| Method | View | Invalid input |
| --- | --- | --- |
| `map_with_singleton_side_named` | One side value | Zero or multiple values fail execution |
| `map_with_map_side_named` | One value per key | Duplicate keys fail execution |
| `map_with_multimap_side_named` | All values per key | Value order is unspecified |

```rust
use ironbeam::{Pipeline, prelude::*};

fn main() -> Result<(), ironbeam::Error> {
    let pipeline = Pipeline::default();
    let orders = pipeline.from_vec("orders", vec!["a".to_owned(), "b".to_owned()])?;
    let rates = pipeline.from_vec("rates", vec![("a".to_owned(), 10_i32)])?;
    let totals = orders
        .map_with_map_side_named("rate-lookup", &rates, |key, rates| {
            rates.get(&key).copied().unwrap_or_default()
        })?
        .collect_seq()?;
    assert_eq!(totals, vec![10, 0]);
    Ok(())
}
```

These helpers materialize one global view from a bounded side collection. For
matching values in each event-time window, use `map_with_windowed_side_named`,
`map_with_windowed_singleton_side_named`,
`map_with_windowed_map_side_named`, or
`map_with_windowed_multimap_side_named`. The windowed methods accept an
unbounded side collection when both inputs have watermarks. They apply one view
after both inputs close the window and preserve each main element's timestamp.
They do not update a result already emitted from that window. The singleton
requires exactly one side value in each main window; a map rejects duplicate
keys within a window. Values under a multimap key have no guaranteed order.

`map_with_windowed_materialized_side_named` accepts a fallible view builder and
a fallible mapping function for application-defined view shapes. For a
transform that emits zero or more values per input and can fail, use
`flat_map_fallible_named`. Its errors fail execution on Direct and the portable
worker.

## Flatten

`flatten_named` combines collections with the same element type and window
declaration. An empty `others` slice creates a one-input Flatten. For example,
`rows.flatten_named("one-input", &[])?` leaves the rows intact while retaining
a portable Flatten transform in the graph.

## Completion barriers

`wait_on_named` holds a main collection until a bounded signal collection
completes. The main collection may be unbounded: Direct completes the bounded
signal before opening the stream reader, then resumes that reader from its
saved cursor after a restart. The checkpoint records the signal source's
content identity, so changing any bounded input invalidates recovery. A
bounded signal may combine multiple sources with `Flatten` and a keyed shuffle.
`wait_on_all_named` accepts several bounded signals of the same
element type and requires all of them to finish. For signals with different
element types, use `wait_on_signals_named` and convert each collection with
`wait_signal`. For matching event-time windows on watermarked streams,
`wait_on_windowed_named` releases main values after both sources close
their matching window. Signal values are ignored, and a window with no signal
elements still closes when the signal watermark passes it. The windowed method
requires matching time and window declarations. It may buffer a full window;
it does not release early when the signal closes before the main watermark.
`wait_on_all_windowed_named` accepts several signals of the same element type.
It uses one keyed co-group to wait for the main input and every signal watermark
to close the matching window. Direct and local Prism agree in a three-source
fixed-window test, including windows without signal elements.

For signals with different element types, convert each collection with
`windowed_wait_signal_named` and pass the handles to
`wait_on_windowed_signals_named`. This also uses one keyed co-group. A shared
job checks three independently watermarked sources on Direct and local Prism.
The two signal branches carry text and Boolean values after their sources.

```rust
let ready = main.wait_on_all_windowed_named(
    "after-windowed-imports",
    &[users_done, orders_done],
)?;
```

```rust
let users_done = users.windowed_wait_signal_named("users-done")?;
let orders_done = orders.windowed_wait_signal_named("orders-done")?;
let ready = main.wait_on_windowed_signals_named(
    "after-windowed-imports",
    &[users_done, orders_done],
)?;
```

```rust
let ready = main.wait_on_signals_named(
    "after-imports",
    &[users_done.wait_signal(), orders_done.wait_signal()],
)?;
```

The signal collections may have different element types. Their values do not
appear in `ready`.

## Batches and custom handlers

Version 5 batch transforms use an explicit size rather than an implicit physical partition boundary. Registered custom handlers have semantic identifiers and versions; those values contribute to graph identity. The [migration guide](../migration/operations.md) describes the effect on v4 programs. Runner-specific capability checks determine whether a stateful graph can execute on Direct or the portable worker.

## Keyed user state

`stateful_value_map_named` gives each key and assigned window one optional
value cell. The callback receives the current value and returns its replacement
and one output. Returning `None` clears the cell.

```rust
use ironbeam_core::Version;

let totals = pairs.stateful_value_map_named::<i64, i64>(
    "running-total",
    "orders/running-total",
    Version { major: 1, minor: 0 },
    |_, amount, prior| {
        let total = prior.unwrap_or(0) + amount;
        Ok((Some(total), total))
    },
)?;
```

`pairs` is a `PCollection<(K, i64)>` with coded keys. The operation currently
accepts bounded input with default global, fixed, or sliding window policy. It
also accepts unbounded global, fixed, and sliding windows on one linear Direct
stream route; Direct checkpoints the coded cells with the source cursor. Beam
does not preserve encounter order across keyed shuffle; callbacks that require
an order should carry an explicit order key. The portable worker uses Beam Fn
State for this value cell. Bounded per-key and fixed-window counts, plus unbounded global,
fixed-window, and overlapping sliding-window counts, match Direct on local Prism.
A supervised child-loss fixture proves replay with an idempotent sink.
A production unbounded source also recovers its keyed counts after child
replacement during residual reopen. Direct and Prism also agree when a row
arrives after its fixed window expires: it starts a fresh value cell.
Timer callbacks and some state families remain open.

`stateful_bag_map_named` gives each key and assigned window a bag of coded
elements. The callback receives the current contents and returns one output
with a mutation: `Keep`, `Clear`, `Append`, or `Replace`. Bag iteration order
is unspecified. The callback should use explicit element keys if its result
depends on order.

```rust
use ironbeam_core::{BagStateMutation, Version};

let counts = pairs.stateful_bag_map_named::<i64, i64>(
    "seen-amounts",
    "orders/seen-amounts",
    Version { major: 1, minor: 0 },
    |_, amount, previous: Vec<i64>| {
        let count = previous.len() as i64 + 1;
        Ok((BagStateMutation::Append(vec![amount]), count))
    },
)?;
```

The Direct runner saves coded bag contents with the source cursor. The
portable worker encodes each item with the declared Beam bag element coder
and uses Fn State under the current bundle instruction. An unbounded
TestStream with two keys and two element batches produces the same five
counts on Direct and local Prism. A production unbounded source retains its
bag contents after Prism replaces a failed execution child and reopens the
source residual. Direct resumes its bag and source cursor from a checkpoint.
A bounded fixture also checks the values presented to callbacks across all
four mutation variants and duplicate entries on both runners. An unbounded
fixed-window fixture keeps two independent bag cells for the same key across
two element batches on both runners. The operation has no timer callback.

`stateful_set_map_named` stores unique coded elements for each key and
window. The callback receives a borrowed `HashSet` and returns one output
with `Keep`, `Clear`, `Insert`, `Remove`, or `Replace`.

```rust
use ironbeam_core::{SetStateMutation, Version};

let membership = pairs.stateful_set_map_named::<i64, bool>(
    "seen-ids",
    "orders/seen-ids",
    Version { major: 1, minor: 0 },
    |_, id, previous| {
        let already_seen = previous.contains(&id);
        Ok((SetStateMutation::Insert(id), already_seen))
    },
)?;
```

The element coder must give the same bytes for equal values. Direct stores a
coded set in its state snapshot. Portable execution declares Beam `SetState`
and uses multimap Fn State keys; each changed set replaces the runner's key
set within the bundle. This can cost work proportional to the set size. A
bounded fixture checks membership, duplicate insertion, and every mutation
variant on Direct and local Prism. An unbounded two-batch fixture retains
membership across batches on both runners. Direct restores a set with its
source cursor, and a production Prism job retains the set after a failed
execution child is replaced and the source residual reopens. SetState has no
timer callback yet. A fixed-window unbounded fixture confirms that two
windows of the same key keep separate sets on Direct and local Prism.

`stateful_map_map_named` stores one coded value per map key inside each
pipeline key and window. The callback receives a borrowed `HashMap` and may
keep, clear, put, remove, or replace entries.

```rust
use ironbeam_core::{MapStateMutation, Version};

let totals = pairs.stateful_map_map_named::<String, i64, i64>(
    "account-totals",
    "billing/account-totals",
    Version { major: 1, minor: 0 },
    |_, (account, amount), previous| {
        let total = previous.get(&account).copied().unwrap_or(0) + amount;
        Ok((MapStateMutation::Put(account, total), total))
    },
)?;
```

Here `pairs` contains `(pipeline_key, (account, amount))` values. The map
key and value types need registered coders. Distinct map keys must encode to
distinct bytes. Direct snapshots the coded map with the source cursor. The
portable worker uses Beam's multimap Fn State protocol. When a callback
changes the map, the worker replaces all entries within its bundle; the
work grows with the number of entries. A bounded mutation fixture matches
Direct and local Prism. Unbounded global and fixed-window fixtures agree
across two batches, and a production Prism job retains its map after worker
replacement. Timer callbacks remain unavailable.

`stateful_multimap_map_named` stores multiple values per map key for each
pipeline key and window. It retains duplicates. The callback receives a
borrowed `HashMap<MK, Vec<MV>>` and may keep, clear, append, remove all values
at one key, or replace the full multimap.

```rust
use ironbeam_core::{MultimapStateMutation, Version};

let counts = pairs.stateful_multimap_map_named::<String, i64, usize>(
    "account-events",
    "billing/account-events",
    Version { major: 1, minor: 0 },
    |_, (account, amount), previous| {
        let count = previous.get(&account).map_or(0, Vec::len) + 1;
        Ok((MultimapStateMutation::Append(account, amount), count))
    },
)?;
```

Here `pairs` contains `(pipeline_key, (account, amount))` values. Values
within each map key are presented in coder-byte order; this is an Ironbeam
choice because Beam does not define multimap iteration order. Both map-key
and value types need registered coders. Distinct map keys must encode to
distinct bytes. Direct saves the cell with the source cursor. The portable
worker uses Beam's multimap Fn State protocol and replaces the cell when it
changes, so a mutation can cost work proportional to its total values.
Bounded, unbounded, fixed-window, checkpoint restart, and Prism worker
replacement fixtures have matching results. Timer callbacks remain
unavailable.

`stateful_ordered_list_map_named` keeps coded values by an explicit millisecond
sort key for each pipeline key and window. The callback reads a slice of
`(sort_key, value)` pairs and returns one output with `Keep`, `Clear`,
`Append`, `ClearRange`, or `Replace`. A clear range includes its start and
excludes its end. Equal sort keys are presented in encoded-value order.

```rust
use ironbeam_core::{OrderedListStateMutation, Version};

let previous = pairs.stateful_ordered_list_map_named::<i64, Vec<i64>>(
    "recent-amounts",
    "orders/recent-amounts",
    Version { major: 1, minor: 0 },
    |_, amount, entries| {
        let seen = entries.iter().map(|(_, value)| *value).collect();
        Ok((OrderedListStateMutation::Append(amount, amount), seen))
    },
)?;
```

Here `pairs` contains `(pipeline_key, amount)` values. The sort key in this
example is the amount; event-time ordering instead requires the caller to
provide an event-time millisecond key. The maximum signed 64-bit sort key is
reserved because Beam's full-range read uses a half-open upper bound. Direct
checkpoints the coded list with its source cursor. Bounded mutation and
unbounded fixed-window fixtures agree with local Prism, including a negative
sort key in the bounded job. A Direct restart
test resumes an open list from its checkpoint. A production unbounded Prism
job retains the list through execution-child replacement; its two residual
values may arrive in either order. The portable worker reads the current
list for each callback. When a mutation only adds entries, it appends those
entries to Fn State. A removal or replacement rewrites the cell within one
bundle. Large lists therefore still increase read and callback cost. Timer callbacks are not yet
available for this API.

`stateful_combining_map_named` keeps one coded accumulator for each key and
window. The callback reads the previous accumulator and can keep it, clear it,
replace it, or add a new accumulator through a named merge function. The merge
function must be associative for its result to remain meaningful when runner
work is split or replayed. Its semantic ID and version are included in the
Beam combining-state declaration.

```rust
use ironbeam_core::{CombiningFunction, CombiningStateMutation, Version};

let previous = pairs.stateful_combining_map_named::<i64, i64>(
    "running-sum",
    "orders/running-sum",
    Version { major: 1, minor: 0 },
    CombiningFunction {
        semantic_id: "orders/checked-add",
        version: Version { major: 1, minor: 0 },
    },
    |left, right| left.checked_add(right).ok_or_else(|| ironbeam_core::Error::incompatible("sum overflow")),
    |_, amount, prior| {
        Ok((CombiningStateMutation::Add(amount), prior.copied().unwrap_or_default()))
    },
)?;
```

Here `pairs` contains `(key, amount)` values. Each output is the sum before
the current amount is added. Direct and pinned local Prism agree on bounded
add, clear, and replace operations and on two unbounded fixed windows. Direct
checkpoints the coded accumulator; a local Prism job retains it after a
supervised execution child is replaced. If a runner returns several partial
accumulators, the worker merges them before invoking the callback and writes
back one coded accumulator. A fake Fn State runner verifies this case. Timer
callbacks remain separate from this API.

### Timers with value state

`stateful_timer_map_named` attaches a value cell and one or more timer families
to a keyed unbounded collection. The first callback receives each element and
may arm a timer. The second callback receives the timer firing and may emit
values. This example retains the latest integer for each key until the event
watermark reaches the requested time:

```rust
use std::collections::BTreeMap;
use ironbeam_core::{TimedStateResult, UserTimerDomain, UserTimerRequest, Version};

let flushed = pairs.stateful_timer_map_named::<i64, i64>(
    "flush",
    "orders/event-flush",
    Version { major: 1, minor: 0 },
    BTreeMap::from([("event".to_string(), UserTimerDomain::EventTime)]),
    |_, amount, _, metadata| {
        Ok(TimedStateResult {
            state: Some(amount),
            outputs: Vec::new(),
            timers: vec![UserTimerRequest::Set {
                family_id: "event".into(),
                dynamic_tag: "flush".into(),
                fire_timestamp_ns: metadata.timestamp_ns + 5_000_000,
                output_timestamp_ns: None,
            }],
        })
    },
    |_, state, _event| {
        Ok(TimedStateResult {
            state: None,
            outputs: state.into_iter().collect(),
            timers: Vec::new(),
        })
    },
)?;
```

The deadline is expressed in nanoseconds and must align to a millisecond.
Setting the same family and dynamic tag again replaces the earlier deadline
for that key and window. `UserTimerRequest::Clear` removes one such timer.
With no explicit output timestamp, an event timer uses its fire time; a
processing-time timer uses the input timestamp as its output timestamp. The
caller may set `output_timestamp_ns: Some(metadata.timestamp_ns)` to retain
the element's event time when the timer fires later. The emitted element keeps
the timer's assigned window. One input may belong to two sliding windows;
each window has its own timer slot and callback.

The current callback execution path is available on Direct for one unbounded
route with default global, fixed, or sliding windows and zero allowed
lateness. Beam lowering records the timer families, and the portable worker
executes them through Fn State and Fn Data. Global-window event- and
processing-time timers, two distinct fixed windows, and two overlapping
sliding windows have matching Direct and pinned local Prism fixtures. The
windowed fixtures compare firing deadlines, output timestamps, interval
bounds, and sink rows.
A production event timer also survives replacement of the Prism execution
child after it is armed. A separate local Prism job replaces the execution
child during a timer callback; the supervisor replays that timer input and
commits each output once. Other failure points and timer callback shapes need
further tests.
