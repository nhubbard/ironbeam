use ironbeam_transforms::ElementwiseExt;

pub fn global_vector_fixture(
    pipeline: &Pipeline,
    nonempty: PathBuf,
    empty: PathBuf,
) -> Result<(), Error> {
    let nonempty_sink = nonempty.clone();
    pipeline
        .from_vec("vector-numbers", vec![2_i32, 3_i32])?
        .combine_globally_named("vector-sum", |values| values.into_iter().sum::<i32>())?
        .sink_named_with_config(
            "vector-capture",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::AtLeastOnce,
                commit: true,
            },
            format!("global-vector:{}", nonempty.display()),
            move || {
                Ok(Box::new(GlobalFoldSink {
                    path: nonempty_sink.clone(),
                    values: Vec::new(),
                }))
            },
        )?;
    let empty_sink = empty.clone();
    pipeline
        .from_vec("vector-empty", Vec::<i32>::new())?
        .combine_globally_named("vector-empty-sum", |values| values.into_iter().sum::<i32>())?
        .sink_named_with_config(
            "vector-empty-capture",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::AtLeastOnce,
                commit: true,
            },
            format!("global-vector-empty:{}", empty.display()),
            move || {
                Ok(Box::new(GlobalFoldSink {
                    path: empty_sink.clone(),
                    values: Vec::new(),
                }))
            },
        )?;
    Ok(())
}

struct OrderedDigits;

impl OrderedFoldFn<(i32, i32), i32, i32> for OrderedDigits {
    fn create(&self) -> i32 {
        0
    }
    fn add_input(&self, accumulator: &mut i32, value: (i32, i32)) {
        *accumulator = *accumulator * 10 + value.1;
    }
    fn finish(&self, accumulator: i32) -> i32 {
        accumulator
    }
}

/// The same ordered keyed graph is built by the driver and the Prism worker.
pub fn ordered_keyed_fixture(pipeline: &Pipeline, path: PathBuf) -> Result<Sink<i32>, Error> {
    let sink_path = path.clone();
    pipeline
        .from_vec(
            "ordered-source",
            vec![
                (1_000_000_i64, (1_i32, (2_i32, 3_i32))),
                (7_000_000, (1, (1, 2))),
                (5_000_000, (2, (1, 9))),
                (12_000_000, (1, (2, 5))),
                (11_000_000, (1, (1, 4))),
            ],
        )?
        .timestamps_from_pairs_named("ordered-time")?
        .fixed_windows("ordered-window", 10_000_000)?
        .map_values_batches_ordered_by_named(
            "ordered-value-batches",
            2,
            |value| value.0,
            |values| {
                values
                    .iter()
                    .map(|value| (value.0, value.1 + i32::try_from(values.len()).unwrap()))
                    .collect()
            },
        )?
        .fold_values_ordered_by_named("ordered-fold", |value| value.0, OrderedDigits)?
        .reify_timestamps_named("ordered-result-time")?
        .with_window_named("ordered-result-window")?
        .map_named(
            "ordered-capture-value",
            |(window, (timestamp, (key, value)))| {
                i32::try_from(window.start_ns / 1_000_000).unwrap() * 1_000_000
                    + i32::try_from(timestamp / 1_000_000).unwrap() * 10_000
                    + key * 1_000
                    + value
            },
        )?
        .sink_named_with_config(
            "ordered-capture",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::AtLeastOnce,
                commit: true,
            },
            format!("ordered-keyed:{}", path.display()),
            move || {
                Ok(Box::new(GlobalFoldSink {
                    path: sink_path.clone(),
                    values: Vec::new(),
                }))
            },
        )
}

/// Exercise explicit ordering after an unbounded source crosses two watermarks.
pub fn streaming_ordered_keyed_fixture(
    pipeline: &Pipeline,
    path: PathBuf,
) -> Result<Sink<i32>, Error> {
    let sink_path = path.clone();
    pipeline
        .test_stream_source(
            "ordered-stream-source",
            vec![
                TestStreamEvent::Elements(vec![
                    ((1_i32, (2_i32, 3_i32)), 1_000_000),
                    ((1, (1, 2)), 7_000_000),
                    ((2, (1, 9)), 5_000_000),
                ]),
                TestStreamEvent::AdvanceWatermark(10_000_000),
                TestStreamEvent::Elements(vec![
                    ((1, (2, 5)), 12_000_000),
                    ((1, (1, 4)), 11_000_000),
                ]),
                TestStreamEvent::AdvanceWatermark(20_000_000),
            ],
        )?
        .fixed_windows("ordered-stream-window", 10_000_000)?
        .map_values_batches_named("ordered-stream-value-map", 1, |values| {
            vec![(values[0].0, values[0].1 + 1)]
        })?
        .map_values_batches_ordered_by_named(
            "ordered-stream-value-batches",
            2,
            |value| value.0,
            |values| {
                values
                    .iter()
                    .map(|value| (value.0, value.1 + i32::try_from(values.len()).unwrap()))
                    .collect()
            },
        )?
        .fold_values_ordered_by_named("ordered-stream-fold", |value| value.0, OrderedDigits)?
        .reify_timestamps_named("ordered-stream-result-time")?
        .with_window_named("ordered-stream-result-window")?
        .map_named(
            "ordered-stream-capture-value",
            |(window, (timestamp, (key, value)))| {
                i32::try_from(window.start_ns / 1_000_000).unwrap() * 1_000_000
                    + i32::try_from(timestamp / 1_000_000).unwrap() * 10_000
                    + key * 1_000
                    + value
            },
        )?
        .sink_named_with_config(
            "ordered-stream-capture",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::AtLeastOnce,
                commit: true,
            },
            format!("ordered-stream-keyed:{}", path.display()),
            move || {
                Ok(Box::new(GlobalFoldSink {
                    path: sink_path.clone(),
                    values: Vec::new(),
                }))
            },
        )
}

/// Fold each key in explicit order across early and final global panes.
/// The final pane reuses an order key from the earlier discarded pane.
pub fn streaming_global_ordered_keyed_fixture(
    pipeline: &Pipeline,
    path: PathBuf,
) -> Result<Sink<String>, Error> {
    let sink_path = path.clone();
    pipeline
        .test_stream_source(
            "ordered-global-stream-source",
            vec![
                TestStreamEvent::Elements(vec![
                    ((1_i32, (2_i32, 3_i32)), 1_000_000),
                    ((1, (1, 2)), 2_000_000),
                    ((2, (1, 9)), 3_000_000),
                ]),
                TestStreamEvent::Elements(vec![((1, (1, 4)), 4_000_000)]),
            ],
        )?
        .early_count_trigger_named("ordered-global-trigger", 2)?
        .fold_values_ordered_by_named("ordered-global-fold", |value| value.0, OrderedDigits)?
        .reify_timestamps_named("ordered-global-time")?
        .reify_panes_named("ordered-global-pane")?
        .map_named("ordered-global-row", |(pane, (time, (key, digits)))| {
            format!("{key}:{:?}:{}:{time}:{digits}", pane.timing, pane.index)
        })?
        .sink_named_with_config(
            "ordered-global-sink",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            format!("ordered-global:{}", path.display()),
            move || {
                Ok(Box::new(IdempotentRowSink {
                    directory: sink_path.clone(),
                    pending: Vec::new(),
                }))
            },
        )
}

/// Select the latest event per key in early and final global panes.
pub fn streaming_global_latest_keyed_fixture(
    pipeline: &Pipeline,
    path: PathBuf,
    deterministic_ties: bool,
) -> Result<Sink<String>, Error> {
    let sink_path = path.clone();
    let source = pipeline
        .test_stream_source(
            "latest-global-stream-source",
            vec![
                TestStreamEvent::Elements(vec![
                    ((1_i32, 3_i32), 1_000_000),
                    ((1, 2), 2_000_000),
                    ((2, 9), 3_000_000),
                ]),
                TestStreamEvent::Elements(vec![((1, 4), 4_000_000)]),
            ],
        )?
        .early_count_trigger_named("latest-global-trigger", 2)?;
    let latest = if deterministic_ties {
        source.latest_per_key_by_named("latest-global-keyed", |value| *value)?
    } else {
        source.latest_per_key_named("latest-global-keyed")?
    };
    latest
        .reify_timestamps_named("latest-global-time")?
        .reify_panes_named("latest-global-pane")?
        .map_named("latest-global-row", |(pane, (time, (key, value)))| {
            format!("{key}:{:?}:{}:{time}:{value}", pane.timing, pane.index)
        })?
        .sink_named_with_config(
            "latest-global-sink",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            format!("latest-global:{}", path.display()),
            move || {
                Ok(Box::new(IdempotentRowSink {
                    directory: sink_path.clone(),
                    pending: Vec::new(),
                }))
            },
        )
}

pub fn ordered_global_fixture(
    pipeline: &Pipeline,
    nonempty: PathBuf,
    empty: PathBuf,
) -> Result<(Sink<i32>, Sink<i32>), Error> {
    let nonempty_sink = nonempty.clone();
    let nonempty_result = pipeline
        .from_vec(
            "ordered-global-source",
            vec![(1_000_000_i64, (2_i32, 3_i32)), (7_000_000, (1, 2))],
        )?
        .timestamps_from_pairs_named("ordered-global-time")?
        .fold_globally_ordered_by_named("ordered-global-fold", |value| value.0, OrderedDigits)?
        .reify_timestamps_named("ordered-global-result-time")?
        .map_named("ordered-global-capture-value", |(timestamp, value)| {
            i32::try_from(timestamp / 1_000_000).unwrap() * 10_000 + value
        })?
        .sink_named_with_config(
            "ordered-global-capture",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::AtLeastOnce,
                commit: true,
            },
            format!("ordered-global:{}", nonempty.display()),
            move || {
                Ok(Box::new(GlobalFoldSink {
                    path: nonempty_sink.clone(),
                    values: Vec::new(),
                }))
            },
        )?;
    let empty_sink = empty.clone();
    let empty_result = pipeline
        .from_vec("ordered-global-empty-source", Vec::<(i32, i32)>::new())?
        .fold_globally_ordered_by_named(
            "ordered-global-empty-fold",
            |value| value.0,
            OrderedDigits,
        )?
        .sink_named_with_config(
            "ordered-global-empty-capture",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::AtLeastOnce,
                commit: true,
            },
            format!("ordered-global-empty:{}", empty.display()),
            move || {
                Ok(Box::new(GlobalFoldSink {
                    path: empty_sink.clone(),
                    values: Vec::new(),
                }))
            },
        )?;
    Ok((nonempty_result, empty_result))
}

/// Exercise deterministic ordered helpers on one bounded fanout graph.
pub fn ordered_helpers_fixture(pipeline: &Pipeline, path: PathBuf) -> Result<Sink<String>, Error> {
    let source = pipeline.from_vec(
        "ordered-helpers-source",
        vec![(3_i32, 20_i32), (1, 10), (4, 10), (2, 30)],
    )?;
    let list = source
        .to_list_ordered_by_named("ordered-helpers-list", |row| row.0)?
        .flat_map_named("ordered-helpers-list-rows", |rows| {
            rows.into_iter()
                .enumerate()
                .map(|(index, (rank, value))| format!("list:{index}:{rank}:{value}"))
                .collect::<Vec<_>>()
        })?;
    let take = source
        .take_ordered_by_named("ordered-helpers-take", 2, |row| row.0)?
        .map_named("ordered-helpers-take-rows", |(rank, value)| {
            format!("take:{rank}:{value}")
        })?;
    let first = source
        .first_ordered_by_named("ordered-helpers-first", |row| row.0)?
        .map_named("ordered-helpers-first-rows", |(rank, value)| {
            format!("first:{rank}:{value}")
        })?;
    let distinct = source
        .distinct_by_ordered_by_named("ordered-helpers-distinct", |row| row.1, |row| row.0)?
        .map_named("ordered-helpers-distinct-rows", |(rank, value)| {
            format!("distinct:{rank}:{value}")
        })?;
    let sink_path = path.clone();
    list.flatten_named("ordered-helpers-flatten", &[take, first, distinct])?
        .sink_named_with_config(
            "ordered-helpers-sink",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::AtLeastOnce,
                commit: true,
            },
            format!("ordered-helpers:{}", path.display()),
            move || {
                Ok(Box::new(GlobalFoldSink::<String> {
                    path: sink_path.clone(),
                    values: Vec::new(),
                }))
            },
        )
}

/// Select one helper branch, or their merged output, in the streaming fixture.
#[derive(Clone, Copy)]
pub enum StreamingOrderedHelperRoute {
    All,
    List,
    Take,
    First,
    Distinct,
}

/// Exercise explicit-order helpers in two closed windows of an unbounded source.
pub fn streaming_ordered_helpers_fixture(
    pipeline: &Pipeline,
    path: PathBuf,
    route: StreamingOrderedHelperRoute,
) -> Result<Sink<String>, Error> {
    let source = pipeline
        .test_stream_source(
            "streaming-ordered-helpers-source",
            vec![
                TestStreamEvent::Elements(vec![
                    ((3_i32, 20_i32), 3_000_000),
                    ((1, 10), 1_000_000),
                    ((2, 10), 2_000_000),
                ]),
                TestStreamEvent::AdvanceWatermark(10_000_000),
                TestStreamEvent::Elements(vec![
                    ((5, 10), 12_000_000),
                    ((6, 30), 13_000_000),
                    ((4, 10), 11_000_000),
                ]),
                TestStreamEvent::AdvanceWatermark(20_000_000),
            ],
        )?
        .fixed_windows("streaming-ordered-helpers-window", 10_000_000)?;
    let list = source
        .to_list_ordered_by_named("streaming-ordered-helpers-list", |row| row.0)?
        .reify_timestamps_named("streaming-ordered-helpers-list-time")?
        .with_window_named("streaming-ordered-helpers-list-window")?
        .flat_map_named("streaming-ordered-helpers-list-rows", |(window, (time, rows))| {
            rows.into_iter()
                .enumerate()
                .map(|(index, (rank, value))| {
                    format!(
                        "list:{}:{}:{index}:{rank}:{value}",
                        window.start_ns / 1_000_000,
                        time / 1_000_000
                    )
                })
                .collect::<Vec<_>>()
        })?;
    let take = source
        .take_ordered_by_named("streaming-ordered-helpers-take", 2, |row| row.0)?
        .reify_timestamps_named("streaming-ordered-helpers-take-time")?
        .with_window_named("streaming-ordered-helpers-take-window")?
        .map_named("streaming-ordered-helpers-take-rows", |(window, (time, (rank, value)))| {
            format!("take:{}:{}:{rank}:{value}", window.start_ns / 1_000_000, time / 1_000_000)
        })?;
    let first = source
        .first_ordered_by_named("streaming-ordered-helpers-first", |row| row.0)?
        .reify_timestamps_named("streaming-ordered-helpers-first-time")?
        .with_window_named("streaming-ordered-helpers-first-window")?
        .map_named("streaming-ordered-helpers-first-rows", |(window, (time, (rank, value)))| {
            format!("first:{}:{}:{rank}:{value}", window.start_ns / 1_000_000, time / 1_000_000)
        })?;
    let distinct = source
        .distinct_by_ordered_by_named("streaming-ordered-helpers-distinct", |row| row.1, |row| row.0)?
        .reify_timestamps_named("streaming-ordered-helpers-distinct-time")?
        .with_window_named("streaming-ordered-helpers-distinct-window")?
        .map_named("streaming-ordered-helpers-distinct-rows", |(window, (time, (rank, value)))| {
            format!("distinct:{}:{}:{rank}:{value}", window.start_ns / 1_000_000, time / 1_000_000)
        })?;
    let sink_path = path.clone();
    let output = match route {
        StreamingOrderedHelperRoute::All => list.flatten_named(
            "streaming-ordered-helpers-flatten",
            &[take, first, distinct],
        )?,
        StreamingOrderedHelperRoute::List => list,
        StreamingOrderedHelperRoute::Take => take,
        StreamingOrderedHelperRoute::First => first,
        StreamingOrderedHelperRoute::Distinct => distinct,
    };
    output.sink_named_with_config(
        "streaming-ordered-helpers-sink",
        SinkSpec {
            mode: SinkMode::Append,
            replay: Replay::Idempotent,
            commit: true,
        },
        format!("streaming-ordered-helpers:{}", path.display()),
        move || {
            Ok(Box::new(IdempotentRowSink {
                directory: sink_path.clone(),
                pending: Vec::new(),
            }))
        },
    )
}
