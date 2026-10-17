use super::*;
use ironbeam_core::PaneTiming;
use ironbeam_transforms::{ElementwiseExt, KeyedExt};

/// One value per window, including the earliest event timestamp.
pub fn distinct_window_fixture(pipeline: &Pipeline, path: PathBuf) -> Result<Sink<String>, Error> {
    let sink_path = path.clone();
    pipeline
        .from_vec(
            "distinct-input",
            vec![
                ("a".to_owned(), 1_000_000_i64),
                ("a".to_owned(), 2_000_000),
                ("a".to_owned(), 11_000_000),
                ("b".to_owned(), 12_000_000),
            ],
        )?
        .assign_timestamps_named("distinct-timestamps", |row| row.1)?
        .map_named("distinct-values", |row| row.0)?
        .fixed_windows("distinct-windows", 10_000_000)?
        .distinct_per_window_named("distinct")?
        .reify_timestamps_named("distinct-output-time")?
        .with_window_named("distinct-window-label")?
        .map_named("distinct-row", |(window, (timestamp, value))| {
            format!("{}:{timestamp}:{value}", window.start_ns)
        })?
        .sink_named_with_config(
            "distinct-capture",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::AtLeastOnce,
                commit: true,
            },
            format!("distinct-window:{}", path.display()),
            move || {
                Ok(Box::new(GlobalFoldSink {
                    path: sink_path.clone(),
                    values: Vec::new(),
                }))
            },
        )
}

/// Choose a stable representative for each derived key and window.
pub fn representative_distinct_fixture(
    pipeline: &Pipeline,
    path: PathBuf,
) -> Result<Sink<String>, Error> {
    let sink_path = path.clone();
    pipeline
        .from_vec(
            "representative-input",
            vec![
                ("a2".to_owned(), 1_000_000_i64),
                ("a1".to_owned(), 2_000_000),
                ("b2".to_owned(), 3_000_000),
                ("b1".to_owned(), 4_000_000),
                ("a3".to_owned(), 11_000_000),
            ],
        )?
        .assign_timestamps_named("representative-timestamps", |row| row.1)?
        .map_named("representative-values", |row| row.0)?
        .fixed_windows("representative-windows", 10_000_000)?
        .distinct_by_per_window_named("representatives", |value| value[..1].to_owned())?
        .reify_timestamps_named("representative-output-time")?
        .with_window_named("representative-window-label")?
        .map_named("representative-row", |(window, (timestamp, value))| {
            format!("{}:{timestamp}:{value}", window.start_ns)
        })?
        .sink_named_with_config(
            "representative-capture",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::AtLeastOnce,
                commit: true,
            },
            format!("representative-distinct:{}", path.display()),
            move || {
                Ok(Box::new(GlobalFoldSink {
                    path: sink_path.clone(),
                    values: Vec::new(),
                }))
            },
        )
}

/// Read representatives from a watermarked source in two fixed windows.
pub fn streaming_representative_distinct_fixture(
    pipeline: &Pipeline,
    directory: PathBuf,
) -> Result<Sink<String>, Error> {
    let sink_directory = directory.clone();
    pipeline
        .test_stream_source(
            "representative-stream",
            vec![
                TestStreamEvent::Elements(vec![
                    ("a2".to_owned(), 1_000_000),
                    ("a1".to_owned(), 2_000_000),
                    ("b2".to_owned(), 3_000_000),
                    ("b1".to_owned(), 4_000_000),
                ]),
                TestStreamEvent::AdvanceWatermark(10_000_000),
                TestStreamEvent::Elements(vec![("a3".to_owned(), 11_000_000)]),
                TestStreamEvent::AdvanceWatermark(20_000_000),
            ],
        )?
        .fixed_windows("representative-windows", 10_000_000)?
        .distinct_by_per_window_named("representatives", |value| value[..1].to_owned())?
        .reify_timestamps_named("representative-output-time")?
        .with_window_named("representative-window-label")?
        .map_named("representative-row", |(window, (timestamp, value))| {
            format!("{}:{timestamp}:{value}", window.start_ns)
        })?
        .sink_named_with_config(
            "representative-capture",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            format!("representative-stream:{}", directory.display()),
            move || {
                Ok(Box::new(IdempotentRowSink {
                    directory: sink_directory.clone(),
                    pending: Vec::new(),
                }))
            },
        )
}

/// Count repeated values in each closed window of a watermarked source.
pub fn streaming_count_per_element_fixture(
    pipeline: &Pipeline,
    directory: PathBuf,
) -> Result<Sink<String>, Error> {
    let sink_directory = directory.clone();
    pipeline
        .test_stream_source(
            "count-stream",
            vec![
                TestStreamEvent::Elements(vec![
                    ("a".to_owned(), 1_000_000),
                    ("a".to_owned(), 2_000_000),
                    ("b".to_owned(), 3_000_000),
                ]),
                TestStreamEvent::AdvanceWatermark(10_000_000),
                TestStreamEvent::Elements(vec![("a".to_owned(), 11_000_000)]),
                TestStreamEvent::AdvanceWatermark(20_000_000),
            ],
        )?
        .fixed_windows("count-windows", 10_000_000)?
        .count_per_element_named("counts")?
        .reify_timestamps_named("count-output-time")?
        .with_window_named("count-window-label")?
        .map_named("count-row", |(window, (timestamp, (value, count)))| {
            format!("{}:{timestamp}:{value}:{count}", window.start_ns)
        })?
        .sink_named_with_config(
            "count-capture",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            format!("count-stream:{}", directory.display()),
            move || {
                Ok(Box::new(IdempotentRowSink {
                    directory: sink_directory.clone(),
                    pending: Vec::new(),
                }))
            },
        )
}

/// Count repeated values in an unbounded global window with an early pane.
pub fn streaming_global_count_per_element_fixture(
    pipeline: &Pipeline,
    directory: PathBuf,
) -> Result<Sink<String>, Error> {
    let sink_directory = directory.clone();
    pipeline
        .test_stream_source(
            "global-count-stream",
            vec![
                TestStreamEvent::Elements(vec![
                    ("a".to_owned(), 1_000_000),
                    ("a".to_owned(), 2_000_000),
                    ("b".to_owned(), 3_000_000),
                ]),
                TestStreamEvent::AdvanceWatermark(10_000_000),
                TestStreamEvent::Elements(vec![("a".to_owned(), 11_000_000)]),
            ],
        )?
        .early_count_trigger_named("global-count-trigger", 2)?
        .count_per_element_named("global-counts")?
        .reify_timestamps_named("global-count-output-time")?
        .reify_panes_named("global-count-pane")?
        .map_named("global-count-row", |(pane, (timestamp, (value, count)))| {
            format!(
                "{value}:{:?}:{}:{timestamp}:{count}",
                pane.timing, pane.index
            )
        })?
        .sink_named_with_config(
            "global-count-capture",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            format!("global-count-stream:{}", directory.display()),
            move || {
                Ok(Box::new(IdempotentRowSink {
                    directory: sink_directory.clone(),
                    pending: Vec::new(),
                }))
            },
        )
}

/// Keep one copy of each keyed value in each discarding global pane.
pub fn streaming_global_distinct_per_key_fixture(
    pipeline: &Pipeline,
    directory: PathBuf,
) -> Result<Sink<String>, Error> {
    let sink_directory = directory.clone();
    pipeline
        .test_stream_source(
            "global-distinct-stream",
            vec![
                TestStreamEvent::Elements(vec![
                    (("k".to_owned(), "a".to_owned()), 1_000_000),
                    (("k".to_owned(), "a".to_owned()), 2_000_000),
                    (("k".to_owned(), "b".to_owned()), 3_000_000),
                ]),
                TestStreamEvent::AdvanceWatermark(10_000_000),
                TestStreamEvent::Elements(vec![(("k".to_owned(), "a".to_owned()), 11_000_000)]),
            ],
        )?
        .early_count_trigger_named("global-distinct-trigger", 2)?
        .distinct_per_key_named("global-distinct-group", "global-distinct")?
        .reify_panes_named("global-distinct-pane")?
        .map_named("global-distinct-row", |(pane, (group, value))| {
            format!("{group}:{value}:{:?}:{}", pane.timing, pane.index)
        })?
        .sink_named_with_config(
            "global-distinct-capture",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            format!("global-distinct-stream:{}", directory.display()),
            move || {
                Ok(Box::new(IdempotentRowSink {
                    directory: sink_directory.clone(),
                    pending: Vec::new(),
                }))
            },
        )
}

pub fn streaming_distinct_window_fixture(
    pipeline: &Pipeline,
    directory: PathBuf,
) -> Result<Sink<String>, Error> {
    streaming_distinct_fixture(pipeline, directory, false)
}

pub fn late_distinct_window_fixture(
    pipeline: &Pipeline,
    directory: PathBuf,
) -> Result<Sink<String>, Error> {
    streaming_distinct_fixture(pipeline, directory, true)
}

fn streaming_distinct_fixture(
    pipeline: &Pipeline,
    directory: PathBuf,
    late: bool,
) -> Result<Sink<String>, Error> {
    let events = if late {
        vec![
            TestStreamEvent::Elements(vec![("a".to_owned(), 1_000_000)]),
            TestStreamEvent::AdvanceWatermark(10_000_000),
            TestStreamEvent::Elements(vec![
                ("a".to_owned(), 2_000_000),
                ("a".to_owned(), 3_000_000),
            ]),
            TestStreamEvent::AdvanceWatermark(15_000_000),
        ]
    } else {
        vec![
            TestStreamEvent::Elements(vec![
                ("a".to_owned(), 1_000_000),
                ("a".to_owned(), 2_000_000),
            ]),
            TestStreamEvent::Elements(vec![
                ("a".to_owned(), 3_000_000),
                ("a".to_owned(), 4_000_000),
            ]),
            TestStreamEvent::Elements(vec![("b".to_owned(), 5_000_000)]),
            TestStreamEvent::AdvanceWatermark(10_000_000),
        ]
    };
    let windowed = pipeline
        .test_stream_source("distinct-stream", events)?
        .fixed_windows("distinct-windows", 10_000_000)?;
    let triggered = if late {
        windowed
            .allowed_lateness_named("distinct-lateness", 5_000_000)?
            .late_count_trigger_named("distinct-trigger", 2)?
    } else {
        windowed.early_count_trigger_named("distinct-trigger", 2)?
    };
    let sink_directory = directory.clone();
    let annotated = triggered
        .distinct_per_window_named("distinct")?
        .reify_timestamps_named("distinct-time")?
        .reify_panes_named("distinct-pane")?;
    let rows = if late {
        annotated.flat_map_fallible_named("distinct-row", |(pane, (timestamp, value))| {
            if pane.timing == PaneTiming::Late
                && std::env::var_os("IRONBEAM_V5_PRISM_KILL_LATE_DISTINCT").is_some()
            {
                super::await_supervised_source_replacement(Some(&[1]))?;
            }
            Ok(vec![format!(
                "{value}:{timestamp}:{:?}:{}",
                pane.timing, pane.index
            )])
        })?
    } else {
        annotated.map_named("distinct-row", |(pane, (timestamp, value))| {
            format!("{value}:{timestamp}:{:?}:{}", pane.timing, pane.index)
        })?
    };
    rows.sink_named_with_config(
        "distinct-capture",
        SinkSpec {
            mode: SinkMode::Append,
            replay: Replay::Idempotent,
            commit: true,
        },
        format!("distinct-stream:{late}:{}", directory.display()),
        move || {
            Ok(Box::new(IdempotentRowSink {
                directory: sink_directory.clone(),
                pending: Vec::new(),
            }))
        },
    )
}
