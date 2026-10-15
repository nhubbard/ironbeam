use super::*;
use ironbeam_transforms::ElementwiseExt;

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
    triggered
        .distinct_per_window_named("distinct")?
        .reify_timestamps_named("distinct-time")?
        .reify_panes_named("distinct-pane")?
        .map_named("distinct-row", |(pane, (timestamp, value))| {
            format!("{value}:{timestamp}:{:?}:{}", pane.timing, pane.index)
        })?
        .sink_named_with_config(
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
