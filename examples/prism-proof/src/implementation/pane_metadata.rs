struct PaneCheckingSink(GlobalFoldSink<String>);

impl TypedSink<String> for PaneCheckingSink {
    fn write(&mut self, value: String) -> Result<(), Error> {
        self.0.write(value)
    }

    fn write_with_metadata(
        &mut self,
        value: String,
        metadata: ElementMetadata,
    ) -> Result<(), Error> {
        let pane = metadata.pane;
        let suffix = format!(
            ":{:?}:{}:{}:{}:{}",
            pane.timing, pane.is_first, pane.is_last, pane.index, pane.nonspeculative_index
        );
        if !value.ends_with(&suffix) {
            return Err(Error::execution(
                "pane-result",
                "runner",
                format!(
                    "sink pane metadata differs from reified element pane: value={value}, actual={pane:?}"
                ),
            ));
        }
        self.0.write(value)
    }

    fn commit(&mut self) -> Result<(), Error> {
        self.0.commit()
    }

    fn abort(&mut self) -> Result<(), Error> {
        self.0.abort()
    }
}

/// Compare source and grouped pane metadata through Direct and native Beam GBK.
pub fn pane_metadata_fixture(pipeline: &Pipeline, path: PathBuf) -> Result<Sink<String>, Error> {
    pane_metadata_fixture_for(pipeline, path, PaneFixtureWindow::Fixed)
}

#[derive(Clone, Copy, Debug)]
pub enum PaneFixtureWindow {
    Fixed,
    Sliding,
    Session,
}

pub fn pane_metadata_fixture_for(
    pipeline: &Pipeline,
    path: PathBuf,
    window: PaneFixtureWindow,
) -> Result<Sink<String>, Error> {
    pane_metadata_fixture_with_trigger(pipeline, path, window, false)
}

pub fn pane_metadata_fixture_with_trigger(
    pipeline: &Pipeline,
    path: PathBuf,
    window: PaneFixtureWindow,
    early_count_one: bool,
) -> Result<Sink<String>, Error> {
    pane_metadata_fixture_with_count_mode(pipeline, path, window, early_count_one.then_some(false))
}

pub fn pane_metadata_fixture_with_accumulating_count(
    pipeline: &Pipeline,
    path: PathBuf,
    window: PaneFixtureWindow,
) -> Result<Sink<String>, Error> {
    pane_metadata_fixture_with_count_mode(pipeline, path, window, Some(true))
}

fn pane_metadata_fixture_with_count_mode(
    pipeline: &Pipeline,
    path: PathBuf,
    window: PaneFixtureWindow,
    count_mode: Option<bool>,
) -> Result<Sink<String>, Error> {
    let values = pipeline
        .from_vec(
            "pane-events",
            vec![
                (1_000_000_i64, ("a".to_owned(), 2_i32)),
                (2_000_000, ("a".to_owned(), 3)),
            ],
        )?
        .timestamps_from_pairs_named("pane-event-time")?;
    let values = match window {
        PaneFixtureWindow::Fixed => values.fixed_windows("pane-windows", 10_000_000)?,
        PaneFixtureWindow::Sliding => {
            values.sliding_windows("pane-windows", 10_000_000, 5_000_000, 0)?
        }
        PaneFixtureWindow::Session => values.session_windows_named("pane-windows", 10_000_000)?,
    };
    let values = match count_mode {
        Some(true) => values.accumulating_count_trigger_named("pane-count-trigger", 1)?,
        Some(false) => values.early_count_trigger_named("pane-count-trigger", 1)?,
        None => values,
    };
    let raw = values.reify_panes_named("source-pane")?.map_named(
        "source-pane-row",
        |(pane, (key, value))| {
            format!(
                "source:{key}:{value}:{:?}:{}:{}:{}:{}",
                pane.timing, pane.is_first, pane.is_last, pane.index, pane.nonspeculative_index
            )
        },
    )?;
    let grouped = values
        .group_by_key_named("pane-group")?
        .reify_panes_named("group-pane")?
        .map_named("group-pane-row", |(pane, (key, mut values))| {
            values.sort_unstable();
            format!(
                "group:{key}:{values:?}:{:?}:{}:{}:{}:{}",
                pane.timing, pane.is_first, pane.is_last, pane.index, pane.nonspeculative_index
            )
        })?;
    let sink_path = path.clone();
    raw.flatten_named("pane-merge", &[grouped])?
        .sink_named_with_config(
            "pane-result",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::AtLeastOnce,
                commit: true,
            },
            format!("pane-result:{}", path.display()),
            move || {
                Ok(Box::new(PaneCheckingSink(GlobalFoldSink {
                    path: sink_path.clone(),
                    values: Vec::new(),
                })))
            },
        )
}

/// Characterize repeated count firings across separate TestStream advances.
pub fn count_trigger_stream_fixture(
    pipeline: &Pipeline,
    path: PathBuf,
) -> Result<Sink<String>, Error> {
    count_trigger_stream_fixture_with_mode(pipeline, path, false, false)
}

pub fn accumulating_count_trigger_stream_fixture(
    pipeline: &Pipeline,
    path: PathBuf,
) -> Result<Sink<String>, Error> {
    count_trigger_stream_fixture_with_mode(pipeline, path, true, false)
}

pub fn accumulating_latest_count_trigger_stream_fixture(
    pipeline: &Pipeline,
    path: PathBuf,
) -> Result<Sink<String>, Error> {
    count_trigger_stream_fixture_with_mode(pipeline, path, true, true)
}

/// Emit one on-time pane, one late pane, and drop an element after expiry.
pub fn allowed_lateness_fixture(pipeline: &Pipeline, path: PathBuf) -> Result<Sink<String>, Error> {
    let rows = pipeline
        .test_stream_source(
            "late-events",
            vec![
                TestStreamEvent::Elements(vec![(('a'.to_string(), 2_i32), 2_000_000)]),
                TestStreamEvent::AdvanceWatermark(10_000_000),
                TestStreamEvent::Elements(vec![(('a'.to_string(), 3_i32), 3_000_000)]),
                TestStreamEvent::AdvanceWatermark(15_000_000),
                TestStreamEvent::Elements(vec![(('a'.to_string(), 4_i32), 4_000_000)]),
                TestStreamEvent::AdvanceWatermark(20_000_000),
            ],
        )?
        .fixed_windows("late-windows", 10_000_000)?
        .allowed_lateness_named("late-policy", 5_000_000)?
        .group_by_key_named("late-group")?
        .reify_timestamps_named("late-timestamp")?
        .reify_panes_named("late-pane")?
        .map_named("late-row", |(pane, (timestamp, (key, values)))| {
            format!(
                "{key}:{values:?}:{timestamp}:{:?}:{}:{}:{}:{}",
                pane.timing, pane.is_first, pane.is_last, pane.index, pane.nonspeculative_index
            )
        })?;
    let sink_path = path.clone();
    rows.sink_named_with_config(
        "late-result",
        SinkSpec {
            mode: SinkMode::Append,
            replay: Replay::AtLeastOnce,
            commit: true,
        },
        format!("late-result:{}", path.display()),
        move || {
            Ok(Box::new(PaneCheckingSink(GlobalFoldSink {
                path: sink_path.clone(),
                values: Vec::new(),
            })))
        },
    )
}

/// Repeated late count firing with a partial pane at window expiry.
pub fn late_count_fixture(pipeline: &Pipeline, path: PathBuf) -> Result<Sink<String>, Error> {
    late_count_fixture_with_mode(pipeline, path, false)
}

pub fn accumulating_late_count_fixture(pipeline: &Pipeline, path: PathBuf) -> Result<Sink<String>, Error> {
    late_count_fixture_with_mode(pipeline, path, true)
}

fn late_count_fixture_with_mode(pipeline: &Pipeline, path: PathBuf, accumulating: bool) -> Result<Sink<String>, Error> {
    let windowed = pipeline
        .test_stream_source(
            "late-count-events",
            vec![
                TestStreamEvent::Elements(vec![(('a'.to_string(), 2_i32), 2_000_000)]),
                TestStreamEvent::AdvanceWatermark(10_000_000),
                TestStreamEvent::Elements(vec![(('a'.to_string(), 3_i32), 3_000_000)]),
                TestStreamEvent::AdvanceWatermark(11_000_000),
                TestStreamEvent::Elements(vec![(('a'.to_string(), 5_i32), 5_000_000)]),
                TestStreamEvent::AdvanceWatermark(12_000_000),
                TestStreamEvent::Elements(vec![(('a'.to_string(), 7_i32), 7_000_000)]),
                TestStreamEvent::AdvanceWatermark(15_000_000),
                TestStreamEvent::AdvanceWatermark(20_000_000),
            ],
        )?
        .fixed_windows("late-count-windows", 10_000_000)?
        .allowed_lateness_named("late-count-lateness", 5_000_000)?;
    let triggered = if accumulating {
        windowed.accumulating_late_count_trigger_named("late-count-policy", 2)?
    } else {
        windowed.late_count_trigger_named("late-count-policy", 2)?
    };
    let rows = triggered
        .group_by_key_named("late-count-group")?
        .reify_timestamps_named("late-count-timestamp")?
        .reify_panes_named("late-count-pane")?
        .map_named("late-count-row", |(pane, (timestamp, (key, values)))| {
            format!(
                "{key}:{values:?}:{timestamp}:{:?}:{}:{}:{}:{}",
                pane.timing, pane.is_first, pane.is_last, pane.index, pane.nonspeculative_index
            )
        })?;
    let sink_path = path.clone();
    rows.sink_named_with_config(
        "late-count-result",
        SinkSpec { mode: SinkMode::Append, replay: Replay::AtLeastOnce, commit: true },
        format!("late-count-result:{}", path.display()),
        move || Ok(Box::new(PaneCheckingSink(GlobalFoldSink {
            path: sink_path.clone(), values: Vec::new(),
        }))),
    )
}

pub fn sliding_allowed_lateness_fixture(
    pipeline: &Pipeline,
    path: PathBuf,
) -> Result<Sink<String>, Error> {
    let rows = pipeline
        .test_stream_source(
            "sliding-late-events",
            vec![
                TestStreamEvent::Elements(vec![(('a'.to_string(), 2_i32), 6_000_000)]),
                TestStreamEvent::AdvanceWatermark(10_000_000),
                TestStreamEvent::Elements(vec![(('a'.to_string(), 3_i32), 7_000_000)]),
                TestStreamEvent::AdvanceWatermark(12_000_000),
                TestStreamEvent::AdvanceWatermark(15_000_000),
                TestStreamEvent::Elements(vec![(('a'.to_string(), 4_i32), 8_000_000)]),
                TestStreamEvent::AdvanceWatermark(17_000_000),
                TestStreamEvent::AdvanceWatermark(20_000_000),
            ],
        )?
        .sliding_windows("sliding-late-windows", 10_000_000, 5_000_000, 0)?
        .allowed_lateness_named("sliding-late-policy", 5_000_000)?
        .group_by_key_named("sliding-late-group")?
        .reify_timestamps_named("sliding-late-timestamp")?
        .reify_panes_named("sliding-late-pane")?
        .with_window_named("sliding-late-interval")?
        .map_named("sliding-late-row", |(interval, (pane, (timestamp, (key, values))))| {
            format!(
                "{}..{}:{key}:{values:?}:{timestamp}:{:?}:{}:{}:{}:{}",
                interval.start_ns,
                interval.end_ns,
                pane.timing,
                pane.is_first,
                pane.is_last,
                pane.index,
                pane.nonspeculative_index
            )
        })?;
    let sink_path = path.clone();
    rows.sink_named_with_config(
        "sliding-late-result",
        SinkSpec {
            mode: SinkMode::Append,
            replay: Replay::AtLeastOnce,
            commit: true,
        },
        format!("sliding-late-result:{}", path.display()),
        move || {
            Ok(Box::new(PaneCheckingSink(GlobalFoldSink {
                path: sink_path.clone(),
                values: Vec::new(),
            })))
        },
    )
}

fn count_trigger_stream_fixture_with_mode(
    pipeline: &Pipeline,
    path: PathBuf,
    accumulating: bool,
    latest_time: bool,
) -> Result<Sink<String>, Error> {
    let values = pipeline
        .test_stream_source(
            "count-events",
            vec![
                TestStreamEvent::Elements(vec![(('a'.to_string(), 2_i32), 1_000_000)]),
                TestStreamEvent::AdvanceWatermark(2_000_000),
                TestStreamEvent::Elements(vec![(('a'.to_string(), 3_i32), 3_000_000)]),
                TestStreamEvent::AdvanceWatermark(10_000_000),
            ],
        )?
        .fixed_windows("count-windows", 10_000_000)?;
    let values = if latest_time {
        values.latest_in_pane_output_time_named("count-output-time")?
    } else {
        values
    };
    let values = if accumulating {
        values.accumulating_count_trigger_named("count-trigger", 1)?
    } else {
        values.early_count_trigger_named("count-trigger", 1)?
    };
    let rows = values
        .group_by_key_named("count-group")?
        .reify_timestamps_named("count-timestamp")?
        .reify_panes_named("count-pane")?
        .map_named("count-row", move |(pane, (timestamp, (key, mut values)))| {
            values.sort_unstable();
            let timestamp = if latest_time {
                format!(":{timestamp}")
            } else {
                String::new()
            };
            format!(
                "group:{key}:{values:?}{timestamp}:{:?}:{}:{}:{}:{}",
                pane.timing, pane.is_first, pane.is_last, pane.index, pane.nonspeculative_index
            )
        })?;
    let sink_path = path.clone();
    rows.sink_named_with_config(
        "count-result",
        SinkSpec {
            mode: SinkMode::Append,
            replay: Replay::AtLeastOnce,
            commit: true,
        },
        format!("count-result:{}", path.display()),
        move || {
            Ok(Box::new(PaneCheckingSink(GlobalFoldSink {
                path: sink_path.clone(),
                values: Vec::new(),
            })))
        },
    )
}
