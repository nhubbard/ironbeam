struct PaneCheckingSink(GlobalFoldSink<String>);

pub fn processing_time_trigger_collection(
    pipeline: &Pipeline,
    accumulating: bool,
) -> Result<PCollection<String>, Error> {
    let values = pipeline
        .test_stream_source(
            "processing-events",
            vec![
                TestStreamEvent::Elements(vec![(('a'.to_string(), 2_i32), 1_000_000)]),
                TestStreamEvent::AdvanceProcessingTime(5_000_000),
                TestStreamEvent::Elements(vec![(('a'.to_string(), 3_i32), 2_000_000)]),
                TestStreamEvent::AdvanceProcessingTime(5_000_000),
                TestStreamEvent::AdvanceWatermark(10_000_000),
            ],
        )?
        .fixed_windows("processing-windows", 10_000_000)?;
    let values = if accumulating {
        values.accumulating_processing_time_trigger_named("processing-trigger", 5_000_000)?
    } else {
        values.early_processing_time_trigger_named("processing-trigger", 5_000_000)?
    };
    values
        .group_by_key_named("processing-group")?
        .reify_panes_named("processing-pane")?
        .map_named("processing-row", |(pane, (key, values))| {
            format!(
                "group:{key}:{values:?}:{:?}:{}:{}:{}:{}",
                pane.timing,
                pane.is_first,
                pane.is_last,
                pane.index,
                pane.nonspeculative_index
            )
        })
}

pub fn processing_time_trigger_fixture(
    pipeline: &Pipeline,
    path: PathBuf,
    accumulating: bool,
) -> Result<Sink<String>, Error> {
    let sink_path = path.clone();
    processing_time_trigger_collection(pipeline, accumulating)?.sink_named_with_config(
        "processing-result",
        SinkSpec {
            mode: SinkMode::Append,
            replay: Replay::AtLeastOnce,
            commit: true,
        },
        format!("processing-result:{}:{accumulating}", path.display()),
        move || {
            Ok(Box::new(PaneCheckingSink(GlobalFoldSink {
                path: sink_path.clone(),
                values: Vec::new(),
            })))
        },
    )
}

pub fn dual_processing_time_cogroup_collection(
    pipeline: &Pipeline,
    accumulating: bool,
) -> Result<PCollection<String>, Error> {
    let make_source = |key: &str, value| {
        let source = pipeline
            .test_stream_source(
                key,
                vec![
                    TestStreamEvent::Elements(vec![((1_i32, value), 1_000_000)]),
                    TestStreamEvent::AdvanceWatermark(0),
                    TestStreamEvent::AdvanceProcessingTime(5_000_000),
                    TestStreamEvent::AdvanceWatermark(10_000_000),
                ],
            )?
            .fixed_windows(&format!("{key}-window"), 10_000_000)?;
        if accumulating {
            source.accumulating_processing_time_trigger_named(
                &format!("{key}-trigger"),
                5_000_000,
            )
        } else {
            source.early_processing_time_trigger_named(&format!("{key}-trigger"), 5_000_000)
        }
    };
    make_source("dual-processing-first", 2)?
        .co_group_homogeneous_named(
            "dual-processing-group",
            &[make_source("dual-processing-second", 4)?],
        )?
        .reify_panes_named("dual-processing-pane")?
        .map_named("dual-processing-row", |(pane, (key, values))| {
            format!(
                "group:{key}:{values:?}:{:?}:{}:{}:{}:{}",
                pane.timing,
                pane.is_first,
                pane.is_last,
                pane.index,
                pane.nonspeculative_index
            )
        })
}

pub fn dual_processing_time_cogroup_fixture(
    pipeline: &Pipeline,
    path: PathBuf,
    accumulating: bool,
) -> Result<Sink<String>, Error> {
    let sink_path = path.clone();
    dual_processing_time_cogroup_collection(pipeline, accumulating)?.sink_named_with_config(
        "dual-processing-result",
        SinkSpec {
            mode: SinkMode::Append,
            replay: Replay::AtLeastOnce,
            commit: true,
        },
        format!("dual-processing-result:{}:{accumulating}", path.display()),
        move || {
            Ok(Box::new(PaneCheckingSink(GlobalFoldSink {
                path: sink_path.clone(),
                values: Vec::new(),
            })))
        },
    )
}

pub fn dual_count_or_time_cogroup_collection(
    pipeline: &Pipeline,
) -> Result<PCollection<String>, Error> {
    let make_source = |key: &str, value, first: bool| {
        let mut events = vec![TestStreamEvent::Elements(vec![((1_i32, value), 1_000_000)])];
        events.push(TestStreamEvent::AdvanceWatermark(0));
        if first {
            events.push(TestStreamEvent::Elements(vec![((1_i32, 3_i32), 2_000_000)]));
        }
        events.push(TestStreamEvent::AdvanceProcessingTime(5_000_000));
        events.push(TestStreamEvent::AdvanceProcessingTime(5_000_000));
        events.push(TestStreamEvent::AdvanceWatermark(10_000_000));
        pipeline
            .test_stream_source(key, events)?
            .fixed_windows(&format!("{key}-window"), 10_000_000)?
            .accumulating_count_or_processing_time_trigger_named(
                &format!("{key}-trigger"),
                2,
                5_000_000,
            )
    };
    make_source("dual-count-time-first", 2, true)?
        .co_group_homogeneous_named(
            "dual-count-time-group",
            &[make_source("dual-count-time-second", 4, false)?],
        )?
        .reify_panes_named("dual-count-time-pane")?
        .map_named("dual-count-time-row", |(pane, (key, values))| {
            format!("{key}:{values:?}:{:?}:{}", pane.timing, pane.index)
        })
}

pub fn dual_count_or_time_cogroup_fixture(
    pipeline: &Pipeline,
    path: PathBuf,
) -> Result<Sink<String>, Error> {
    let sink_path = path.clone();
    dual_count_or_time_cogroup_collection(pipeline)?.sink_named_with_config(
        "dual-count-time-capture",
        SinkSpec {
            mode: SinkMode::Append,
            replay: Replay::AtLeastOnce,
            commit: true,
        },
        format!("dual-count-time:{}", path.display()),
        move || {
            Ok(Box::new(GlobalFoldSink {
                path: sink_path.clone(),
                values: Vec::new(),
            }))
        },
    )
}

/// Two unbounded keyed sources keep a window for a late co-group pane.
pub fn dual_late_cogroup_collection(pipeline: &Pipeline) -> Result<PCollection<String>, Error> {
    dual_late_cogroup_collection_with_trigger(pipeline, false)
}

pub fn dual_late_count_cogroup_collection(pipeline: &Pipeline) -> Result<PCollection<String>, Error> {
    dual_late_cogroup_collection_with_trigger(pipeline, true)
}

fn dual_late_cogroup_collection_with_trigger(
    pipeline: &Pipeline,
    count_trigger: bool,
) -> Result<PCollection<String>, Error> {
    let late_events = if count_trigger {
        vec![
            TestStreamEvent::AdvanceWatermark(11_000_000),
            TestStreamEvent::Elements(vec![((1, 5), 5_000_000)]),
            TestStreamEvent::AdvanceWatermark(12_000_000),
            TestStreamEvent::Elements(vec![((1, 7), 7_000_000)]),
            TestStreamEvent::AdvanceWatermark(15_000_000),
        ]
    } else {
        vec![
            TestStreamEvent::AdvanceWatermark(15_000_000),
            TestStreamEvent::Elements(vec![((1, 5), 5_000_000)]),
        ]
    };
    let mut first_events = vec![
        TestStreamEvent::Elements(vec![((1_i32, 2_i32), 2_000_000)]),
        TestStreamEvent::AdvanceWatermark(10_000_000),
        TestStreamEvent::Elements(vec![((1, 3), 3_000_000)]),
    ];
    first_events.extend(late_events);
    let first = pipeline
        .test_stream_source(
            "dual-late-first",
            first_events,
        )?
        .fixed_windows("dual-late-first-window", 10_000_000)?
        .allowed_lateness_named("dual-late-first-policy", 5_000_000)?;
    let first = if count_trigger {
        first.late_count_trigger_named("dual-late-first-trigger", 2)?
    } else {
        first
    };
    let mut second_events = vec![
        TestStreamEvent::Elements(vec![((1_i32, 4_i32), 4_000_000)]),
        TestStreamEvent::AdvanceWatermark(10_000_000),
    ];
    if !count_trigger {
        second_events.push(TestStreamEvent::AdvanceWatermark(15_000_000));
    }
    let second = pipeline
        .test_stream_source(
            "dual-late-second",
            second_events,
        )?
        .fixed_windows("dual-late-second-window", 10_000_000)?
        .allowed_lateness_named("dual-late-second-policy", 5_000_000)?;
    let second = if count_trigger {
        second.late_count_trigger_named("dual-late-second-trigger", 2)?
    } else {
        second
    };
    first
        .co_group_homogeneous_named("dual-late-group", &[second])?
        .reify_panes_named("dual-late-pane")?
        .map_named("dual-late-row", |(pane, (key, buckets))| {
            format!(
                "{key}:{buckets:?}:{:?}:{}:{}:{}:{}",
                pane.timing,
                pane.is_first,
                pane.is_last,
                pane.index,
                pane.nonspeculative_index
            )
        })
}

pub fn dual_late_cogroup_fixture(pipeline: &Pipeline, path: PathBuf) -> Result<Sink<String>, Error> {
    dual_late_fixture(pipeline, path, false)
}

pub fn dual_late_count_cogroup_fixture(pipeline: &Pipeline, path: PathBuf) -> Result<Sink<String>, Error> {
    dual_late_fixture(pipeline, path, true)
}

fn dual_late_fixture(pipeline: &Pipeline, path: PathBuf, count_trigger: bool) -> Result<Sink<String>, Error> {
    let sink_path = path.clone();
    dual_late_cogroup_collection_with_trigger(pipeline, count_trigger)?.sink_named_with_config(
        "dual-late-result",
        SinkSpec {
            mode: SinkMode::Append,
            replay: Replay::AtLeastOnce,
            commit: true,
        },
        format!("dual-late-result:{}", path.display()),
        move || {
            Ok(Box::new(PaneCheckingSink(GlobalFoldSink {
                path: sink_path.clone(),
                values: Vec::new(),
            })))
        },
    )
}

fn global_count_values(pipeline: &Pipeline) -> Result<PCollection<(String, i32)>, Error> {
    pipeline.test_stream_source(
        "global-count-source",
        vec![
            TestStreamEvent::Elements(vec![
                (("a".to_owned(), 2_i32), 2_000_000),
                (("a".to_owned(), 3), 3_000_000),
            ]),
            TestStreamEvent::AdvanceProcessingTime(1_000_000),
            TestStreamEvent::Elements(vec![(
                ("a".to_owned(), 4),
                4_000_000,
            )]),
        ],
    )
}

/// An unbounded keyed batch emits at the count threshold and flushes its tail.
pub fn global_keyed_batches_collection(pipeline: &Pipeline) -> Result<PCollection<String>, Error> {
    keyed_batches_collection(pipeline, false)
}

pub fn timed_keyed_batches_collection(pipeline: &Pipeline) -> Result<PCollection<String>, Error> {
    pipeline
        .test_stream_source(
            "timed-batches-source",
            vec![
                TestStreamEvent::Elements(vec![(('a'.to_string(), 2_i32), 1_000_000)]),
                TestStreamEvent::AdvanceWatermark(0),
                TestStreamEvent::AdvanceProcessingTime(5_000_000),
                TestStreamEvent::Elements(vec![
                    (('a'.to_string(), 3_i32), 2_000_000),
                    (('a'.to_string(), 4_i32), 3_000_000),
                ]),
                TestStreamEvent::AdvanceProcessingTime(5_000_000),
                TestStreamEvent::AdvanceWatermark(10_000_000),
            ],
        )?
        .fixed_windows("timed-batches-window", 10_000_000)?
        .group_into_batches_with_max_buffering_duration_named("timed-batches", 2, 5_000_000)?
        .reify_panes_named("timed-batches-pane")?
        .map_named("timed-batches-row", |(pane, (key, values))| {
            format!(
                "{key}:{values:?}:{:?}:{}:{}:{}:{}",
                pane.timing, pane.is_first, pane.is_last, pane.index, pane.nonspeculative_index
            )
        })
}

pub fn timed_keyed_batches_fixture(
    pipeline: &Pipeline,
    path: PathBuf,
) -> Result<Sink<String>, Error> {
    let sink_path = path.clone();
    timed_keyed_batches_collection(pipeline)?.sink_named_with_config(
        "timed-batches-capture",
        SinkSpec {
            mode: SinkMode::Append,
            replay: Replay::AtLeastOnce,
            commit: true,
        },
        format!("timed-batches:{}", path.display()),
        move || {
            Ok(Box::new(PaneCheckingSink(GlobalFoldSink {
                path: sink_path.clone(),
                values: Vec::new(),
            })))
        },
    )
}

pub fn windowed_keyed_batches_collection(pipeline: &Pipeline) -> Result<PCollection<String>, Error> {
    keyed_batches_collection(pipeline, true)
}

fn keyed_batches_collection(pipeline: &Pipeline, windowed: bool) -> Result<PCollection<String>, Error> {
    let source = if windowed {
        pipeline.test_stream_source(
            "global-count-source",
            vec![
                TestStreamEvent::Elements(vec![
                    (("a".to_owned(), 2_i32), 2_000_000),
                    (("a".to_owned(), 3), 3_000_000),
                ]),
                TestStreamEvent::AdvanceProcessingTime(1_000_000),
                TestStreamEvent::Elements(vec![(("a".to_owned(), 4), 4_000_000)]),
                TestStreamEvent::AdvanceWatermark(10_000_000),
            ],
        )?.fixed_windows("global-batches-window", 10_000_000)?
    } else {
        global_count_values(pipeline)?
    };
    source
        .group_into_batches_named("global-batches", 2)?
        .reify_panes_named("global-batches-pane")?
        .map_named("global-batches-row", |(pane, (key, values))| {
            format!(
                "{key}:{values:?}:{:?}:{}:{}:{}:{}",
                pane.timing,
                pane.is_first,
                pane.is_last,
                pane.index,
                pane.nonspeculative_index
            )
        })
}

pub fn global_keyed_batches_fixture(
    pipeline: &Pipeline,
    path: PathBuf,
) -> Result<Sink<String>, Error> {
    keyed_batches_fixture(pipeline, path, false)
}

pub fn windowed_keyed_batches_fixture(
    pipeline: &Pipeline,
    path: PathBuf,
) -> Result<Sink<String>, Error> {
    keyed_batches_fixture(pipeline, path, true)
}

fn keyed_batches_fixture(pipeline: &Pipeline, path: PathBuf, windowed: bool) -> Result<Sink<String>, Error> {
    let sink_path = path.clone();
    keyed_batches_collection(pipeline, windowed)?.sink_named_with_config(
        "global-batches-capture",
        SinkSpec {
            mode: SinkMode::Append,
            replay: Replay::AtLeastOnce,
            commit: true,
        },
        format!("global-batches:{}", path.display()),
        move || {
            Ok(Box::new(PaneCheckingSink(GlobalFoldSink {
                path: sink_path.clone(),
                values: Vec::new(),
            })))
        },
    )
}

/// Repeated count firings in the global window, followed by a final pane.
pub fn global_count_collection(
    pipeline: &Pipeline,
    accumulating: bool,
) -> Result<PCollection<String>, Error> {
    let values = global_count_values(pipeline)?;
    let values = if accumulating {
        values.accumulating_count_trigger_named("global-count-trigger", 2)?
    } else {
        values.early_count_trigger_named("global-count-trigger", 2)?
    };
    values
        .group_by_key_named("global-count-group")?
        .reify_panes_named("global-count-pane")?
        .map_named("global-count-row", |(pane, (key, values))| {
            format!(
                "{key}:{values:?}:{:?}:{}:{}:{}:{}",
                pane.timing,
                pane.is_first,
                pane.is_last,
                pane.index,
                pane.nonspeculative_index
            )
        })
}

/// A keyed combine uses each count-triggered global group pane.
pub fn global_keyed_combine_collection(
    pipeline: &Pipeline,
    accumulating: bool,
) -> Result<PCollection<String>, Error> {
    let values = global_count_values(pipeline)?;
    let values = if accumulating {
        values.accumulating_count_trigger_named("global-count-trigger", 2)?
    } else {
        values.early_count_trigger_named("global-count-trigger", 2)?
    };
    values
        .combine_values_lifted_named("global-count-combine", ProductionSum)?
        .reify_panes_named("global-count-pane")?
        .map_named("global-count-row", |(pane, (key, sum))| {
            format!(
                "{key}:{sum}:{:?}:{}:{}:{}:{}",
                pane.timing,
                pane.is_first,
                pane.is_last,
                pane.index,
                pane.nonspeculative_index
            )
        })
}

pub fn global_keyed_combine_fixture(
    pipeline: &Pipeline,
    path: PathBuf,
    accumulating: bool,
) -> Result<Sink<String>, Error> {
    let sink_path = path.clone();
    global_keyed_combine_collection(pipeline, accumulating)?.sink_named_with_config(
        "global-count-capture",
        SinkSpec {
            mode: SinkMode::Append,
            replay: Replay::AtLeastOnce,
            commit: true,
        },
        format!("global-count:combine:{}", path.display()),
        move || {
            Ok(Box::new(PaneCheckingSink(GlobalFoldSink {
                path: sink_path.clone(),
                values: Vec::new(),
            })))
        },
    )
}

pub fn global_count_fixture(
    pipeline: &Pipeline,
    path: PathBuf,
    accumulating: bool,
) -> Result<Sink<String>, Error> {
    let sink_path = path.clone();
    global_count_collection(pipeline, accumulating)?.sink_named_with_config(
        "global-count-capture",
        SinkSpec {
            mode: SinkMode::Append,
            replay: Replay::AtLeastOnce,
            commit: true,
        },
        format!("global-count:{}", path.display()),
        move || {
            Ok(Box::new(PaneCheckingSink(GlobalFoldSink {
                path: sink_path.clone(),
                values: Vec::new(),
            })))
        },
    )
}

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
    late_count_fixture_with_mode(pipeline, path, LateCountFixtureMode::Discarding)
}

pub fn accumulating_late_count_fixture(pipeline: &Pipeline, path: PathBuf) -> Result<Sink<String>, Error> {
    late_count_fixture_with_mode(pipeline, path, LateCountFixtureMode::Accumulating)
}

pub fn combined_count_fixture(pipeline: &Pipeline, path: PathBuf) -> Result<Sink<String>, Error> {
    late_count_fixture_with_mode(pipeline, path, LateCountFixtureMode::Combined)
}

pub fn accumulating_combined_count_fixture(
    pipeline: &Pipeline,
    path: PathBuf,
) -> Result<Sink<String>, Error> {
    late_count_fixture_with_mode(pipeline, path, LateCountFixtureMode::CombinedAccumulating)
}

#[derive(Clone, Copy)]
enum LateCountFixtureMode {
    Discarding,
    Accumulating,
    Combined,
    CombinedAccumulating,
}

fn late_count_fixture_with_mode(pipeline: &Pipeline, path: PathBuf, mode: LateCountFixtureMode) -> Result<Sink<String>, Error> {
    let ordinary_events = vec![
        TestStreamEvent::Elements(vec![(('a'.to_string(), 2_i32), 2_000_000)]),
        TestStreamEvent::AdvanceWatermark(10_000_000),
        TestStreamEvent::Elements(vec![(('a'.to_string(), 3_i32), 3_000_000)]),
        TestStreamEvent::AdvanceWatermark(11_000_000),
        TestStreamEvent::Elements(vec![(('a'.to_string(), 5_i32), 5_000_000)]),
        TestStreamEvent::AdvanceWatermark(12_000_000),
        TestStreamEvent::Elements(vec![(('a'.to_string(), 7_i32), 7_000_000)]),
        TestStreamEvent::AdvanceWatermark(15_000_000),
        TestStreamEvent::AdvanceWatermark(20_000_000),
    ];
    let events = if matches!(mode, LateCountFixtureMode::CombinedAccumulating) {
        vec![
            TestStreamEvent::Elements(vec![
                (('a'.to_string(), 2_i32), 2_000_000),
                (('a'.to_string(), 3_i32), 3_000_000),
            ]),
            TestStreamEvent::AdvanceWatermark(10_000_000),
            TestStreamEvent::Elements(vec![
                (('a'.to_string(), 5_i32), 5_000_000),
                (('a'.to_string(), 7_i32), 7_000_000),
            ]),
            TestStreamEvent::AdvanceWatermark(15_000_000),
        ]
    } else if matches!(mode, LateCountFixtureMode::Combined) {
        vec![
            TestStreamEvent::Elements(vec![
                (('a'.to_string(), 2_i32), 2_000_000),
                (('a'.to_string(), 3_i32), 3_000_000),
            ]),
            TestStreamEvent::AdvanceWatermark(4_000_000),
            TestStreamEvent::Elements(vec![(('a'.to_string(), 4_i32), 4_000_000)]),
            TestStreamEvent::AdvanceWatermark(10_000_000),
            TestStreamEvent::Elements(vec![(('a'.to_string(), 5_i32), 5_000_000)]),
            TestStreamEvent::AdvanceWatermark(11_000_000),
            TestStreamEvent::Elements(vec![(('a'.to_string(), 7_i32), 7_000_000)]),
            TestStreamEvent::AdvanceWatermark(12_000_000),
            TestStreamEvent::Elements(vec![(('a'.to_string(), 11_i32), 8_000_000)]),
            TestStreamEvent::AdvanceWatermark(15_000_000),
        ]
    } else {
        ordinary_events
    };
    let windowed = pipeline
        .test_stream_source(
            "late-count-events",
            events,
        )?
        .fixed_windows("late-count-windows", 10_000_000)?
        .allowed_lateness_named("late-count-lateness", 5_000_000)?;
    let triggered = match mode {
        LateCountFixtureMode::Discarding => windowed.late_count_trigger_named("late-count-policy", 2)?,
        LateCountFixtureMode::Accumulating => windowed.accumulating_late_count_trigger_named("late-count-policy", 2)?,
        LateCountFixtureMode::Combined => windowed.early_and_late_count_trigger_named("late-count-policy", 2, 2)?,
        LateCountFixtureMode::CombinedAccumulating => windowed.accumulating_early_and_late_count_trigger_named("late-count-policy", 2, 2)?,
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
        SinkSpec {
            mode: SinkMode::Append,
            replay: if matches!(mode, LateCountFixtureMode::CombinedAccumulating) {
                Replay::Idempotent
            } else {
                Replay::AtLeastOnce
            },
            commit: true,
        },
        format!("late-count-result:{}", path.display()),
        move || {
            if matches!(mode, LateCountFixtureMode::CombinedAccumulating) {
                Ok(Box::new(IdempotentRowSink {
                    directory: sink_path.clone(),
                    pending: Vec::new(),
                }) as Box<dyn TypedSink<String>>)
            } else {
                Ok(Box::new(PaneCheckingSink(GlobalFoldSink {
                    path: sink_path.clone(), values: Vec::new(),
                })) as Box<dyn TypedSink<String>>)
            }
        },
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
