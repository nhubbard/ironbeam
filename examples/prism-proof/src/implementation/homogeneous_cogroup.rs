/// Twelve bounded sources exercise co-group beyond the fixed typed arity.
pub fn homogeneous_twelve_way_cogroup_collection(
    pipeline: &Pipeline,
) -> Result<PCollection<String>, Error> {
    let inputs = (0..12)
        .map(|index| {
            let mut rows = if index == 4 {
                Vec::new()
            } else {
                vec![(1_000_000_i64, ("a".to_owned(), index))]
            };
            if index == 0 {
                rows.push((2_000_000, ("a".to_owned(), 100)));
            }
            if index == 11 {
                rows.push((0, ("z".to_owned(), 99)));
            }
            pipeline
                .from_vec(&format!("homogeneous-input-{index}"), rows)?
                .timestamps_from_pairs_named(&format!("homogeneous-time-{index}"))?
                .fixed_windows(&format!("homogeneous-window-{index}"), 10_000_000)
        })
        .collect::<Result<Vec<_>, Error>>()?;
    inputs[0]
        .co_group_homogeneous_named("homogeneous-group", &inputs[1..])?
        .reify_timestamps_named("homogeneous-result-time")?
        .with_window_named("homogeneous-result-window")?
        .map_named("homogeneous-result-row", |(window, (time, (key, mut buckets)))| {
            for bucket in &mut buckets {
                bucket.sort_unstable();
            }
            format!(
                "{}..{}:{time}:{key}:{buckets:?}",
                window.start_ns, window.end_ns
            )
        })
}

pub fn homogeneous_twelve_way_cogroup_fixture(
    pipeline: &Pipeline,
    path: PathBuf,
) -> Result<Sink<String>, Error> {
    let sink_path = path.clone();
    homogeneous_twelve_way_cogroup_collection(pipeline)?.sink_named_with_config(
        "homogeneous-result",
        SinkSpec {
            mode: SinkMode::Append,
            replay: Replay::AtLeastOnce,
            commit: true,
        },
        format!("homogeneous-result:{}", path.display()),
        move || {
            Ok(Box::new(GlobalFoldSink {
                path: sink_path.clone(),
                values: Vec::new(),
            }))
        },
    )
}

/// A bridge from the last branch merges two sessions from the first branch.
pub fn homogeneous_twelve_way_session_collection(
    pipeline: &Pipeline,
) -> Result<PCollection<String>, Error> {
    let source = pipeline
        .from_vec(
            "homogeneous-session-source",
            vec![
                (0_i64, ("a".to_owned(), (0_i32, 10_i32))),
                (4_000_000, ("a".to_owned(), (11, 40))),
                (8_000_000, ("a".to_owned(), (0, 80))),
            ],
        )?
        .timestamps_from_pairs_named("homogeneous-session-time")?
        .session_windows_named("homogeneous-session-windows", 5_000_000)?;
    let inputs = (0..12)
        .map(|index| {
            source
                .filter_named(
                    &format!("homogeneous-session-branch-{index}"),
                    move |(_, (branch, _))| *branch == index,
                )?
                .map_named(&format!("homogeneous-session-value-{index}"), |(key, (_, value))| {
                    (key, value)
                })
        })
        .collect::<Result<Vec<_>, Error>>()?;
    inputs[0]
        .co_group_homogeneous_named("homogeneous-session-group", &inputs[1..])?
        .with_window_named("homogeneous-session-result-window")?
        .map_named("homogeneous-session-result-row", |(window, (key, mut buckets))| {
            for bucket in &mut buckets {
                bucket.sort_unstable();
            }
            format!("{}..{}:{key}:{buckets:?}", window.start_ns, window.end_ns)
        })
}

pub fn homogeneous_twelve_way_session_fixture(
    pipeline: &Pipeline,
    path: PathBuf,
) -> Result<Sink<String>, Error> {
    let sink_path = path.clone();
    homogeneous_twelve_way_session_collection(pipeline)?.sink_named_with_config(
        "homogeneous-session-result",
        SinkSpec {
            mode: SinkMode::Append,
            replay: Replay::AtLeastOnce,
            commit: true,
        },
        format!("homogeneous-session-result:{}", path.display()),
        move || {
            Ok(Box::new(GlobalFoldSink {
                path: sink_path.clone(),
                values: Vec::new(),
            }))
        },
    )
}

/// Three branches from one unbounded source share its upstream path.
pub fn shared_unbounded_homogeneous_cogroup_collection(
    pipeline: &Pipeline,
) -> Result<PCollection<String>, Error> {
    let source = pipeline
        .test_stream_source(
            "shared-homogeneous-source",
            vec![
                TestStreamEvent::Elements(vec![
                    (("a".to_owned(), (0_i32, 10_i32)), 1_000_000),
                    (("a".to_owned(), (1, 20)), 2_000_000),
                    (("a".to_owned(), (2, 30)), 3_000_000),
                ]),
                TestStreamEvent::AdvanceWatermark(10_000_000),
                TestStreamEvent::Elements(vec![(("late".to_owned(), (2, 99)), 0)]),
            ],
        )?
        .fixed_windows("shared-homogeneous-window", 10_000_000)?;
    let inputs = (0..3)
        .map(|index| {
            source
                .filter_named(
                    &format!("shared-homogeneous-branch-{index}"),
                    move |(_, (branch, _))| *branch == index,
                )?
                .map_named(&format!("shared-homogeneous-value-{index}"), |(key, (_, value))| {
                    (key, value)
                })
        })
        .collect::<Result<Vec<_>, Error>>()?;
    inputs[0]
        .co_group_homogeneous_named("shared-homogeneous-group", &inputs[1..])?
        .reify_timestamps_named("shared-homogeneous-time")?
        .with_window_named("shared-homogeneous-output-window")?
        .map_named("shared-homogeneous-row", |(window, (time, (key, mut buckets)))| {
            for bucket in &mut buckets {
                bucket.sort_unstable();
            }
            format!("{}..{}:{time}:{key}:{buckets:?}", window.start_ns, window.end_ns)
        })
}

pub fn shared_unbounded_homogeneous_cogroup_fixture(
    pipeline: &Pipeline,
    path: PathBuf,
) -> Result<Sink<String>, Error> {
    let sink_path = path.clone();
    shared_unbounded_homogeneous_cogroup_collection(pipeline)?.sink_named_with_config(
        "shared-homogeneous-result",
        SinkSpec {
            mode: SinkMode::Append,
            replay: Replay::AtLeastOnce,
            commit: true,
        },
        format!("shared-homogeneous-result:{}", path.display()),
        move || {
            Ok(Box::new(GlobalFoldSink {
                path: sink_path.clone(),
                values: Vec::new(),
            }))
        },
    )
}

/// A single TestStream branches into three inputs with retained late values.
pub fn shared_accumulating_late_cogroup_collection(
    pipeline: &Pipeline,
) -> Result<PCollection<String>, Error> {
    let source = pipeline
        .test_stream_source(
            "shared-accumulating-source",
            vec![
                TestStreamEvent::Elements(vec![
                    (("a".to_owned(), (0_i32, 2_i32)), 2_000_000),
                    (("a".to_owned(), (1, 4)), 4_000_000),
                ]),
                TestStreamEvent::AdvanceWatermark(10_000_000),
                TestStreamEvent::Elements(vec![(("a".to_owned(), (0, 3)), 3_000_000)]),
                TestStreamEvent::AdvanceWatermark(11_000_000),
                TestStreamEvent::Elements(vec![(("a".to_owned(), (0, 5)), 5_000_000)]),
                TestStreamEvent::AdvanceWatermark(12_000_000),
                TestStreamEvent::AdvanceWatermark(15_000_000),
            ],
        )?
        .fixed_windows("shared-accumulating-window", 10_000_000)?
        .allowed_lateness_named("shared-accumulating-lateness", 5_000_000)?
        .accumulating_late_count_trigger_named("shared-accumulating-trigger", 2)?;
    let inputs = (0..3)
        .map(|index| {
            source
                .filter_named(&format!("shared-accumulating-branch-{index}"), move |(_, (branch, _))| {
                    *branch == index
                })?
                .map_named(&format!("shared-accumulating-value-{index}"), |(key, (_, value))| {
                    (key, value)
                })
        })
        .collect::<Result<Vec<_>, Error>>()?;
    inputs[0]
        .co_group_homogeneous_named("shared-accumulating-group", &inputs[1..])?
        .reify_panes_named("shared-accumulating-pane")?
        .map_named("shared-accumulating-row", |(pane, (key, mut buckets))| {
            for bucket in &mut buckets {
                bucket.sort_unstable();
            }
            format!("{key}:{buckets:?}:{:?}:{}", pane.timing, pane.index)
        })
}

pub fn shared_accumulating_late_cogroup_fixture(
    pipeline: &Pipeline,
    path: PathBuf,
) -> Result<Sink<String>, Error> {
    let sink_path = path.clone();
    shared_accumulating_late_cogroup_collection(pipeline)?.sink_named_with_config(
        "shared-accumulating-result",
        SinkSpec {
            mode: SinkMode::Append,
            replay: Replay::AtLeastOnce,
            commit: true,
        },
        format!("shared-accumulating-result:{}", path.display()),
        move || {
            Ok(Box::new(GlobalFoldSink {
                path: sink_path.clone(),
                values: Vec::new(),
            }))
        },
    )
}

/// Two branches of one unbounded source retain values across early count panes.
pub fn shared_accumulating_early_cogroup_collection(
    pipeline: &Pipeline,
) -> Result<PCollection<String>, Error> {
    let source = pipeline
        .test_stream_source(
            "shared-early-source",
            vec![
                TestStreamEvent::Elements(vec![(
                    ("a".to_owned(), (0_i32, 2_i32)),
                    2_000_000,
                )]),
                TestStreamEvent::Elements(vec![(
                    ("a".to_owned(), (1_i32, 4_i32)),
                    4_000_000,
                )]),
                TestStreamEvent::AdvanceWatermark(10_000_000),
            ],
        )?
        .fixed_windows("shared-early-window", 10_000_000)?
        .accumulating_count_trigger_named("shared-early-trigger", 1)?;
    let inputs = (0..2)
        .map(|index| {
            source
                .filter_named(&format!("shared-early-branch-{index}"), move |(_, (branch, _))| {
                    *branch == index
                })?
                .map_named(&format!("shared-early-value-{index}"), |(key, (_, value))| {
                    (key, value)
                })
        })
        .collect::<Result<Vec<_>, Error>>()?;
    inputs[0]
        .co_group_homogeneous_named("shared-early-group", &inputs[1..])?
        .reify_panes_named("shared-early-pane")?
        .map_named("shared-early-row", |(pane, (key, buckets))| {
            format!("{key}:{buckets:?}:{:?}:{}", pane.timing, pane.index)
        })
}

pub fn shared_accumulating_early_cogroup_fixture(
    pipeline: &Pipeline,
    path: PathBuf,
) -> Result<Sink<String>, Error> {
    let sink_path = path.clone();
    shared_accumulating_early_cogroup_collection(pipeline)?.sink_named_with_config(
        "shared-early-result",
        SinkSpec {
            mode: SinkMode::Append,
            replay: Replay::AtLeastOnce,
            commit: true,
        },
        format!("shared-early-result:{}", path.display()),
        move || {
            Ok(Box::new(GlobalFoldSink {
                path: sink_path.clone(),
                values: Vec::new(),
            }))
        },
    )
}
