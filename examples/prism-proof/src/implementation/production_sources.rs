struct ProductionSource<T>(std::collections::VecDeque<UnboundedPoll<Timestamped<T>>>);

impl<T: Element> UnboundedSource<Timestamped<T>> for ProductionSource<T> {
    fn poll(&mut self, _: usize) -> Result<UnboundedPoll<Timestamped<T>>, Error> {
        Ok(self.0.pop_front().unwrap_or(UnboundedPoll::Pending))
    }
}

fn streaming_window_source(
    pipeline: &Pipeline,
    include_late_row: bool,
    sliding: bool,
) -> Result<PCollection<i32>, Error> {
    let source = pipeline
        .unbounded_source_with_factory(
            "stream-group-events",
            SourceProgress {
                stable_split_id: true,
                replay: Replay::AtLeastOnce,
                reports_event_time: true,
            },
            format!("stream-group-source-v1:late={include_late_row}"),
            0,
            move |position| {
                let skip = match position.as_deref() {
                    None => 0,
                    Some([1]) => 1,
                    Some([2]) => 2,
                    _ => return Err(Error::incompatible("unexpected grouped source cursor")),
                };
                let mut last_rows = vec![(4, 110_000_000)];
                if include_late_row {
                    // This row belongs to the already closed first window.
                    last_rows.push((9, 30_000_000));
                }
                let polls = [
                    (vec![(1, 10_000_000), (2, 20_000_000)], 1, 20_000_000, false),
                    (vec![(3, 30_000_000)], 2, 100_000_000, false),
                    (last_rows, 3, 200_000_000, true),
                ]
                .into_iter()
                .skip(skip)
                .map(|(rows, cursor, watermark, completed)| {
                    UnboundedPoll::Ready(UnboundedBatch {
                        elements: rows
                            .into_iter()
                            .map(|(value, timestamp_ns)| Timestamped {
                                value,
                                timestamp_ns,
                            })
                            .collect(),
                        progress: ProgressSignal {
                            source_key: "stream-group-events".into(),
                            split_id: "stable-split".into(),
                            checkpoint_position: Some(vec![cursor]),
                            event_time_ns: Some(watermark),
                            completed,
                        },
                    })
                })
                .collect();
                Ok(Box::new(ProductionSource(polls)) as TypedUnboundedSourceReader<i32>)
            },
        )?;
    if sliding {
        source.sliding_windows("stream-slide-window", 100_000_000, 50_000_000, 0)
    } else {
        source.fixed_windows("stream-group-window", 100_000_000)
    }
}

/// Two numbered Partition branches from one resumable unbounded source.
pub fn streaming_partition_fixture(
    pipeline: &Pipeline,
    directory: PathBuf,
) -> Result<Sink<String>, Error> {
    let sink_directory = directory.clone();
    let source = streaming_window_source(pipeline, false, false)?;
    let branches = source.partition_with_semantic_named(
        "stream-partition-classify",
        "stream-partition-branch",
        2,
        "tests/prism-stream-partition",
        Version { major: 1, minor: 0 },
        |value| value.rem_euclid(2) as usize,
    )?;
    let mut rows = Vec::new();
    for (index, branch) in branches.into_iter().enumerate() {
        rows.push(
            branch
                .with_window_named(&format!("stream-partition-bounds-{index}"))?
                .reify_timestamps_named(&format!("stream-partition-stamp-{index}"))?
                .map_named(&format!("stream-partition-row-{index}"), move |(stamp, (window, value))| {
                    format!(
                        "{index}:{value}:{stamp}:{}..{}",
                        window.start_ns, window.end_ns
                    )
                })?,
        );
    }
    let first = rows.remove(0);
    first
        .flatten_named("stream-partition-merge", &rows)?
        .sink_named_with_config(
            "stream-partition-capture",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            format!("stream-partition-capture:{}", directory.display()),
            move || {
                Ok(Box::new(IdempotentRowSink {
                    directory: sink_directory.clone(),
                    pending: Vec::new(),
                }))
            },
        )
}

/// One unbounded fixed-window group exercised by both Direct and Prism.
pub fn streaming_group_fixture(
    pipeline: &Pipeline,
    directory: PathBuf,
) -> Result<Sink<(i64, i32)>, Error> {
    let sink_directory = directory.clone();
    streaming_window_source(pipeline, true, false)?
        .map_named("stream-group-key", |value| (value % 2, value))?
        .group_by_key_named("stream-group-group")?
        .map_named("stream-group-sum", |(key, values)| {
            key * 100 + values.iter().sum::<i32>()
        })?
        .reify_timestamps_named("stream-group-time")?
        .sink_named_with_config(
            "stream-group-capture",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            format!("stream-group-capture:{}", directory.display()),
            move || {
                Ok(Box::new(IdempotentValueSink {
                    directory: sink_directory.clone(),
                    pending: Vec::new(),
                }))
            },
        )
}

/// A resumable sliding-window group with a late row after both old windows close.
pub fn streaming_sliding_group_fixture(
    pipeline: &Pipeline,
    directory: PathBuf,
) -> Result<Sink<String>, Error> {
    let sink_directory = directory.clone();
    streaming_window_source(pipeline, true, true)?
        .map_named("stream-slide-key", |value| (value % 2, value))?
        .group_by_key_named("stream-slide-group")?
        .reify_timestamps_named("stream-slide-time")?
        .with_window_named("stream-slide-interval")?
        .map_named("stream-slide-row", |(window, (time, (key, mut values)))| {
            values.sort_unstable();
            format!("{key}|{values:?}|{time}|{}|{}", window.start_ns, window.end_ns)
        })?
        .sink_named_with_config(
            "stream-slide-capture",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            format!("stream-slide-capture:{}", directory.display()),
            move || {
                Ok(Box::new(IdempotentRowSink {
                    directory: sink_directory.clone(),
                    pending: Vec::new(),
                }))
            },
        )
}

/// A TestStream bridge merges two open sessions after a checkpoint boundary.
pub fn streaming_session_group_fixture(
    pipeline: &Pipeline,
    directory: PathBuf,
) -> Result<Sink<String>, Error> {
    let sink_directory = directory.clone();
    pipeline.test_stream_source(
        "stream-session-events",
        vec![
            TestStreamEvent::Elements(vec![
                ((3_i32, 11_i32), -10_000_000),
                ((1_i32, 1_i32), 0),
                ((1, 2), 8_000_000),
                ((2, 7), 4_000_000),
            ]),
            TestStreamEvent::AdvanceWatermark(1_000_000),
            TestStreamEvent::Elements(vec![((1, 3), 4_000_000)]),
            TestStreamEvent::AdvanceWatermark(20_000_000),
            TestStreamEvent::Elements(vec![((1, 99), 0)]),
        ],
    )?
    .group_by_session_named("stream-session-group", 5_000_000)?
    .reify_timestamps_named("stream-session-time")?
    .with_window_named("stream-session-bounds")?
    .map_named("stream-session-row", |(window, (time, (key, mut values)))| {
        values.sort_unstable();
        format!("{key}|{values:?}|{time}|{}|{}", window.start_ns, window.end_ns)
    })?
    .sink_named_with_config(
        "stream-session-capture",
        SinkSpec { mode: SinkMode::Append, replay: Replay::Idempotent, commit: true },
        format!("stream-session-capture:{}", directory.display()),
        move || Ok(Box::new(IdempotentRowSink {
            directory: sink_directory.clone(),
            pending: Vec::new(),
        })),
    )
}

/// A candidate past its assigned interval can still join a live keyed session.
pub fn streaming_session_expiry_fixture(
    pipeline: &Pipeline,
    directory: PathBuf,
) -> Result<Sink<String>, Error> {
    let sink_directory = directory.clone();
    pipeline
        .test_stream_source(
            "session-expiry-events",
            vec![
                TestStreamEvent::Elements(vec![((1_i32, 1_i32), 0), ((1, 2), 4_000_000)]),
                TestStreamEvent::AdvanceWatermark(6_000_000),
                TestStreamEvent::Elements(vec![((1, 3), 0)]),
                TestStreamEvent::AdvanceWatermark(10_000_000),
                TestStreamEvent::Elements(vec![((1, 88), 0)]),
            ],
        )?
        .group_by_session_named("session-expiry-group", 5_000_000)?
        .with_window_named("session-expiry-window")?
        .map_named("session-expiry-row", |(window, (key, mut values))| {
            values.sort_unstable();
            format!("{key}|{values:?}|{}|{}", window.start_ns, window.end_ns)
        })?
        .sink_named_with_config(
            "session-expiry-capture",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            format!("session-expiry-capture:{}", directory.display()),
            move || {
                Ok(Box::new(IdempotentRowSink {
                    directory: sink_directory.clone(),
                    pending: Vec::new(),
                }))
            },
        )
}

/// Compare output-time policies after a late bridge joins two open sessions.
pub fn streaming_session_output_time_fixture(
    pipeline: &Pipeline,
    directory: PathBuf,
    policy: ironbeam_core::WindowOutputTime,
) -> Result<Sink<String>, Error> {
    let sink_directory = directory.clone();
    let keyed = pipeline.test_stream_source(
        "stream-session-time-events",
        vec![
            TestStreamEvent::Elements(vec![
                ((3_i32, 11_i32), -10_000_000),
                ((1_i32, 1_i32), 0),
                ((1, 2), 8_000_000),
                ((2, 7), 4_000_000),
            ]),
            TestStreamEvent::AdvanceWatermark(1_000_000),
            TestStreamEvent::Elements(vec![((1, 3), 4_000_000)]),
            TestStreamEvent::AdvanceWatermark(20_000_000),
            TestStreamEvent::Elements(vec![((1, 99), 0)]),
        ],
    )?
    .session_windows_named("stream-session-time-window", 5_000_000)?;
    let keyed = match policy {
        ironbeam_core::WindowOutputTime::EndOfWindow => {
            keyed.end_of_window_output_time_named("stream-session-time-policy")?
        }
        ironbeam_core::WindowOutputTime::LatestInPane => {
            keyed.latest_in_pane_output_time_named("stream-session-time-policy")?
        }
        ironbeam_core::WindowOutputTime::EarliestInPane => keyed,
    };
    keyed
        .group_by_key_named("stream-session-time-group")?
        .reify_timestamps_named("stream-session-time-reify")?
        .with_window_named("stream-session-time-bounds")?
        .map_named("stream-session-time-row", |(window, (time, (key, mut values)))| {
            values.sort_unstable();
            format!("{key}|{values:?}|{time}|{}|{}", window.start_ns, window.end_ns)
        })?
        .sink_named_with_config(
            "stream-session-time-capture",
            SinkSpec { mode: SinkMode::Append, replay: Replay::Idempotent, commit: true },
            format!("stream-session-time-capture:{}", directory.display()),
            move || Ok(Box::new(IdempotentRowSink {
                directory: sink_directory.clone(),
                pending: Vec::new(),
            })),
        )
}

/// Explicitly ordered multi-row keyed batches on the same unbounded source.
pub fn streaming_ordered_batch_fixture(
    pipeline: &Pipeline,
    directory: PathBuf,
) -> Result<Sink<(i64, i32)>, Error> {
    let sink_directory = directory.clone();
    streaming_window_source(pipeline, false, false)?
        .map_named("stream-batch-key", |value| (value % 2, (value, value)))?
        .map_values_batches_ordered_by_named(
            "stream-batch-ordered",
            2,
            |value| value.0,
            |values| {
                values
                    .iter()
                    .map(|value| value.1 * 10 + i32::try_from(values.len()).unwrap())
                    .collect()
            },
        )?
        .map_named("stream-batch-value", |(key, value)| key * 100 + value)?
        .reify_timestamps_named("stream-batch-time")?
        .sink_named_with_config(
            "stream-batch-capture",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            format!("stream-batch-capture:{}", directory.display()),
            move || {
                Ok(Box::new(IdempotentValueSink {
                    directory: sink_directory.clone(),
                    pending: Vec::new(),
                }))
            },
        )
}

struct IdempotentRowSink {
    directory: PathBuf,
    pending: Vec<String>,
}

impl TypedSink<String> for IdempotentRowSink {
    fn write(&mut self, value: String) -> Result<(), Error> {
        self.pending.push(value);
        Ok(())
    }

    fn commit(&mut self) -> Result<(), Error> {
        use std::io::Write;
        std::fs::create_dir_all(&self.directory)
            .map_err(|error| Error::incompatible(error.to_string()))?;
        for row in self.pending.drain(..) {
            let digest = hex::encode(Sha256::digest(row.as_bytes()));
            let target = self.directory.join(format!("row-{digest}"));
            let mut temporary = tempfile::NamedTempFile::new_in(&self.directory)
                .map_err(|error| Error::incompatible(error.to_string()))?;
            temporary
                .write_all(row.as_bytes())
                .and_then(|()| temporary.as_file().sync_all())
                .map_err(|error| Error::incompatible(error.to_string()))?;
            match temporary.persist_noclobber(&target) {
                Ok(_) => {}
                Err(error) if error.error.kind() == std::io::ErrorKind::AlreadyExists => {
                    if std::fs::read(&target)
                        .map_err(|read| Error::incompatible(read.to_string()))?
                        != row.as_bytes()
                    {
                        return Err(Error::incompatible("idempotent row changed on replay"));
                    }
                }
                Err(error) => return Err(Error::incompatible(error.error.to_string())),
            }
        }
        Ok(())
    }

    fn abort(&mut self) -> Result<(), Error> {
        self.pending.clear();
        Ok(())
    }
}

/// A resumable production reader joined with a bounded side in two windows.
pub fn production_join_cogroup_fixture(
    pipeline: &Pipeline,
    directory: PathBuf,
) -> Result<Sink<String>, Error> {
    let live = pipeline
        .unbounded_source_with_factory(
            "production-join-events",
            SourceProgress {
                stable_split_id: true,
                replay: Replay::AtLeastOnce,
                reports_event_time: true,
            },
            "prism-production-join-source-v1".into(),
            0,
            move |position| {
                let (values, cursor, completed) = match position.as_deref() {
                    None => (vec![7, 9], 1, false),
                    Some([1]) => (vec![8], 2, true),
                    _ => return Err(Error::incompatible("unexpected production join cursor")),
                };
                let signal = ProgressSignal {
                    source_key: "production-join-events".into(),
                    split_id: "production-join-split".into(),
                    checkpoint_position: Some(vec![cursor]),
                    event_time_ns: Some(i64::from(cursor) * 1_000_000),
                    completed,
                };
                let batch = UnboundedPoll::Ready(UnboundedBatch {
                    elements: values
                        .into_iter()
                        .map(|value| Timestamped {
                            value,
                            timestamp_ns: i64::from(cursor) * 1_000_000,
                        })
                        .collect(),
                    progress: signal,
                });
                Ok(
                    Box::new(ProductionSource([UnboundedPoll::Pending, batch].into()))
                        as TypedUnboundedSourceReader<i32>,
                )
            },
        )?
        .map_named("production-join-key", |value| ("a".to_owned(), value))?
        .fixed_windows("production-join-window", 2_000_000)?;
    let bounded = pipeline
        .from_vec(
            "production-join-bounded",
            vec![
                (0_i64, ("a".to_owned(), 10_i32)),
                (2_000_000, ("a".to_owned(), 20)),
            ],
        )?
        .timestamps_from_pairs_named("production-join-bounded-time")?
        .fixed_windows("production-join-bounded-window", 2_000_000)?;
    let grouped =
        ironbeam_core::cogroup_by_key_2_named("production-join-cogroup", &live, &bounded)?
            .reify_timestamps_named("production-join-group-time")?
            .with_window_named("production-join-group-window")?
            .map_named(
                "production-join-group-row",
                |(window, (time, (key, (mut left, mut right))))| {
                    left.sort_unstable();
                    right.sort_unstable();
                    format!(
                        "C|{key}|{left:?}|{right:?}|{time}|{}|{}",
                        window.start_ns, window.end_ns
                    )
                },
            )?;
    let joined = live
        .join_full_named("production-join-full", &bounded)?
        .reify_timestamps_named("production-join-time")?
        .with_window_named("production-join-result-window")?
        .map_named(
            "production-join-row",
            |(window, (time, (key, (left, right))))| {
                format!(
                    "J|{key}|{left:?}|{right:?}|{time}|{}|{}",
                    window.start_ns, window.end_ns
                )
            },
        )?;
    let sink_directory = directory.clone();
    grouped
        .flatten_named("production-join-merge", &[joined])?
        .sink_named_with_config(
            "production-join-capture",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            format!("production-join:{}", directory.display()),
            move || {
                Ok(Box::new(IdempotentRowSink {
                    directory: sink_directory.clone(),
                    pending: Vec::new(),
                }))
            },
        )
}
