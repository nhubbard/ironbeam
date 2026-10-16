struct LabelBatch;

impl DynOp for LabelBatch {
    fn apply(&self, input: Partition) -> Partition {
        let values = *input.downcast::<Vec<i32>>().expect("i32 batch");
        Box::new(
            values
                .into_iter()
                .map(|value| format!("value:{value}"))
                .collect::<Vec<String>>(),
        )
    }
}

struct BatchLength;

impl DynOp for BatchLength {
    fn apply(&self, input: Partition) -> Partition {
        let values = input.downcast::<Vec<i32>>().expect("i32 batch");
        Box::new(vec![
            i32::try_from(values.len()).expect("batch length fits i32")
        ])
    }
}

/// Shared proof that an explicit size produces the same batch lengths on both runners.
pub fn explicit_custom_batches_fixture(
    pipeline: &Pipeline,
    path: PathBuf,
) -> Result<Sink<i32>, Error> {
    let sink_path = path.clone();
    pipeline
        .from_vec("custom-batch-input", vec![1_i32, 2, 3, 4, 5])?
        .apply_dyn_transform_with_semantic_named::<i32>(
            "custom-batch-length",
            2,
            "prism-proof/batch-length",
            Version { major: 1, minor: 0 },
            Arc::new(BatchLength),
        )?
        .sink_named_with_config(
            "custom-batch-result",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::AtLeastOnce,
                commit: true,
            },
            format!("custom-batches:{}", path.display()),
            move || {
                Ok(Box::new(GlobalFoldSink {
                    path: sink_path.clone(),
                    values: Vec::new(),
                }))
            },
        )
}

pub struct ExpandSingleton;

impl DynOp for ExpandSingleton {
    fn apply(&self, input: Partition) -> Partition {
        let [value]: [i32; 1] = (*input.downcast::<Vec<i32>>().expect("i32 singleton"))
            .try_into()
            .expect("one input row");
        Box::new(vec![value, value + 10])
    }
}

pub fn bounded_map_filter_fixture(pipeline: &Pipeline, path: PathBuf) -> Result<Sink<i32>, Error> {
    let sink_path = path.clone();
    pipeline
        .from_vec("input", vec![1_i32, 2, 3, 4])?
        .apply_dyn_transform_with_semantic_named::<i32>(
            "bounded-custom-batch",
            4,
            "prism-proof/reverse-batch",
            Version { major: 1, minor: 0 },
            Arc::new(ReverseBatch),
        )?
        .apply_dyn_transform_with_semantic_named::<String>(
            "bounded-label-batch",
            4,
            "prism-proof/label-batch",
            Version { major: 1, minor: 0 },
            Arc::new(LabelBatch),
        )?
        .map_named("parse-labelled/v1", |value| {
            value
                .strip_prefix("value:")
                .expect("label prefix")
                .parse::<i32>()
                .expect("integer label")
        })?
        .map_named("double/v1", |value| value * 2)?
        .filter_named("divisible-by-four/v1", |value| value % 4 == 0)?
        .flat_map_named("duplicate-plus-one/v1", |value| vec![value, value + 1])?
        .sink_named_with_config(
            "result",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::AtLeastOnce,
                commit: true,
            },
            format!("bounded-contract:{}", path.display()),
            move || {
                Ok(Box::new(GlobalFoldSink {
                    path: sink_path.clone(),
                    values: Vec::new(),
                }))
            },
        )
}

/// One runner-owned value cell per key. Counts are independent of input order.
pub fn bounded_value_state_fixture(
    pipeline: &Pipeline,
    path: PathBuf,
) -> Result<Sink<String>, Error> {
    let sink_path = path.clone();
    pipeline
        .from_vec("value-state-input", vec![(1_i32, 11_i32), (1, 13), (2, 17), (1, 19)])?
        .stateful_value_map_named::<i32, i32>(
            "value-state-count",
            "prism-proof/key-count-state",
            Version { major: 1, minor: 0 },
            |_, _, prior| {
                let count = prior.unwrap_or(0) + 1;
                Ok((Some(count), count))
            },
        )?
        .map_named("value-state-label", |(key, count)| format!("{key}:{count}"))?
        .sink_named_with_config(
            "value-state-result",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::AtLeastOnce,
                commit: true,
            },
            format!("value-state-result:{}", path.display()),
            move || {
                Ok(Box::new(GlobalFoldSink {
                    path: sink_path.clone(),
                    values: Vec::new(),
                }))
            },
        )
}

/// One state cell per key and fixed window, including a repeated key in two windows.
pub fn windowed_value_state_fixture(
    pipeline: &Pipeline,
    path: PathBuf,
) -> Result<Sink<String>, Error> {
    let sink_path = path.clone();
    pipeline
        .from_vec("windowed-state-input", vec![(1_i32, 1_i32), (1, 2), (1, 11), (1, 3), (2, 4)])?
        .assign_timestamps_named("windowed-state-time", |(_, value)| i64::from(*value) * 1_000_000)?
        .fixed_windows("windowed-state-window", 10_000_000)?
        .stateful_value_map_named::<i32, i32>(
            "windowed-state-count",
            "prism-proof/windowed-key-count-state",
            Version { major: 1, minor: 0 },
            |_, _, prior| {
                let count = prior.unwrap_or(0) + 1;
                Ok((Some(count), count))
            },
        )?
        .map_named("windowed-state-label", |(key, count)| format!("{key}:{count}"))?
        .sink_named_with_config(
            "windowed-state-result",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::AtLeastOnce,
                commit: true,
            },
            format!("windowed-state-result:{}", path.display()),
            move || {
                Ok(Box::new(GlobalFoldSink {
                    path: sink_path.clone(),
                    values: Vec::new(),
                }))
            },
        )
}

struct IdempotentStateLabelSink {
    directory: PathBuf,
    pending: Vec<String>,
}

impl TypedSink<String> for IdempotentStateLabelSink {
    fn write(&mut self, value: String) -> Result<(), Error> {
        self.pending.push(value);
        Ok(())
    }

    fn commit(&mut self) -> Result<(), Error> {
        std::fs::create_dir_all(&self.directory)
            .map_err(|error| Error::incompatible(error.to_string()))?;
        for value in self.pending.drain(..) {
            let path = self.directory.join(value.replace(':', "_"));
            use std::io::Write;
            let mut temporary = tempfile::NamedTempFile::new_in(&self.directory)
                .map_err(|error| Error::incompatible(error.to_string()))?;
            temporary.write_all(value.as_bytes())
                .and_then(|()| temporary.as_file().sync_all())
                .map_err(|error| Error::incompatible(error.to_string()))?;
            match temporary.persist_noclobber(&path) {
                Ok(_) => {}
                Err(error) if error.error.kind() == std::io::ErrorKind::AlreadyExists => {
                    if std::fs::read_to_string(&path)
                        .map_err(|error| Error::incompatible(error.to_string()))? != value
                    {
                        return Err(Error::incompatible("idempotent state sink found a conflicting row"));
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

struct WindowedStateLabelSink(IdempotentStateLabelSink);

impl TypedSink<String> for WindowedStateLabelSink {
    fn write(&mut self, value: String) -> Result<(), Error> {
        self.0.write(value)
    }

    fn write_with_metadata(&mut self, value: String, metadata: ElementMetadata) -> Result<(), Error> {
        let window = metadata.assigned_window.ok_or_else(|| Error::incompatible("state result has no assigned window"))?;
        self.0.write(format!("{}:{}:{value}", window.start_ns, window.end_ns))
    }

    fn commit(&mut self) -> Result<(), Error> {
        self.0.commit()
    }

    fn abort(&mut self) -> Result<(), Error> {
        self.0.abort()
    }
}

/// The same keyed count with replay-safe output for a supervised worker retry.
pub fn replay_value_state_fixture(
    pipeline: &Pipeline,
    directory: PathBuf,
) -> Result<Sink<String>, Error> {
    let sink_directory = directory.clone();
    pipeline
        .from_vec("value-state-input", vec![(1_i32, 11_i32), (1, 13), (2, 17), (1, 19)])?
        .stateful_value_map_named::<i32, i32>(
            "value-state-count",
            "prism-proof/key-count-state",
            Version { major: 1, minor: 0 },
            |_, _, prior| {
                let count = prior.unwrap_or(0) + 1;
                Ok((Some(count), count))
            },
        )?
        .map_named("value-state-label", |(key, count)| format!("{key}:{count}"))?
        .sink_named_with_config(
            "value-state-result",
            SinkSpec { mode: SinkMode::Append, replay: Replay::Idempotent, commit: true },
            format!("value-state-replay:{}", directory.display()),
            move || Ok(Box::new(IdempotentStateLabelSink { directory: sink_directory.clone(), pending: Vec::new() })),
        )
}

/// Global keyed state across two unbounded TestStream element batches.
pub fn streaming_value_state_fixture(
    pipeline: &Pipeline,
    directory: PathBuf,
) -> Result<Sink<String>, Error> {
    let sink_directory = directory.clone();
    pipeline
        .test_stream_source(
            "streaming-state-input",
            vec![
                TestStreamEvent::Elements(vec![((1_i32, 11_i32), 0), ((2, 17), 0)]),
                TestStreamEvent::AdvanceWatermark(1_000_000),
                TestStreamEvent::Elements(vec![((1, 13), 2_000_000), ((2, 19), 2_000_000), ((1, 23), 2_000_000)]),
                TestStreamEvent::AdvanceWatermark(3_000_000),
            ],
        )?
        .stateful_value_map_named::<i32, (i32, i32)>(
            "streaming-state-count",
            "prism-proof/streaming-key-count-state",
            Version { major: 1, minor: 0 },
            |_, value, prior| {
                let count = prior.unwrap_or(0) + 1;
                Ok((Some(count), (value, count)))
            },
        )?
        .map_named("streaming-state-label", |(key, (value, count))| format!("{key}:{value}:{count}"))?
        .sink_named_with_config(
            "streaming-state-result",
            SinkSpec { mode: SinkMode::Append, replay: Replay::Idempotent, commit: true },
            format!("streaming-value-state:{}", directory.display()),
            move || Ok(Box::new(IdempotentStateLabelSink { directory: sink_directory.clone(), pending: Vec::new() })),
        )
}

/// One TestStream feeds a stateful branch and a stateless branch independently.
pub fn streaming_branch_value_state_fixture(
    pipeline: &Pipeline,
    stateful_directory: PathBuf,
    stateless_directory: PathBuf,
) -> Result<(Sink<String>, Sink<String>), Error> {
    streaming_branch_value_state_fixture_impl(
        pipeline,
        stateful_directory,
        stateless_directory,
        false,
    )
}

/// One TestStream feeds two independently stateful branches.
pub fn streaming_two_branch_value_state_fixture(
    pipeline: &Pipeline,
    left_directory: PathBuf,
    right_directory: PathBuf,
) -> Result<(Sink<String>, Sink<String>), Error> {
    streaming_branch_value_state_fixture_impl(pipeline, left_directory, right_directory, true)
}

/// A keyed TestStream flushes byte-limited batches and a final window batch.
pub fn streaming_byte_limited_batches_fixture(
    pipeline: &Pipeline,
    directory: PathBuf,
) -> Result<Sink<String>, Error> {
    let sink_directory = directory.clone();
    let short_bytes = "a".to_owned().encoded_len()?;
    pipeline
        .test_stream_source(
            "byte-batch-input",
            vec![
                TestStreamEvent::Elements(vec![
                    ((1_i32, "a".to_owned()), 1_000_000),
                    ((1, "bbb".to_owned()), 2_000_000),
                    ((2, "x".to_owned()), 2_000_000),
                    ((1, "c".to_owned()), 3_000_000),
                    ((1, "d".to_owned()), 4_000_000),
                    ((1, "oversized".to_owned()), 5_000_000),
                    ((1, "e".to_owned()), 6_000_000),
                    ((2, "y".to_owned()), 7_000_000),
                ]),
                TestStreamEvent::AdvanceWatermark(10_000_000),
            ],
        )?
        .fixed_windows("byte-batch-window", 10_000_000)?
        .group_into_batches_with_limits_named("byte-batches", 2, short_bytes * 2)?
        .reify_timestamps_named("byte-batch-timestamps")?
        .flat_map_fallible_named("byte-batch-recovery-probe", |(timestamp, (key, values))| {
            if key == 1
                && values.len() == 1
                && values[0] == "a"
                && std::env::var_os("IRONBEAM_V5_PRISM_KILL_BYTE_BATCH").is_some()
            {
                await_supervised_source_replacement(Some(&[1]))?;
            }
            Ok(vec![(timestamp, (key, values))])
        })?
        .map_named("byte-batch-label", |(timestamp, (key, values))| {
            format!("{key}:{timestamp}:{values:?}")
        })?
        .sink_named_with_config(
            "byte-batch-result",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            format!("byte-batch-result:{}", directory.display()),
            move || {
                Ok(Box::new(IdempotentStateLabelSink {
                    directory: sink_directory.clone(),
                    pending: Vec::new(),
                }))
            },
        )
}

/// Two events occupy three overlapping sliding-window byte-batch cells.
pub fn streaming_sliding_byte_batches_fixture(
    pipeline: &Pipeline,
    directory: PathBuf,
) -> Result<Sink<String>, Error> {
    let sink_directory = directory.clone();
    pipeline
        .test_stream_source(
            "sliding-byte-batch-input",
            vec![
                TestStreamEvent::Elements(vec![
                    ((1_i32, "a".to_owned()), 1_000_000),
                    ((1, "b".to_owned()), 4_000_000),
                ]),
                TestStreamEvent::AdvanceWatermark(9_000_000),
            ],
        )?
        .sliding_windows("sliding-byte-batch-window", 6_000_000, 3_000_000, 0)?
        .group_into_batches_with_limits_named("sliding-byte-batches", 10, 1_000)?
        .reify_timestamps_named("sliding-byte-timestamps")?
        .with_window_named("sliding-byte-interval")?
        .map_named("sliding-byte-label", |(window, (timestamp, (key, values)))| {
            format!(
                "{}..{}:{key}:{timestamp}:{values:?}",
                window.start_ns, window.end_ns
            )
        })?
        .sink_named_with_config(
            "sliding-byte-result",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            format!("sliding-byte-result:{}", directory.display()),
            move || {
                Ok(Box::new(IdempotentStateLabelSink {
                    directory: sink_directory.clone(),
                    pending: Vec::new(),
                }))
            },
        )
}

fn streaming_branch_value_state_fixture_impl(
    pipeline: &Pipeline,
    stateful_directory: PathBuf,
    stateless_directory: PathBuf,
    second_stateful: bool,
) -> Result<(Sink<String>, Sink<String>), Error> {
    let source = pipeline.test_stream_source(
        "branch-state-input",
        vec![
            TestStreamEvent::Elements(vec![((1_i32, 2_i32), 0)]),
            TestStreamEvent::Elements(vec![((1_i32, 5_i32), 1_000_000)]),
            TestStreamEvent::AdvanceWatermark(2_000_000),
        ],
    )?;
    let stateful_sink_directory = stateful_directory.clone();
    let stateful = source
        .stateful_value_map_named::<i32, i32>(
            "branch-value-state",
            "prism-proof/branch-value-state",
            Version { major: 1, minor: 0 },
            |_, value, prior| {
                if value == 2
                    && std::env::var_os("IRONBEAM_V5_PRISM_KILL_TWO_BRANCH_STATE").is_some()
                {
                    await_supervised_source_replacement(Some(&[1]))?;
                }
                let next = prior.unwrap_or(0) + value;
                Ok((Some(next), next))
            },
        )?
        .map_named("branch-value-label", |(key, value)| format!("{key}:{value}"))?
        .sink_named_with_config(
            "branch-value-result",
            SinkSpec { mode: SinkMode::Append, replay: Replay::Idempotent, commit: true },
            format!("branch-value-result:{}", stateful_directory.display()),
            move || Ok(Box::new(IdempotentStateLabelSink {
                directory: stateful_sink_directory.clone(),
                pending: Vec::new(),
            })),
        )?;
    let stateless_sink_directory = stateless_directory.clone();
    let right_source = if second_stateful {
        source.stateful_value_map_named::<i32, i32>(
            "branch-right-value-state",
            "prism-proof/branch-right-value-state",
            Version { major: 1, minor: 0 },
            |_, value, prior| {
                let next = prior.unwrap_or(0) + value * 10;
                Ok((Some(next), next))
            },
        )?
    } else {
        source
    };
    let stateless = right_source
        .map_named("branch-raw-label", |(key, value)| format!("{key}:{value}"))?
        .sink_named_with_config(
            "branch-raw-result",
            SinkSpec { mode: SinkMode::Append, replay: Replay::Idempotent, commit: true },
            format!("branch-raw-result:{}", stateless_directory.display()),
            move || Ok(Box::new(IdempotentStateLabelSink {
                directory: stateless_sink_directory.clone(),
                pending: Vec::new(),
            })),
        )?;
    Ok((stateful, stateless))
}

/// BagState retains all values for each key across unbounded element batches.
pub fn streaming_bag_state_fixture(
    pipeline: &Pipeline,
    directory: PathBuf,
) -> Result<Sink<String>, Error> {
    let sink_directory = directory.clone();
    pipeline
        .test_stream_source(
            "streaming-bag-input",
            vec![
                TestStreamEvent::Elements(vec![((1_i32, 11_i32), 0), ((2, 17), 0)]),
                TestStreamEvent::AdvanceWatermark(1_000_000),
                TestStreamEvent::Elements(vec![((1, 13), 2_000_000), ((2, 19), 2_000_000), ((1, 23), 2_000_000)]),
                TestStreamEvent::AdvanceWatermark(3_000_000),
            ],
        )?
        .stateful_bag_map_named::<i32, (i32, i32)>(
            "streaming-bag-count",
            "prism-proof/streaming-key-bag-state",
            Version { major: 1, minor: 0 },
            |_, value, prior: Vec<i32>| {
                let count = i32::try_from(prior.len()).expect("test bag fits i32") + 1;
                Ok((BagStateMutation::Append(vec![value]), (value, count)))
            },
        )?
        .map_named("streaming-bag-label", |(key, (value, count))| format!("{key}:{value}:{count}"))?
        .sink_named_with_config(
            "streaming-bag-result",
            SinkSpec { mode: SinkMode::Append, replay: Replay::Idempotent, commit: true },
            format!("streaming-bag-state:{}", directory.display()),
            move || Ok(Box::new(IdempotentStateLabelSink { directory: sink_directory.clone(), pending: Vec::new() })),
        )
}

/// Two unbounded element batches retain distinct bag cells by key and fixed window.
pub fn streaming_windowed_bag_state_fixture(
    pipeline: &Pipeline,
    directory: PathBuf,
) -> Result<Sink<String>, Error> {
    let sink_directory = directory.clone();
    pipeline
        .test_stream_source(
            "streaming-windowed-bag-input",
            vec![
                TestStreamEvent::Elements(vec![((1_i32, 11_i32), 0), ((1, 17), 20_000_000)]),
                TestStreamEvent::AdvanceWatermark(1_000_000),
                TestStreamEvent::Elements(vec![((1, 13), 2_000_000), ((1, 19), 22_000_000)]),
                TestStreamEvent::AdvanceWatermark(30_000_000),
            ],
        )?
        .fixed_windows("streaming-windowed-bag-window", 10_000_000)?
        .stateful_bag_map_named::<i32, (i32, i32)>(
            "streaming-windowed-bag-count",
            "prism-proof/streaming-windowed-key-bag-state",
            Version { major: 1, minor: 0 },
            |_, value, prior: Vec<i32>| {
                let count = i32::try_from(prior.len()).expect("fixture bag fits i32") + 1;
                Ok((BagStateMutation::Append(vec![value]), (value, count)))
            },
        )?
        .map_named("streaming-windowed-bag-label", |(key, (value, count))| format!("{key}:{value}:{count}"))?
        .sink_named_with_config(
            "streaming-windowed-bag-result",
            SinkSpec { mode: SinkMode::Append, replay: Replay::Idempotent, commit: true },
            format!("streaming-windowed-bag-state:{}", directory.display()),
            move || Ok(Box::new(WindowedStateLabelSink(IdempotentStateLabelSink {
                directory: sink_directory.clone(), pending: Vec::new(),
            }))),
        )
}

/// Mutation and duplicate-content contract for one Beam bag cell per key.
pub fn bounded_bag_mutations_fixture(
    pipeline: &Pipeline,
    directory: PathBuf,
) -> Result<Sink<String>, Error> {
    let sink_directory = directory.clone();
    pipeline
        .from_vec(
            "bag-mutations-input",
            vec![(1_i32, 11_i32), (1, 13), (2, 7), (1, 13), (1, 0), (1, -1), (1, 17), (1, -2), (1, 19)],
        )?
        .stateful_bag_map_named::<i32, (i32, String)>(
            "bag-mutations",
            "prism-proof/bag-mutations",
            Version { major: 1, minor: 0 },
            |_, value, mut prior: Vec<i32>| {
                prior.sort_unstable();
                let seen = prior.iter().map(ToString::to_string).collect::<Vec<_>>().join(",");
                let mutation = match value {
                    0 => BagStateMutation::Keep,
                    -1 => BagStateMutation::Clear,
                    -2 => BagStateMutation::Replace(vec![99]),
                    value => BagStateMutation::Append(vec![value]),
                };
                Ok((mutation, (value, seen)))
            },
        )?
        .map_named("bag-mutations-label", |(key, (value, seen))| format!("{key}:{value}:{seen}"))?
        .sink_named_with_config(
            "bag-mutations-result",
            SinkSpec { mode: SinkMode::Append, replay: Replay::Idempotent, commit: true },
            format!("bag-mutations:{}", directory.display()),
            move || Ok(Box::new(IdempotentStateLabelSink { directory: sink_directory.clone(), pending: Vec::new() })),
        )
}

/// Two unbounded element batches retain distinct value cells in fixed windows.
pub fn streaming_windowed_value_state_fixture(
    pipeline: &Pipeline,
    directory: PathBuf,
) -> Result<Sink<String>, Error> {
    let sink_directory = directory.clone();
    pipeline
        .test_stream_source(
            "streaming-windowed-state-input",
            vec![
                TestStreamEvent::Elements(vec![((1_i32, 11_i32), 0), ((1, 17), 20_000_000)]),
                TestStreamEvent::AdvanceWatermark(1_000_000),
                TestStreamEvent::Elements(vec![((1, 13), 2_000_000), ((1, 19), 22_000_000)]),
                TestStreamEvent::AdvanceWatermark(10_000_000),
                TestStreamEvent::Elements(vec![((1, 23), 0)]),
                TestStreamEvent::AdvanceWatermark(30_000_000),
            ],
        )?
        .fixed_windows("streaming-windowed-state-window", 10_000_000)?
        .stateful_value_map_named::<i32, (i32, i32)>(
            "streaming-windowed-state-count",
            "prism-proof/streaming-windowed-key-count-state",
            Version { major: 1, minor: 0 },
            |_, value, prior| {
                let count = prior.unwrap_or(0) + 1;
                Ok((Some(count), (value, count)))
            },
        )?
        .map_named("streaming-windowed-state-label", |(key, (value, count))| format!("{key}:{value}:{count}"))?
        .sink_named_with_config(
            "streaming-windowed-state-result",
            SinkSpec { mode: SinkMode::Append, replay: Replay::Idempotent, commit: true },
            format!("streaming-windowed-value-state:{}", directory.display()),
            move || Ok(Box::new(IdempotentStateLabelSink { directory: sink_directory.clone(), pending: Vec::new() })),
        )
}

/// One keyed cell per assigned sliding window across three unbounded batches.
pub fn streaming_sliding_value_state_fixture(
    pipeline: &Pipeline,
    directory: PathBuf,
) -> Result<Sink<String>, Error> {
    let sink_directory = directory.clone();
    pipeline
        .test_stream_source(
            "streaming-sliding-state-input",
            vec![
                TestStreamEvent::Elements(vec![
                    ((1_i32, 11_i32), 6_000_000),
                    ((1, 12), 7_000_000),
                ]),
                TestStreamEvent::AdvanceWatermark(8_000_000),
                TestStreamEvent::Elements(vec![((1, 13), 12_000_000)]),
                TestStreamEvent::AdvanceWatermark(16_000_000),
                TestStreamEvent::Elements(vec![((1, 17), 18_000_000)]),
                TestStreamEvent::AdvanceWatermark(26_000_000),
            ],
        )?
        .sliding_windows("streaming-sliding-state-window", 10_000_000, 5_000_000, 0)?
        .stateful_value_map_named::<i32, (i32, i32)>(
            "streaming-sliding-state-count",
            "prism-proof/streaming-sliding-key-count-state",
            Version { major: 1, minor: 0 },
            |_, value, prior| {
                let count = prior.unwrap_or(0) + 1;
                Ok((Some(count), (value, count)))
            },
        )?
        .map_named("streaming-sliding-state-label", |(key, (value, count))| format!("{key}:{value}:{count}"))?
        .sink_named_with_config(
            "streaming-sliding-state-result",
            SinkSpec { mode: SinkMode::Append, replay: Replay::Idempotent, commit: true },
            format!("streaming-sliding-value-state:{}", directory.display()),
            move || Ok(Box::new(WindowedStateLabelSink(IdempotentStateLabelSink {
                directory: sink_directory.clone(), pending: Vec::new(),
            }))),
        )
}

/// Both arms of a fallible helper must survive portable fanout and rejoin.
pub fn dead_letter_fixture(pipeline: &Pipeline, path: PathBuf) -> Result<Sink<String>, Error> {
    let sink_path = path.clone();
    let input = pipeline.from_vec(
        "dead-letter-input",
        vec![
            "7".to_owned(),
            "bad".to_owned(),
            "3".to_owned(),
            "x".to_owned(),
        ],
    )?;
    let (good, errors) = input.map_catching_named(
        "dead-letter-classify",
        "dead-letter-good",
        "dead-letter-errors",
        |value| value.parse::<i32>().map_err(|_| "invalid integer"),
    )?;
    let good = good.map_named("dead-letter-good-label", |value| format!("good:{value}"))?;
    let errors = errors.map_named("dead-letter-error-label", |letter| {
        format!("error:{}:{}", letter.element, letter.error)
    })?;
    good.flatten_named("dead-letter-merge", &[errors])?
        .sink_named_with_config(
            "dead-letter-result",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::AtLeastOnce,
                commit: true,
            },
            format!("dead-letter-contract:{}", path.display()),
            move || {
                Ok(Box::new(GlobalFoldSink {
                    path: sink_path.clone(),
                    values: Vec::new(),
                }))
            },
        )
}

fn validation_fixture(
    pipeline: &Pipeline,
    path: PathBuf,
    fail_fast: bool,
) -> Result<Sink<String>, Error> {
    let sink_path = path.clone();
    let input = pipeline.from_vec(
        "validation-input",
        vec![
            ProductionAvroRow {
                id: 1,
                label: "one".into(),
            },
            ProductionAvroRow {
                id: -1,
                label: "invalid".into(),
            },
            ProductionAvroRow {
                id: 2,
                label: "two".into(),
            },
        ],
    )?;
    let valid = if fail_fast {
        input.validate_fail_fast_named("validation-check")?
    } else {
        input.validate_skip_invalid_named("validation-check")?
    };
    valid
        .map_named("validation-label", |row| {
            format!("ok:{}:{}", row.id, row.label)
        })?
        .sink_named_with_config(
            "validation-result",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::AtLeastOnce,
                commit: true,
            },
            format!("validation-contract:{}", path.display()),
            move || {
                Ok(Box::new(GlobalFoldSink {
                    path: sink_path.clone(),
                    values: Vec::new(),
                }))
            },
        )
}

pub fn validation_skip_fixture(pipeline: &Pipeline, path: PathBuf) -> Result<Sink<String>, Error> {
    validation_fixture(pipeline, path, false)
}

pub fn validation_fail_fixture(pipeline: &Pipeline, path: PathBuf) -> Result<Sink<String>, Error> {
    validation_fixture(pipeline, path, true)
}

struct WrongOutputType;

impl DynOp for WrongOutputType {
    fn apply(&self, _input: Partition) -> Partition {
        Box::new(vec!["wrong".to_owned()])
    }
}

/// A dynamic operator with a mismatched declared output must abort before sink commit.
pub fn invalid_dynop_fixture(pipeline: &Pipeline, path: PathBuf) -> Result<Sink<i32>, Error> {
    let sink_path = path.clone();
    pipeline
        .from_vec("invalid-dynop-input", vec![1_i32])?
        .apply_dyn_transform_with_semantic_named::<i32>(
            "invalid-dynop-output",
            1,
            "prism-proof/invalid-dynop-output",
            Version { major: 1, minor: 0 },
            Arc::new(WrongOutputType),
        )?
        .sink_named_with_config(
            "invalid-dynop-result",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::AtLeastOnce,
                commit: true,
            },
            format!("invalid-dynop-contract:{}", path.display()),
            move || {
                Ok(Box::new(GlobalFoldSink {
                    path: sink_path.clone(),
                    values: Vec::new(),
                }))
            },
        )
}
