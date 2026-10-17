struct IdempotentValueSink {
    directory: PathBuf,
    pending: Vec<(i64, i32, WindowInterval)>,
}

impl TypedSink<(i64, i32)> for IdempotentValueSink {
    fn write(&mut self, _: (i64, i32)) -> Result<(), Error> {
        Err(Error::incompatible(
            "production fixture requires window metadata",
        ))
    }

    fn write_with_metadata(
        &mut self,
        value: (i64, i32),
        metadata: ElementMetadata,
    ) -> Result<(), Error> {
        if !matches!(metadata.window, Window::Fixed { .. } | Window::FixedOffset { .. } | Window::Sliding { .. })
            || metadata.timestamp_ns != value.0
        {
            return Err(Error::incompatible(
                "production fixture has wrong event-time metadata",
            ));
        }
        let interval = metadata
            .assigned_window
            .ok_or_else(|| Error::incompatible("production fixture has no assigned window"))?;
        self.pending.push((value.0, value.1, interval));
        Ok(())
    }

    fn commit(&mut self) -> Result<(), Error> {
        use std::io::Write;
        std::fs::create_dir_all(&self.directory)
            .map_err(|error| Error::incompatible(error.to_string()))?;
        for (timestamp, value, interval) in self.pending.drain(..) {
            let target = self.directory.join(format!("value-{value}"));
            let expected = format!(
                "{timestamp}:{value}:{}:{}\n",
                interval.start_ns, interval.end_ns
            )
            .into_bytes();
            let mut temporary = tempfile::NamedTempFile::new_in(&self.directory)
                .map_err(|error| Error::incompatible(error.to_string()))?;
            temporary
                .write_all(&expected)
                .and_then(|()| temporary.as_file().sync_all())
                .map_err(|error| Error::incompatible(error.to_string()))?;
            match temporary.persist_noclobber(&target) {
                Ok(_) => {}
                Err(error) if error.error.kind() == std::io::ErrorKind::AlreadyExists => {
                    let existing = std::fs::read(&target)
                        .map_err(|error| Error::incompatible(error.to_string()))?;
                    if existing != expected {
                        return Err(Error::incompatible(
                            "idempotent sink found conflicting value",
                        ));
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

fn await_supervised_source_replacement(position: Option<&[u8]>) -> Result<(), Error> {
    if position == Some(&[1][..]) {
        if let (Some(trigger), Some(killed)) = (
            std::env::var_os("IRONBEAM_V5_SUPERVISOR_TRIGGER"),
            std::env::var_os("IRONBEAM_V5_SUPERVISOR_KILLED"),
        ) {
            if !Path::new(&killed).exists() {
                let trigger = Path::new(&trigger);
                use std::io::Write;
                let mut pending = tempfile::NamedTempFile::new_in(
                    trigger.parent().unwrap_or_else(|| Path::new(".")),
                )
                .map_err(|error| Error::incompatible(error.to_string()))?;
                write!(pending, "{}", std::process::id())
                    .and_then(|()| pending.as_file().sync_all())
                    .map_err(|error| Error::incompatible(error.to_string()))?;
                if let Err(error) = pending.persist_noclobber(trigger) {
                    if error.error.kind() != std::io::ErrorKind::AlreadyExists {
                        return Err(Error::incompatible(error.error.to_string()));
                    }
                }
                let deadline =
                    std::time::Instant::now() + std::time::Duration::from_secs(30);
                while !Path::new(&killed).exists() {
                    if std::time::Instant::now() >= deadline {
                        return Err(Error::incompatible(
                            "supervised worker loss timed out",
                        ));
                    }
                    std::thread::sleep(std::time::Duration::from_millis(10));
                }
            }
        }
    }
    Ok(())
}

fn production_events_source(pipeline: &Pipeline) -> Result<PCollection<i32>, Error> {
    pipeline
        .unbounded_source_with_factory(
            "production-events",
            SourceProgress {
                stable_split_id: true,
                replay: Replay::AtLeastOnce,
                reports_event_time: true,
            },
            "prism-production-source-v1".into(),
            0,
            move |position| {
                await_supervised_source_replacement(position.as_deref())?;
                let (values, cursor, completed) = match position.as_deref() {
                    None => (vec![7, 9], 1, false),
                    Some([1]) => (vec![8], 2, true),
                    _ => return Err(Error::incompatible("unexpected production source cursor")),
                };
                let signal = ProgressSignal {
                    source_key: "production-events".into(),
                    split_id: "stable-split".into(),
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
        )
}

/// Replayable production source shared by value and bag state proofs.
fn production_state_source(pipeline: &Pipeline) -> Result<PCollection<(i32, i32)>, Error> {
    pipeline
        .unbounded_source_with_factory(
            "production-state-events",
            SourceProgress { stable_split_id: true, replay: Replay::AtLeastOnce, reports_event_time: true },
            "prism-production-state-source-v1".into(),
            0,
            move |position| {
                await_supervised_source_replacement(position.as_deref())?;
                let batches: Vec<(Vec<i32>, u8, bool)> = match position.as_deref() {
                    None => vec![(vec![7, 9], 1, false), (vec![8], 2, true)],
                    Some([1]) => vec![(vec![8], 2, true)],
                    _ => return Err(Error::incompatible("unexpected production state cursor")),
                };
                let polls = batches
                    .into_iter()
                    .flat_map(|(values, cursor, completed)| {
                        [
                            UnboundedPoll::Pending,
                            UnboundedPoll::Ready(UnboundedBatch {
                                elements: values
                                    .into_iter()
                                    .map(|value| Timestamped {
                                        value: (1_i32, value),
                                        timestamp_ns: i64::from(cursor) * 1_000_000,
                                    })
                                    .collect(),
                                progress: ProgressSignal {
                                    source_key: "production-state-events".into(),
                                    split_id: "stable-state-split".into(),
                                    checkpoint_position: Some(vec![cursor]),
                                    event_time_ns: Some(i64::from(cursor) * 1_000_000),
                                    completed,
                                },
                            }),
                        ]
                    })
                    .collect();
                Ok(Box::new(ProductionSource(polls))
                    as TypedUnboundedSourceReader<(i32, i32)>)
            },
        )
}

/// Production unbounded source feeding one keyed value cell and idempotent output.
pub fn production_value_state_fixture(
    pipeline: &Pipeline,
    directory: PathBuf,
) -> Result<Sink<String>, Error> {
    let sink_directory = directory.clone();
    production_state_source(pipeline)?
        .stateful_value_map_named::<i32, (i32, i32)>(
            "production-state-count",
            "prism-proof/production-key-count-state",
            Version { major: 1, minor: 0 },
            |_, value, prior| {
                let count = prior.unwrap_or(0) + 1;
                Ok((Some(count), (value, count)))
            },
        )?
        .map_named("production-state-label", |(key, (value, count))| format!("{key}:{value}:{count}"))?
        .sink_named_with_config(
            "production-state-result",
            SinkSpec { mode: SinkMode::Append, replay: Replay::Idempotent, commit: true },
            format!("production-value-state:{}", directory.display()),
            move || Ok(Box::new(IdempotentStateLabelSink { directory: sink_directory.clone(), pending: Vec::new() })),
        )
}

/// Production unbounded source feeding one keyed bag and idempotent output.
pub fn production_bag_state_fixture(
    pipeline: &Pipeline,
    directory: PathBuf,
) -> Result<Sink<String>, Error> {
    let sink_directory = directory.clone();
    production_state_source(pipeline)?
        .stateful_bag_map_named::<i32, (i32, i32)>(
            "production-state-count",
            "prism-proof/production-key-bag-state",
            Version { major: 1, minor: 0 },
            |_, value, prior: Vec<i32>| {
                let count = i32::try_from(prior.len()).expect("fixture bag fits i32") + 1;
                Ok((BagStateMutation::Append(vec![value]), (value, count)))
            },
        )?
        .map_named("production-state-label", |(key, (value, count))| format!("{key}:{value}:{count}"))?
        .sink_named_with_config(
            "production-state-result",
            SinkSpec { mode: SinkMode::Append, replay: Replay::Idempotent, commit: true },
            format!("production-bag-state:{}", directory.display()),
            move || Ok(Box::new(IdempotentStateLabelSink { directory: sink_directory.clone(), pending: Vec::new() })),
        )
}

/// Shared driver/worker graph for a production reader and idempotent sink.
pub fn production_stream_fixture(
    pipeline: &Pipeline,
    directory: impl AsRef<Path>,
) -> Result<(), Error> {
    let directory = directory.as_ref().to_path_buf();
    let sink_directory = directory.clone();
    let increment_directory = directory.clone();
    let timed = production_events_source(pipeline)?
        .fixed_windows("production-window", 2_000_000)?
        .reify_timestamps_named("production-time")?;
    timed
        .map_named("production-double", |(timestamp, value)| {
            (timestamp, value * 2)
        })?
        .sink_named_with_config(
            "production-capture",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            format!("idempotent-values:{}", directory.display()),
            move || {
                Ok(Box::new(IdempotentValueSink {
                    directory: sink_directory.clone(),
                    pending: Vec::new(),
                }))
            },
        )?;
    timed
        .map_named("production-increment", |(timestamp, value)| {
            (timestamp, value + 1)
        })?
        .sink_named_with_config(
            "production-capture-increment",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            format!("idempotent-increments:{}", directory.display()),
            move || {
                Ok(Box::new(IdempotentValueSink {
                    directory: increment_directory.clone(),
                    pending: Vec::new(),
                }))
            },
        )?;
    let final_values = timed
        .map_named("production-final-value", |(_, value)| value)?
        .map_with_semantic_named(
            "production-semantic-map",
            "prism-proof/production-identity",
            Version {
                major: 1,
                minor: u16::from(
                    std::env::var_os("IRONBEAM_V5_PRISM_SEMANTIC_VERSION_DRIFT").is_some(),
                ),
            },
            |value| value + 1,
        )?
        .map_named("production-semantic-restore", |value| value - 1)?
        .flat_map_with_semantic_named(
            "production-semantic-expand",
            "prism-proof/production-expand",
            Version { major: 1, minor: 0 },
            |value| vec![value, -value],
        )?
        .filter_with_semantic_named(
            "production-semantic-filter",
            "prism-proof/production-filter",
            Version { major: 1, minor: 0 },
            |value| *value > 0,
        )?
        .reshuffle_named("production-finalization-boundary")?;
    finalization_probe_sink(
        &final_values,
        "production-finalization",
        directory.join("fragments"),
    )?;
    let jsonl_staging = directory.join("jsonl-fragments");
    ironbeam_io_jsonl::jsonl_fragment_sink(
        &final_values,
        "production-jsonl-final",
        &jsonl_staging,
        &jsonl_staging.to_string_lossy(),
        "prism-run-fixture",
    )?;
    let object_storage: Arc<dyn ObjectIO> = Arc::new(
        LocalObjectIO::new(directory.join("object-results"))
            .map_err(|error| Error::incompatible(error.to_string()))?,
    );
    ironbeam_io_jsonl::cloud_jsonl_fragment_sink(
        &final_values,
        "production-object-jsonl-final",
        object_storage,
        ("stage", "production/jsonl"),
        ("output", "production.jsonl"),
        &directory.to_string_lossy(),
        "prism-run-fixture",
    )?;
    let protobuf_staging = directory.join("protobuf-fragments");
    let protobuf_values = final_values
        .map_named("production-protobuf-row", |value| ProductionProtoRow {
            value,
        })?
        .reshuffle_named("production-protobuf-boundary")?;
    ironbeam_io_protobuf::protobuf_fragment_sink(
        &protobuf_values,
        "production-protobuf-final",
        &protobuf_staging,
        &protobuf_staging.to_string_lossy(),
        "prism-run-fixture",
    )?;
    let avro_staging = directory.join("avro-fragments");
    let avro_values = final_values
        .map_named("production-avro-row", |id| ProductionAvroRow {
            id,
            label: format!("production-{id}"),
        })?
        .reshuffle_named("production-avro-boundary")?;
    ironbeam_io_avro::avro_fragment_sink(
        &avro_values,
        "production-avro-final",
        &avro_staging,
        &avro_staging.to_string_lossy(),
        "prism-run-fixture",
        PRODUCTION_AVRO_SCHEMA,
    )?;
    let tfrecord_staging = directory.join("tfrecord-fragments");
    let tfrecord_values = final_values
        .map_named("production-tfrecord-proto", |value| {
            prost_v5::Message::encode_to_vec(&ProductionProtoRow { value })
        })?
        .reshuffle_named("production-tfrecord-boundary")?;
    ironbeam_io_tfrecord::tfrecord_fragment_sink(
        &tfrecord_values,
        "production-tfrecord-final",
        &tfrecord_staging,
        &tfrecord_staging.to_string_lossy(),
        "prism-run-fixture",
    )?;
    let msgpack_staging = directory.join("msgpack-fragments");
    let msgpack_values = final_values.reshuffle_named("production-msgpack-boundary")?;
    ironbeam_io_msgpack::msgpack_fragment_sink(
        &msgpack_values,
        "production-msgpack-final",
        &msgpack_staging,
        &msgpack_staging.to_string_lossy(),
        "prism-run-fixture",
    )?;
    let cbor_staging = directory.join("cbor-fragments");
    let cbor_values = final_values
        .reshuffle_named("production-cbor-middle-boundary")?
        .reshuffle_named("production-cbor-boundary")?;
    ironbeam_io_cbor::cbor_fragment_sink(
        &cbor_values,
        "production-cbor-final",
        &cbor_staging,
        &cbor_staging.to_string_lossy(),
        "prism-run-fixture",
    )?;
    let grouped_directory = directory.clone();
    let folded_directory = directory.clone();
    let global_fold_directory = directory.clone();
    timed
        .map_named("production-global-values", |(_, value)| value)?
        .combine_globally_lifted_named("production-global-sum", ProductionSum)?
        .reify_timestamps_named("production-global-time")?
        .map_named("production-global-result", |(timestamp, sum)| {
            (timestamp, 50_000 + sum)
        })?
        .sink_named_with_config(
            "production-capture-global-fold",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            format!("idempotent-global-folds:{}", directory.display()),
            move || {
                Ok(Box::new(IdempotentValueSink {
                    directory: global_fold_directory.clone(),
                    pending: Vec::new(),
                }))
            },
        )?;
    let fold_pairs = timed.map_named("production-fold-pairs", |(_, value)| (value % 2, value))?;
    fold_pairs
        .combine_values_lifted_named("production-fold-sum", ProductionSum)?
        .reify_timestamps_named("production-fold-time")?
        .map_named("production-fold-result", |(timestamp, (key, sum))| {
            (timestamp, 30_000 + key * 100 + sum)
        })?
        .sink_named_with_config(
            "production-capture-fold",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            format!("idempotent-folds:{}", directory.display()),
            move || {
                Ok(Box::new(IdempotentValueSink {
                    directory: folded_directory.clone(),
                    pending: Vec::new(),
                }))
            },
        )?;
    let shared_group_directory = directory.clone();
    fold_pairs
        .group_by_key_named("production-fold-shared-group")?
        .reify_timestamps_named("production-fold-shared-time")?
        .map_named(
            "production-fold-shared-result",
            |(timestamp, (key, values))| {
                (
                    timestamp,
                    40_000 + key * 100 + values.into_iter().sum::<i32>(),
                )
            },
        )?
        .sink_named_with_config(
            "production-capture-fold-shared",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            format!("idempotent-fold-shared:{}", directory.display()),
            move || {
                Ok(Box::new(IdempotentValueSink {
                    directory: shared_group_directory.clone(),
                    pending: Vec::new(),
                }))
            },
        )?;
    timed
        .map_named("production-key", |(timestamp, value)| {
            (value % 2, (timestamp, value))
        })?
        .group_by_key_named("production-group")?
        .reify_timestamps_named("production-group-time")?
        .map_named("production-group-result", |(timestamp, (key, values))| {
            let count = i32::try_from(values.len()).expect("small production fixture group");
            let sum = values.into_iter().map(|(_, value)| value).sum::<i32>();
            (timestamp, 20_000 + count * 1_000 + key * 100 + sum)
        })?
        .sink_named_with_config(
            "production-capture-group",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            format!("idempotent-groups:{}", directory.display()),
            move || {
                Ok(Box::new(IdempotentValueSink {
                    directory: grouped_directory.clone(),
                    pending: Vec::new(),
                }))
            },
        )?;
    Ok(())
}
