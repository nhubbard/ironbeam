// Provider-bound TFRecord input used by the local portable worker proof.

/// Build one fixed TFRecord object set in both the driver and worker.
pub fn portable_object_tfrecord_collection(
    pipeline: &Pipeline,
    object_root: &Path,
) -> Result<PCollection<String>, Error> {
    let mut stores = ironbeam_cloud_objects::ObjectStores::new();
    stores
        .register(
            "file",
            LocalObjectIO::new(object_root)
                .map_err(|error| Error::incompatible(error.to_string()))?,
        )
        .map_err(|error| Error::incompatible(error.to_string()))?;
    ironbeam_io_tfrecord::cloud_tfrecord_streaming_uri_prefix_source(
        pipeline,
        "portable-object-tfrecord-source",
        &stores,
        "file://bucket/input/",
    )?
    .flat_map_fallible_named("portable-object-tfrecord-text", |record| {
        String::from_utf8(record)
            .map(|text| vec![text])
            .map_err(|error| {
                Error::execution("portable-object-tfrecord-text", "decode", error.to_string())
            })
    })
}

/// Reopen a provider-bound TFRecord source after replacing its execution child.
pub fn portable_object_tfrecord_replay_fixture(
    pipeline: &Pipeline,
    object_root: &Path,
    directory: PathBuf,
) -> Result<Sink<String>, Error> {
    let sink_directory = directory.clone();
    portable_object_tfrecord_collection(pipeline, object_root)?
        .flat_map_fallible_named("portable-object-tfrecord-replay-row", |row| {
            if row == "one" && std::env::var_os("IRONBEAM_V5_PRISM_KILL_OBJECT_TFRECORD").is_some()
            {
                await_supervised_source_replacement(Some(&[1]))?;
            }
            Ok(vec![row])
        })?
        .sink_named_with_config(
            "portable-object-tfrecord-replay-result",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            format!("portable-object-tfrecord-replay:{}", directory.display()),
            move || {
                Ok(Box::new(IdempotentRowSink {
                    directory: sink_directory.clone(),
                    pending: Vec::new(),
                }))
            },
        )
}
