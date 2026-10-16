/// Build provider-bound CBOR and MessagePack sources in the driver and worker.
pub fn portable_object_binary_collection(
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
    let cbor = ironbeam_io_cbor::cloud_cbor_streaming_uri_prefix_source::<ProductionAvroRow>(
        pipeline,
        "portable-object-cbor-source",
        &stores,
        "file://bucket/cbor/",
    )?
    .map_named("portable-object-cbor-row", |row| {
        format!("cbor-{}:{}", row.id, row.label)
    })?;
    let msgpack =
        ironbeam_io_msgpack::cloud_msgpack_streaming_uri_prefix_source::<ProductionAvroRow>(
            pipeline,
            "portable-object-msgpack-source",
            &stores,
            "file://bucket/msgpack/",
        )?
        .map_named("portable-object-msgpack-row", |row| {
            format!("msgpack-{}:{}", row.id, row.label)
        })?;
    cbor.flatten_named("portable-object-binary-flatten", &[msgpack])
}

/// Reopen provider-bound binary objects after replacing the execution child.
pub fn portable_object_binary_replay_fixture(
    pipeline: &Pipeline,
    object_root: &Path,
    directory: PathBuf,
) -> Result<Sink<String>, Error> {
    let sink_directory = directory.clone();
    portable_object_binary_collection(pipeline, object_root)?
        .flat_map_fallible_named("portable-object-binary-replay-row", |row| {
            if row == "cbor-1:one"
                && std::env::var_os("IRONBEAM_V5_PRISM_KILL_OBJECT_BINARY").is_some()
            {
                await_supervised_source_replacement(Some(&[1]))?;
            }
            Ok(vec![row])
        })?
        .sink_named_with_config(
            "portable-object-binary-replay-result",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            format!("portable-object-binary-replay:{}", directory.display()),
            move || {
                Ok(Box::new(IdempotentRowSink {
                    directory: sink_directory.clone(),
                    pending: Vec::new(),
                }))
            },
        )
}
