// Provider-bound Protobuf input used by the local portable worker proof.

/// Build one fixed Protobuf object set in both the driver and worker.
pub fn portable_object_protobuf_collection(
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
    ironbeam_io_protobuf::cloud_protobuf_streaming_uri_prefix_source::<ProductionProtoRow>(
        pipeline,
        "portable-object-protobuf-source",
        &stores,
        "file://bucket/input/",
    )?
    .map_named("portable-object-protobuf-text", |record| {
        format!("row-{}", record.value)
    })
}

/// Reopen a provider-bound Protobuf source after replacing its execution child.
pub fn portable_object_protobuf_replay_fixture(
    pipeline: &Pipeline,
    object_root: &Path,
    directory: PathBuf,
) -> Result<Sink<String>, Error> {
    let sink_directory = directory.clone();
    portable_object_protobuf_collection(pipeline, object_root)?
        .flat_map_fallible_named("portable-object-protobuf-replay-row", |row| {
            if row == "row-1" && std::env::var_os("IRONBEAM_V5_PRISM_KILL_OBJECT_PROTOBUF").is_some()
            {
                await_supervised_source_replacement(Some(&[1]))?;
            }
            Ok(vec![row])
        })?
        .sink_named_with_config(
            "portable-object-protobuf-replay-result",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            format!("portable-object-protobuf-replay:{}", directory.display()),
            move || {
                Ok(Box::new(IdempotentRowSink {
                    directory: sink_directory.clone(),
                    pending: Vec::new(),
                }))
            },
        )
}
