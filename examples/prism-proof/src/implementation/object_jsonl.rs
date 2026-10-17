// Provider-bound streaming JSONL source used by the local portable worker proof.

/// Build the same provider-bound JSONL graph in the driver and worker.
pub fn portable_object_jsonl_collection(
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
    portable_object_jsonl_collection_with_stores(pipeline, &stores)
}

/// Read the JSONL fixture through the registry supplied to the worker builder.
pub fn portable_object_jsonl_collection_with_stores(
    pipeline: &Pipeline,
    stores: &ironbeam_cloud_objects::ObjectStores,
) -> Result<PCollection<String>, Error> {
    ironbeam_io_jsonl::cloud_jsonl_streaming_uri_glob_source::<i32>(
        pipeline,
        "portable-object-jsonl-source",
        stores,
        "file://bucket/input/*.jsonl",
    )?
    .map_named("portable-object-jsonl-row", |value| format!("row-{value}"))
}

/// Reopen a provider-bound JSONL source after replacing its execution child.
pub fn portable_object_jsonl_replay_fixture(
    pipeline: &Pipeline,
    object_root: &Path,
    directory: PathBuf,
) -> Result<Sink<String>, Error> {
    let mut stores = ironbeam_cloud_objects::ObjectStores::new();
    stores
        .register(
            "file",
            LocalObjectIO::new(object_root)
                .map_err(|error| Error::incompatible(error.to_string()))?,
        )
        .map_err(|error| Error::incompatible(error.to_string()))?;
    portable_object_jsonl_replay_fixture_with_stores(pipeline, &stores, directory)
}

/// Build a replay fixture with the same registry used by the worker runtime.
pub fn portable_object_jsonl_replay_fixture_with_stores(
    pipeline: &Pipeline,
    stores: &ironbeam_cloud_objects::ObjectStores,
    directory: PathBuf,
) -> Result<Sink<String>, Error> {
    let sink_directory = directory.clone();
    portable_object_jsonl_collection_with_stores(pipeline, stores)?
        .flat_map_fallible_named("portable-object-jsonl-replay-row", |row| {
            if row == "row-1"
                && std::env::var_os("IRONBEAM_V5_PRISM_KILL_OBJECT_JSONL").is_some()
            {
                await_supervised_source_replacement(Some(&[1]))?;
            }
            Ok(vec![row])
        })?
        .sink_named_with_config(
            "portable-object-jsonl-replay-result",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            format!("portable-object-jsonl-replay:{}", directory.display()),
            move || {
                Ok(Box::new(IdempotentRowSink {
                    directory: sink_directory.clone(),
                    pending: Vec::new(),
                }))
            },
        )
}
