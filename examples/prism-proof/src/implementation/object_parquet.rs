// Provider-bound Parquet input used by the local portable worker proof.

pub fn portable_object_parquet_collection(
    pipeline: &Pipeline,
    object_root: &Path,
) -> Result<PCollection<String>, Error> {
    let mut stores = ironbeam_cloud_objects::ObjectStores::new();
    stores
        .register("file", LocalObjectIO::new(object_root).map_err(|error| {
            Error::incompatible(error.to_string())
        })?)
        .map_err(|error| Error::incompatible(error.to_string()))?;
    ironbeam_io_parquet::cloud_parquet_streaming_uri_prefix_source::<ProductionAvroRow>(
        pipeline,
        "portable-object-parquet-source",
        &stores,
        "file://bucket/input/",
    )?
    .map_named("portable-object-parquet-row", |row| {
        format!("{}:{}", row.id, row.label)
    })
}

pub fn portable_object_parquet_fixture(
    pipeline: &Pipeline,
    object_root: &Path,
    path: PathBuf,
) -> Result<Sink<String>, Error> {
    let sink_path = path.clone();
    portable_object_parquet_collection(pipeline, object_root)?.sink_named_with_config(
        "portable-object-parquet-result",
        SinkSpec {
            mode: SinkMode::Append,
            replay: Replay::AtLeastOnce,
            commit: true,
        },
        format!("portable-object-parquet-result:{}", path.display()),
        move || {
            Ok(Box::new(GlobalFoldSink {
                path: sink_path.clone(),
                values: Vec::new(),
            }))
        },
    )
}

/// Reopens a multi-object Parquet prefix after the execution child is replaced.
pub fn portable_object_parquet_replay_fixture(
    pipeline: &Pipeline,
    object_root: &Path,
    directory: PathBuf,
) -> Result<Sink<String>, Error> {
    let sink_directory = directory.clone();
    portable_object_parquet_collection(pipeline, object_root)?
        .flat_map_fallible_named("portable-object-parquet-replay-row", |row| {
            if row == "10:fifth"
                && std::env::var_os("IRONBEAM_V5_PRISM_KILL_OBJECT_PARQUET").is_some()
            {
                await_supervised_source_replacement(Some(&[1]))?;
            }
            Ok(vec![row])
        })?
        .sink_named_with_config(
            "portable-object-parquet-replay-result",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            format!("portable-object-parquet-replay:{}", directory.display()),
            move || {
                Ok(Box::new(IdempotentRowSink {
                    directory: sink_directory.clone(),
                    pending: Vec::new(),
                }))
            },
        )
}
