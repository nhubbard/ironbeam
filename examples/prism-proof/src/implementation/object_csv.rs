// Provider-bound streaming CSV input used by the local portable worker proof.

/// Build one fixed CSV object set in both the driver and worker.
pub fn portable_object_csv_collection(
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
    ironbeam_io_csv::cloud_csv_streaming_uri_prefix_source::<ProductionAvroRow>(
        pipeline,
        "portable-object-csv-source",
        &stores,
        "file://bucket/input/",
    )?
    .map_named("portable-object-csv-row", |row| {
        format!("{}:{}", row.id, row.label)
    })
}

/// Reopen a provider-bound CSV source after replacing its execution child.
pub fn portable_object_csv_replay_fixture(
    pipeline: &Pipeline,
    object_root: &Path,
    directory: PathBuf,
) -> Result<Sink<String>, Error> {
    let sink_directory = directory.clone();
    portable_object_csv_collection(pipeline, object_root)?
        .flat_map_fallible_named("portable-object-csv-replay-row", |row| {
            if row == "1:one" && std::env::var_os("IRONBEAM_V5_PRISM_KILL_OBJECT_CSV").is_some()
            {
                await_supervised_source_replacement(Some(&[1]))?;
            }
            Ok(vec![row])
        })?
        .sink_named_with_config(
            "portable-object-csv-replay-result",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            format!("portable-object-csv-replay:{}", directory.display()),
            move || {
                Ok(Box::new(IdempotentRowSink {
                    directory: sink_directory.clone(),
                    pending: Vec::new(),
                }))
            },
        )
}
