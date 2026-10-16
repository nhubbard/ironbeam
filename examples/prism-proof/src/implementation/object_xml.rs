/// Build the same provider-bound XML object set in the driver and worker.
pub fn portable_object_xml_collection(
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
    ironbeam_io_xml::cloud_xml_uri_prefix_source::<ProductionAvroRow>(
        pipeline,
        "portable-object-xml-source",
        &stores,
        "file://bucket/input/",
    )?
    .map_named("portable-object-xml-row", |row| {
        format!("{}:{}", row.id, row.label)
    })
}

/// Reopen XML objects after replacing the portable worker's execution child.
pub fn portable_object_xml_replay_fixture(
    pipeline: &Pipeline,
    object_root: &Path,
    directory: PathBuf,
) -> Result<Sink<String>, Error> {
    let sink_directory = directory.clone();
    portable_object_xml_collection(pipeline, object_root)?
        .flat_map_fallible_named("portable-object-xml-replay-row", |row| {
            if row == "1:one" && std::env::var_os("IRONBEAM_V5_PRISM_KILL_OBJECT_XML").is_some()
            {
                await_supervised_source_replacement(Some(&[1]))?;
            }
            Ok(vec![row])
        })?
        .sink_named_with_config(
            "portable-object-xml-replay-result",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            format!("portable-object-xml-replay:{}", directory.display()),
            move || {
                Ok(Box::new(IdempotentRowSink {
                    directory: sink_directory.clone(),
                    pending: Vec::new(),
                }))
            },
        )
}
