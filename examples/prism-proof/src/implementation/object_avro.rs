// Provider-bound Avro source used by the local portable worker proof.

pub const OBJECT_AVRO_READER_SCHEMA: &str = r#"{"type":"record","name":"ProductionAvroRow","fields":[{"name":"id","type":"int"},{"name":"label","type":"string"},{"name":"version","type":"int","default":5}]}"#;

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct EvolvedAvroRow {
    pub id: i32,
    pub label: String,
    pub version: i32,
}

impl_postcard_element!(EvolvedAvroRow, "tests/prism-object-avro-evolved-row/v1");

pub fn portable_object_avro_collection(
    pipeline: &Pipeline,
    object_root: &Path,
) -> Result<PCollection<String>, Error> {
    let mut stores = ironbeam_cloud_objects::ObjectStores::new();
    stores
        .register("file", LocalObjectIO::new(object_root).map_err(|error| {
            Error::incompatible(error.to_string())
        })?)
        .map_err(|error| Error::incompatible(error.to_string()))?;
    ironbeam_io_avro::cloud_avro_streaming_uri_source_with_reader_schema::<EvolvedAvroRow>(
        pipeline,
        "portable-object-avro-source",
        &stores,
        "file://bucket/input/rows.avro",
        OBJECT_AVRO_READER_SCHEMA,
    )?
    .map_named("portable-object-avro-row", |row| {
        format!("{}:{}:{}", row.id, row.label, row.version)
    })
}

pub fn portable_object_avro_fixture(
    pipeline: &Pipeline,
    object_root: &Path,
    path: PathBuf,
) -> Result<Sink<String>, Error> {
    let sink_path = path.clone();
    portable_object_avro_collection(pipeline, object_root)?.sink_named_with_config(
        "portable-object-avro-result",
        SinkSpec {
            mode: SinkMode::Append,
            replay: Replay::AtLeastOnce,
            commit: true,
        },
        format!("portable-object-avro-result:{}", path.display()),
        move || {
            Ok(Box::new(GlobalFoldSink {
                path: sink_path.clone(),
                values: Vec::new(),
            }))
        },
    )
}
