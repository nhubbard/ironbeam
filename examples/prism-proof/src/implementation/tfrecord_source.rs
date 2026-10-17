// A file-backed TFRecord source opened independently by the portable worker.

pub fn portable_tfrecord_collection(
    pipeline: &Pipeline,
    path: &Path,
) -> Result<PCollection<String>, Error> {
    ironbeam_io_tfrecord::tfrecord_streaming_source(pipeline, "portable-tfrecord-source", path)?
        .map_named("portable-tfrecord-text", |record| {
            String::from_utf8(record).expect("fixture contains UTF-8 records")
        })
}

pub fn portable_tfrecord_fixture(
    pipeline: &Pipeline,
    input: &Path,
    result: PathBuf,
) -> Result<Sink<String>, Error> {
    let sink_path = result.clone();
    portable_tfrecord_collection(pipeline, input)?.sink_named_with_config(
        "portable-tfrecord-result",
        SinkSpec {
            mode: SinkMode::Append,
            replay: Replay::AtLeastOnce,
            commit: true,
        },
        format!("portable-tfrecord-result:{}", result.display()),
        move || {
            Ok(Box::new(GlobalFoldSink {
                path: sink_path.clone(),
                values: Vec::new(),
            }))
        },
    )
}
