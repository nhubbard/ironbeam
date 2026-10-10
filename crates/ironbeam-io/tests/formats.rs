#![cfg(feature = "tfrecord")]
use ironbeam_core::{ErrorKind, Pipeline};
use ironbeam_direct::{DirectRunner, JobConfig};
use ironbeam_io::*;
use std::fs;

#[cfg(feature = "compression-gzip")]
#[test]
fn gzip_round_trips_tfrecord_lazy_source() {
    let dir = tempfile::tempdir().unwrap();
    let tf_path = dir.path().join("rows.tfrecord.gz");
    let tf_rows = vec![b"first".to_vec(), b"second".to_vec()];
    let pipeline = Pipeline::default();
    let tf_input = pipeline.from_vec("tf", tf_rows.clone()).unwrap();
    DirectRunner
        .run_sink(
            &tfrecord_sink(&tf_input, "tf-sink", &tf_path).unwrap(),
            &JobConfig::default(),
        )
        .unwrap();
    let reader = Pipeline::default();
    let tf_output = tfrecord_streaming_source(&reader, "tf-read", &tf_path)
        .unwrap()
        .output("tf-out")
        .unwrap();
    assert_eq!(
        DirectRunner
            .run_collect(&tf_output, &JobConfig::default())
            .unwrap()
            .0,
        tf_rows
    );
}

#[cfg(feature = "parallel-io")]
#[test]
fn parallel_tfrecord_sink_keeps_records_valid() {
    let dir = tempfile::tempdir().unwrap();
    let records = (0_u8..7)
        .map(|id| vec![id; usize::from(id) + 1])
        .collect::<Vec<_>>();
    let path = dir.path().join("rows.tfrecord");
    let pipeline = Pipeline::default();
    let input = pipeline.from_vec("input", records.clone()).unwrap();
    DirectRunner
        .run_sink(
            &tfrecord_parallel_sink(&input, "write", &path, 5).unwrap(),
            &JobConfig::default(),
        )
        .unwrap();
    let reader = Pipeline::default();
    let output = tfrecord_source(&reader, "read", &path)
        .unwrap()
        .output("out")
        .unwrap();
    assert_eq!(
        DirectRunner
            .run_collect(&output, &JobConfig::default())
            .unwrap()
            .0,
        records
    );
    let reader = Pipeline::default();
    let output = tfrecord_sharded_source(&reader, "shards", &path, 2)
        .unwrap()
        .output("shard-out")
        .unwrap();
    assert_eq!(
        DirectRunner
            .run_collect(&output, &JobConfig::default())
            .unwrap()
            .0,
        records
    );
}

#[cfg(feature = "tfrecord")]
#[test]
fn tfrecord_crc_and_truncation_are_checked() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("rows.tfrecord");
    let rows = vec![b"one".to_vec(), b"two".to_vec()];
    let p = Pipeline::default();
    let input = p.from_vec("input", rows.clone()).unwrap();
    DirectRunner
        .run_sink(
            &tfrecord_sink(&input, "sink", &path).unwrap(),
            &JobConfig::default(),
        )
        .unwrap();
    let p = Pipeline::default();
    let output = tfrecord_source(&p, "source", &path)
        .unwrap()
        .output("out")
        .unwrap();
    assert_eq!(
        DirectRunner
            .run_collect(&output, &JobConfig::default())
            .unwrap()
            .0,
        rows
    );
    let lazy_pipeline = Pipeline::default();
    let lazy_output = tfrecord_streaming_source(&lazy_pipeline, "lazy", &path)
        .unwrap()
        .output("lazy-out")
        .unwrap();
    assert_eq!(
        DirectRunner
            .run_collect(&lazy_output, &JobConfig::default())
            .unwrap()
            .0,
        rows
    );

    let mut bad = fs::read(&path).unwrap();
    bad[12] ^= 1;
    fs::write(&path, bad).unwrap();
    let p = Pipeline::default();
    let output = tfrecord_source(&p, "bad-source", &path)
        .unwrap()
        .output("bad-out")
        .unwrap();
    assert_eq!(
        DirectRunner
            .run_collect(&output, &JobConfig::default())
            .err()
            .unwrap()
            .kind,
        ErrorKind::Io
    );
    let lazy_bad_pipeline = Pipeline::default();
    let lazy_bad = tfrecord_streaming_source(&lazy_bad_pipeline, "lazy-bad", &path)
        .unwrap()
        .output("lazy-bad-out")
        .unwrap();
    assert_eq!(
        DirectRunner
            .run_collect(&lazy_bad, &JobConfig::default())
            .err()
            .unwrap()
            .kind,
        ErrorKind::Io
    );
    let bytes = fs::read(&path).unwrap();
    fs::write(&path, &bytes[..7]).unwrap();
    let p = Pipeline::default();
    let output = tfrecord_source(&p, "truncated", &path)
        .unwrap()
        .output("truncated-out")
        .unwrap();
    assert_eq!(
        DirectRunner
            .run_collect(&output, &JobConfig::default())
            .err()
            .unwrap()
            .kind,
        ErrorKind::Io
    );
}
