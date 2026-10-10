#![cfg(any(feature = "protobuf", feature = "tfrecord"))]
use ironbeam_core::{Element, ErrorKind, Pipeline};
use ironbeam_direct::{DirectRunner, JobConfig};
use ironbeam_io::*;
use std::fs;

#[cfg(all(
    feature = "compression-gzip",
    feature = "protobuf",
    feature = "tfrecord"
))]
#[test]
fn gzip_round_trips_protobuf_and_tfrecord_lazy_sources() {
    let dir = tempfile::tempdir().unwrap();
    let proto_path = dir.path().join("rows.pb.gz");
    let proto_rows = vec![
        ProtoRow {
            id: 1,
            name: "Ada".into(),
        },
        ProtoRow {
            id: 2,
            name: "Lin".into(),
        },
    ];
    let pipeline = Pipeline::default();
    let proto_input = pipeline.from_vec("proto", proto_rows.clone()).unwrap();
    DirectRunner
        .run_sink(
            &protobuf_sink(&proto_input, "proto-sink", &proto_path).unwrap(),
            &JobConfig::default(),
        )
        .unwrap();
    let reader = Pipeline::default();
    let proto_output = protobuf_streaming_source::<ProtoRow>(&reader, "proto-read", &proto_path)
        .unwrap()
        .output("proto-out")
        .unwrap();
    assert_eq!(
        DirectRunner
            .run_collect(&proto_output, &JobConfig::default())
            .unwrap()
            .0,
        proto_rows.clone()
    );
    let reader = Pipeline::default();
    let output = protobuf_sharded_source::<ProtoRow>(&reader, "shards", &proto_path, 2)
        .unwrap()
        .output("shard-out")
        .unwrap();
    assert_eq!(
        DirectRunner
            .run_collect(&output, &JobConfig::default())
            .unwrap()
            .0,
        proto_rows.clone()
    );

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

#[cfg(feature = "protobuf")]
#[derive(Clone, PartialEq, prost::Message)]
struct ProtoRow {
    #[prost(int32, tag = "1")]
    id: i32,
    #[prost(string, tag = "2")]
    name: String,
}
#[cfg(feature = "protobuf")]
impl Element for ProtoRow {
    const TYPE_ID: &'static str = "tests/proto-row";
    const CODER_ID: &'static str = "tests/proto-row-protobuf";
}
#[cfg(all(feature = "parallel-io", feature = "protobuf", feature = "tfrecord"))]
#[test]
fn parallel_protobuf_and_tfrecord_sinks_keep_records_valid() {
    let dir = tempfile::tempdir().unwrap();
    let proto_rows = (0..7)
        .map(|id| ProtoRow {
            id,
            name: format!("row-{id}"),
        })
        .collect::<Vec<_>>();
    let path = dir.path().join("rows.pb");
    let pipeline = Pipeline::default();
    let input = pipeline.from_vec("input", proto_rows.clone()).unwrap();
    DirectRunner
        .run_sink(
            &protobuf_parallel_sink(&input, "write", &path, 5).unwrap(),
            &JobConfig::default(),
        )
        .unwrap();
    let reader = Pipeline::default();
    let output = protobuf_source::<ProtoRow>(&reader, "read", &path)
        .unwrap()
        .output("out")
        .unwrap();
    assert_eq!(
        DirectRunner
            .run_collect(&output, &JobConfig::default())
            .unwrap()
            .0,
        proto_rows.clone()
    );
    let reader = Pipeline::default();
    let output = protobuf_sharded_source::<ProtoRow>(&reader, "shards", &path, 2)
        .unwrap()
        .output("shard-out")
        .unwrap();
    assert_eq!(
        DirectRunner
            .run_collect(&output, &JobConfig::default())
            .unwrap()
            .0,
        proto_rows
    );

    let records = (0..7).map(|id| vec![id as u8; id + 1]).collect::<Vec<_>>();
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
        records.clone()
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
#[cfg(feature = "protobuf")]
#[test]
fn protobuf_length_delimited_records_round_trip() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("rows.pb");
    let rows = vec![
        ProtoRow {
            id: 1,
            name: "Ada".into(),
        },
        ProtoRow {
            id: 2,
            name: "Lin".into(),
        },
    ];
    let p = Pipeline::default();
    let input = p.from_vec("input", rows.clone()).unwrap();
    let sink = protobuf_sink(&input, "sink", &path).unwrap();
    DirectRunner.run_sink(&sink, &JobConfig::default()).unwrap();
    let p = Pipeline::default();
    let output = protobuf_source::<ProtoRow>(&p, "source", &path)
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
    let lazy_output = protobuf_streaming_source::<ProtoRow>(&lazy_pipeline, "lazy", &path)
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
    fs::write(&path, [0xff_u8, 0xff, 0xff, 0xff, 0x7f]).unwrap();
    let invalid_pipeline = Pipeline::default();
    let invalid = protobuf_streaming_source::<ProtoRow>(&invalid_pipeline, "invalid", &path)
        .unwrap()
        .output("invalid-out")
        .unwrap();
    assert_eq!(
        DirectRunner
            .run_collect(&invalid, &JobConfig::default())
            .err()
            .unwrap()
            .kind,
        ErrorKind::Io
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
