use ironbeam_core::{Element, ErrorKind, Pipeline};
use ironbeam_direct::{DirectCollectExt, DirectRunner, JobConfig};
use ironbeam_io_protobuf::*;
use std::fs;

#[derive(Clone, PartialEq, prost::Message)]
struct ProtoRow {
    #[prost(int32, tag = "1")]
    id: i32,
    #[prost(string, tag = "2")]
    name: String,
}
impl Element for ProtoRow {
    const TYPE_ID: &'static str = "tests/proto-row";
    const CODER_ID: &'static str = "tests/proto-row-protobuf";
}

#[cfg(feature = "compression-gzip")]
#[test]
fn gzip_round_trips_protobuf_lazy_and_sharded_sources() {
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
        proto_rows
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
        proto_rows
    );
}

#[cfg(feature = "parallel-io")]
#[test]
fn parallel_protobuf_sink_keeps_records_valid() {
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
        proto_rows
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
}

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

#[test]
fn protobuf_glob_reads_multiple_files_in_path_order() {
    let dir = tempfile::tempdir().unwrap();
    for id in [2, 1] {
        let path = dir.path().join(format!("part-{id}.pb"));
        let input = Pipeline::default()
            .from_vec(
                "input",
                vec![ProtoRow {
                    id,
                    name: format!("row-{id}"),
                }],
            )
            .unwrap();
        DirectRunner
            .run_sink(
                &protobuf_sink(&input, "write", &path).unwrap(),
                &JobConfig::default(),
            )
            .unwrap();
    }
    let pattern = dir.path().join("part-*.pb");
    let result =
        protobuf_glob_source::<ProtoRow>(&Pipeline::default(), "glob", pattern.to_str().unwrap())
            .unwrap()
            .output("out")
            .unwrap()
            .collect_seq()
            .unwrap();
    assert_eq!(
        result.iter().map(|row| row.id).collect::<Vec<_>>(),
        vec![1, 2]
    );
}

#[test]
fn protobuf_lazy_source_rejects_malformed_record_boundaries() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("bad.pb");
    for bytes in [
        vec![0x80],
        vec![0x80; 10],
        vec![0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x02],
        vec![5, 0],
        vec![1, 0xff],
    ] {
        fs::write(&path, bytes).unwrap();
        let output = protobuf_streaming_source::<ProtoRow>(&Pipeline::default(), "read", &path)
            .unwrap()
            .output("out")
            .unwrap();
        assert_eq!(output.collect_seq().unwrap_err().kind, ErrorKind::Io);
    }
}

#[cfg(feature = "parallel-io")]
#[test]
fn protobuf_parallel_sink_rejects_zero_shards() {
    let input = Pipeline::default()
        .from_vec(
            "input",
            vec![ProtoRow {
                id: 1,
                name: String::new(),
            }],
        )
        .unwrap();
    let dir = tempfile::tempdir().unwrap();
    let error = protobuf_parallel_sink(&input, "write", dir.path().join("rows.pb"), 0)
        .err()
        .expect("zero shards must fail before writing");
    assert_eq!(error.kind, ErrorKind::Graph);
}
