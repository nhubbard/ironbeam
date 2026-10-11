//! Decode `TFRecord` bytes written by published Ironbeam 4 with the v5 adapter.

use ironbeam_core::{Element, ErrorKind, Pipeline};
use ironbeam_direct::{DirectRunner, JobConfig};
use ironbeam_io_tfrecord::{
    Example, tfrecord_examples_sharded_source, tfrecord_examples_source,
    tfrecord_examples_streaming_source,
};
use ironbeam_v4::io::tfrecord::write_tfrecord_vec;
use ironbeam_v4::io::tfrecord_proto as v4_proto;
use prost::Message;

#[test]
fn v5_tfrecord_example_sources_decode_v4_wire_records() {
    let mut fields = std::collections::BTreeMap::new();
    fields.insert(
        "count".to_owned(),
        v4_proto::Feature {
            kind: Some(v4_proto::feature::Kind::Int64List(v4_proto::Int64List {
                value: vec![3, 5],
            })),
        },
    );
    let old = v4_proto::Example {
        features: Some(v4_proto::Features { feature: fields }),
    };
    let wire = old.encode_to_vec();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("examples.tfrecord");
    write_tfrecord_vec(&path, &[wire.clone(), wire.clone()]).unwrap();
    let pipeline = Pipeline::default();
    let eager = tfrecord_examples_source(&pipeline, "eager", &path)
        .unwrap()
        .output("eager-out")
        .unwrap();
    let lazy = tfrecord_examples_streaming_source(&pipeline, "lazy", &path)
        .unwrap()
        .output("lazy-out")
        .unwrap();
    let sharded = tfrecord_examples_sharded_source(&pipeline, "shards", &path, 1)
        .unwrap()
        .output("shards-out")
        .unwrap();
    for output in [&eager, &lazy, &sharded] {
        let records = DirectRunner
            .run_collect(output, &JobConfig::default())
            .unwrap()
            .0;
        assert_eq!(records.len(), 2);
        for record in records {
            assert_eq!(Element::encode(&record).unwrap(), wire);
            let decoded = <Example as Element>::decode(&wire).unwrap();
            assert_eq!(record, decoded);
        }
    }
}

#[test]
fn v5_tfrecord_example_source_reports_bad_protobuf_after_crc_validation() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("bad.tfrecord");
    write_tfrecord_vec(&path, &[vec![0xff]]).unwrap();
    let pipeline = Pipeline::default();
    let output = tfrecord_examples_streaming_source(&pipeline, "bad", &path)
        .unwrap()
        .output("out")
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
