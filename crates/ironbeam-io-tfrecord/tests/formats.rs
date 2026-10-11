use ironbeam_core::{Element, ErrorKind, Operation, Pipeline, Replay, SinkMode};
use ironbeam_direct::{DirectCollectExt, DirectRunner, JobConfig};
use ironbeam_io_tfrecord::*;
use prost::Message;
use std::fs;

#[test]
fn append_sink_keeps_complete_frames_and_declares_bundle_replay() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("rows.tfrecord");
    let seed = Pipeline::default()
        .from_vec("seed", vec![b"old".to_vec()])
        .unwrap();
    DirectRunner
        .run_sink(
            &tfrecord_sink(&seed, "write-seed", &path).unwrap(),
            &JobConfig::default(),
        )
        .unwrap();
    let pipeline = Pipeline::default();
    let input = pipeline
        .from_vec("input", vec![b"new".to_vec(), b"later".to_vec()])
        .unwrap();
    let sink = tfrecord_append_sink(&input, "append", &path).unwrap();
    let (graph, _) = pipeline.snapshot().unwrap();
    assert!(matches!(
        &graph.node("append").unwrap().operation,
        Operation::Sink { spec }
            if spec.mode == SinkMode::Append && spec.replay == Replay::AtLeastOnce && spec.commit
    ));
    DirectRunner.run_sink(&sink, &JobConfig::default()).unwrap();
    let output = tfrecord_source(&Pipeline::default(), "read", &path)
        .unwrap()
        .output("out")
        .unwrap();
    assert_eq!(
        output.collect_seq().unwrap(),
        [b"old".to_vec(), b"new".to_vec(), b"later".to_vec()]
    );
    assert_eq!(
        tfrecord_append_sink(&input, "compressed", dir.path().join("rows.tfrecord.gz"))
            .err()
            .unwrap()
            .kind,
        ErrorKind::Graph
    );

    let previous = fs::read(&path).unwrap();
    let failed = Pipeline::default()
        .from_vec("failed-input", vec![b"broken".to_vec()])
        .unwrap()
        .map_named("fail-before-commit", |_value| -> Vec<u8> {
            panic!("expected transform failure")
        })
        .unwrap();
    let failed_sink = tfrecord_append_sink(&failed, "failed-append", &path).unwrap();
    assert_eq!(
        DirectRunner
            .run_sink(&failed_sink, &JobConfig::default())
            .err()
            .unwrap()
            .kind,
        ErrorKind::Execution
    );
    assert_eq!(fs::read(&path).unwrap(), previous);
}

#[test]
fn append_sink_creates_parent_and_reports_unwritable_destination() {
    let dir = tempfile::tempdir().unwrap();
    let pipeline = Pipeline::default();
    let input = pipeline.from_vec("input", vec![b"one".to_vec()]).unwrap();
    let nested = dir.path().join("nested").join("rows.tfrecord");
    DirectRunner
        .run_sink(
            &tfrecord_append_sink(&input, "append-nested", &nested).unwrap(),
            &JobConfig::default(),
        )
        .unwrap();
    assert!(nested.exists());

    let destination_is_directory = dir.path().join("existing-directory");
    fs::create_dir(&destination_is_directory).unwrap();
    assert_eq!(
        DirectRunner
            .run_sink(
                &tfrecord_append_sink(&input, "append-directory", &destination_is_directory)
                    .unwrap(),
                &JobConfig::default(),
            )
            .err()
            .unwrap()
            .kind,
        ErrorKind::Io
    );
}

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

#[test]
fn tfrecord_rejects_corrupt_headers_and_lengths_in_both_read_modes() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("bad.tfrecord");
    let input = Pipeline::default()
        .from_vec("input", vec![b"one".to_vec()])
        .unwrap();
    DirectRunner
        .run_sink(
            &tfrecord_sink(&input, "write", &path).unwrap(),
            &JobConfig::default(),
        )
        .unwrap();
    let valid = fs::read(&path).unwrap();
    let mut bad_length_crc = valid.clone();
    bad_length_crc[8] ^= 1;
    let mut bad_data_crc = valid.clone();
    *bad_data_crc.last_mut().unwrap() ^= 1;
    let mut huge_length = u64::MAX.to_le_bytes().to_vec();
    let crc = crc32c::crc32c(&huge_length)
        .rotate_right(15)
        .wrapping_add(0xa282_ead8);
    huge_length.extend_from_slice(&crc.to_le_bytes());
    for bytes in [
        valid[..4].to_vec(),
        valid[..13].to_vec(),
        bad_length_crc,
        bad_data_crc,
        huge_length,
    ] {
        fs::write(&path, bytes).unwrap();
        let eager = tfrecord_source(&Pipeline::default(), "eager", &path)
            .unwrap()
            .output("out")
            .unwrap();
        let lazy = tfrecord_streaming_source(&Pipeline::default(), "lazy", &path)
            .unwrap()
            .output("out")
            .unwrap();
        assert_eq!(eager.collect_seq().unwrap_err().kind, ErrorKind::Io);
        assert_eq!(lazy.collect_seq().unwrap_err().kind, ErrorKind::Io);
    }
}

#[test]
fn tfrecord_examples_round_trip_eager_lazy_and_sharded() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("examples.tfrecord");
    let example = Example {
        features: Some(Features {
            feature: std::collections::BTreeMap::from([(
                "count".into(),
                Feature {
                    kind: Some(tfrecord_proto::feature::Kind::Int64List(Int64List {
                        value: vec![3, 5],
                    })),
                },
            )]),
        }),
    };
    let wire = example.encode_to_vec();
    let input = Pipeline::default()
        .from_vec("input", vec![wire.clone(), wire.clone()])
        .unwrap();
    DirectRunner
        .run_sink(
            &tfrecord_sink(&input, "write", &path).unwrap(),
            &JobConfig::default(),
        )
        .unwrap();
    for source in [
        tfrecord_examples_source(&Pipeline::default(), "eager", &path).unwrap(),
        tfrecord_examples_streaming_source(&Pipeline::default(), "lazy", &path).unwrap(),
        tfrecord_examples_sharded_source(&Pipeline::default(), "sharded", &path, 1).unwrap(),
    ] {
        let output = source.output("out").unwrap();
        assert_eq!(
            output.collect_seq().unwrap(),
            vec![example.clone(), example.clone()]
        );
    }
    assert_eq!(<Example as Element>::decode(&wire).unwrap(), example);
}

#[test]
fn tfrecord_examples_reject_invalid_protobuf_after_crc() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("bad.tfrecord");
    let input = Pipeline::default()
        .from_vec("input", vec![vec![0xff]])
        .unwrap();
    DirectRunner
        .run_sink(
            &tfrecord_sink(&input, "write", &path).unwrap(),
            &JobConfig::default(),
        )
        .unwrap();
    let output = tfrecord_examples_streaming_source(&Pipeline::default(), "read", &path)
        .unwrap()
        .output("out")
        .unwrap();
    assert_eq!(output.collect_seq().unwrap_err().kind, ErrorKind::Io);
}

#[test]
fn tfrecord_glob_reads_multiple_files() {
    let dir = tempfile::tempdir().unwrap();
    for id in [1_u8, 2] {
        let path = dir.path().join(format!("part-{id}.tfrecord"));
        let input = Pipeline::default()
            .from_vec("input", vec![vec![id]])
            .unwrap();
        DirectRunner
            .run_sink(
                &tfrecord_sink(&input, "write", &path).unwrap(),
                &JobConfig::default(),
            )
            .unwrap();
    }
    let pattern = dir.path().join("*.tfrecord");
    let output = tfrecord_glob_source(&Pipeline::default(), "glob", pattern.to_str().unwrap())
        .unwrap()
        .output("out")
        .unwrap();
    assert_eq!(output.collect_seq().unwrap(), vec![vec![1], vec![2]]);
}

#[cfg(feature = "parallel-io")]
#[test]
fn tfrecord_parallel_sink_rejects_zero_shards() {
    let input = Pipeline::default()
        .from_vec("input", vec![vec![1_u8]])
        .unwrap();
    let dir = tempfile::tempdir().unwrap();
    let error = tfrecord_parallel_sink(&input, "write", dir.path().join("bad.tfrecord"), 0)
        .err()
        .expect("zero shards must fail before writing");
    assert_eq!(error.kind, ErrorKind::Graph);
}
