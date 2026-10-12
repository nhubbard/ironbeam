use ironbeam::v5::{Pipeline, Version, prelude::*};

#[test]
fn facade_runs_versioned_semantic_map() {
    let values = Pipeline::default()
        .from_vec("source", vec![2_i32, 4])
        .unwrap()
        .map_with_semantic_named(
            "scale",
            "user/scale",
            Version { major: 1, minor: 0 },
            |value| value * 3,
        )
        .unwrap()
        .collect_seq()
        .unwrap();
    assert_eq!(values, [6, 12]);
}

#[test]
fn facade_prelude_runs_v4_shaped_helper_chain_without_compat() {
    let pipeline = Pipeline::default();
    let result = pipeline
        .from_vec("numbers", vec![1_i32, 2, 3, 4])
        .unwrap()
        .map_named("double", |value| value * 2)
        .unwrap()
        .filter_eq_named("fours", &4)
        .unwrap()
        .collect_seq()
        .unwrap();
    assert_eq!(result, vec![4]);
}

#[test]
fn facade_prelude_composes_keyed_aggregates_and_a_join() {
    let pipeline = Pipeline::default();
    let left = pipeline
        .from_vec(
            "left",
            vec![
                ("a".to_owned(), 2_i32),
                ("a".to_owned(), 3),
                ("b".to_owned(), 4),
            ],
        )
        .unwrap()
        .sum_per_key_named("left-sum")
        .unwrap();
    let right = pipeline
        .from_vec("right", vec![("a".to_owned(), 7_i32), ("b".to_owned(), 8)])
        .unwrap()
        .sum_per_key_named("right-sum")
        .unwrap();
    let mut totals = left
        .join_inner_named("join", &right)
        .unwrap()
        .map_named("add", |(key, (left, right))| (key, left + right))
        .unwrap()
        .collect_seq()
        .unwrap();
    totals.sort();
    assert_eq!(totals, [("a".to_owned(), 12), ("b".to_owned(), 12)]);
}

#[cfg(feature = "cloud")]
#[test]
fn facade_cloud_module_routes_provider_neutral_objects() {
    use ironbeam::v5::{
        cloud::{LocalObjectIO, ObjectStores},
        expand_cloud_glob, read_cloud_jsonl_vec, write_cloud_jsonl_vec,
    };

    #[derive(Debug, PartialEq, serde::Deserialize, serde::Serialize)]
    struct Row {
        id: u32,
    }

    let directory = tempfile::tempdir().unwrap();
    let mut stores = ObjectStores::new();
    stores
        .register("local", LocalObjectIO::new(directory.path()).unwrap())
        .unwrap();
    stores
        .write("local://bucket/rows.jsonl", b"{\"id\":1}\n")
        .unwrap();
    assert_eq!(
        stores.read("local://bucket/rows.jsonl").unwrap(),
        b"{\"id\":1}\n"
    );
    let provider = stores.provider("local").unwrap();
    assert_eq!(
        read_cloud_jsonl_vec::<Row, _>(&*provider, "bucket", "rows.jsonl").unwrap(),
        [Row { id: 1 }]
    );
    write_cloud_jsonl_vec(&*provider, "bucket", "other.jsonl", &[Row { id: 2 }]).unwrap();
    assert_eq!(
        expand_cloud_glob(&*provider, "bucket", "*.jsonl").unwrap(),
        ["other.jsonl", "rows.jsonl"]
    );
}

#[cfg(feature = "v5-regex")]
#[test]
fn facade_prelude_exposes_isolated_regex_helpers() {
    let result = Pipeline::default()
        .from_vec("input", vec!["a=7".to_owned(), "other".to_owned()])
        .unwrap()
        .regex_extract_named("extract", r"\d+", 0)
        .unwrap()
        .output("out")
        .unwrap()
        .collect_seq()
        .unwrap();
    assert_eq!(result, ["7"]);
}

#[cfg(feature = "v5-approx")]
#[test]
fn facade_prelude_exposes_isolated_approximate_distinct_helpers() {
    let result = Pipeline::default()
        .from_vec("input", vec![1_u32, 1, 2, 3])
        .unwrap()
        .approx_count_distinct_named("estimate")
        .unwrap()
        .output("out")
        .unwrap()
        .collect_seq()
        .unwrap();
    assert_eq!(result, [3]);
}

#[test]
fn facade_exposes_isolated_jsonl_connector() {
    use ironbeam::v5::{DirectRunner, JobConfig, jsonl_sink, jsonl_source};
    #[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
    struct Row {
        value: i32,
    }
    impl ironbeam::v5::Element for Row {
        const TYPE_ID: &'static str = "facade/jsonl-row";
        const CODER_ID: &'static str = "facade/jsonl-row";
    }
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("values.jsonl");
    let pipeline = Pipeline::default();
    let input = pipeline
        .from_vec("input", vec![Row { value: 1 }, Row { value: 2 }])
        .unwrap();
    DirectRunner
        .run_sink(
            &jsonl_sink(&input, "write", &path).unwrap(),
            &JobConfig::default(),
        )
        .unwrap();
    let output = jsonl_source::<Row>(&Pipeline::default(), "read", &path)
        .unwrap()
        .output("out")
        .unwrap();
    assert_eq!(
        output.collect_seq().unwrap(),
        vec![Row { value: 1 }, Row { value: 2 }]
    );
}

#[cfg(feature = "v5-io-protobuf")]
#[test]
fn facade_exposes_isolated_protobuf_connector() {
    use ironbeam::v5::{DirectRunner, Element, JobConfig, protobuf_sink, protobuf_source};
    #[derive(Clone, PartialEq, prost::Message)]
    struct Row {
        #[prost(int32, tag = "1")]
        value: i32,
    }
    impl Element for Row {
        const TYPE_ID: &'static str = "facade/protobuf-row";
        const CODER_ID: &'static str = "facade/protobuf-row";
    }
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("values.pb");
    let input = Pipeline::default()
        .from_vec("input", vec![Row { value: 1 }, Row { value: 2 }])
        .unwrap();
    DirectRunner
        .run_sink(
            &protobuf_sink(&input, "write", &path).unwrap(),
            &JobConfig::default(),
        )
        .unwrap();
    let output = protobuf_source::<Row>(&Pipeline::default(), "read", &path)
        .unwrap()
        .output("out")
        .unwrap();
    assert_eq!(
        output.collect_seq().unwrap(),
        vec![Row { value: 1 }, Row { value: 2 }]
    );
}

#[cfg(feature = "v5-io-tfrecord")]
#[test]
fn facade_exposes_isolated_tfrecord_connector() {
    use ironbeam::v5::{DirectRunner, JobConfig, tfrecord_sink, tfrecord_source};
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("values.tfrecord");
    let values = vec![b"first".to_vec(), b"second".to_vec()];
    let input = Pipeline::default()
        .from_vec("input", values.clone())
        .unwrap();
    DirectRunner
        .run_sink(
            &tfrecord_sink(&input, "write", &path).unwrap(),
            &JobConfig::default(),
        )
        .unwrap();
    let output = tfrecord_source(&Pipeline::default(), "read", &path)
        .unwrap()
        .output("out")
        .unwrap();
    assert_eq!(output.collect_seq().unwrap(), values);
}

#[cfg(feature = "v5-io-csv")]
#[test]
fn facade_exposes_isolated_csv_connector() {
    use ironbeam::v5::{DirectRunner, JobConfig, csv_sink, csv_source};
    #[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
    struct Row {
        value: i32,
    }
    impl ironbeam::v5::Element for Row {
        const TYPE_ID: &'static str = "facade/csv-row";
        const CODER_ID: &'static str = "facade/csv-row";
    }
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("values.csv");
    let pipeline = Pipeline::default();
    let input = pipeline
        .from_vec("input", vec![Row { value: 1 }, Row { value: 2 }])
        .unwrap();
    let sink = csv_sink(&input, "write", &path).unwrap();
    DirectRunner.run_sink(&sink, &JobConfig::default()).unwrap();
    let pipeline = Pipeline::default();
    let output = csv_source::<Row>(&pipeline, "read", &path)
        .unwrap()
        .output("out")
        .unwrap();
    assert_eq!(
        output.collect_seq().unwrap(),
        vec![Row { value: 1 }, Row { value: 2 }]
    );
}

#[cfg(feature = "v5-io-xml")]
#[test]
fn facade_exposes_isolated_xml_connector() {
    use ironbeam::v5::{DirectRunner, JobConfig, xml_sink, xml_source};
    #[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
    struct Row {
        value: i32,
    }
    impl ironbeam::v5::Element for Row {
        const TYPE_ID: &'static str = "facade/xml-row";
        const CODER_ID: &'static str = "facade/xml-row";
    }
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("values.xml");
    let pipeline = Pipeline::default();
    let input = pipeline
        .from_vec("input", vec![Row { value: 1 }, Row { value: 2 }])
        .unwrap();
    DirectRunner
        .run_sink(
            &xml_sink(&input, "write", &path).unwrap(),
            &JobConfig::default(),
        )
        .unwrap();
    let output = xml_source::<Row>(&Pipeline::default(), "read", &path)
        .unwrap()
        .output("out")
        .unwrap();
    assert_eq!(
        output.collect_seq().unwrap(),
        vec![Row { value: 1 }, Row { value: 2 }]
    );
}

#[cfg(feature = "v5-io-msgpack")]
#[test]
fn facade_exposes_isolated_msgpack_connector() {
    use ironbeam::v5::{DirectRunner, JobConfig, msgpack_sink, msgpack_source};
    #[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
    struct Row {
        value: i32,
    }
    impl ironbeam::v5::Element for Row {
        const TYPE_ID: &'static str = "facade/msgpack-row";
        const CODER_ID: &'static str = "facade/msgpack-row";
    }
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("values.msgpack");
    let pipeline = Pipeline::default();
    let input = pipeline
        .from_vec("input", vec![Row { value: 1 }, Row { value: 2 }])
        .unwrap();
    DirectRunner
        .run_sink(
            &msgpack_sink(&input, "write", &path).unwrap(),
            &JobConfig::default(),
        )
        .unwrap();
    let output = msgpack_source::<Row>(&Pipeline::default(), "read", &path)
        .unwrap()
        .output("out")
        .unwrap();
    assert_eq!(
        output.collect_seq().unwrap(),
        vec![Row { value: 1 }, Row { value: 2 }]
    );
}

#[cfg(feature = "v5-io-cbor")]
#[test]
fn facade_exposes_isolated_cbor_connector() {
    use ironbeam::v5::{DirectRunner, JobConfig, cbor_sink, cbor_source};
    #[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
    struct Row {
        value: i32,
    }
    impl ironbeam::v5::Element for Row {
        const TYPE_ID: &'static str = "facade/cbor-row";
        const CODER_ID: &'static str = "facade/cbor-row";
    }
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("values.cbor");
    let pipeline = Pipeline::default();
    let input = pipeline
        .from_vec("input", vec![Row { value: 1 }, Row { value: 2 }])
        .unwrap();
    DirectRunner
        .run_sink(
            &cbor_sink(&input, "write", &path).unwrap(),
            &JobConfig::default(),
        )
        .unwrap();
    let output = cbor_source::<Row>(&Pipeline::default(), "read", &path)
        .unwrap()
        .output("out")
        .unwrap();
    assert_eq!(
        output.collect_seq().unwrap(),
        vec![Row { value: 1 }, Row { value: 2 }]
    );
}

#[cfg(feature = "v5-io-avro")]
#[test]
fn facade_exposes_native_avro_without_v4_bridge() {
    use ironbeam::v5::{DirectRunner, JobConfig, avro_sink, avro_source};
    #[derive(Clone, serde::Serialize, serde::Deserialize, PartialEq, Debug)]
    struct Value {
        value: i32,
    }
    impl ironbeam::v5::Element for Value {
        const TYPE_ID: &'static str = "facade/value";
        const CODER_ID: &'static str = "facade/value";
    }
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("values.avro");
    let schema = r#"{"type":"record","name":"Value","fields":[{"name":"value","type":"int"}]}"#;
    let pipeline = Pipeline::default();
    let input = pipeline
        .from_vec("input", vec![Value { value: 3 }])
        .unwrap();
    let writer = avro_sink(&input, "write", &path, schema).unwrap();
    DirectRunner
        .run_sink(&writer, &JobConfig::default())
        .unwrap();
    let pipeline = Pipeline::default();
    let output = avro_source::<Value>(&pipeline, "read", &path)
        .unwrap()
        .output("out")
        .unwrap();
    assert_eq!(output.collect_seq().unwrap(), vec![Value { value: 3 }]);

    #[cfg(feature = "parallel-io")]
    {
        let parallel_path = dir.path().join("parallel.avro");
        let writer =
            ironbeam::v5::avro_parallel_sink(&input, "write-parallel", &parallel_path, schema, 2)
                .unwrap();
        DirectRunner
            .run_sink(&writer, &JobConfig::default())
            .unwrap();
        let output = avro_source::<Value>(&Pipeline::default(), "read-parallel", &parallel_path)
            .unwrap()
            .output("parallel-out")
            .unwrap();
        assert_eq!(output.collect_seq().unwrap(), vec![Value { value: 3 }]);
    }
}

#[cfg(feature = "v5-io-parquet")]
#[test]
fn facade_exposes_parquet_without_v4_bridge() {
    use ironbeam::v5::{DirectRunner, JobConfig, parquet_sink, parquet_source};
    #[derive(Clone, serde::Serialize, serde::Deserialize, PartialEq, Debug)]
    struct Value {
        value: i32,
    }
    impl ironbeam::v5::Element for Value {
        const TYPE_ID: &'static str = "facade/parquet-value";
        const CODER_ID: &'static str = "facade/parquet-value";
    }
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("values.parquet");
    let pipeline = Pipeline::default();
    let input = pipeline
        .from_vec("input", vec![Value { value: 3 }])
        .unwrap();
    let writer = parquet_sink(&input, "write", &path).unwrap();
    DirectRunner
        .run_sink(&writer, &JobConfig::default())
        .unwrap();
    let pipeline = Pipeline::default();
    let output = parquet_source::<Value>(&pipeline, "read", &path)
        .unwrap()
        .output("out")
        .unwrap();
    assert_eq!(output.collect_seq().unwrap(), vec![Value { value: 3 }]);
}
