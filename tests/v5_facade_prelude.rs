#![cfg(feature = "v5-preview")]

use ironbeam::v5::{Pipeline, prelude::*};

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
        .output("result")
        .unwrap()
        .collect_seq()
        .unwrap();
    assert_eq!(result, vec![4]);
}

#[cfg(feature = "v5-io-avro")]
#[test]
fn facade_exposes_native_avro_without_v4_bridge() {
    use ironbeam::v5::{DirectRunner, JobConfig, avro_sink, avro_source};
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("values.avro");
    let schema = r#"{"type":"record","name":"Value","fields":[{"name":"value","type":"int"}]}"#;
    #[derive(Clone, serde::Serialize, serde::Deserialize, PartialEq, Debug)]
    struct Value {
        value: i32,
    }
    impl ironbeam::v5::Element for Value {
        const TYPE_ID: &'static str = "facade/value";
        const CODER_ID: &'static str = "facade/value";
    }
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
}
