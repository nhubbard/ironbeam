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
    struct Row { value: i32 }
    impl ironbeam::v5::Element for Row {
        const TYPE_ID: &'static str = "facade/xml-row";
        const CODER_ID: &'static str = "facade/xml-row";
    }
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("values.xml");
    let pipeline = Pipeline::default();
    let input = pipeline.from_vec("input", vec![Row { value: 1 }, Row { value: 2 }]).unwrap();
    DirectRunner.run_sink(&xml_sink(&input, "write", &path).unwrap(), &JobConfig::default()).unwrap();
    let output = xml_source::<Row>(&Pipeline::default(), "read", &path).unwrap().output("out").unwrap();
    assert_eq!(output.collect_seq().unwrap(), vec![Row { value: 1 }, Row { value: 2 }]);
}

#[cfg(feature = "v5-io-msgpack")]
#[test]
fn facade_exposes_isolated_msgpack_connector() {
    use ironbeam::v5::{DirectRunner, JobConfig, msgpack_sink, msgpack_source};
    #[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
    struct Row { value: i32 }
    impl ironbeam::v5::Element for Row {
        const TYPE_ID: &'static str = "facade/msgpack-row";
        const CODER_ID: &'static str = "facade/msgpack-row";
    }
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("values.msgpack");
    let pipeline = Pipeline::default();
    let input = pipeline.from_vec("input", vec![Row { value: 1 }, Row { value: 2 }]).unwrap();
    DirectRunner.run_sink(&msgpack_sink(&input, "write", &path).unwrap(), &JobConfig::default()).unwrap();
    let output = msgpack_source::<Row>(&Pipeline::default(), "read", &path).unwrap().output("out").unwrap();
    assert_eq!(output.collect_seq().unwrap(), vec![Row { value: 1 }, Row { value: 2 }]);
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

#[cfg(feature = "v5-io-parquet")]
#[test]
fn facade_exposes_parquet_without_v4_bridge() {
    use ironbeam::v5::{DirectRunner, JobConfig, parquet_sink, parquet_source};
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("values.parquet");
    #[derive(Clone, serde::Serialize, serde::Deserialize, PartialEq, Debug)]
    struct Value {
        value: i32,
    }
    impl ironbeam::v5::Element for Value {
        const TYPE_ID: &'static str = "facade/parquet-value";
        const CODER_ID: &'static str = "facade/parquet-value";
    }
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
