use apache_avro::{Schema, Writer, types::Value};
use ironbeam_core::{Element, ErrorKind, Pipeline};
use ironbeam_direct::{DirectRunner, JobConfig};
use ironbeam_io_avro::{avro_source_with_reader_schema, avro_streaming_source_with_reader_schema};
use serde::Deserialize;
use std::collections::{BTreeMap, HashMap};
use std::fs;

#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
struct Renamed {
    new_name: String,
}
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
struct NestedRows {
    items: Vec<Renamed>,
    labels: BTreeMap<String, Renamed>,
}
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
struct OptionalRow {
    nested: Option<Renamed>,
}
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
struct PrimaryWins {
    current: String,
    renamed: String,
}
impl Element for PrimaryWins {
    const TYPE_ID: &'static str = "tests/primary-field-wins";
    const CODER_ID: &'static str = "tests/primary-field-wins-json";
}
impl Element for OptionalRow {
    const TYPE_ID: &'static str = "tests/optional-alias-row";
    const CODER_ID: &'static str = "tests/optional-alias-row-json";
}
impl Element for NestedRows {
    const TYPE_ID: &'static str = "tests/nested-alias-rows";
    const CODER_ID: &'static str = "tests/nested-alias-rows-json";
}

const NESTED_WRITER: &str = r#"{"type":"record","name":"Outer","fields":[{"name":"items","type":{"type":"array","items":{"type":"record","name":"Item","fields":[{"name":"old_name","type":"string"}]}}},{"name":"labels","type":{"type":"map","values":{"type":"record","name":"Label","fields":[{"name":"old_name","type":"string"}]}}}]}"#;
const NESTED_READER: &str = r#"{"type":"record","name":"Outer","fields":[{"name":"items","type":{"type":"array","items":{"type":"record","name":"Item","fields":[{"name":"new_name","type":"string","aliases":["old_name"]}]}}},{"name":"labels","type":{"type":"map","values":{"type":"record","name":"Label","fields":[{"name":"new_name","type":"string","aliases":["old_name"]}]}}}]}"#;
const UNION_WRITER: &str = r#"{"type":"record","name":"Outer","fields":[{"name":"nested","type":["null",{"type":"record","name":"Inner","fields":[{"name":"old_name","type":"string"}]}]}]}"#;
const UNION_READER: &str = r#"{"type":"record","name":"Outer","fields":[{"name":"nested","type":["null",{"type":"record","name":"Inner","fields":[{"name":"new_name","type":"string","aliases":["old_name"]}]}]}]}"#;

fn write_value(path: &std::path::Path, schema: &str, value: Value) {
    let schema = Schema::parse_str(schema).unwrap();
    let mut writer = Writer::new(&schema, fs::File::create(path).unwrap()).unwrap();
    writer.append_value(value).unwrap();
    writer.flush().unwrap();
}

#[test]
fn nested_aliases_resolve_inside_arrays_and_maps() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("nested.avro");
    write_value(
        &path,
        NESTED_WRITER,
        Value::Record(vec![
            (
                "items".into(),
                Value::Array(vec![Value::Record(vec![(
                    "old_name".into(),
                    Value::String("array".into()),
                )])]),
            ),
            (
                "labels".into(),
                Value::Map(HashMap::from([(
                    "key".into(),
                    Value::Record(vec![("old_name".into(), Value::String("map".into()))]),
                )])),
            ),
        ]),
    );
    let expected = vec![NestedRows {
        items: vec![Renamed {
            new_name: "array".into(),
        }],
        labels: BTreeMap::from([(
            "key".into(),
            Renamed {
                new_name: "map".into(),
            },
        )]),
    }];
    for streaming in [false, true] {
        let pipeline = Pipeline::default();
        let source = if streaming {
            avro_streaming_source_with_reader_schema::<NestedRows>(
                &pipeline,
                "nested",
                &path,
                NESTED_READER,
            )
        } else {
            avro_source_with_reader_schema::<NestedRows>(&pipeline, "nested", &path, NESTED_READER)
        };
        let output = source.unwrap().output("output").unwrap();
        assert_eq!(
            DirectRunner
                .run_collect(&output, &JobConfig::default())
                .unwrap()
                .0,
            expected
        );
    }
}

#[test]
fn aliases_resolve_inside_union_records() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("union.avro");
    write_value(
        &path,
        UNION_WRITER,
        Value::Record(vec![(
            "nested".into(),
            Value::Union(
                1,
                Box::new(Value::Record(vec![(
                    "old_name".into(),
                    Value::String("union".into()),
                )])),
            ),
        )]),
    );
    for streaming in [false, true] {
        let pipeline = Pipeline::default();
        let source = if streaming {
            avro_streaming_source_with_reader_schema::<OptionalRow>(
                &pipeline,
                "union",
                &path,
                UNION_READER,
            )
        } else {
            avro_source_with_reader_schema::<OptionalRow>(&pipeline, "union", &path, UNION_READER)
        };
        let output = source.unwrap().output("output").unwrap();
        assert_eq!(
            DirectRunner
                .run_collect(&output, &JobConfig::default())
                .unwrap()
                .0,
            vec![OptionalRow {
                nested: Some(Renamed {
                    new_name: "union".into()
                })
            }]
        );
    }
}

#[test]
fn ambiguous_field_aliases_fail_closed() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("ambiguous.avro");
    let writer = r#"{"type":"record","name":"Row","fields":[{"name":"old_a","type":"string"},{"name":"old_b","type":"string"}]}"#;
    let reader = r#"{"type":"record","name":"Row","fields":[{"name":"new_name","type":"string","aliases":["old_a","old_b"]}]}"#;
    write_value(
        &path,
        writer,
        Value::Record(vec![
            ("old_a".into(), Value::String("a".into())),
            ("old_b".into(), Value::String("b".into())),
        ]),
    );
    let pipeline = Pipeline::default();
    let output = avro_source_with_reader_schema::<String>(&pipeline, "ambiguous", &path, reader)
        .unwrap()
        .output("output")
        .unwrap();
    let error = DirectRunner
        .run_collect(&output, &JobConfig::default())
        .err()
        .unwrap();
    assert_eq!(error.kind, ErrorKind::Io);
    assert!(error.message.contains("several writer fields match alias"));
}

#[test]
fn primary_reader_field_wins_over_another_fields_alias() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("primary.avro");
    let writer = r#"{"type":"record","name":"Row","fields":[{"name":"current","type":"string"}]}"#;
    let reader = r#"{"type":"record","name":"Row","fields":[{"name":"current","type":"string"},{"name":"renamed","type":"string","aliases":["current"],"default":"fallback"}]}"#;
    write_value(
        &path,
        writer,
        Value::Record(vec![("current".into(), Value::String("original".into()))]),
    );
    let pipeline = Pipeline::default();
    let output = avro_source_with_reader_schema::<PrimaryWins>(&pipeline, "primary", &path, reader)
        .unwrap()
        .output("output")
        .unwrap();
    assert_eq!(
        DirectRunner
            .run_collect(&output, &JobConfig::default())
            .unwrap()
            .0,
        vec![PrimaryWins {
            current: "original".into(),
            renamed: "fallback".into()
        }]
    );
}
