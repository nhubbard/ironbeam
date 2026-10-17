use super::*;
use beam_proto::pipeline_v1::{
    atomic_type_value, field_type, field_value, AtomicType, AtomicTypeValue, Field, FieldType,
    FieldValue, Row, Schema,
};
use ironbeam_coder_row::{BeamRow, RowSchema};

struct ProofRowSchema;

impl RowSchema for ProofRowSchema {
    const TYPE_ID: &'static str = "tests/prism-beam-row-v1";

    fn schema() -> Schema {
        Schema {
            id: "94195347-f63e-4381-8caa-8e88f2714660".into(),
            fields: vec![Field {
                name: "id".into(),
                r#type: Some(FieldType {
                    nullable: false,
                    type_info: Some(field_type::TypeInfo::AtomicType(AtomicType::Int32 as i32)),
                }),
                ..Default::default()
            }],
            ..Default::default()
        }
    }
}

pub fn beam_row_fixture(pipeline: &Pipeline, path: PathBuf) -> Result<Sink<String>, Error> {
    let sink_path = path.clone();
    pipeline
        .from_vec("beam-row-input", vec![1_i32, 2])?
        .map_named("beam-row-build", |id| {
            BeamRow::<ProofRowSchema>::new(Row {
                values: vec![FieldValue {
                    field_value: Some(field_value::FieldValue::AtomicValue(AtomicTypeValue {
                        value: Some(atomic_type_value::Value::Int32(id)),
                    })),
                }],
            })
            .expect("fixture row matches fixed schema")
        })?
        .map_named("beam-row-read", |row| {
            let Some(field_value::FieldValue::AtomicValue(AtomicTypeValue {
                value: Some(atomic_type_value::Value::Int32(id)),
            })) = row.as_row().values[0].field_value.as_ref()
            else {
                panic!("row coder changed the fixture value");
            };
            format!("id={id}")
        })?
        .sink_named_with_config(
            "beam-row-capture",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::AtLeastOnce,
                commit: true,
            },
            format!("beam-row:{}", path.display()),
            move || {
                Ok(Box::new(GlobalFoldSink {
                    path: sink_path.clone(),
                    values: Vec::new(),
                }))
            },
        )
}

pub fn beam_row_stream_fixture(pipeline: &Pipeline, path: PathBuf) -> Result<Sink<String>, Error> {
    let row = |id| {
        BeamRow::<ProofRowSchema>::new(Row {
            values: vec![FieldValue {
                field_value: Some(field_value::FieldValue::AtomicValue(AtomicTypeValue {
                    value: Some(atomic_type_value::Value::Int32(id)),
                })),
            }],
        })
        .expect("fixture row matches fixed schema")
    };
    let sink_path = path.clone();
    pipeline
        .test_stream_source(
            "beam-row-stream",
            vec![
                TestStreamEvent::Elements(vec![(row(1), 1_000_000), (row(2), 2_000_000)]),
                TestStreamEvent::AdvanceWatermark(10_000_000),
                TestStreamEvent::Elements(vec![(row(3), 11_000_000)]),
                TestStreamEvent::AdvanceWatermark(20_000_000),
            ],
        )?
        .fixed_windows("beam-row-stream-window", 10_000_000)?
        .map_named("beam-row-stream-key", |row| ("a".to_owned(), row))?
        .group_by_key_named("beam-row-stream-group")?
        .reify_timestamps_named("beam-row-stream-time")?
        .map_named("beam-row-stream-output", |(timestamp, (key, values))| {
            let mut ids = values
                .iter()
                .map(|row| {
                    let Some(field_value::FieldValue::AtomicValue(AtomicTypeValue {
                        value: Some(atomic_type_value::Value::Int32(id)),
                    })) = row.as_row().values[0].field_value.as_ref()
                    else {
                        panic!("row coder changed the fixture value");
                    };
                    *id
                })
                .collect::<Vec<_>>();
            ids.sort_unstable();
            format!("{key}:{timestamp}:{ids:?}")
        })?
        .sink_named_with_config(
            "beam-row-stream-capture",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            format!("beam-row-stream:{}", path.display()),
            move || {
                Ok(Box::new(IdempotentRowSink {
                    directory: sink_path.clone(),
                    pending: Vec::new(),
                }))
            },
        )
}
