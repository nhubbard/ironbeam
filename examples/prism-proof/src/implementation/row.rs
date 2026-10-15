use super::*;
use beam_proto::pipeline_v1::{
    atomic_type_value, field_type, field_value, AtomicType, AtomicTypeValue, Field, FieldType,
    FieldValue, Row, Schema,
};
use ironbeam_harness::coders::row::{BeamRow, RowSchema};

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
