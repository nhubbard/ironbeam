use super::*;
use beam_proto::pipeline_v1::{
    atomic_type_value, field_type, field_value, AtomicType, AtomicTypeValue, Field, FieldType,
    FieldValue, LogicalType, LogicalTypeValue, Row, Schema,
};
use ironbeam_coder_row::{BeamRow, RowSchema};

struct DecimalRowSchema;

impl RowSchema for DecimalRowSchema {
    const TYPE_ID: &'static str = "tests/prism-beam-decimal-row-v1";

    fn schema() -> Schema {
        Schema {
            id: "16966849-998c-46b7-8045-0c8813e72343".into(),
            fields: vec![Field {
                name: "amount".into(),
                r#type: Some(FieldType {
                    type_info: Some(field_type::TypeInfo::LogicalType(Box::new(LogicalType {
                        urn: "beam:logical_type:decimal:v1".into(),
                        representation: Some(Box::new(FieldType {
                            type_info: Some(field_type::TypeInfo::AtomicType(
                                AtomicType::Bytes as i32,
                            )),
                            ..Default::default()
                        })),
                        ..Default::default()
                    }))),
                    ..Default::default()
                }),
                ..Default::default()
            }],
            ..Default::default()
        }
    }
}

pub fn beam_decimal_row_fixture(pipeline: &Pipeline, path: PathBuf) -> Result<Sink<String>, Error> {
    let sink_path = path.clone();
    pipeline
        .from_vec("beam-decimal-input", vec![1_i32, 2])?
        .map_named("beam-decimal-build", |id| {
            let decimal = if id == 1 { "1.25" } else { "-100.123" };
            BeamRow::<DecimalRowSchema>::new(Row {
                values: vec![FieldValue {
                    field_value: Some(field_value::FieldValue::LogicalTypeValue(Box::new(
                        LogicalTypeValue {
                            value: Some(Box::new(FieldValue {
                                field_value: Some(field_value::FieldValue::AtomicValue(
                                    AtomicTypeValue {
                                        value: Some(atomic_type_value::Value::Bytes(
                                            decimal.as_bytes().to_vec(),
                                        )),
                                    },
                                )),
                            })),
                        },
                    ))),
                }],
            })
            .expect("fixture decimal matches fixed schema")
        })?
        .map_named("beam-decimal-read", |row| {
            let Some(field_value::FieldValue::LogicalTypeValue(value)) =
                row.as_row().values[0].field_value.as_ref()
            else {
                panic!("decimal logical value missing");
            };
            let Some(field_value::FieldValue::AtomicValue(AtomicTypeValue {
                value: Some(atomic_type_value::Value::Bytes(bytes)),
            })) = value
                .value
                .as_ref()
                .and_then(|value| value.field_value.as_ref())
            else {
                panic!("decimal representation missing");
            };
            format!(
                "amount={}",
                std::str::from_utf8(bytes).expect("decimal text is UTF-8")
            )
        })?
        .sink_named_with_config(
            "beam-decimal-capture",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::AtLeastOnce,
                commit: true,
            },
            format!("beam-decimal:{}", path.display()),
            move || {
                Ok(Box::new(GlobalFoldSink {
                    path: sink_path.clone(),
                    values: Vec::new(),
                }))
            },
        )
}
