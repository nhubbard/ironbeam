use super::*;
use beam_proto::pipeline_v1::{
    atomic_type_value, field_type, field_value, AtomicType, AtomicTypeValue, Field, FieldType,
    FieldValue, Row, Schema,
};
use ironbeam_coder_row::{BeamRow, RowSchema};

struct OldRow;
struct NewRow;

fn field(name: &str, kind: AtomicType, position: i32, nullable: bool) -> Field {
    Field {
        name: name.into(),
        encoding_position: position,
        r#type: Some(FieldType {
            nullable,
            type_info: Some(field_type::TypeInfo::AtomicType(kind as i32)),
        }),
        ..Default::default()
    }
}

impl RowSchema for OldRow {
    const TYPE_ID: &'static str = "tests/prism-row-projection-old/v1";

    fn schema() -> Schema {
        Schema {
            id: "2e805783-88c7-4c55-9fd1-f2ce10a8acf5".into(),
            encoding_positions_set: true,
            fields: vec![
                field("id", AtomicType::Int32, 0, false),
                field("label", AtomicType::String, 1, false),
            ],
            ..Default::default()
        }
    }
}

impl RowSchema for NewRow {
    const TYPE_ID: &'static str = "tests/prism-row-projection-new/v2";

    fn schema() -> Schema {
        Schema {
            id: "ee0a2874-3bbb-4545-a333-8f39fc55f83f".into(),
            encoding_positions_set: true,
            fields: vec![
                field("label", AtomicType::String, 1, false),
                field("id", AtomicType::Int32, 0, false),
                field("note", AtomicType::String, 2, true),
            ],
            ..Default::default()
        }
    }
}

pub fn beam_row_projection_fixture(
    pipeline: &Pipeline,
    path: PathBuf,
) -> Result<Sink<String>, Error> {
    let sink_path = path.clone();
    pipeline
        .from_vec("beam-projection-input", vec![1_i32, 2])?
        .map_named("beam-projection-build", |id| {
            BeamRow::<OldRow>::new(Row {
                values: vec![
                    FieldValue {
                        field_value: Some(field_value::FieldValue::AtomicValue(AtomicTypeValue {
                            value: Some(atomic_type_value::Value::Int32(id)),
                        })),
                    },
                    FieldValue {
                        field_value: Some(field_value::FieldValue::AtomicValue(AtomicTypeValue {
                            value: Some(atomic_type_value::Value::String(
                                if id == 1 { "one" } else { "two" }.into(),
                            )),
                        })),
                    },
                ],
            })
            .expect("fixture row matches writer schema")
        })?
        .map_named("beam-projection-project", |row| {
            row.project::<NewRow>()
                .expect("fixture schemas are compatible")
        })?
        .map_named("beam-projection-read", |row| {
            let values = &row.as_row().values;
            let Some(field_value::FieldValue::AtomicValue(AtomicTypeValue {
                value: Some(atomic_type_value::Value::String(label)),
            })) = values[0].field_value.as_ref()
            else {
                panic!("projected label missing");
            };
            let Some(field_value::FieldValue::AtomicValue(AtomicTypeValue {
                value: Some(atomic_type_value::Value::Int32(id)),
            })) = values[1].field_value.as_ref()
            else {
                panic!("projected ID missing");
            };
            assert!(values[2].field_value.is_none());
            format!("{id}:{label}")
        })?
        .sink_named_with_config(
            "beam-projection-capture",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::AtLeastOnce,
                commit: true,
            },
            format!("beam-projection:{}", path.display()),
            move || {
                Ok(Box::new(GlobalFoldSink {
                    path: sink_path.clone(),
                    values: Vec::new(),
                }))
            },
        )
}
