use super::*;
use beam_proto::pipeline_v1::{
    atomic_type_value, field_type, field_value, AtomicType, AtomicTypeValue, Field, FieldType,
    FieldValue, Row, RowType, Schema,
};
use ironbeam_coder_row::{BeamRow, RowSchema};

struct OldRow;
struct NewRow;

fn detail_type(reader: bool) -> FieldType {
    let fields = if reader {
        vec![
            field("category", AtomicType::String, 1, false),
            field("code", AtomicType::Int32, 0, false),
            field("note", AtomicType::String, 2, true),
        ]
    } else {
        vec![
            field("code", AtomicType::Int32, 0, false),
            field("category", AtomicType::String, 1, false),
        ]
    };
    FieldType {
        nullable: true,
        type_info: Some(field_type::TypeInfo::RowType(RowType {
            schema: Some(Schema {
                encoding_positions_set: true,
                fields,
                ..Default::default()
            }),
        })),
    }
}

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
    const TYPE_ID: &'static str = "tests/prism-row-projection-old/v2";

    fn schema() -> Schema {
        Schema {
            id: "41368115-8441-4d7b-ba4c-5c325dd44b5c".into(),
            encoding_positions_set: true,
            fields: vec![
                field("id", AtomicType::Int32, 0, false),
                field("label", AtomicType::String, 1, false),
                Field {
                    name: "detail".into(),
                    encoding_position: 2,
                    r#type: Some(detail_type(false)),
                    ..Default::default()
                },
            ],
            ..Default::default()
        }
    }
}

impl RowSchema for NewRow {
    const TYPE_ID: &'static str = "tests/prism-row-projection-new/v3";

    fn schema() -> Schema {
        Schema {
            id: "dbf12db1-29e6-45eb-94d1-e4f684df4a25".into(),
            encoding_positions_set: true,
            fields: vec![
                field("label", AtomicType::String, 1, false),
                field("id", AtomicType::Int32, 0, false),
                Field {
                    name: "detail".into(),
                    encoding_position: 2,
                    r#type: Some(detail_type(true)),
                    ..Default::default()
                },
                field("note", AtomicType::String, 3, true),
            ],
            ..Default::default()
        }
    }
}

fn old_row(id: i32, label: &str) -> BeamRow<OldRow> {
    BeamRow::<OldRow>::new(Row {
        values: vec![
            FieldValue {
                field_value: Some(field_value::FieldValue::AtomicValue(AtomicTypeValue {
                    value: Some(atomic_type_value::Value::Int32(id)),
                })),
            },
            FieldValue {
                field_value: Some(field_value::FieldValue::AtomicValue(AtomicTypeValue {
                    value: Some(atomic_type_value::Value::String(label.into())),
                })),
            },
            FieldValue {
                field_value: (id != 2).then(|| {
                    field_value::FieldValue::RowValue(Row {
                        values: vec![
                            FieldValue {
                                field_value: Some(field_value::FieldValue::AtomicValue(
                                    AtomicTypeValue {
                                        value: Some(atomic_type_value::Value::Int32(id * 10)),
                                    },
                                )),
                            },
                            FieldValue {
                                field_value: Some(field_value::FieldValue::AtomicValue(
                                    AtomicTypeValue {
                                        value: Some(atomic_type_value::Value::String(label.into())),
                                    },
                                )),
                            },
                        ],
                    })
                }),
            },
        ],
    })
    .expect("fixture row matches writer schema")
}

fn projected_values(row: &BeamRow<NewRow>) -> (i32, String, Option<(i32, String)>) {
    let fields = &row.as_row().values;
    let Some(field_value::FieldValue::AtomicValue(AtomicTypeValue {
        value: Some(atomic_type_value::Value::String(label)),
    })) = fields[0].field_value.as_ref()
    else {
        panic!("projected label missing");
    };
    let Some(field_value::FieldValue::AtomicValue(AtomicTypeValue {
        value: Some(atomic_type_value::Value::Int32(id)),
    })) = fields[1].field_value.as_ref()
    else {
        panic!("projected ID missing");
    };
    let detail = match fields[2].field_value.as_ref() {
        Some(field_value::FieldValue::RowValue(inner)) => {
            let Some(field_value::FieldValue::AtomicValue(AtomicTypeValue {
                value: Some(atomic_type_value::Value::String(category)),
            })) = inner.values[0].field_value.as_ref()
            else {
                panic!("projected category missing");
            };
            let Some(field_value::FieldValue::AtomicValue(AtomicTypeValue {
                value: Some(atomic_type_value::Value::Int32(code)),
            })) = inner.values[1].field_value.as_ref()
            else {
                panic!("projected code missing");
            };
            assert!(inner.values[2].field_value.is_none());
            Some((*code, category.clone()))
        }
        None => None,
        _ => panic!("projected detail has wrong type"),
    };
    assert!(fields[3].field_value.is_none());
    (*id, label.clone(), detail)
}

pub fn beam_row_projection_fixture(
    pipeline: &Pipeline,
    path: PathBuf,
) -> Result<Sink<String>, Error> {
    let sink_path = path.clone();
    pipeline
        .from_vec("beam-projection-input", vec![1_i32, 2])?
        .map_named("beam-projection-build", |id| {
            old_row(id, if id == 1 { "one" } else { "two" })
        })?
        .map_named("beam-projection-project", |row| {
            row.project::<NewRow>()
                .expect("fixture schemas are compatible")
        })?
        .map_named("beam-projection-read", |row| {
            let (id, label, detail) = projected_values(&row);
            format!("{id}:{label}:{detail:?}")
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

pub fn beam_row_stream_projection_fixture(
    pipeline: &Pipeline,
    directory: PathBuf,
) -> Result<Sink<String>, Error> {
    let sink_directory = directory.clone();
    pipeline
        .test_stream_source(
            "beam-stream-projection-source",
            vec![
                TestStreamEvent::Elements(vec![
                    (old_row(1, "one"), 1_000_000),
                    (old_row(2, "two"), 2_000_000),
                ]),
                TestStreamEvent::AdvanceWatermark(10_000_000),
                TestStreamEvent::Elements(vec![(old_row(3, "three"), 11_000_000)]),
                TestStreamEvent::AdvanceWatermark(20_000_000),
            ],
        )?
        .fixed_windows("beam-stream-projection-windows", 10_000_000)?
        .map_named("beam-stream-projection-project", |row| {
            row.project::<NewRow>()
                .expect("fixture schemas are compatible")
        })?
        .map_named("beam-stream-projection-key", |row| ("a".to_owned(), row))?
        .group_by_key_named("beam-stream-projection-group")?
        .reify_timestamps_named("beam-stream-projection-time")?
        .map_named(
            "beam-stream-projection-output",
            |(timestamp, (key, rows))| {
                let mut values = rows.iter().map(projected_values).collect::<Vec<_>>();
                values.sort_unstable();
                format!("{key}:{timestamp}:{values:?}")
            },
        )?
        .sink_named_with_config(
            "beam-stream-projection-capture",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            format!("beam-stream-projection:{}", directory.display()),
            move || {
                Ok(Box::new(IdempotentRowSink {
                    directory: sink_directory.clone(),
                    pending: Vec::new(),
                }))
            },
        )
}
