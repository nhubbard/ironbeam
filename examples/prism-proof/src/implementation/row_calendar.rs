use super::*;
use beam_proto::pipeline_v1::{
    atomic_type_value, field_type, field_value, AtomicType, AtomicTypeValue, Field, FieldType,
    FieldValue, LogicalType, LogicalTypeValue, Row, RowType, Schema,
};
use ironbeam_coder_row::{BeamRow, RowSchema};

struct CalendarRowSchema;

const fn int64_type() -> FieldType {
    FieldType {
        nullable: false,
        type_info: Some(field_type::TypeInfo::AtomicType(AtomicType::Int64 as i32)),
    }
}

fn field(name: &str, ty: FieldType) -> Field {
    Field {
        name: name.into(),
        r#type: Some(ty),
        ..Default::default()
    }
}

fn logical_type(urn: &str, representation: FieldType) -> FieldType {
    FieldType {
        nullable: false,
        type_info: Some(field_type::TypeInfo::LogicalType(Box::new(LogicalType {
            urn: urn.into(),
            representation: Some(Box::new(representation)),
            ..Default::default()
        }))),
    }
}

impl RowSchema for CalendarRowSchema {
    const TYPE_ID: &'static str = "tests/prism-beam-calendar-row-v1";

    fn schema() -> Schema {
        let uuid_schema = Schema {
            fields: vec![
                field("MostSignificantBits", int64_type()),
                field("LeastSignificantBits", int64_type()),
            ],
            ..Default::default()
        };
        Schema {
            id: "ee36ab85-41fb-46d5-b6fc-227603d3cf8a".into(),
            fields: vec![
                field(
                    "date",
                    logical_type("beam:logical_type:date:v1", int64_type()),
                ),
                field(
                    "time",
                    logical_type("beam:logical_type:time:v1", int64_type()),
                ),
                field(
                    "uuid",
                    logical_type(
                        "beam:logical_type:uuid:v1",
                        FieldType {
                            nullable: false,
                            type_info: Some(field_type::TypeInfo::RowType(RowType {
                                schema: Some(uuid_schema),
                            })),
                        },
                    ),
                ),
            ],
            ..Default::default()
        }
    }
}

const fn int64(value: i64) -> FieldValue {
    FieldValue {
        field_value: Some(field_value::FieldValue::AtomicValue(AtomicTypeValue {
            value: Some(atomic_type_value::Value::Int64(value)),
        })),
    }
}

fn logical(value: FieldValue) -> FieldValue {
    FieldValue {
        field_value: Some(field_value::FieldValue::LogicalTypeValue(Box::new(
            LogicalTypeValue {
                value: Some(Box::new(value)),
            },
        ))),
    }
}

fn logical_int64(value: i64) -> FieldValue {
    logical(int64(value))
}

fn row_for(id: i64) -> BeamRow<CalendarRowSchema> {
    BeamRow::<CalendarRowSchema>::new(Row {
        values: vec![
            logical_int64(id - 2),
            logical_int64(id * 1_000_000_000),
            logical(FieldValue {
                field_value: Some(field_value::FieldValue::RowValue(Row {
                    values: vec![int64(id), int64(-id)],
                })),
            }),
        ],
    })
    .expect("calendar fixture matches its Beam schema")
}

fn logical_i64(field: &FieldValue) -> i64 {
    let Some(field_value::FieldValue::LogicalTypeValue(value)) = &field.field_value else {
        panic!("calendar logical value missing");
    };
    let Some(field_value::FieldValue::AtomicValue(AtomicTypeValue {
        value: Some(atomic_type_value::Value::Int64(value)),
    })) = value
        .value
        .as_ref()
        .and_then(|value| value.field_value.as_ref())
    else {
        panic!("calendar int64 representation missing");
    };
    *value
}

pub fn beam_calendar_row_fixture(
    pipeline: &Pipeline,
    path: PathBuf,
) -> Result<Sink<String>, Error> {
    let sink_path = path.clone();
    pipeline
        .from_vec("beam-calendar-input", vec![1_i64, 2])?
        .map_named("beam-calendar-build", row_for)?
        .reshuffle_named("beam-calendar-wire-boundary")?
        .map_named("beam-calendar-read", |row| {
            let fields = &row.as_row().values;
            let date = logical_i64(&fields[0]);
            let time = logical_i64(&fields[1]);
            let Some(field_value::FieldValue::LogicalTypeValue(uuid)) = &fields[2].field_value
            else {
                panic!("UUID logical value missing");
            };
            let Some(field_value::FieldValue::RowValue(bits)) = uuid
                .value
                .as_ref()
                .and_then(|value| value.field_value.as_ref())
            else {
                panic!("UUID row representation missing");
            };
            let Some(field_value::FieldValue::AtomicValue(AtomicTypeValue {
                value: Some(atomic_type_value::Value::Int64(most)),
            })) = &bits.values[0].field_value
            else {
                panic!("UUID most significant bits missing");
            };
            let Some(field_value::FieldValue::AtomicValue(AtomicTypeValue {
                value: Some(atomic_type_value::Value::Int64(least)),
            })) = &bits.values[1].field_value
            else {
                panic!("UUID least significant bits missing");
            };
            format!("date={date},time={time},uuid={most}:{least}")
        })?
        .sink_named_with_config(
            "beam-calendar-capture",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::AtLeastOnce,
                commit: true,
            },
            format!("beam-calendar:{}", path.display()),
            move || {
                Ok(Box::new(GlobalFoldSink {
                    path: sink_path.clone(),
                    values: Vec::new(),
                }))
            },
        )
}
