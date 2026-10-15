use beam_proto::pipeline_v1::{
    atomic_type_value, field_type, field_value, AtomicType, AtomicTypeValue, Field, FieldType,
    FieldValue, LogicalType, LogicalTypeValue, Row, RowType, Schema,
};
use ironbeam_coder_row::{decode_row, encode_row};

fn atomic(kind: AtomicType) -> FieldType {
    FieldType {
        nullable: false,
        type_info: Some(field_type::TypeInfo::AtomicType(kind as i32)),
    }
}

fn field(name: &str, kind: AtomicType) -> Field {
    Field {
        name: name.into(),
        r#type: Some(atomic(kind)),
        ..Default::default()
    }
}

fn timestamp_schema(subseconds: AtomicType) -> Schema {
    Schema {
        fields: vec![Field {
            name: "f_timestamp".into(),
            r#type: Some(FieldType {
                nullable: false,
                type_info: Some(field_type::TypeInfo::LogicalType(Box::new(LogicalType {
                    urn: "beam:logical_type:timestamp:v1".into(),
                    representation: Some(Box::new(FieldType {
                        nullable: false,
                        type_info: Some(field_type::TypeInfo::RowType(RowType {
                            schema: Some(Schema {
                                fields: vec![
                                    field("seconds", AtomicType::Int64),
                                    field("subseconds", subseconds),
                                ],
                                ..Default::default()
                            }),
                        })),
                    })),
                    ..Default::default()
                }))),
            }),
            ..Default::default()
        }],
        ..Default::default()
    }
}

fn timestamp(seconds: i64, subseconds: i32, kind: AtomicType) -> Row {
    let subseconds = if kind == AtomicType::Int16 {
        atomic_type_value::Value::Int16(subseconds)
    } else {
        atomic_type_value::Value::Int32(subseconds)
    };
    let component = |value| FieldValue {
        field_value: Some(field_value::FieldValue::AtomicValue(AtomicTypeValue {
            value: Some(value),
        })),
    };
    Row {
        values: vec![FieldValue {
            field_value: Some(field_value::FieldValue::LogicalTypeValue(Box::new(
                LogicalTypeValue {
                    value: Some(Box::new(FieldValue {
                        field_value: Some(field_value::FieldValue::RowValue(Row {
                            values: vec![
                                component(atomic_type_value::Value::Int64(seconds)),
                                component(subseconds),
                            ],
                        })),
                    })),
                },
            ))),
        }],
    }
}

#[test]
fn beam_timestamp_logical_rows_match_pinned_precision_examples() {
    // model/fn-execution/.../standard_coders.yaml at the pinned Beam SHA.
    for (kind, seconds, subseconds, bytes) in [
        (
            AtomicType::Int16,
            1_597_328_054,
            999,
            b"\x01\x00\x02\x00\xb6\x95\xd5\xf9\x05\x03\xe7".as_slice(),
        ),
        (
            AtomicType::Int16,
            -2,
            500,
            b"\x01\x00\x02\x00\xfe\xff\xff\xff\xff\xff\xff\xff\xff\x01\x01\xf4",
        ),
        (
            AtomicType::Int32,
            1_597_328_054,
            123_456,
            b"\x01\x00\x02\x00\xb6\x95\xd5\xf9\x05\xc0\xc4\x07",
        ),
        (
            AtomicType::Int32,
            1_597_328_054,
            123_456_789,
            b"\x01\x00\x02\x00\xb6\x95\xd5\xf9\x05\x95\x9a\xef\x3a",
        ),
        (
            AtomicType::Int32,
            -2,
            999_999_999,
            b"\x01\x00\x02\x00\xfe\xff\xff\xff\xff\xff\xff\xff\xff\x01\xff\x93\xeb\xdc\x03",
        ),
    ] {
        let schema = timestamp_schema(kind);
        let row = timestamp(seconds, subseconds, kind);
        assert_eq!(encode_row(&schema, &row).unwrap(), bytes);
        assert_eq!(decode_row(&schema, bytes).unwrap(), row);
    }
}
