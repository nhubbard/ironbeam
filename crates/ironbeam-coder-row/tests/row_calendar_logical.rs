use beam_proto::pipeline_v1::{
    atomic_type_value, field_type, field_value, AtomicType, AtomicTypeValue, Field, FieldType,
    FieldValue, LogicalType, LogicalTypeValue, Row, RowType, Schema,
};
use ironbeam_coder_row::{decode_row, encode_row};

const fn int64_type() -> FieldType {
    FieldType {
        nullable: false,
        type_info: Some(field_type::TypeInfo::AtomicType(AtomicType::Int64 as i32)),
    }
}

const fn int32_type() -> FieldType {
    FieldType {
        nullable: false,
        type_info: Some(field_type::TypeInfo::AtomicType(AtomicType::Int32 as i32)),
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

const fn int64(value: i64) -> FieldValue {
    FieldValue {
        field_value: Some(field_value::FieldValue::AtomicValue(AtomicTypeValue {
            value: Some(atomic_type_value::Value::Int64(value)),
        })),
    }
}

const fn int32(value: i32) -> FieldValue {
    FieldValue {
        field_value: Some(field_value::FieldValue::AtomicValue(AtomicTypeValue {
            value: Some(atomic_type_value::Value::Int32(value)),
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

#[test]
fn beam_date_and_time_use_their_int64_representations() {
    // Pinned Beam schema.proto defines a date as epoch days. Its Java Time
    // logical type represents local time as nanoseconds since midnight.
    let schema = Schema {
        fields: vec![
            field(
                "date",
                logical_type("beam:logical_type:date:v1", int64_type()),
            ),
            field(
                "time",
                logical_type("beam:logical_type:time:v1", int64_type()),
            ),
        ],
        ..Default::default()
    };
    let row = Row {
        values: vec![logical(int64(-1)), logical(int64(86_399_999_999_999))],
    };
    let representation_schema = Schema {
        fields: vec![field("date", int64_type()), field("time", int64_type())],
        ..Default::default()
    };
    let representation = Row {
        values: vec![int64(-1), int64(86_399_999_999_999)],
    };
    let bytes = encode_row(&schema, &row).unwrap();
    assert_eq!(
        bytes,
        encode_row(&representation_schema, &representation).unwrap()
    );
    assert_eq!(decode_row(&schema, &bytes).unwrap(), row);
}

#[test]
fn beam_uuid_uses_two_int64_row_fields_in_most_least_order() {
    // Pinned Beam UuidLogicalType uses a row of the most and least significant
    // signed 64-bit halves, in that order.
    let representation_schema = Schema {
        fields: vec![
            field("MostSignificantBits", int64_type()),
            field("LeastSignificantBits", int64_type()),
        ],
        ..Default::default()
    };
    let representation = Row {
        values: vec![int64(0x1234_5678_90ab_cdef), int64(-2)],
    };
    let schema = Schema {
        fields: vec![field(
            "uuid",
            logical_type(
                "beam:logical_type:uuid:v1",
                FieldType {
                    nullable: false,
                    type_info: Some(field_type::TypeInfo::RowType(RowType {
                        schema: Some(representation_schema.clone()),
                    })),
                },
            ),
        )],
        ..Default::default()
    };
    let row = Row {
        values: vec![logical(FieldValue {
            field_value: Some(field_value::FieldValue::RowValue(representation.clone())),
        })],
    };
    let bytes = encode_row(&schema, &row).unwrap();
    let nested = encode_row(&representation_schema, &representation).unwrap();
    assert_eq!(&bytes[..2], &[1, 0]);
    assert_eq!(&bytes[2..], nested.as_slice());
    assert_eq!(decode_row(&schema, &bytes).unwrap(), row);
}

#[test]
fn beam_nanos_instant_and_duration_use_seconds_and_nanos_rows() {
    // Pinned Beam's NanosType uses this representation for both logical URNs.
    let representation_schema = Schema {
        fields: vec![field("seconds", int64_type()), field("nanos", int32_type())],
        ..Default::default()
    };
    let nanos_type = FieldType {
        nullable: false,
        type_info: Some(field_type::TypeInfo::RowType(RowType {
            schema: Some(representation_schema.clone()),
        })),
    };
    let schema = Schema {
        fields: vec![
            field(
                "instant",
                logical_type("beam:logical_type:nanos_instant:v1", nanos_type.clone()),
            ),
            field(
                "duration",
                logical_type("beam:logical_type:nanos_duration:v1", nanos_type),
            ),
        ],
        ..Default::default()
    };
    let instant = Row {
        values: vec![int64(-1), int32(999_999_999)],
    };
    let duration = Row {
        values: vec![int64(12), int32(345_678_901)],
    };
    let row = Row {
        values: vec![
            logical(FieldValue {
                field_value: Some(field_value::FieldValue::RowValue(instant.clone())),
            }),
            logical(FieldValue {
                field_value: Some(field_value::FieldValue::RowValue(duration.clone())),
            }),
        ],
    };
    let bytes = encode_row(&schema, &row).unwrap();
    let representation_bytes = [
        encode_row(&representation_schema, &instant).unwrap(),
        encode_row(&representation_schema, &duration).unwrap(),
    ]
    .concat();
    assert_eq!(&bytes[..2], &[2, 0]);
    assert_eq!(&bytes[2..], representation_bytes.as_slice());
    assert_eq!(decode_row(&schema, &bytes).unwrap(), row);
}
