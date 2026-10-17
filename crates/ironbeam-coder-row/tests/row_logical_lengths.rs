use beam_proto::pipeline_v1::{
    atomic_type_value, field_type, field_value, AtomicType, AtomicTypeValue, Field, FieldType,
    FieldValue, LogicalType, LogicalTypeValue, Row, Schema,
};
use ironbeam_coder_row::{decode_row, encode_row};

fn logical_field(name: &str, urn: &str, atomic: AtomicType) -> Field {
    Field {
        name: name.into(),
        r#type: Some(FieldType {
            nullable: true,
            type_info: Some(field_type::TypeInfo::LogicalType(Box::new(LogicalType {
                urn: urn.into(),
                representation: Some(Box::new(FieldType {
                    type_info: Some(field_type::TypeInfo::AtomicType(atomic as i32)),
                    ..Default::default()
                })),
                ..Default::default()
            }))),
        }),
        ..Default::default()
    }
}

fn logical_string(text: &str) -> FieldValue {
    logical_value(atomic_type_value::Value::String(text.into()))
}

fn logical_bytes(bytes: &[u8]) -> FieldValue {
    logical_value(atomic_type_value::Value::Bytes(bytes.to_vec()))
}

fn logical_value(value: atomic_type_value::Value) -> FieldValue {
    FieldValue {
        field_value: Some(field_value::FieldValue::LogicalTypeValue(Box::new(
            LogicalTypeValue {
                value: Some(Box::new(FieldValue {
                    field_value: Some(field_value::FieldValue::AtomicValue(AtomicTypeValue {
                        value: Some(value),
                    })),
                })),
            },
        ))),
    }
}

#[test]
fn beam_reference_fixed_and_variable_length_logical_fields() {
    let schema = Schema {
        fields: vec![
            logical_field(
                "f_char",
                "beam:logical_type:fixed_char:v1",
                AtomicType::String,
            ),
            logical_field(
                "f_varchar",
                "beam:logical_type:var_char:v1",
                AtomicType::String,
            ),
            logical_field(
                "f_bytes",
                "beam:logical_type:fixed_bytes:v1",
                AtomicType::Bytes,
            ),
            logical_field(
                "f_varbytes",
                "beam:logical_type:var_bytes:v1",
                AtomicType::Bytes,
            ),
        ],
        ..Default::default()
    };
    let examples = [
        (
            Row {
                values: vec![
                    logical_string("ABCDE"),
                    logical_string("ABCDE"),
                    logical_bytes(b"ABCDE"),
                    logical_bytes(b"ABCDE"),
                ],
            },
            b"\x04\x00\x05ABCDE\x05ABCDE\x05ABCDE\x05ABCDE".as_slice(),
        ),
        (
            Row {
                values: vec![
                    logical_string("A\n   "),
                    logical_string("A\n"),
                    logical_bytes(b"A\n\0\0\0"),
                    logical_bytes(b"A\n"),
                ],
            },
            b"\x04\x00\x05A\n   \x02A\n\x05A\n\0\0\0\x02A\n".as_slice(),
        ),
        (
            Row {
                values: vec![
                    logical_string("null?"),
                    FieldValue { field_value: None },
                    FieldValue { field_value: None },
                    logical_bytes(b"null"),
                ],
            },
            b"\x04\x01\x06\x05null?\x04null".as_slice(),
        ),
    ];
    for (row, bytes) in examples {
        assert_eq!(encode_row(&schema, &row).unwrap(), bytes);
        assert_eq!(decode_row(&schema, bytes).unwrap(), row);
    }
}
