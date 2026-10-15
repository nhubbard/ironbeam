use beam_proto::pipeline_v1::{
    atomic_type_value, field_type, field_value, AtomicType, AtomicTypeValue, Field, FieldType,
    FieldValue, LogicalType, LogicalTypeValue, Row, Schema,
};
use ironbeam_coder_row::{decode_row, encode_row};

fn decimal_schema() -> Schema {
    Schema {
        fields: vec![
            Field {
                name: "f_float".into(),
                r#type: Some(FieldType {
                    type_info: Some(field_type::TypeInfo::AtomicType(AtomicType::Float as i32)),
                    ..Default::default()
                }),
                ..Default::default()
            },
            Field {
                name: "f_decimal".into(),
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
            },
        ],
        ..Default::default()
    }
}

fn decimal_value(text: &str) -> FieldValue {
    FieldValue {
        field_value: Some(field_value::FieldValue::LogicalTypeValue(Box::new(
            LogicalTypeValue {
                value: Some(Box::new(FieldValue {
                    field_value: Some(field_value::FieldValue::AtomicValue(AtomicTypeValue {
                        value: Some(atomic_type_value::Value::Bytes(text.as_bytes().to_vec())),
                    })),
                })),
            },
        ))),
    }
}

fn row(float: f32, decimal: &str) -> Row {
    Row {
        values: vec![
            FieldValue {
                field_value: Some(field_value::FieldValue::AtomicValue(AtomicTypeValue {
                    value: Some(atomic_type_value::Value::Float(float)),
                })),
            },
            decimal_value(decimal),
        ],
    }
}

#[test]
#[allow(clippy::approx_constant)] // The pinned Beam fixture uses exactly 3.1415.
fn beam_reference_decimal_rows_match_pinned_wire_examples() {
    let schema = decimal_schema();
    let examples: &[(f32, &str, &[u8])] = &[
        (0.0, "0.0", &[2, 0, 0, 0, 0, 0, 1, 1, 0]),
        (1.0, "1.0", &[2, 0, 0x3f, 0x80, 0, 0, 1, 1, 10]),
        (
            3.1415,
            "3.1415",
            &[2, 0, 0x40, 0x49, 0x0e, 0x56, 4, 2, 0x7a, 0xb7],
        ),
        (
            -100.123,
            "-100.123",
            &[2, 0, 0xc2, 0xc8, 0x3e, 0xfa, 3, 3, 0xfe, 0x78, 0xe5],
        ),
    ];
    for &(float, decimal, bytes) in examples {
        assert_eq!(encode_row(&schema, &row(float, decimal)).unwrap(), bytes);
        assert_eq!(decode_row(&schema, bytes).unwrap(), row(float, decimal));
    }
}

#[test]
fn decimal_scale_and_large_integer_round_trip() {
    let schema = decimal_schema();
    for decimal in [
        "-0.0010",
        "123456789012345678901234567890.125",
        "1e2",
        "1e-1001",
    ] {
        let value = row(0.0, decimal);
        let bytes = encode_row(&schema, &value).unwrap();
        assert_eq!(decode_row(&schema, &bytes).unwrap(), value);
    }
}

#[test]
fn malformed_decimal_values_and_bytes_fail() {
    let schema = decimal_schema();
    for decimal in ["", "--1", "1..0", "1x", "1e", "1e2147483649"] {
        assert!(
            encode_row(&schema, &row(0.0, decimal)).is_err(),
            "{decimal}"
        );
    }
    assert!(decode_row(&schema, &[2, 0, 0, 0, 0, 0, 0, 0]).is_err());
}
