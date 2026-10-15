use beam_proto::pipeline_v1::{
    atomic_type_value, field_type, field_value, ArrayType, ArrayTypeValue, AtomicType,
    AtomicTypeValue, Field, FieldType, FieldValue, LogicalType, LogicalTypeValue, MapType,
    MapTypeEntry, MapTypeValue, Row, RowType, Schema,
};
use ironbeam_coder_row::{decode_row, decode_row_evolved, encode_row};
use prost::Message;

fn field(name: &str, ty: FieldType) -> Field {
    Field {
        name: name.into(),
        r#type: Some(ty),
        ..Default::default()
    }
}

const fn atomic(kind: AtomicType, nullable: bool) -> FieldType {
    FieldType {
        nullable,
        type_info: Some(field_type::TypeInfo::AtomicType(kind as i32)),
    }
}

const fn value(atom: atomic_type_value::Value) -> FieldValue {
    FieldValue {
        field_value: Some(field_value::FieldValue::AtomicValue(AtomicTypeValue {
            value: Some(atom),
        })),
    }
}

const fn null() -> FieldValue {
    FieldValue { field_value: None }
}

fn schema(fields: Vec<Field>) -> Schema {
    Schema {
        fields,
        ..Default::default()
    }
}

#[test]
fn beam_reference_row_with_strings_numbers_and_array() {
    use atomic_type_value::Value;
    let schema = schema(vec![
        field("str", atomic(AtomicType::String, false)),
        field("i32", atomic(AtomicType::Int32, false)),
        field("f64", atomic(AtomicType::Double, false)),
        field(
            "arr",
            FieldType {
                nullable: false,
                type_info: Some(field_type::TypeInfo::ArrayType(Box::new(ArrayType {
                    element_type: Some(Box::new(atomic(AtomicType::String, false))),
                }))),
            },
        ),
    ]);
    let row = Row {
        values: vec![
            value(Value::String("foo".into())),
            value(Value::Int32(9001)),
            value(Value::Double(0.1)),
            FieldValue {
                field_value: Some(field_value::FieldValue::ArrayValue(ArrayTypeValue {
                    element: ["foo", "bar", "baz"]
                        .into_iter()
                        .map(|text| value(Value::String(text.into())))
                        .collect(),
                })),
            },
        ],
    };
    let expected = b"\x04\x00\x03foo\xa9\x46\x3f\xb9\x99\x99\x99\x99\x99\x9a\x00\x00\x00\x03\x03foo\x03bar\x03baz";
    assert_eq!(encode_row(&schema, &row).unwrap(), expected);
    assert_eq!(decode_row(&schema, expected).unwrap(), row);
    // Schema payload and row bytes from Beam's pinned standard_coders.yaml.
    let payload = b"\n\t\n\x03str\x1a\x02\x10\x07\n\t\n\x03i32\x1a\x02\x10\x03\n\t\n\x03f64\x1a\x02\x10\x06\n\r\n\x03arr\x1a\x06\x1a\x04\n\x02\x10\x07\x12$4e5e554c-d4c1-4a5d-b5e1-f3293a6b9f05";
    let beam_schema = Schema::decode(payload.as_slice()).unwrap();
    assert_eq!(encode_row(&beam_schema, &row).unwrap(), expected);
    assert_eq!(decode_row(&beam_schema, expected).unwrap(), row);
}

#[test]
fn beam_reference_nullable_row_and_schema_encoding_positions() {
    use atomic_type_value::Value;
    let nullable = schema(vec![
        field("str", atomic(AtomicType::String, true)),
        field("i32", atomic(AtomicType::Int32, true)),
        field("f64", atomic(AtomicType::Double, true)),
    ]);
    let empty = Row {
        values: vec![null(), null(), null()],
    };
    assert_eq!(encode_row(&nullable, &empty).unwrap(), [3, 1, 7]);
    assert_eq!(decode_row(&nullable, &[3, 1, 7]).unwrap(), empty);

    let partial = Row {
        values: vec![
            value(Value::String("foo".into())),
            value(Value::Int32(9001)),
            null(),
        ],
    };
    let expected = b"\x03\x01\x04\x03foo\xa9\x46";
    assert_eq!(encode_row(&nullable, &partial).unwrap(), expected);
    assert_eq!(decode_row(&nullable, expected).unwrap(), partial);

    let mut reordered = schema(vec![
        field("str", atomic(AtomicType::String, false)),
        field("f_bool", atomic(AtomicType::Boolean, false)),
        field("i32", atomic(AtomicType::Int32, true)),
    ]);
    reordered.encoding_positions_set = true;
    reordered.fields[0].encoding_position = 1;
    reordered.fields[1].encoding_position = 2;
    reordered.fields[2].encoding_position = 0;
    let row = Row {
        values: vec![
            value(Value::String("str2".into())),
            value(Value::Boolean(false)),
            value(Value::Int32(21)),
        ],
    };
    let expected = b"\x03\x00\x15\x04str2\x00";
    assert_eq!(encode_row(&reordered, &row).unwrap(), expected);
    assert_eq!(decode_row(&reordered, expected).unwrap(), row);

    // The pinned Python RowCoder numbers null bits in schema field order,
    // even when the value bytes follow encoding positions.
    let null_first_on_wire = Row {
        values: vec![
            value(Value::String("str2".into())),
            value(Value::Boolean(false)),
            null(),
        ],
    };
    let python_layout = b"\x03\x01\x04\x04str2\x00";
    assert_eq!(
        encode_row(&reordered, &null_first_on_wire).unwrap(),
        python_layout
    );
    assert_eq!(
        decode_row(&reordered, python_layout).unwrap(),
        null_first_on_wire
    );
}

#[test]
fn beam_reference_map_with_nullable_values() {
    use atomic_type_value::Value;
    let schema = schema(vec![field(
        "f_map",
        FieldType {
            nullable: false,
            type_info: Some(field_type::TypeInfo::MapType(Box::new(MapType {
                key_type: Some(Box::new(atomic(AtomicType::String, false))),
                value_type: Some(Box::new(atomic(AtomicType::Int64, true))),
            }))),
        },
    )]);
    let row = Row {
        values: vec![FieldValue {
            field_value: Some(field_value::FieldValue::MapValue(MapTypeValue {
                entries: vec![
                    MapTypeEntry {
                        key: Some(value(Value::String("foo".into()))),
                        value: Some(value(Value::Int64(9001))),
                    },
                    MapTypeEntry {
                        key: Some(value(Value::String("bar".into()))),
                        value: Some(value(Value::Int64(i64::MAX))),
                    },
                ],
            })),
        }],
    };
    let expected = b"\x01\x00\x00\x00\x00\x02\x03foo\x01\xa9\x46\x03bar\x01\xff\xff\xff\xff\xff\xff\xff\xff\x7f";
    assert_eq!(encode_row(&schema, &row).unwrap(), expected);
    assert_eq!(decode_row(&schema, expected).unwrap(), row);
}

#[test]
fn beam_reference_logical_millis_and_nested_representations() {
    use atomic_type_value::Value;
    let schema = schema(vec![
        field(
            "timestamp",
            FieldType {
                nullable: false,
                type_info: Some(field_type::TypeInfo::LogicalType(Box::new(LogicalType {
                    urn: "beam:logical_type:millis_instant:v1".into(),
                    representation: Some(Box::new(atomic(AtomicType::Int64, false))),
                    ..Default::default()
                }))),
            },
        ),
        field("label", atomic(AtomicType::String, false)),
        field("millis", atomic(AtomicType::Int64, false)),
    ]);
    let millis = 1_597_328_054_123_i64;
    let row = Row {
        values: vec![
            FieldValue {
                field_value: Some(field_value::FieldValue::LogicalTypeValue(Box::new(
                    LogicalTypeValue {
                        value: Some(Box::new(value(Value::Int64(millis)))),
                    },
                ))),
            },
            value(Value::String("2020-08-13T14:14:14.123Z".into())),
            value(Value::Int64(millis)),
        ],
    };
    let expected = b"\x03\x00\x80\x00\x01\x73\xe8\x2b\xd7\x6b\x18\x32\x30\x32\x30\x2d\x30\x38\x2d\x31\x33\x54\x31\x34\x3a\x31\x34\x3a\x31\x34\x2e\x31\x32\x33\x5a\xeb\xae\xaf\xc1\xbe\x2e";
    assert_eq!(encode_row(&schema, &row).unwrap(), expected);
    assert_eq!(decode_row(&schema, expected).unwrap(), row);

    let nested_type = FieldType {
        nullable: false,
        type_info: Some(field_type::TypeInfo::RowType(RowType {
            schema: Some(Schema {
                fields: vec![field("seconds", atomic(AtomicType::Int64, false))],
                ..Default::default()
            }),
        })),
    };
    let schema = Schema {
        fields: vec![field(
            "instant",
            FieldType {
                nullable: false,
                type_info: Some(field_type::TypeInfo::LogicalType(Box::new(LogicalType {
                    urn: "beam:logical_type:micros_instant:v1".into(),
                    representation: Some(Box::new(nested_type)),
                    ..Default::default()
                }))),
            },
        )],
        ..Default::default()
    };
    let row = Row {
        values: vec![FieldValue {
            field_value: Some(field_value::FieldValue::LogicalTypeValue(Box::new(
                LogicalTypeValue {
                    value: Some(Box::new(FieldValue {
                        field_value: Some(field_value::FieldValue::RowValue(Row {
                            values: vec![value(Value::Int64(2))],
                        })),
                    })),
                },
            ))),
        }],
    };
    assert_eq!(encode_row(&schema, &row).unwrap(), [1, 0, 1, 0, 2]);
    assert_eq!(decode_row(&schema, &[1, 0, 1, 0, 2]).unwrap(), row);
}

#[test]
fn beam_static_row_option_omits_field_count_and_null_mask() {
    use atomic_type_value::Value;
    let mut schema = schema(vec![
        field("number", atomic(AtomicType::Int32, false)),
        field("label", atomic(AtomicType::String, false)),
    ]);
    schema.options.push(beam_proto::pipeline_v1::Option {
        name: "beam:option:row:static_encoding".into(),
        value: Some(value(Value::Boolean(true))),
        ..Default::default()
    });
    let row = Row {
        values: vec![
            value(Value::Int32(42)),
            value(Value::String("hello world!".into())),
        ],
    };
    let expected = b"\x2a\x0chello world!";
    assert_eq!(encode_row(&schema, &row).unwrap(), expected);
    assert_eq!(decode_row(&schema, expected).unwrap(), row);
}

#[test]
fn row_schema_evolution_adds_removes_and_reorders_fields() {
    use atomic_type_value::Value;
    let old = schema(vec![
        field("number", atomic(AtomicType::Int32, false)),
        field("label", atomic(AtomicType::String, false)),
    ]);
    let row = Row {
        values: vec![value(Value::Int32(42)), value(Value::String("beam".into()))],
    };
    let bytes = encode_row(&old, &row).unwrap();

    let reduced = schema(vec![field("number", atomic(AtomicType::Int32, false))]);
    assert_eq!(
        decode_row_evolved(&old, &reduced, &bytes).unwrap().values,
        [value(Value::Int32(42))]
    );

    let mut expanded = schema(vec![
        field("extra", atomic(AtomicType::String, true)),
        field("label", atomic(AtomicType::String, false)),
        field("number", atomic(AtomicType::Int32, false)),
    ]);
    expanded.encoding_positions_set = true;
    expanded.fields[0].encoding_position = 2;
    expanded.fields[1].encoding_position = 1;
    expanded.fields[2].encoding_position = 0;
    assert_eq!(
        decode_row_evolved(&old, &expanded, &bytes).unwrap().values,
        [
            null(),
            value(Value::String("beam".into())),
            value(Value::Int32(42))
        ]
    );

    expanded.fields[2].r#type = Some(atomic(AtomicType::Int64, false));
    assert!(decode_row_evolved(&old, &expanded, &bytes).is_err());
}

#[test]
fn implicit_row_evolution_uses_encoded_field_count() {
    use atomic_type_value::Value;
    let writer = schema(vec![
        field("number", atomic(AtomicType::Int32, false)),
        field("label", atomic(AtomicType::String, false)),
    ]);
    let original = Row {
        values: vec![value(Value::Int32(42)), value(Value::String("beam".into()))],
    };
    let bytes = encode_row(&writer, &original).unwrap();

    let reader_new = schema(vec![
        field("number", atomic(AtomicType::Int32, false)),
        field("label", atomic(AtomicType::String, false)),
        field("note", atomic(AtomicType::String, true)),
    ]);
    assert_eq!(
        decode_row(&reader_new, &bytes).unwrap().values,
        [
            original.values[0].clone(),
            original.values[1].clone(),
            null()
        ]
    );
    let mut required_new = reader_new.clone();
    required_new.fields[2].r#type.as_mut().unwrap().nullable = false;
    assert!(decode_row(&required_new, &bytes).is_err());

    let reader_old = schema(vec![field("number", atomic(AtomicType::Int32, false))]);
    assert_eq!(
        decode_row(&reader_old, &bytes).unwrap().values,
        [original.values[0].clone()]
    );
    assert!(decode_row(&writer, &[bytes.as_slice(), b"garbage"].concat()).is_err());
    assert!(decode_row(&reader_old, &[1, 2, 0, 42]).is_err());
}

#[test]
fn nested_row_rejects_unknown_trailing_fields_without_writer_schema() {
    use atomic_type_value::Value;
    let nested = schema(vec![field("number", atomic(AtomicType::Int32, false))]);
    let outer = schema(vec![field(
        "nested",
        FieldType {
            nullable: false,
            type_info: Some(field_type::TypeInfo::RowType(RowType {
                schema: Some(nested),
            })),
        },
    )]);
    let bytes = [1, 0, 2, 0, 42, 4, b'b', b'e', b'a', b'm'];
    assert!(decode_row(&outer, &bytes).is_err());
    // A newer nested reader can still fill a nullable trailing field.
    let mut reader = outer;
    if let Some(field_type::TypeInfo::RowType(inner)) =
        &mut reader.fields[0].r#type.as_mut().unwrap().type_info
    {
        inner
            .schema
            .as_mut()
            .unwrap()
            .fields
            .push(field("note", atomic(AtomicType::String, true)));
    }
    let old = Row {
        values: vec![FieldValue {
            field_value: Some(field_value::FieldValue::RowValue(Row {
                values: vec![value(Value::Int32(42))],
            })),
        }],
    };
    let old_outer = schema(vec![field(
        "nested",
        FieldType {
            nullable: false,
            type_info: Some(field_type::TypeInfo::RowType(RowType {
                schema: Some(schema(vec![field(
                    "number",
                    atomic(AtomicType::Int32, false),
                )])),
            })),
        },
    )]);
    let old_bytes = encode_row(&old_outer, &old).unwrap();
    let decoded = decode_row(&reader, &old_bytes).unwrap();
    let Some(field_value::FieldValue::RowValue(inner)) = &decoded.values[0].field_value else {
        panic!("expected nested row");
    };
    assert_eq!(inner.values, [value(Value::Int32(42)), null()]);
}

#[test]
fn explicit_writer_schema_projects_nested_rows() {
    use atomic_type_value::Value;
    let inner_writer = schema(vec![
        field("number", atomic(AtomicType::Int32, false)),
        field("label", atomic(AtomicType::String, false)),
    ]);
    let inner_reader = schema(vec![
        field("number", atomic(AtomicType::Int32, false)),
        field("label", atomic(AtomicType::String, false)),
        field("note", atomic(AtomicType::String, true)),
    ]);
    let outer = |inner: Schema| {
        schema(vec![field(
            "nested",
            FieldType {
                nullable: true,
                type_info: Some(field_type::TypeInfo::RowType(RowType {
                    schema: Some(inner),
                })),
            },
        )])
    };
    let writer = outer(inner_writer);
    let reader = outer(inner_reader);
    let row = Row {
        values: vec![FieldValue {
            field_value: Some(field_value::FieldValue::RowValue(Row {
                values: vec![value(Value::Int32(42)), value(Value::String("beam".into()))],
            })),
        }],
    };
    let bytes = encode_row(&writer, &row).unwrap();
    let projected = decode_row_evolved(&writer, &reader, &bytes).unwrap();
    let Some(field_value::FieldValue::RowValue(inner)) = &projected.values[0].field_value else {
        panic!("expected nested row");
    };
    assert_eq!(
        inner.values,
        [
            value(Value::Int32(42)),
            value(Value::String("beam".into())),
            null()
        ]
    );
    assert_eq!(
        decode_row(&reader, &encode_row(&reader, &projected).unwrap()).unwrap(),
        projected
    );

    let old_reader = outer(schema(vec![field(
        "number",
        atomic(AtomicType::Int32, false),
    )]));
    assert_eq!(
        decode_row_evolved(&writer, &old_reader, &bytes)
            .unwrap()
            .values[0],
        FieldValue {
            field_value: Some(field_value::FieldValue::RowValue(Row {
                values: vec![value(Value::Int32(42))],
            })),
        }
    );

    let mut incompatible = reader.clone();
    if let Some(field_type::TypeInfo::RowType(inner)) =
        &mut incompatible.fields[0].r#type.as_mut().unwrap().type_info
    {
        inner.schema.as_mut().unwrap().fields[0].r#type = Some(atomic(AtomicType::Int64, false));
    }
    assert!(decode_row_evolved(&writer, &incompatible, &bytes).is_err());

    let mut missing_reader_schema = reader.clone();
    if let Some(field_type::TypeInfo::RowType(inner)) = &mut missing_reader_schema.fields[0]
        .r#type
        .as_mut()
        .unwrap()
        .type_info
    {
        inner.schema = None;
    }
    assert!(decode_row_evolved(&writer, &missing_reader_schema, &bytes).is_err());

    let mut missing_reader_type = reader.clone();
    missing_reader_type.fields[0].r#type = None;
    assert!(decode_row_evolved(&writer, &missing_reader_type, &bytes).is_err());

    let required_nested_field = outer(schema(vec![
        field("number", atomic(AtomicType::Int32, false)),
        field("label", atomic(AtomicType::String, false)),
        field("note", atomic(AtomicType::String, false)),
    ]));
    assert!(decode_row_evolved(&writer, &required_nested_field, &bytes).is_err());

    let null_row = Row {
        values: vec![null()],
    };
    assert_eq!(
        decode_row_evolved(&writer, &reader, &encode_row(&writer, &null_row).unwrap()).unwrap(),
        null_row
    );
    let mut nonnullable_reader = reader;
    nonnullable_reader.fields[0]
        .r#type
        .as_mut()
        .unwrap()
        .nullable = false;
    assert!(decode_row_evolved(
        &writer,
        &nonnullable_reader,
        &encode_row(&writer, &null_row).unwrap()
    )
    .is_err());
}

#[test]
fn malformed_rows_fail_without_partial_decode() {
    use atomic_type_value::Value;
    let mut schema = schema(vec![field("flag", atomic(AtomicType::Boolean, false))]);
    assert!(decode_row(&schema, &[1, 0, 2]).is_err());
    assert!(decode_row(&schema, &[1, 0, 1, 2]).is_err());
    assert!(decode_row(&schema, &[1, 1, 1]).is_err());
    assert!(decode_row(&schema, &[1, 0, 1, 0]).is_err());
    assert!(encode_row(
        &schema,
        &Row {
            values: vec![null()]
        }
    )
    .is_err());
    assert!(encode_row(
        &schema,
        &Row {
            values: vec![value(Value::Int32(1))]
        }
    )
    .is_err());
    schema.encoding_positions_set = true;
    schema.fields[0].encoding_position = 1;
    assert!(encode_row(
        &schema,
        &Row {
            values: vec![value(Value::Boolean(true))]
        }
    )
    .is_err());

    let decimal = Schema {
        fields: vec![field(
            "amount",
            FieldType {
                nullable: false,
                type_info: Some(field_type::TypeInfo::LogicalType(Box::new(LogicalType {
                    urn: "beam:logical_type:decimal:v1".into(),
                    representation: Some(Box::new(atomic(AtomicType::Bytes, false))),
                    ..Default::default()
                }))),
            },
        )],
        ..Default::default()
    };
    let amount = Row {
        values: vec![FieldValue {
            field_value: Some(field_value::FieldValue::LogicalTypeValue(Box::new(
                LogicalTypeValue {
                    value: Some(Box::new(value(Value::Bytes(vec![0])))),
                },
            ))),
        }],
    };
    assert!(encode_row(&decimal, &amount).is_err());
    assert!(decode_row(&decimal, &[1, 0, 0]).is_err());
}
