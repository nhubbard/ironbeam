use beam_proto::pipeline_v1::{
    atomic_type_value, field_type, field_value, AtomicType, AtomicTypeValue, Field, FieldType,
    FieldValue, Row, Schema,
};
use ironbeam_coder_row::{BeamRow, RowSchema};
use ironbeam_core::Element;
use prost::Message;

struct NumberSchema;

impl RowSchema for NumberSchema {
    const TYPE_ID: &'static str = "tests/number-row/v1";

    fn schema() -> Schema {
        Schema {
            id: "6c6d333b-68e2-4b70-82c0-23946c7fc73f".into(),
            fields: vec![Field {
                name: "number".into(),
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

#[test]
fn typed_row_validates_and_round_trips_as_a_beam_element() {
    let value = Row {
        values: vec![FieldValue {
            field_value: Some(field_value::FieldValue::AtomicValue(AtomicTypeValue {
                value: Some(atomic_type_value::Value::Int32(42)),
            })),
        }],
    };
    let row = BeamRow::<NumberSchema>::new(value.clone()).unwrap();
    assert!(BeamRow::<NumberSchema>::supports_coder());
    assert_eq!(
        BeamRow::<NumberSchema>::coder_payload(),
        NumberSchema::schema().encode_to_vec()
    );
    assert_eq!(row.as_row(), &value);
    let bytes = row.encode().unwrap();
    assert_eq!(bytes, [1, 0, 42]);
    assert_eq!(
        BeamRow::<NumberSchema>::decode(&bytes).unwrap().into_row(),
        value
    );
    let cloned = row.clone();
    assert_eq!(cloned.into_row(), value);
    assert_eq!(row.into_row(), value);
    assert!(BeamRow::<NumberSchema>::new(Row { values: Vec::new() }).is_err());
    assert!(BeamRow::<NumberSchema>::decode(&[]).is_err());
}
