use beam_proto::pipeline_v1::{
    atomic_type_value, field_type, field_value, AtomicType, AtomicTypeValue, Field, FieldType,
    FieldValue, Row, Schema,
};
use ironbeam_coder_row::{BeamRow, RowSchema};
use ironbeam_core::Element;

struct Writer;
struct Reader;
struct IncompatibleReader;

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

impl RowSchema for Writer {
    const TYPE_ID: &'static str = "tests/writer-row/v1";

    fn schema() -> Schema {
        Schema {
            id: "775d29cb-2e48-4cef-b021-cd346ee4d565".into(),
            encoding_positions_set: true,
            fields: vec![
                field("id", AtomicType::Int32, 0, false),
                field("label", AtomicType::String, 1, false),
            ],
            ..Default::default()
        }
    }
}

impl RowSchema for Reader {
    const TYPE_ID: &'static str = "tests/reader-row/v2";

    fn schema() -> Schema {
        Schema {
            id: "43b3599b-4bc3-490e-baa5-f736624bb813".into(),
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

impl RowSchema for IncompatibleReader {
    const TYPE_ID: &'static str = "tests/incompatible-reader-row/v2";

    fn schema() -> Schema {
        Schema {
            id: "51c6a902-3a74-4bfd-98c4-e6aa1bbf508d".into(),
            encoding_positions_set: true,
            fields: vec![
                field("id", AtomicType::String, 0, false),
                field("label", AtomicType::String, 1, false),
            ],
            ..Default::default()
        }
    }
}

#[test]
fn typed_projection_reorders_and_adds_nullable_field() {
    let old = BeamRow::<Writer>::new(Row {
        values: vec![
            FieldValue {
                field_value: Some(field_value::FieldValue::AtomicValue(AtomicTypeValue {
                    value: Some(atomic_type_value::Value::Int32(7)),
                })),
            },
            FieldValue {
                field_value: Some(field_value::FieldValue::AtomicValue(AtomicTypeValue {
                    value: Some(atomic_type_value::Value::String("seven".into())),
                })),
            },
        ],
    })
    .unwrap();
    let projected = old.project::<Reader>().unwrap();
    let implicit = BeamRow::<Reader>::decode(&old.encode().unwrap()).unwrap();
    assert_eq!(implicit.as_row(), projected.as_row());
    assert!(old.project::<IncompatibleReader>().is_err());
    assert_eq!(projected.as_row().values[0], old.as_row().values[1]);
    assert_eq!(projected.as_row().values[1], old.as_row().values[0]);
    assert!(projected.as_row().values[2].field_value.is_none());
    let bytes = projected.encode().unwrap();
    let older = BeamRow::<Writer>::decode(&bytes).unwrap();
    assert_eq!(older.as_row(), old.as_row());
    assert_eq!(
        BeamRow::<Reader>::decode(&bytes).unwrap().into_row(),
        projected.into_row()
    );
}
