# Schema rows

Apache Beam's RowCoder encodes fields according to a serialized Beam schema. Ironbeam provides this coder in `ironbeam-coder-row`, a separate crate that does not depend on the worker harness. A row has a schema type fixed by the application. The schema payload is part of the logical graph and its execution identity, so a driver and worker must rebuild the same schema.

## Define a row type

Implement `RowSchema` with a stable type ID and a deterministic `Schema`. The example below declares one nonnullable 32-bit integer field. The application constructs the value with `BeamRow::new`, which checks it against the schema before it enters a collection.

```rust
use beam_proto::pipeline_v1::{
    atomic_type_value, field_type, field_value, AtomicType, AtomicTypeValue,
    Field, FieldType, FieldValue, Row, Schema,
};
use ironbeam_coder_row::{BeamRow, RowSchema};

struct IdSchema;

impl RowSchema for IdSchema {
    const TYPE_ID: &'static str = "example/id-row/v1";

    fn schema() -> Schema {
        Schema {
            id: "f2740e85-7e46-44bb-9fe5-856c1ce55937".into(),
            fields: vec![Field {
                name: "id".into(),
                r#type: Some(FieldType {
                    nullable: false,
                    type_info: Some(field_type::TypeInfo::AtomicType(
                        AtomicType::Int32 as i32,
                    )),
                }),
                ..Default::default()
            }],
            ..Default::default()
        }
    }
}

let row = BeamRow::<IdSchema>::new(Row {
    values: vec![FieldValue {
        field_value: Some(field_value::FieldValue::AtomicValue(AtomicTypeValue {
            value: Some(atomic_type_value::Value::Int32(42)),
        })),
    }],
})?;
```

The `Row` and `Schema` types come from `beam-proto`. Both the schema ID and the field definitions must remain stable for an existing type ID. When the meaning of a row type changes, use a new type ID. A changed schema payload also changes the graph fingerprint.

## Use rows in a pipeline

`BeamRow<IdSchema>` implements `Element`. It can pass through ordinary typed collection transforms. Direct encodes it with the Beam row bytes; portable lowering emits `beam:coder:row:v1` with the serialized schema payload. The existing [Prism proof](https://github.com/nhubbard/ironbeam/blob/main/examples/prism-proof/src/implementation/row.rs) builds two rows, reads their fields in a later transform, and checks the same output on Direct and Prism.

For schema evolution, `decode_row_evolved` accepts an explicit writer schema and reader schema. A typed row can use `old_row.project::<NewSchema>()` to make the same projection inside a transform. Encoding positions identify corresponding fields: the reader may reorder them, remove trailing positions, or add nullable fields at the end. A new field receives null. The projection also recurses into nested row fields. The call fails if an atomic field type changes or a new nonnullable field has no value. The [projection proof](https://github.com/nhubbard/ironbeam/blob/main/examples/prism-proof/src/implementation/row_projection.rs) runs a reordered row with one added nullable field on Direct and local Prism.

Beam row bytes also carry a field count. When decoding a complete row, `decode_row` and `BeamRow<S>::decode` can add nullable trailing fields or discard trailing fields that the reader does not know. A newly added nonnullable field fails validation. This implicit form does not check the unknown fields' types; use `decode_row_evolved` when both schemas are available and type compatibility matters. A nested row has no separate byte length, so a nested reader with fewer fields must use a known writer schema and `decode_row_evolved`. Beam's Python and Java SDKs differ in how they number null bits for reordered fields. Ironbeam follows the Python layout used by the pinned standard coder examples; avoid nullable reordered fields at cross-SDK boundaries until that upstream difference is resolved.

The decimal logical type uses a bytes representation in Beam's schema. In Ironbeam's `FieldValue`, put a UTF-8 decimal string inside that bytes value, such as `"3.1415"` or `"-100.123"`. The codec writes Beam's scale and signed integer format; it does not send the text itself. Decoding returns decimal text in the same representation. Tests match the pinned Beam decimal examples and run a decimal row pipeline on Direct and local Prism. Decimal values preserve their scale, including trailing fractional zeros. The [Beam semantics ledger](../concepts/beam-semantics.md) records the other tested field types and remaining cases.
