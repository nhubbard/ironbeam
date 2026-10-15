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

For schema evolution, `decode_row_evolved` accepts an explicit writer schema and reader schema. It can project compatible added, removed, and reordered fields. An incoming row does not identify its writer schema by itself, so the caller must supply that schema. Decimal fields and implicit writer-schema discovery are not yet supported by this codec. The [Beam semantics ledger](../concepts/beam-semantics.md) records the tested field types and remaining cases.
