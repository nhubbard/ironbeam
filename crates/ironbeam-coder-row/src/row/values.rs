use anyhow::{anyhow, bail, Result};
use beam_proto::pipeline_v1::{
    atomic_type_value, field_type, field_value, ArrayTypeValue, AtomicTypeValue, FieldType,
    FieldValue, IterableTypeValue, LogicalTypeValue, MapTypeEntry, MapTypeValue,
};

use super::atomic::{decode_atomic, encode_atomic, read_count, take};
use super::{decode_row_from, encode_row_into, MAX_DEPTH};

pub(super) fn encode_value(
    ty: &FieldType,
    value: &FieldValue,
    out: &mut Vec<u8>,
    depth: usize,
) -> Result<()> {
    if depth >= MAX_DEPTH {
        bail!("row nesting exceeds limit");
    }
    match (ty.type_info.as_ref(), value.field_value.as_ref()) {
        (
            Some(field_type::TypeInfo::AtomicType(kind)),
            Some(field_value::FieldValue::AtomicValue(atom)),
        ) => encode_atomic(*kind, atom, out),
        (
            Some(field_type::TypeInfo::ArrayType(array)),
            Some(field_value::FieldValue::ArrayValue(values)),
        ) => encode_sequence(array.element_type.as_deref(), &values.element, out, depth),
        (
            Some(field_type::TypeInfo::IterableType(iterable)),
            Some(field_value::FieldValue::IterableValue(values)),
        ) => encode_sequence(
            iterable.element_type.as_deref(),
            &values.element,
            out,
            depth,
        ),
        (
            Some(field_type::TypeInfo::MapType(map)),
            Some(field_value::FieldValue::MapValue(values)),
        ) => {
            let key = map
                .key_type
                .as_deref()
                .ok_or_else(|| anyhow!("map key type missing"))?;
            let item = map
                .value_type
                .as_deref()
                .ok_or_else(|| anyhow!("map value type missing"))?;
            out.extend_from_slice(&i32::try_from(values.entries.len())?.to_be_bytes());
            for entry in &values.entries {
                encode_nullable(key, entry.key.as_ref(), out, depth + 1)?;
                encode_nullable(item, entry.value.as_ref(), out, depth + 1)?;
            }
            Ok(())
        }
        (
            Some(field_type::TypeInfo::RowType(row_type)),
            Some(field_value::FieldValue::RowValue(row)),
        ) => encode_row_into(
            row_type
                .schema
                .as_ref()
                .ok_or_else(|| anyhow!("nested row schema missing"))?,
            row,
            out,
            depth + 1,
        ),
        (
            Some(field_type::TypeInfo::LogicalType(logical)),
            Some(field_value::FieldValue::LogicalTypeValue(value)),
        ) => {
            let representation = value
                .value
                .as_deref()
                .ok_or_else(|| anyhow!("logical value missing"))?;
            if logical.urn == "beam:logical_type:millis_instant:v1" {
                let Some(field_value::FieldValue::AtomicValue(AtomicTypeValue {
                    value: Some(atomic_type_value::Value::Int64(millis)),
                })) = representation.field_value.as_ref()
                else {
                    bail!("millis instant requires an int64 representation");
                };
                out.extend_from_slice(&(millis.cast_unsigned() ^ (1_u64 << 63)).to_be_bytes());
                return Ok(());
            }
            if logical.urn == "beam:logical_type:decimal:v1" {
                bail!("decimal requires Beam's scale and integer coder");
            }
            encode_value(
                logical
                    .representation
                    .as_deref()
                    .ok_or_else(|| anyhow!("logical representation missing"))?,
                representation,
                out,
                depth + 1,
            )
        }
        _ => bail!("row value does not match field type"),
    }
}

pub(super) fn decode_value(ty: &FieldType, input: &mut &[u8], depth: usize) -> Result<FieldValue> {
    if depth >= MAX_DEPTH {
        bail!("row nesting exceeds limit");
    }
    let field_value = match ty
        .type_info
        .as_ref()
        .ok_or_else(|| anyhow!("row field type missing"))?
    {
        field_type::TypeInfo::AtomicType(kind) => {
            field_value::FieldValue::AtomicValue(decode_atomic(*kind, input)?)
        }
        field_type::TypeInfo::ArrayType(array) => {
            field_value::FieldValue::ArrayValue(ArrayTypeValue {
                element: decode_sequence(array.element_type.as_deref(), input, depth)?,
            })
        }
        field_type::TypeInfo::IterableType(iterable) => {
            field_value::FieldValue::IterableValue(IterableTypeValue {
                element: decode_sequence(iterable.element_type.as_deref(), input, depth)?,
            })
        }
        field_type::TypeInfo::MapType(map) => {
            let key = map
                .key_type
                .as_deref()
                .ok_or_else(|| anyhow!("map key type missing"))?;
            let item = map
                .value_type
                .as_deref()
                .ok_or_else(|| anyhow!("map value type missing"))?;
            let count = read_count(input)?;
            let mut entries = Vec::new();
            for _ in 0..count {
                entries.push(MapTypeEntry {
                    key: Some(decode_nullable(key, input, depth + 1)?),
                    value: Some(decode_nullable(item, input, depth + 1)?),
                });
            }
            field_value::FieldValue::MapValue(MapTypeValue { entries })
        }
        field_type::TypeInfo::RowType(row_type) => {
            field_value::FieldValue::RowValue(decode_row_from(
                row_type
                    .schema
                    .as_ref()
                    .ok_or_else(|| anyhow!("nested row schema missing"))?,
                input,
                depth + 1,
            )?)
        }
        field_type::TypeInfo::LogicalType(logical) => {
            let representation = if logical.urn == "beam:logical_type:millis_instant:v1" {
                let encoded = u64::from_be_bytes(take(input, 8)?.try_into()?);
                FieldValue {
                    field_value: Some(field_value::FieldValue::AtomicValue(AtomicTypeValue {
                        value: Some(atomic_type_value::Value::Int64(
                            (encoded ^ (1_u64 << 63)).cast_signed(),
                        )),
                    })),
                }
            } else if logical.urn == "beam:logical_type:decimal:v1" {
                bail!("decimal requires Beam's scale and integer coder");
            } else {
                decode_value(
                    logical
                        .representation
                        .as_deref()
                        .ok_or_else(|| anyhow!("logical representation missing"))?,
                    input,
                    depth + 1,
                )?
            };
            field_value::FieldValue::LogicalTypeValue(Box::new(LogicalTypeValue {
                value: Some(Box::new(representation)),
            }))
        }
    };
    Ok(FieldValue {
        field_value: Some(field_value),
    })
}

fn encode_sequence(
    ty: Option<&FieldType>,
    values: &[FieldValue],
    out: &mut Vec<u8>,
    depth: usize,
) -> Result<()> {
    let ty = ty.ok_or_else(|| anyhow!("sequence element type missing"))?;
    out.extend_from_slice(&i32::try_from(values.len())?.to_be_bytes());
    for value in values {
        encode_nullable(ty, Some(value), out, depth + 1)?;
    }
    Ok(())
}

fn decode_sequence(
    ty: Option<&FieldType>,
    input: &mut &[u8],
    depth: usize,
) -> Result<Vec<FieldValue>> {
    let ty = ty.ok_or_else(|| anyhow!("sequence element type missing"))?;
    let count = read_count(input)?;
    let mut values = Vec::new();
    for _ in 0..count {
        values.push(decode_nullable(ty, input, depth + 1)?);
    }
    Ok(values)
}

fn encode_nullable(
    ty: &FieldType,
    value: Option<&FieldValue>,
    out: &mut Vec<u8>,
    depth: usize,
) -> Result<()> {
    let value = value.ok_or_else(|| anyhow!("row value missing"))?;
    if ty.nullable {
        out.push(u8::from(value.field_value.is_some()));
        if value.field_value.is_none() {
            return Ok(());
        }
    } else if value.field_value.is_none() {
        bail!("null in nonnullable nested field");
    }
    encode_value(ty, value, out, depth + 1)
}

fn decode_nullable(ty: &FieldType, input: &mut &[u8], depth: usize) -> Result<FieldValue> {
    if ty.nullable {
        match take(input, 1)?[0] {
            0 => return Ok(FieldValue { field_value: None }),
            1 => {}
            _ => bail!("invalid nullable field marker"),
        }
    }
    decode_value(ty, input, depth + 1)
}
