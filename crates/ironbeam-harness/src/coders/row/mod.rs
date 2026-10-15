//! Beam's schema row bytes for fixed, known schemas.
//!
//! This codec operates on Beam's protobuf `Schema` and `Row` values. It does
//! not register a PCollection coder: the graph must carry the schema payload
//! before a row can cross an SDK boundary as `beam:coder:row:v1`.

use anyhow::{anyhow, bail, Result};

mod atomic;
mod values;

use atomic::{read_varint, take};
use beam_proto::pipeline_v1::{
    atomic_type_value, field_value, AtomicTypeValue, FieldValue, Row, Schema,
};
use values::{decode_value, encode_value};

use super::encode_varint;

pub(super) const MAX_DEPTH: usize = 64;

/// Encode one Beam row using the schema's declared field order and encoding positions.
pub fn encode_row(schema: &Schema, row: &Row) -> Result<Vec<u8>> {
    let mut output = Vec::new();
    encode_row_into(schema, row, &mut output, 0)?;
    Ok(output)
}

/// Decode one complete Beam row. Trailing bytes are rejected.
pub fn decode_row(schema: &Schema, bytes: &[u8]) -> Result<Row> {
    let mut input = bytes;
    let row = decode_row_from(schema, &mut input, 0)?;
    if !input.is_empty() {
        bail!("row coder has trailing bytes");
    }
    Ok(row)
}

fn encoding_order(schema: &Schema) -> Result<Vec<usize>> {
    let count = schema.fields.len();
    if !schema.encoding_positions_set {
        return Ok((0..count).collect());
    }
    let mut positions = vec![usize::MAX; count];
    for (index, field) in schema.fields.iter().enumerate() {
        let position = usize::try_from(field.encoding_position)
            .map_err(|_| anyhow!("negative row encoding position"))?;
        let slot = positions
            .get_mut(position)
            .ok_or_else(|| anyhow!("row encoding position outside schema"))?;
        if *slot != usize::MAX {
            bail!("duplicate row encoding position");
        }
        *slot = index;
    }
    if positions.contains(&usize::MAX) {
        bail!("row encoding positions must be contiguous");
    }
    Ok(positions)
}

fn static_encoding(schema: &Schema) -> bool {
    schema.options.iter().any(|option| {
        option.name == "beam:option:row:static_encoding"
            && matches!(
                option
                    .value
                    .as_ref()
                    .and_then(|value| value.field_value.as_ref()),
                Some(field_value::FieldValue::AtomicValue(AtomicTypeValue {
                    value: Some(atomic_type_value::Value::Boolean(true)),
                }))
            )
    })
}

fn encode_row_into(schema: &Schema, row: &Row, output: &mut Vec<u8>, depth: usize) -> Result<()> {
    if depth >= MAX_DEPTH {
        bail!("row nesting exceeds limit");
    }
    if row.values.len() != schema.fields.len() {
        bail!("row value count does not match schema");
    }
    let order = encoding_order(schema)?;
    let static_encoding = static_encoding(schema);
    if !static_encoding {
        encode_varint(schema.fields.len() as u64, output)?;
    }
    let mut nulls = vec![0_u8; schema.fields.len().div_ceil(8)];
    for (index, value) in row.values.iter().enumerate() {
        if value.field_value.is_none() {
            nulls[index / 8] |= 1 << (index % 8);
            if !schema.fields[index]
                .r#type
                .as_ref()
                .ok_or_else(|| anyhow!("row field lacks type"))?
                .nullable
            {
                bail!(
                    "null in nonnullable row field {}",
                    schema.fields[index].name
                );
            }
        }
    }
    if static_encoding {
        if nulls.iter().any(|byte| *byte != 0) {
            bail!("static row encoding cannot represent null fields");
        }
    } else if nulls.iter().any(|byte| *byte != 0) {
        encode_varint(nulls.len() as u64, output)?;
        output.extend_from_slice(&nulls);
    } else {
        encode_varint(0, output)?;
    }
    for index in order {
        let value = &row.values[index];
        if value.field_value.is_some() {
            let field_type = schema.fields[index]
                .r#type
                .as_ref()
                .ok_or_else(|| anyhow!("row field lacks type"))?;
            encode_value(field_type, value, output, depth + 1)?;
        }
    }
    Ok(())
}

fn decode_row_from(schema: &Schema, input: &mut &[u8], depth: usize) -> Result<Row> {
    if depth >= MAX_DEPTH {
        bail!("row nesting exceeds limit");
    }
    let nulls = if static_encoding(schema) {
        &[][..]
    } else {
        let count = read_varint(input)?;
        if count != schema.fields.len() as u64 {
            bail!("row field count does not match schema");
        }
        let mask_len = usize::try_from(read_varint(input)?)?;
        if mask_len > schema.fields.len().div_ceil(8) {
            bail!("row null mask exceeds schema width");
        }
        take(input, mask_len)?
    };
    let mut values = vec![FieldValue { field_value: None }; schema.fields.len()];
    for index in encoding_order(schema)? {
        let field_type = schema.fields[index]
            .r#type
            .as_ref()
            .ok_or_else(|| anyhow!("row field lacks type"))?;
        let is_null = nulls
            .get(index / 8)
            .is_some_and(|byte| byte & (1 << (index % 8)) != 0);
        if is_null {
            if !field_type.nullable {
                bail!(
                    "null in nonnullable row field {}",
                    schema.fields[index].name
                );
            }
        } else {
            values[index] = decode_value(field_type, input, depth + 1)?;
        }
    }
    Ok(Row { values })
}
