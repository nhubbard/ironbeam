//! Beam's schema row bytes for fixed, known schemas.
//!
//! This codec operates on Beam's protobuf `Schema` and `Row` values. It does
//! not register a `PCollection` coder: the graph must carry the schema payload
//! before a row can cross an SDK boundary as `beam:coder:row:v1`.

use std::marker::PhantomData;

use anyhow::{anyhow, bail, Result};

mod atomic;
mod decimal;
mod values;

use atomic::{read_varint, take};
use beam_proto::pipeline_v1::{
    atomic_type_value, field_type, field_value, ArrayTypeValue, AtomicTypeValue, FieldType,
    FieldValue, IterableTypeValue, MapType, MapTypeEntry, MapTypeValue, Row, Schema,
};
use ironbeam_core::{Element, Error};
use prost::Message;
use values::{decode_value, encode_value};

use crate::encode_varint;

const MAX_DEPTH: usize = 64;

/// A schema fixed by the application binary. Its type ID must change when a
/// semantic row type changes; the serialized schema also enters graph identity.
pub trait RowSchema: Send + Sync + 'static {
    const TYPE_ID: &'static str;
    /// Return the fixed Beam schema used to validate and code this row type.
    fn schema() -> Schema;
}

/// A row whose bytes and portable coder use Beam's schema row format.
pub struct BeamRow<S: RowSchema> {
    value: Row,
    marker: PhantomData<S>,
}

impl<S: RowSchema> Clone for BeamRow<S> {
    fn clone(&self) -> Self {
        Self {
            value: self.value.clone(),
            marker: PhantomData,
        }
    }
}

impl<S: RowSchema> BeamRow<S> {
    /// Validate a value against the schema before admitting it to a pipeline.
    ///
    /// # Errors
    ///
    /// Returns an error when the row does not match the declared schema.
    pub fn new(value: Row) -> Result<Self> {
        encode_row(&S::schema(), &value)?;
        Ok(Self {
            value,
            marker: PhantomData,
        })
    }

    /// Borrow the validated row without changing its schema.
    #[must_use]
    pub const fn as_row(&self) -> &Row {
        &self.value
    }

    /// Take ownership of the validated row value.
    #[must_use]
    pub fn into_row(self) -> Row {
        self.value
    }

    /// Project this row into another fixed schema using the declared writer schema.
    ///
    /// # Errors
    ///
    /// Returns an error when the schemas have incompatible fields or the
    /// projected row violates the reader schema.
    pub fn project<T: RowSchema>(&self) -> Result<BeamRow<T>> {
        let bytes = encode_row(&S::schema(), &self.value)?;
        let projected = decode_row_evolved(&S::schema(), &T::schema(), &bytes)?;
        BeamRow::<T>::new(projected)
    }
}

impl<S: RowSchema> Element for BeamRow<S> {
    const TYPE_ID: &'static str = S::TYPE_ID;
    const CODER_ID: &'static str = "beam:coder:row:v1";

    fn supports_coder() -> bool {
        true
    }

    fn coder_payload() -> Vec<u8> {
        S::schema().encode_to_vec()
    }

    fn encode(&self) -> std::result::Result<Vec<u8>, Error> {
        encode_row(&S::schema(), &self.value)
            .map_err(|error| Error::incompatible(error.to_string()))
    }

    fn decode(bytes: &[u8]) -> std::result::Result<Self, Error> {
        let value = decode_row(&S::schema(), bytes)
            .map_err(|error| Error::incompatible(error.to_string()))?;
        Ok(Self {
            value,
            marker: PhantomData,
        })
    }
}

/// Encode one Beam row using the schema's declared field order and encoding positions.
///
/// # Errors
///
/// Returns an error for a malformed schema or a value that does not match it.
pub fn encode_row(schema: &Schema, row: &Row) -> Result<Vec<u8>> {
    let mut output = Vec::new();
    encode_row_into(schema, row, &mut output, 0)?;
    Ok(output)
}

/// Decode one complete Beam row.
///
/// A shorter encoded row gains nullable trailing fields; a longer row drops
/// fields beyond the reader schema. The latter is safe only here, where the
/// byte slice bounds the complete row. Nested rows with unknown trailing
/// fields require an explicit writer schema.
///
/// # Errors
///
/// Returns an error for malformed bytes or a malformed schema.
pub fn decode_row(schema: &Schema, bytes: &[u8]) -> Result<Row> {
    let mut input = bytes;
    let encoded_count = if static_encoding(schema) {
        schema.fields.len() as u64
    } else {
        read_varint(&mut input)?
    };
    input = bytes;
    let row = decode_row_from(schema, &mut input, 0)?;
    if !input.is_empty() && encoded_count <= schema.fields.len() as u64 {
        bail!("row coder has trailing bytes");
    }
    Ok(row)
}

/// Decode with the writer's schema, then project fields into a newer schema.
///
/// Encoding positions identify fields across a reorder. Fields added by the
/// reader are null; removed fields are ignored. Projection recurses into nested
/// rows. Incompatible field types are rejected.
///
/// # Errors
///
/// Returns an error for malformed bytes or incompatible schemas.
pub fn decode_row_evolved(writer: &Schema, reader: &Schema, bytes: &[u8]) -> Result<Row> {
    let old = decode_row(writer, bytes)?;
    project_row(writer, reader, &old, 0)
}

fn project_row(writer: &Schema, reader: &Schema, old: &Row, depth: usize) -> Result<Row> {
    if depth >= MAX_DEPTH {
        bail!("row nesting exceeds limit");
    }
    let old_order = encoding_order(writer)?;
    let new_order = encoding_order(reader)?;
    if old.values.len() != writer.fields.len() {
        bail!("writer row value count does not match schema");
    }
    let mut values = vec![FieldValue { field_value: None }; reader.fields.len()];
    for (position, &new_index) in new_order.iter().enumerate() {
        let Some(&old_index) = old_order.get(position) else {
            if !reader.fields[new_index]
                .r#type
                .as_ref()
                .ok_or_else(|| anyhow!("reader row field lacks type"))?
                .nullable
            {
                bail!("new nonnullable row field has no encoded value");
            }
            continue;
        };
        let old_type = writer.fields[old_index]
            .r#type
            .as_ref()
            .ok_or_else(|| anyhow!("writer row field lacks type"))?;
        let new_type = reader.fields[new_index]
            .r#type
            .as_ref()
            .ok_or_else(|| anyhow!("reader row field lacks type"))?;
        values[new_index] =
            project_field_value(old_type, new_type, &old.values[old_index], depth + 1)
                .map_err(|error| anyhow!("row field at encoding position {position}: {error}"))?;
    }
    Ok(Row { values })
}

fn project_field_value(
    writer: &FieldType,
    reader: &FieldType,
    value: &FieldValue,
    depth: usize,
) -> Result<FieldValue> {
    if value.field_value.is_none() && !reader.nullable {
        bail!("null projected into nonnullable row field");
    }
    match (&writer.type_info, &reader.type_info, &value.field_value) {
        (
            Some(field_type::TypeInfo::RowType(writer_row)),
            Some(field_type::TypeInfo::RowType(reader_row)),
            Some(field_value::FieldValue::RowValue(row)),
        ) => {
            let writer_schema = writer_row
                .schema
                .as_ref()
                .ok_or_else(|| anyhow!("writer nested row lacks schema"))?;
            let reader_schema = reader_row
                .schema
                .as_ref()
                .ok_or_else(|| anyhow!("reader nested row lacks schema"))?;
            Ok(FieldValue {
                field_value: Some(field_value::FieldValue::RowValue(project_row(
                    writer_schema,
                    reader_schema,
                    row,
                    depth,
                )?)),
            })
        }
        (Some(field_type::TypeInfo::RowType(_)), Some(field_type::TypeInfo::RowType(_)), None)
        | (
            Some(field_type::TypeInfo::ArrayType(_)),
            Some(field_type::TypeInfo::ArrayType(_)),
            None,
        )
        | (
            Some(field_type::TypeInfo::IterableType(_)),
            Some(field_type::TypeInfo::IterableType(_)),
            None,
        )
        | (Some(field_type::TypeInfo::MapType(_)), Some(field_type::TypeInfo::MapType(_)), None) => {
            Ok(value.clone())
        }
        (
            Some(field_type::TypeInfo::ArrayType(writer_array)),
            Some(field_type::TypeInfo::ArrayType(reader_array)),
            Some(field_value::FieldValue::ArrayValue(values)),
        ) => Ok(FieldValue {
            field_value: Some(field_value::FieldValue::ArrayValue(ArrayTypeValue {
                element: project_sequence(
                    writer_array.element_type.as_deref(),
                    reader_array.element_type.as_deref(),
                    &values.element,
                    depth,
                )?,
            })),
        }),
        (
            Some(field_type::TypeInfo::IterableType(writer_iterable)),
            Some(field_type::TypeInfo::IterableType(reader_iterable)),
            Some(field_value::FieldValue::IterableValue(values)),
        ) => Ok(FieldValue {
            field_value: Some(field_value::FieldValue::IterableValue(IterableTypeValue {
                element: project_sequence(
                    writer_iterable.element_type.as_deref(),
                    reader_iterable.element_type.as_deref(),
                    &values.element,
                    depth,
                )?,
            })),
        }),
        (
            Some(field_type::TypeInfo::MapType(writer_map)),
            Some(field_type::TypeInfo::MapType(reader_map)),
            Some(field_value::FieldValue::MapValue(values)),
        ) => project_map(writer_map, reader_map, values, depth),
        _ if writer.type_info == reader.type_info => Ok(value.clone()),
        _ => bail!("row field type changed"),
    }
}

fn project_map(
    writer_map: &MapType,
    reader_map: &MapType,
    values: &MapTypeValue,
    depth: usize,
) -> Result<FieldValue> {
    let writer_key = writer_map
        .key_type
        .as_deref()
        .ok_or_else(|| anyhow!("writer map lacks key type"))?;
    let reader_key = reader_map
        .key_type
        .as_deref()
        .ok_or_else(|| anyhow!("reader map lacks key type"))?;
    let writer_value = writer_map
        .value_type
        .as_deref()
        .ok_or_else(|| anyhow!("writer map lacks value type"))?;
    let reader_value = reader_map
        .value_type
        .as_deref()
        .ok_or_else(|| anyhow!("reader map lacks value type"))?;
    let entries = values
        .entries
        .iter()
        .map(|entry| {
            Ok(MapTypeEntry {
                key: Some(project_field_value(
                    writer_key,
                    reader_key,
                    entry
                        .key
                        .as_ref()
                        .ok_or_else(|| anyhow!("map key missing"))?,
                    depth + 1,
                )?),
                value: Some(project_field_value(
                    writer_value,
                    reader_value,
                    entry
                        .value
                        .as_ref()
                        .ok_or_else(|| anyhow!("map value missing"))?,
                    depth + 1,
                )?),
            })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(FieldValue {
        field_value: Some(field_value::FieldValue::MapValue(MapTypeValue { entries })),
    })
}

fn project_sequence(
    writer: Option<&FieldType>,
    reader: Option<&FieldType>,
    values: &[FieldValue],
    depth: usize,
) -> Result<Vec<FieldValue>> {
    let writer = writer.ok_or_else(|| anyhow!("writer sequence lacks element type"))?;
    let reader = reader.ok_or_else(|| anyhow!("reader sequence lacks element type"))?;
    values
        .iter()
        .map(|value| project_field_value(writer, reader, value, depth + 1))
        .collect()
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
    let (count, nulls) = if static_encoding(schema) {
        (schema.fields.len(), &[][..])
    } else {
        let count = usize::try_from(read_varint(input)?)?;
        if depth > 0 && count > schema.fields.len() {
            bail!("nested row has unknown trailing fields without a byte boundary");
        }
        let mask_len = usize::try_from(read_varint(input)?)?;
        if mask_len > count.div_ceil(8) {
            bail!("row null mask exceeds encoded width");
        }
        (count, take(input, mask_len)?)
    };
    let mut values = vec![FieldValue { field_value: None }; schema.fields.len()];
    for (position, index) in encoding_order(schema)?.into_iter().enumerate() {
        let field_type = schema.fields[index]
            .r#type
            .as_ref()
            .ok_or_else(|| anyhow!("row field lacks type"))?;
        if position >= count {
            if !field_type.nullable {
                bail!("new nonnullable row field has no encoded value");
            }
            continue;
        }
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
