use anyhow::{anyhow, bail, Result};
use beam_proto::pipeline_v1::{atomic_type_value, AtomicType, AtomicTypeValue};

use crate::{decode_varint, encode_varint};

pub(super) fn encode_atomic(kind: i32, atom: &AtomicTypeValue, out: &mut Vec<u8>) -> Result<()> {
    use atomic_type_value::Value;
    match (AtomicType::try_from(kind).ok(), atom.value.as_ref()) {
        (Some(AtomicType::Byte), Some(Value::Byte(value))) => out.push(u8::try_from(*value)?),
        (Some(AtomicType::Int16), Some(Value::Int16(value))) => {
            out.extend_from_slice(&i16::try_from(*value)?.to_be_bytes());
        }
        (Some(AtomicType::Int32), Some(Value::Int32(value))) => {
            encode_varint(i64::from(*value).cast_unsigned(), out)?;
        }
        (Some(AtomicType::Int64), Some(Value::Int64(value))) => {
            encode_varint(value.cast_unsigned(), out)?;
        }
        (Some(AtomicType::Float), Some(Value::Float(value))) => {
            out.extend_from_slice(&value.to_be_bytes());
        }
        (Some(AtomicType::Double), Some(Value::Double(value))) => {
            out.extend_from_slice(&value.to_be_bytes());
        }
        (Some(AtomicType::String), Some(Value::String(value))) => {
            encode_bytes(value.as_bytes(), out)?;
        }
        (Some(AtomicType::Boolean), Some(Value::Boolean(value))) => out.push(u8::from(*value)),
        (Some(AtomicType::Bytes), Some(Value::Bytes(value))) => encode_bytes(value, out)?,
        _ => bail!("atomic row value does not match field type"),
    }
    Ok(())
}

pub(super) fn decode_atomic(kind: i32, input: &mut &[u8]) -> Result<AtomicTypeValue> {
    use atomic_type_value::Value;
    let value = match AtomicType::try_from(kind)? {
        AtomicType::Byte => Value::Byte(i32::from(take(input, 1)?[0])),
        AtomicType::Int16 => {
            Value::Int16(i32::from(i16::from_be_bytes(take(input, 2)?.try_into()?)))
        }
        AtomicType::Int32 => Value::Int32(i32::try_from(read_varint(input)?.cast_signed())?),
        AtomicType::Int64 => Value::Int64(read_varint(input)?.cast_signed()),
        AtomicType::Float => Value::Float(f32::from_be_bytes(take(input, 4)?.try_into()?)),
        AtomicType::Double => Value::Double(f64::from_be_bytes(take(input, 8)?.try_into()?)),
        AtomicType::String => Value::String(String::from_utf8(read_bytes(input)?.to_vec())?),
        AtomicType::Boolean => match take(input, 1)?[0] {
            0 => Value::Boolean(false),
            1 => Value::Boolean(true),
            _ => bail!("invalid row boolean"),
        },
        AtomicType::Bytes => Value::Bytes(read_bytes(input)?.to_vec()),
        AtomicType::Unspecified => bail!("unspecified atomic row type"),
    };
    Ok(AtomicTypeValue { value: Some(value) })
}

fn encode_bytes(bytes: &[u8], out: &mut Vec<u8>) -> Result<()> {
    encode_varint(bytes.len() as u64, out)?;
    out.extend_from_slice(bytes);
    Ok(())
}

fn read_bytes<'a>(input: &mut &'a [u8]) -> Result<&'a [u8]> {
    let len = usize::try_from(read_varint(input)?)?;
    take(input, len)
}

pub(super) fn read_varint(input: &mut &[u8]) -> Result<u64> {
    let mut cursor = std::io::Cursor::new(*input);
    let value = decode_varint(&mut cursor)?;
    *input = &input[usize::try_from(cursor.position())?..];
    Ok(value)
}

pub(super) fn read_count(input: &mut &[u8]) -> Result<usize> {
    let count = i32::from_be_bytes(take(input, 4)?.try_into()?);
    usize::try_from(count).map_err(|_| anyhow!("negative or state-backed row sequence count"))
}

pub(super) fn take<'a>(input: &mut &'a [u8], count: usize) -> Result<&'a [u8]> {
    if input.len() < count {
        bail!("truncated row value");
    }
    let (head, rest) = input.split_at(count);
    *input = rest;
    Ok(head)
}
