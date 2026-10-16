//! Beam coder bodies for the two intermediate v5 source SDF collections.
//!
//! The pair collection uses `KV<bytes, bytes>` and the sized collection uses
//! `KV<pair, double>`. These functions encode the element body; a caller adds
//! the windowed-value header at a runner Data boundary.

use anyhow::{bail, Context, Result};
use ironbeam_graphx::sdf::{SourceRestriction, MAX_RESTRICTION_BYTES};

use crate::coders::{decode_length_prefix, encode_length_prefix};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourcePair {
    pub element: Vec<u8>,
    pub restriction: SourceRestriction,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SizedSourcePair {
    pub pair: SourcePair,
    pub size: f64,
}

/// Create the only initial restriction for a registered one-split source.
pub fn pair_with_restriction(source_key: &str, impulse: &[u8]) -> Result<Vec<u8>> {
    if !impulse.is_empty() {
        bail!("v5 SDF source trigger must be an empty Impulse element");
    }
    encode_pair(&SourcePair {
        element: impulse.to_vec(),
        restriction: SourceRestriction::initial(source_key),
    })
}

/// The source has exactly one stable split. Emit its initial work estimate.
pub fn split_and_size(source_key: &str, pair_body: &[u8]) -> Result<Vec<u8>> {
    let pair = decode_pair(source_key, pair_body)?;
    encode_sized(&SizedSourcePair { pair, size: 1.0 })
}

/// Apply Beam's default drain rule to a one-split unbounded source.
/// Its restriction has no finite upper bound, so draining discards it. Decode
/// first to reject a malformed or foreign restriction at the worker boundary.
pub fn truncate_sized(source_key: &str, sized_body: &[u8]) -> Result<Option<Vec<u8>>> {
    let _ = decode_sized(source_key, sized_body)?;
    Ok(None)
}

/// Encode an SDF element and restriction as two length-prefixed fields.
pub fn encode_pair(pair: &SourcePair) -> Result<Vec<u8>> {
    let mut output = Vec::new();
    encode_length_prefix(&pair.element, &mut output)?;
    encode_length_prefix(&pair.restriction.encode()?, &mut output)?;
    Ok(output)
}

/// Decode one complete SDF pair and reject trailing or foreign bytes.
pub fn decode_pair(source_key: &str, bytes: &[u8]) -> Result<SourcePair> {
    let (pair, consumed) = decode_pair_prefix(source_key, bytes)?;
    if consumed != bytes.len() {
        bail!("v5 SDF pair has trailing bytes");
    }
    Ok(pair)
}

fn decode_pair_prefix(source_key: &str, bytes: &[u8]) -> Result<(SourcePair, usize)> {
    let (element, element_len) = decode_length_prefix(bytes).context("decode SDF input element")?;
    let (restriction, restriction_len) =
        decode_length_prefix(&bytes[element_len..]).context("decode SDF restriction bytes")?;
    let restriction = SourceRestriction::decode(source_key, restriction)
        .context("validate SDF source restriction")?;
    Ok((
        SourcePair {
            element: element.to_vec(),
            restriction,
        },
        element_len + restriction_len,
    ))
}

/// Encode an SDF pair followed by its finite work-size estimate.
pub fn encode_sized(value: &SizedSourcePair) -> Result<Vec<u8>> {
    validate_size(value.size)?;
    let mut output = encode_pair(&value.pair)?;
    output.extend_from_slice(&value.size.to_be_bytes());
    Ok(output)
}

/// Decode an SDF pair and its work-size estimate for `source_key`.
pub fn decode_sized(source_key: &str, bytes: &[u8]) -> Result<SizedSourcePair> {
    let (pair, consumed) = decode_pair_prefix(source_key, bytes)?;
    let tail = bytes
        .get(consumed..)
        .context("v5 SDF sized pair length exceeds input")?;
    let size_bytes: [u8; 8] = tail
        .try_into()
        .map_err(|_| anyhow::anyhow!("v5 SDF sized pair must end with one double"))?;
    let size = f64::from_be_bytes(size_bytes);
    validate_size(size)?;
    Ok(SizedSourcePair { pair, size })
}

/// Find a full pair body in a possibly fragmented Fn API Data chunk.
pub fn try_pair_body_len(bytes: &[u8]) -> Result<Option<usize>> {
    let Some(first) = try_bytes_body_len(bytes)? else {
        return Ok(None);
    };
    let Some(second) = try_bytes_body_len(&bytes[first..])? else {
        return Ok(None);
    };
    Ok(Some(first + second))
}

/// Return the size of a complete sized-pair body, or `None` if fragmented.
pub fn try_sized_body_len(bytes: &[u8]) -> Result<Option<usize>> {
    let Some(pair_len) = try_pair_body_len(bytes)? else {
        return Ok(None);
    };
    let end = pair_len
        .checked_add(8)
        .context("v5 SDF sized body length overflow")?;
    Ok((bytes.len() >= end).then_some(end))
}

/// Return the size of a complete length-prefixed byte field, if available.
pub fn try_bytes_body_len(bytes: &[u8]) -> Result<Option<usize>> {
    let mut value = 0_u64;
    for index in 0..10 {
        let Some(&byte) = bytes.get(index) else {
            return Ok(None);
        };
        if index == 9 && byte > 1 {
            bail!("v5 SDF byte length overflows varint");
        }
        value |= u64::from(byte & 0x7f) << (7 * index);
        if byte & 0x80 == 0 {
            let len =
                usize::try_from(value).context("v5 SDF byte length exceeds platform range")?;
            if len > MAX_RESTRICTION_BYTES {
                bail!("v5 SDF byte value exceeds 1 MiB");
            }
            let end = (index + 1)
                .checked_add(len)
                .context("v5 SDF byte length overflow")?;
            return Ok((bytes.len() >= end).then_some(end));
        }
    }
    bail!("v5 SDF byte length varint is too long")
}

fn validate_size(size: f64) -> Result<()> {
    if !size.is_finite() || size < 0.0 {
        bail!("v5 SDF restriction size must be finite and nonnegative");
    }
    Ok(())
}

#[cfg(test)]
#[path = "../tests/unit/sdf_wire__tests.rs"]
mod tests;
