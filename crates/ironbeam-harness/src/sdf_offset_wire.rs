//! Beam coder bodies for user-defined offset-restricted transforms.
//!
//! A pair is `KV<bytes, bytes>` and a sized pair is `KV<pair, double>`.
//! The first byte field contains the Ironbeam input coder body. The second
//! contains Beam's two-VarInt `OffsetRange.Coder` body.

use anyhow::{bail, Context, Result};
use ironbeam_core::OffsetRange;

use crate::coders::{decode_length_prefix, encode_length_prefix};

/// An encoded input element and its assigned offset range.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OffsetPair {
    pub element: Vec<u8>,
    pub restriction: OffsetRange,
}

/// An offset pair and its estimated number of positions.
#[derive(Clone, Debug, PartialEq)]
pub struct SizedOffsetPair {
    pub pair: OffsetPair,
    pub size: f64,
}

/// Encode an element with its initial restriction.
pub fn pair_with_restriction(element: &[u8], restriction: OffsetRange) -> Result<Vec<u8>> {
    encode_pair(&OffsetPair {
        element: element.to_vec(),
        restriction,
    })
}

/// Split a finite restriction into consecutive ranges with at most
/// `max_positions` positions each.
pub fn split_and_size(pair_body: &[u8], max_positions: u64) -> Result<Vec<Vec<u8>>> {
    let pair = decode_pair(pair_body)?;
    pair.restriction
        .chunks(max_positions)?
        .map(|restriction| {
            let size = (i128::from(restriction.stop) - i128::from(restriction.start)) as f64;
            encode_sized(&SizedOffsetPair {
                pair: OffsetPair {
                    element: pair.element.clone(),
                    restriction,
                },
                size,
            })
        })
        .collect()
}

/// Keep a bounded restriction when Beam requests a drain.
pub fn truncate_sized(sized_body: &[u8]) -> Result<Vec<u8>> {
    let _ = decode_sized(sized_body)?;
    Ok(sized_body.to_vec())
}

/// Encode a complete input and restriction pair.
pub fn encode_pair(pair: &OffsetPair) -> Result<Vec<u8>> {
    let mut output = Vec::new();
    encode_length_prefix(&pair.element, &mut output)?;
    encode_length_prefix(&pair.restriction.encode_beam()?, &mut output)?;
    Ok(output)
}

/// Decode one complete pair, rejecting extra bytes.
pub fn decode_pair(bytes: &[u8]) -> Result<OffsetPair> {
    let (pair, consumed) = decode_pair_prefix(bytes)?;
    if consumed != bytes.len() {
        bail!("v5 offset SDF pair has trailing bytes");
    }
    Ok(pair)
}

fn decode_pair_prefix(bytes: &[u8]) -> Result<(OffsetPair, usize)> {
    let (element, element_len) =
        decode_length_prefix(bytes).context("decode offset SDF input element")?;
    let (restriction, restriction_len) =
        decode_length_prefix(&bytes[element_len..]).context("decode offset SDF restriction")?;
    let restriction =
        OffsetRange::decode_beam(restriction).context("validate offset SDF restriction")?;
    Ok((
        OffsetPair {
            element: element.to_vec(),
            restriction,
        },
        element_len + restriction_len,
    ))
}

/// Encode a pair followed by a finite work-size estimate.
pub fn encode_sized(value: &SizedOffsetPair) -> Result<Vec<u8>> {
    validate_size(value.size)?;
    let mut output = encode_pair(&value.pair)?;
    output.extend_from_slice(&value.size.to_be_bytes());
    Ok(output)
}

/// Decode one complete pair and its work-size estimate.
pub fn decode_sized(bytes: &[u8]) -> Result<SizedOffsetPair> {
    let (pair, consumed) = decode_pair_prefix(bytes)?;
    let size_bytes: [u8; 8] = bytes
        .get(consumed..)
        .context("offset SDF sized pair exceeds input")?
        .try_into()
        .map_err(|_| anyhow::anyhow!("offset SDF sized pair must end with one double"))?;
    let size = f64::from_be_bytes(size_bytes);
    validate_size(size)?;
    Ok(SizedOffsetPair { pair, size })
}

fn validate_size(size: f64) -> Result<()> {
    if !size.is_finite() || size < 0.0 {
        bail!("offset SDF restriction size must be finite and nonnegative");
    }
    Ok(())
}

#[cfg(test)]
#[path = "../tests/unit/sdf_offset_wire__tests.rs"]
mod tests;
