use anyhow::{anyhow, bail, Result};
use num_bigint::BigInt;

use super::atomic::{read_varint, take};
use crate::encode_varint;

pub(super) fn encode_decimal(text: &str, output: &mut Vec<u8>) -> Result<()> {
    let (integer, scale) = parse_decimal(text)?;
    encode_varint(i64::from(scale).cast_unsigned(), output)?;
    let bytes = integer.to_signed_bytes_be();
    encode_varint(u64::try_from(bytes.len())?, output)?;
    output.extend_from_slice(&bytes);
    Ok(())
}

pub(super) fn decode_decimal(input: &mut &[u8]) -> Result<String> {
    let scale = i32::try_from(read_varint(input)?.cast_signed())?;
    let length = usize::try_from(read_varint(input)?)?;
    let bytes = take(input, length)?;
    if bytes.is_empty() {
        bail!("decimal integer is empty");
    }
    let integer = BigInt::from_signed_bytes_be(bytes);
    Ok(format_decimal(&integer, scale))
}

fn parse_decimal(text: &str) -> Result<(BigInt, i32)> {
    let (coefficient, exponent) = match text.find(['e', 'E']) {
        Some(index) => (&text[..index], text[index + 1..].parse::<i64>()?),
        None => (text, 0),
    };
    let unsigned = coefficient.strip_prefix(['+', '-']).unwrap_or(coefficient);
    if unsigned.is_empty() || unsigned.bytes().filter(|&byte| byte == b'.').count() > 1 {
        bail!("invalid decimal coefficient");
    }
    let fraction = unsigned
        .split_once('.')
        .map_or(0, |(_, fraction)| fraction.len());
    let digits: String = unsigned
        .chars()
        .filter(|&character| character != '.')
        .collect();
    if digits.is_empty() || !digits.bytes().all(|byte| byte.is_ascii_digit()) {
        bail!("invalid decimal digits");
    }
    let scale = i32::try_from(
        i64::try_from(fraction)?
            .checked_sub(exponent)
            .ok_or_else(|| anyhow!("decimal scale is out of range"))?,
    )?;
    let signed = if coefficient.starts_with('-') {
        format!("-{digits}")
    } else {
        digits
    };
    Ok((signed.parse()?, scale))
}

fn format_decimal(integer: &BigInt, scale: i32) -> String {
    let signed = integer.to_string();
    let (sign, digits) = signed
        .strip_prefix('-')
        .map_or(("", signed.as_str()), |digits| ("-", digits));
    if scale == 0 {
        return signed;
    }
    if scale < 0 {
        return format!("{signed}e{}", -i64::from(scale));
    }
    if scale > 1_000 {
        return format!("{signed}e-{}", i64::from(scale));
    }
    let scale = usize::try_from(scale).expect("positive scale fits usize");
    if digits.len() > scale {
        let split = digits.len() - scale;
        format!("{sign}{}.{}", &digits[..split], &digits[split..])
    } else {
        format!("{sign}0.{}{digits}", "0".repeat(scale - digits.len()))
    }
}
