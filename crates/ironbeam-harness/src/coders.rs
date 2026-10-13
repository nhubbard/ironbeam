//! Byte-level encode/decode for the wire coders Dataflow's runner-v2 uses
//! at SDK boundaries:
//!
//! - `beam:coder:varint:v1` — protobuf-style 7-bit-continuation encoding
//! - `beam:coder:length_prefix:v1` — varint length + N bytes
//! - `beam:coder:interval_window:v1` — shifted end millis + duration varint
//! - `beam:coder:windowed_value:v1` over global or interval windows, accepting
//!   Beam pane headers while leaving pane timing out of the v5 element model
//! - `beam:coder:kv:v1` — concatenation of two inner coder outputs
//!
//! The structure of these is defined in Beam's `standard_coders.yaml`
//! fixtures (`sdks/python/apache_beam/portability/api/standard_coders.yaml`
//! in the upstream repo). Our tests cover round-trip; cross-checking against
//! the upstream fixtures is an M4 verification step.
//!
//! All nested coders use *nested* encoding (Beam's term): when two coders
//! are composed, the inner coder is responsible for being self-delimiting.
//! That's why element coders get wrapped in `length_prefix`.

use std::io::{Read, Write};

use anyhow::{anyhow, Context, Result};

// ---- VarInt ---------------------------------------------------------------

pub fn encode_varint<W: Write>(mut n: u64, out: &mut W) -> Result<()> {
    loop {
        let b = (n & 0x7F) as u8;
        n >>= 7;
        if n == 0 {
            out.write_all(&[b])?;
            return Ok(());
        }
        out.write_all(&[b | 0x80])?;
    }
}

pub fn decode_varint<R: Read>(r: &mut R) -> Result<u64> {
    let mut n = 0u64;
    for shift in (0..64).step_by(7) {
        let mut buf = [0u8; 1];
        r.read_exact(&mut buf).context("varint: unexpected EOF")?;
        if shift == 63 && (buf[0] & 0x7f) > 1 {
            return Err(anyhow!("varint: value exceeds u64"));
        }
        n |= u64::from(buf[0] & 0x7F) << shift;
        if buf[0] & 0x80 == 0 {
            return Ok(n);
        }
    }
    Err(anyhow!("varint: value longer than 10 bytes"))
}

// ---- Length-prefix --------------------------------------------------------

pub fn encode_length_prefix<W: Write>(body: &[u8], out: &mut W) -> Result<()> {
    encode_varint(body.len() as u64, out)?;
    out.write_all(body)?;
    Ok(())
}

/// Decode one length-prefixed blob. Returns the body (a slice into `buf`)
/// and the number of bytes consumed from `buf` (header + body).
pub fn decode_length_prefix(buf: &[u8]) -> Result<(&[u8], usize)> {
    let mut cursor = std::io::Cursor::new(buf);
    let len = usize::try_from(decode_varint(&mut cursor)?)
        .map_err(|_| anyhow!("length_prefix: body length exceeds platform range"))?;
    let header_len = cursor.position() as usize;
    let end = header_len
        .checked_add(len)
        .ok_or_else(|| anyhow!("length_prefix: overflow"))?;
    if end > buf.len() {
        return Err(anyhow!(
            "length_prefix: declared body len {len} but only {} bytes left",
            buf.len() - header_len
        ));
    }
    Ok((&buf[header_len..end], end))
}

// ---- Interval window ------------------------------------------------------

/// Encode Beam's interval-window coder: the lexicographically shifted end
/// timestamp in milliseconds followed by a positive duration varint.
pub fn encode_interval_window<W: Write>(
    start_millis: i64,
    end_millis: i64,
    out: &mut W,
) -> Result<()> {
    let duration = end_millis
        .checked_sub(start_millis)
        .filter(|duration| *duration > 0)
        .ok_or_else(|| anyhow!("interval_window: end must follow start without overflow"))?;
    out.write_all(&((end_millis as u64) ^ (1_u64 << 63)).to_be_bytes())?;
    encode_varint(duration as u64, out)
}

/// Decode one Beam interval-window coder value and return its byte length.
pub fn decode_interval_window(buf: &[u8]) -> Result<((i64, i64), usize)> {
    let end_bytes: [u8; 8] = buf
        .get(..8)
        .ok_or_else(|| anyhow!("interval_window: truncated end timestamp"))?
        .try_into()
        .context("interval_window: invalid end timestamp")?;
    let end_millis = (u64::from_be_bytes(end_bytes) ^ (1_u64 << 63)) as i64;
    let mut cursor = std::io::Cursor::new(&buf[8..]);
    let duration = decode_varint(&mut cursor).context("interval_window: invalid duration")?;
    let duration =
        i64::try_from(duration).map_err(|_| anyhow!("interval_window: duration exceeds i64"))?;
    if duration <= 0 {
        return Err(anyhow!("interval_window: duration must be positive"));
    }
    let start_millis = end_millis
        .checked_sub(duration)
        .ok_or_else(|| anyhow!("interval_window: start timestamp overflows"))?;
    Ok(((start_millis, end_millis), 8 + cursor.position() as usize))
}

// ---- Pane info ------------------------------------------------------------

/// The packed byte Beam uses for the default NO_FIRING pane:
/// `is_first=1 | is_last=1 | timing=UNKNOWN(3)`, packed into the low nibble.
/// High nibble is zero → index and nonspeculative_index are elided from the
/// wire (both are 0). See `PaneInfoCoder` in Beam.
pub const PANE_INFO_NO_FIRING: u8 = 0x0F;
/// Prism emits one on-time pane after a bounded native GroupByKey.
pub const PANE_INFO_FIRST_LAST_ON_TIME: u8 = 0x07;

// ---- Windowed value (global window, NO_FIRING pane) -----------------------

/// `beam:coder:windowed_value:v1` with `beam:coder:global_window:v1` and
/// `PANE_INFO_NO_FIRING`. Wire layout:
///
/// - 8 bytes big-endian: timestamp, encoded as `(value ^ Long.MIN_VALUE)` so
///   lexicographic byte order matches numeric order (Beam `InstantCoder`)
/// - 4 bytes big-endian i32: window count (always 1 for global window)
/// - 0 bytes: global window has empty encoding
/// - 1 byte: `PANE_INFO_NO_FIRING`
/// - element body (no length prefix here — caller has already wrapped their
///   element in a length_prefix coder if needed)
pub fn encode_windowed_value<W: Write>(
    timestamp_millis: i64,
    element: &[u8],
    out: &mut W,
) -> Result<()> {
    // InstantCoder: write the value XOR'd with MIN_LONG as unsigned big-endian.
    let shifted = (timestamp_millis as i128) - (i64::MIN as i128); // = unsigned shifted value
    let shifted_u64 = shifted as u64;
    out.write_all(&shifted_u64.to_be_bytes())?;
    // Window count = 1; body = empty (global window encodes to nothing).
    out.write_all(&1i32.to_be_bytes())?;
    // Pane info.
    out.write_all(&[PANE_INFO_NO_FIRING])?;
    // Element body.
    out.write_all(element)?;
    Ok(())
}

/// Encode one value in an interval window. All timestamps and bounds are
/// milliseconds because Beam's interval and windowed-value coders use millis.
pub fn encode_interval_windowed_value<W: Write>(
    timestamp_millis: i64,
    start_millis: i64,
    end_millis: i64,
    element: &[u8],
    out: &mut W,
) -> Result<()> {
    out.write_all(&((timestamp_millis as u64) ^ (1_u64 << 63)).to_be_bytes())?;
    out.write_all(&1_i32.to_be_bytes())?;
    encode_interval_window(start_millis, end_millis, out)?;
    out.write_all(&[PANE_INFO_NO_FIRING])?;
    out.write_all(element)?;
    Ok(())
}

#[derive(Debug, Clone, PartialEq)]
pub struct WindowedValue<'a> {
    pub timestamp_millis: i64,
    pub interval_millis: Option<(i64, i64)>,
    pub element: &'a [u8],
}

/// Decode a global or interval window header, returning `None` until its
/// complete header is available. Beam runners may emit different valid pane
/// timings for the same bounded value; the element body remains opaque here.
pub fn try_decode_windowed_value(
    buf: &[u8],
    interval_window: bool,
) -> Result<Option<(WindowedValue<'_>, usize)>> {
    if buf.len() < 13 {
        return Ok(None);
    }
    let timestamp_millis =
        (u64::from_be_bytes(buf[..8].try_into().unwrap()) ^ (1_u64 << 63)) as i64;
    let window_count = i32::from_be_bytes(buf[8..12].try_into().unwrap());
    if window_count != 1 {
        return Err(anyhow!(
            "windowed_value: expected one window, got {window_count}"
        ));
    }
    let (interval_millis, pane_index) = if interval_window {
        if buf.len() < 21 {
            return Ok(None);
        }
        let mut end = None;
        for (index, byte) in buf.iter().enumerate().take(30).skip(20) {
            if index == 29 && *byte > 1 {
                return Err(anyhow!(
                    "windowed_value: interval duration overflows varint"
                ));
            }
            if byte & 0x80 == 0 {
                end = Some(index + 1);
                break;
            }
        }
        let Some(end) = end else {
            if buf.len() >= 30 {
                return Err(anyhow!(
                    "windowed_value: interval duration varint is too long"
                ));
            }
            return Ok(None);
        };
        let ((start, end_millis), used) = decode_interval_window(&buf[12..end])?;
        (Some((start, end_millis)), 12 + used)
    } else {
        (None, 12)
    };
    let Some(&pane_byte) = buf.get(pane_index) else {
        return Ok(None);
    };
    if pane_byte & 0x80 != 0 {
        return Err(anyhow!(
            "windowed_value: unsupported element-metadata pane {pane_byte:#04x}"
        ));
    }
    let mut header_len = pane_index + 1;
    let index_count = match pane_byte >> 4 {
        0 => 0,
        1 => 1,
        2 => 2,
        _ => {
            return Err(anyhow!(
                "windowed_value: unsupported pane encoding {pane_byte:#04x}"
            ))
        }
    };
    for _ in 0..index_count {
        let mut complete = false;
        for offset in 0..10 {
            let Some(&byte) = buf.get(header_len + offset) else {
                return Ok(None);
            };
            if offset == 9 && byte > 1 {
                return Err(anyhow!("windowed_value: pane index overflows varint"));
            }
            if byte & 0x80 == 0 {
                header_len += offset + 1;
                complete = true;
                break;
            }
        }
        if !complete {
            return Err(anyhow!("windowed_value: pane index is too long"));
        }
    }
    Ok(Some((
        WindowedValue {
            timestamp_millis,
            interval_millis,
            element: &buf[header_len..],
        },
        header_len,
    )))
}

/// Decode one global-window value and return its body offset. Pane timing and
/// indices are consumed but not exposed in the v5 element model. If the
/// element coder embedded in `element` is length-prefixed, the caller unwraps
/// it separately.
pub fn decode_windowed_value(buf: &[u8]) -> Result<(WindowedValue<'_>, usize)> {
    try_decode_windowed_value(buf, false)?
        .ok_or_else(|| anyhow!("windowed_value: truncated global-window header"))
}

// ---- KV -------------------------------------------------------------------
// kv<K,V> is the concatenation of an encoded K followed by an encoded V,
// each self-delimiting via their respective nested coders. Callers wrap K
// and V individually (e.g., in length_prefix) and then call this to stitch.

pub fn encode_kv<W: Write>(encoded_k: &[u8], encoded_v: &[u8], out: &mut W) -> Result<()> {
    out.write_all(encoded_k)?;
    out.write_all(encoded_v)?;
    Ok(())
}

// ---- Tests ----------------------------------------------------------------

#[cfg(test)]
#[path = "../tests/unit/coders__tests.rs"]
mod tests;
