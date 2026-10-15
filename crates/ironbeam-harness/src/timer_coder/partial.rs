//! Prefix-aware TimerCoder parsing for arbitrarily fragmented Fn Data.

use anyhow::{anyhow, bail, Result};

use super::{decode_timer_pane, TimerRecord};

fn try_varint(input: &[u8]) -> Result<Option<(u64, usize)>> {
    let mut value = 0_u64;
    for index in 0..10 {
        let Some(&byte) = input.get(index) else {
            return Ok(None);
        };
        if index == 9 && byte > 1 {
            bail!("timer length varint exceeds 64 bits");
        }
        value |= u64::from(byte & 0x7f) << (index * 7);
        if byte & 0x80 == 0 {
            return Ok(Some((value, index + 1)));
        }
    }
    bail!("timer length varint is too long")
}

/// Return the byte length of one nested bytes or UTF-8 coder value.
pub fn try_nested_bytes_len(input: &[u8]) -> Result<Option<usize>> {
    let Some((length, prefix)) = try_varint(input)? else {
        return Ok(None);
    };
    let length = usize::try_from(length).map_err(|_| anyhow!("timer nested value is too large"))?;
    let end = prefix
        .checked_add(length)
        .ok_or_else(|| anyhow!("timer nested length overflow"))?;
    Ok((input.len() >= end).then_some(end))
}

fn try_pane(input: &[u8]) -> Result<Option<(ironbeam_core::PaneInfo, usize)>> {
    let Some(&header) = input.first() else {
        return Ok(None);
    };
    if header & 0x80 != 0 {
        bail!("timer pane cannot carry element metadata");
    }
    let count = usize::from((header >> 4) & 7);
    if count > 2 {
        bail!("unsupported timer pane index encoding");
    }
    let mut used = 1;
    for _ in 0..count {
        let Some((_, length)) = try_varint(&input[used..])? else {
            return Ok(None);
        };
        used += length;
    }
    let (pane, decoded) = decode_timer_pane(&input[..used])?;
    if decoded != used {
        bail!("timer pane decoder consumed a different length");
    }
    Ok(Some((pane, used)))
}

/// Decode one complete timer record when enough bytes are available. An
/// incomplete prefix returns None; a malformed complete prefix returns an
/// error. Key and window callbacks must report their own incomplete suffixes.
pub fn try_decode_timer_record(
    input: &[u8],
    decode_key: impl Fn(&[u8]) -> Result<Option<usize>>,
    decode_window: impl Fn(&[u8]) -> Result<Option<usize>>,
) -> Result<Option<(TimerRecord, usize)>> {
    let Some(key_len) = decode_key(input)? else {
        return Ok(None);
    };
    if key_len > input.len() {
        bail!("timer key coder exceeded input");
    }
    let mut offset = key_len;
    let Some((tag_len, prefix_len)) = try_varint(&input[offset..])? else {
        return Ok(None);
    };
    let tag_len = usize::try_from(tag_len).map_err(|_| anyhow!("timer tag is too large"))?;
    let tag_end = offset
        .checked_add(prefix_len)
        .and_then(|start| start.checked_add(tag_len))
        .ok_or_else(|| anyhow!("timer tag length overflow"))?;
    let Some(tag) = input.get(offset + prefix_len..tag_end) else {
        return Ok(None);
    };
    let dynamic_tag = std::str::from_utf8(tag)?.to_owned();
    offset = tag_end;
    let Some(count_bytes) = input.get(offset..offset + 4) else {
        return Ok(None);
    };
    let count = i32::from_be_bytes(count_bytes.try_into()?);
    if !(0..=1_000_000).contains(&count) {
        bail!("timer window count is out of range");
    }
    offset += 4;
    let mut encoded_windows = Vec::new();
    for _ in 0..count {
        let Some(used) = decode_window(&input[offset..])? else {
            return Ok(None);
        };
        let Some(window) = input.get(offset..offset.saturating_add(used)) else {
            bail!("timer window coder exceeded input");
        };
        encoded_windows.push(window.to_vec());
        offset += used;
    }
    let clear = match input.get(offset) {
        Some(0) => false,
        Some(1) => true,
        Some(_) => bail!("timer clear bit is invalid"),
        None => return Ok(None),
    };
    offset += 1;
    let (fire_timestamp_millis, hold_timestamp_millis, pane) = if clear {
        (None, None, None)
    } else {
        let Some(fire) = input.get(offset..offset + 8) else {
            return Ok(None);
        };
        let fire = (u64::from_be_bytes(fire.try_into()?) ^ (1_u64 << 63)) as i64;
        offset += 8;
        let Some(hold) = input.get(offset..offset + 8) else {
            return Ok(None);
        };
        let hold = (u64::from_be_bytes(hold.try_into()?) ^ (1_u64 << 63)) as i64;
        offset += 8;
        let Some((pane, used)) = try_pane(&input[offset..])? else {
            return Ok(None);
        };
        offset += used;
        (Some(fire), Some(hold), Some(pane))
    };
    Ok(Some((
        TimerRecord {
            encoded_key: input[..key_len].to_vec(),
            dynamic_tag,
            encoded_windows,
            clear,
            fire_timestamp_millis,
            hold_timestamp_millis,
            pane,
        },
        offset,
    )))
}
