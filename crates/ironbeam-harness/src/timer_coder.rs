//! Beam Fn timer-family records. Key and window bytes are supplied by the
//! nested coders declared in the timer family's portable coder.

use anyhow::{anyhow, bail, Result};
use ironbeam_core::{PaneInfo, PaneTiming};

use crate::coders::{decode_length_prefix, decode_varint, encode_length_prefix, encode_pane_info};

mod partial;
pub use partial::{try_decode_timer_record, try_nested_bytes_len};
mod schedule;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TimerRecord {
    /// Exactly one nested key coder value, including its framing.
    pub encoded_key: Vec<u8>,
    pub dynamic_tag: String,
    /// Each window already encoded by its nested window coder.
    pub encoded_windows: Vec<Vec<u8>>,
    pub clear: bool,
    pub fire_timestamp_millis: Option<i64>,
    pub hold_timestamp_millis: Option<i64>,
    pub pane: Option<PaneInfo>,
}

impl TimerRecord {
    pub fn encode(&self, output: &mut Vec<u8>) -> Result<()> {
        let window_count = i32::try_from(self.encoded_windows.len())
            .map_err(|_| anyhow!("timer record has too many windows"))?;
        if self.clear {
            if self.fire_timestamp_millis.is_some()
                || self.hold_timestamp_millis.is_some()
                || self.pane.is_some()
            {
                bail!("clear timer must omit timestamps and pane");
            }
        } else if self.fire_timestamp_millis.is_none()
            || self.hold_timestamp_millis.is_none()
            || self.pane.is_none()
        {
            bail!("set timer needs fire time, hold time, and pane");
        }
        let mut encoded = self.encoded_key.clone();
        encode_length_prefix(self.dynamic_tag.as_bytes(), &mut encoded)?;
        encoded.extend_from_slice(&window_count.to_be_bytes());
        for window in &self.encoded_windows {
            encoded.extend_from_slice(window);
        }
        encoded.push(u8::from(self.clear));
        if !self.clear {
            for millis in [
                self.fire_timestamp_millis.unwrap(),
                self.hold_timestamp_millis.unwrap(),
            ] {
                encoded.extend_from_slice(&((millis as u64) ^ (1_u64 << 63)).to_be_bytes());
            }
            encode_pane_info(self.pane.unwrap(), &mut encoded)?;
        }
        output.extend_from_slice(&encoded);
        Ok(())
    }
}

/// Decode one timer from a concatenated Fn Data timer-family chunk. The two
/// callbacks consume one nested key and one nested window respectively.
pub fn decode_timer_record(
    input: &[u8],
    decode_key: impl Fn(&[u8]) -> Result<usize>,
    decode_window: impl Fn(&[u8]) -> Result<usize>,
) -> Result<(TimerRecord, usize)> {
    let key_len = decode_key(input)?;
    let mut offset = key_len;
    if offset > input.len() {
        bail!("timer key coder exceeded input");
    }
    let (tag, used) = decode_length_prefix(&input[offset..])?;
    let dynamic_tag = std::str::from_utf8(tag)?.to_owned();
    offset += used;
    let count_bytes: [u8; 4] = input
        .get(offset..offset + 4)
        .ok_or_else(|| anyhow!("truncated timer window count"))?
        .try_into()?;
    let count = i32::from_be_bytes(count_bytes);
    if !(0..=1_000_000).contains(&count) {
        bail!("timer window count is out of range");
    }
    offset += 4;
    let mut encoded_windows = Vec::with_capacity(count as usize);
    for _ in 0..count {
        let used = decode_window(&input[offset..])?;
        if used > input.len() - offset {
            bail!("timer window coder exceeded input");
        }
        encoded_windows.push(input[offset..offset + used].to_vec());
        offset += used;
    }
    let clear = match input.get(offset) {
        Some(0) => false,
        Some(1) => true,
        _ => bail!("timer clear bit is missing or invalid"),
    };
    offset += 1;
    let (fire_timestamp_millis, hold_timestamp_millis, pane) = if clear {
        (None, None, None)
    } else {
        let read_time = |offset: &mut usize| -> Result<i64> {
            let bytes: [u8; 8] = input
                .get(*offset..*offset + 8)
                .ok_or_else(|| anyhow!("truncated timer timestamp"))?
                .try_into()?;
            *offset += 8;
            Ok((u64::from_be_bytes(bytes) ^ (1_u64 << 63)) as i64)
        };
        let fire = read_time(&mut offset)?;
        let hold = read_time(&mut offset)?;
        let (pane, used) = decode_timer_pane(&input[offset..])?;
        offset += used;
        (Some(fire), Some(hold), Some(pane))
    };
    Ok((
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
    ))
}

fn decode_timer_pane(input: &[u8]) -> Result<(PaneInfo, usize)> {
    let header = *input
        .first()
        .ok_or_else(|| anyhow!("truncated timer pane"))?;
    if header & 0x80 != 0 {
        bail!("timer pane cannot carry element metadata");
    }
    let count = (header >> 4) & 7;
    if count > 2 {
        bail!("unsupported timer pane index encoding");
    }
    let mut offset = 1;
    let mut indices = [0_i64; 2];
    for slot in indices.iter_mut().take(count as usize) {
        let mut cursor = std::io::Cursor::new(&input[offset..]);
        *slot = decode_varint(&mut cursor)? as i64;
        offset += cursor.position() as usize;
    }
    let timing = match (header >> 2) & 3 {
        0 => PaneTiming::Early,
        1 => PaneTiming::OnTime,
        2 => PaneTiming::Late,
        _ => PaneTiming::Unknown,
    };
    let pane = PaneInfo {
        is_first: header & 1 != 0,
        is_last: header & 2 != 0,
        timing,
        index: indices[0],
        nonspeculative_index: if timing == PaneTiming::Early {
            -1
        } else if count == 2 {
            indices[1]
        } else {
            indices[0]
        },
        contains_element_metadata: false,
    };
    encode_pane_info(pane, &mut Vec::new())?;
    Ok((pane, offset))
}

#[cfg(test)]
#[path = "../tests/unit/timer_coder__tests.rs"]
mod tests;
