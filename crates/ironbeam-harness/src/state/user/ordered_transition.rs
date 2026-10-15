//! Beam ordered-list Fn State wire entries and bundle-scoped replacement.

use std::io::Cursor;
use std::panic::{catch_unwind, AssertUnwindSafe};

use anyhow::{bail, Result};
use ironbeam_core::UserStateKind;

use super::UserStateAccess;
use crate::coders::{decode_length_prefix, decode_varint, encode_length_prefix, encode_varint};

fn decode_wire(mut bytes: &[u8]) -> Result<Vec<(i64, Vec<u8>)>> {
    let mut entries = Vec::new();
    while !bytes.is_empty() {
        let mut cursor = Cursor::new(bytes);
        let sort_ms = decode_varint(&mut cursor)? as i64;
        if sort_ms == i64::MAX {
            bail!("Beam ordered-list sort key cannot be i64::MAX");
        }
        let offset = usize::try_from(cursor.position())?;
        let (value, used) = decode_length_prefix(&bytes[offset..])?;
        entries.push((sort_ms, value.to_vec()));
        bytes = &bytes[offset + used..];
    }
    entries.sort();
    Ok(entries)
}

fn encode_wire(entries: &[(i64, Vec<u8>)]) -> Result<Vec<u8>> {
    let mut wire = Vec::new();
    for (sort_ms, value) in entries {
        if *sort_ms == i64::MAX {
            bail!("Beam ordered-list sort key cannot be i64::MAX");
        }
        encode_varint(*sort_ms as u64, &mut wire)?;
        encode_length_prefix(value, &mut wire)?;
    }
    Ok(wire)
}

/// Return the newly added multiset entries when no existing entry was
/// removed. Ordered lists may contain duplicate sort-key/value pairs.
fn additions<'a>(
    old: &'a [(i64, Vec<u8>)],
    new: &'a [(i64, Vec<u8>)],
) -> Option<Vec<&'a (i64, Vec<u8>)>> {
    let mut prior = old.iter().peekable();
    let mut appended = Vec::new();
    for entry in new {
        if prior.peek().is_some_and(|candidate| *candidate < entry) {
            return None;
        }
        if prior.peek().is_some_and(|candidate| *candidate == entry) {
            prior.next();
        } else {
            appended.push(entry);
        }
    }
    prior.peek().is_none().then_some(appended)
}

impl UserStateAccess<'_> {
    /// Read, update, and replace a declared ordered-list cell in one bundle.
    /// Runner commit or rollback owns the state transaction.
    pub async fn apply_ordered_state<R>(
        &self,
        state_id: &str,
        callback: impl FnOnce(&[u8]) -> Result<(R, Option<Vec<u8>>)>,
    ) -> Result<R> {
        self.scope(state_id, |kind| {
            matches!(kind, UserStateKind::OrderedList { .. })
        })?;
        let old = decode_wire(&self.read_ordered(state_id, i64::MIN, i64::MAX).await?)?;
        let current = serde_json::to_vec(&old)?;
        let (result, next) = match catch_unwind(AssertUnwindSafe(|| callback(&current))) {
            Ok(result) => result?,
            Err(_) => bail!("Beam ordered-list callback panicked"),
        };
        let mut new: Vec<(i64, Vec<u8>)> = next
            .as_deref()
            .map(serde_json::from_slice)
            .transpose()?
            .unwrap_or_default();
        if new.iter().any(|(sort_ms, _)| *sort_ms == i64::MAX) {
            bail!("Beam ordered-list sort key cannot be i64::MAX");
        }
        new.sort();
        if let Some(appended) = additions(&old, &new) {
            if !appended.is_empty() {
                let entries = appended.into_iter().cloned().collect::<Vec<_>>();
                self.append_ordered(state_id, encode_wire(&entries)?)
                    .await?;
            }
        } else {
            // Prism cannot clear an ordered-list cell before its first
            // append; an empty cell already has nothing to remove.
            self.clear_ordered(state_id, i64::MIN, i64::MAX).await?;
            if !new.is_empty() {
                self.append_ordered(state_id, encode_wire(&new)?).await?;
            }
        }
        Ok(result)
    }
}
