//! One declared `SetState` read, callback, and multimap-key mutation sequence.

use anyhow::{bail, Result};
use std::collections::BTreeSet;
use std::panic::{catch_unwind, AssertUnwindSafe};

use super::UserStateAccess;
use crate::coders::{decode_length_prefix, encode_length_prefix};
use ironbeam_core::UserStateKind;

fn decode_keys(mut bytes: &[u8]) -> Result<BTreeSet<Vec<u8>>> {
    let mut keys = BTreeSet::new();
    while !bytes.is_empty() {
        let (key, used) = decode_length_prefix(bytes)?;
        keys.insert(key.to_vec());
        bytes = &bytes[used..];
    }
    Ok(keys)
}

impl UserStateAccess<'_> {
    /// Read set keys, invoke a callback, and write only changed multimap keys
    /// in the current bundle instruction. The runner commits or rolls back
    /// those requests together with the bundle.
    pub async fn apply_set_state<R>(
        &self,
        state_id: &str,
        callback: impl FnOnce(&[u8]) -> Result<(R, Option<Vec<u8>>)>,
    ) -> Result<R> {
        self.scope(state_id, |kind| matches!(kind, UserStateKind::Set { .. }))?;
        let current = self.read_map_keys(state_id).await?;
        let (result, next) = match catch_unwind(AssertUnwindSafe(|| callback(&current))) {
            Ok(result) => result?,
            Err(_) => bail!("Beam set-state callback panicked"),
        };
        let old = decode_keys(&current)?;
        let new = next
            .as_deref()
            .map(decode_keys)
            .transpose()?
            .unwrap_or_default();
        if old == new {
            return Ok(result);
        }
        let all_keys = self
            .scope(state_id, |kind| matches!(kind, UserStateKind::Set { .. }))?
            .multimap_keys();
        self.client.clear(self.instruction_id, all_keys).await?;
        for key in new {
            let mut encoded = Vec::new();
            encode_length_prefix(&key, &mut encoded)?;
            self.append_map_value(state_id, encoded, Vec::new()).await?;
        }
        Ok(result)
    }
}
