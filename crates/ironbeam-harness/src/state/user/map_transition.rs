//! One declared `MapState` read, callback, and multimap mutation sequence.

use std::collections::BTreeMap;
use std::panic::{catch_unwind, AssertUnwindSafe};

use anyhow::{bail, Result};
use ironbeam_core::UserStateKind;

use super::UserStateAccess;
use crate::coders::{decode_length_prefix, encode_length_prefix};

fn decode_entries(bytes: &[u8]) -> Result<BTreeMap<Vec<u8>, Vec<u8>>> {
    let encoded: Vec<(Vec<u8>, Vec<u8>)> = serde_json::from_slice(bytes)?;
    let mut entries = BTreeMap::new();
    for (key, value) in encoded {
        if entries.insert(key, value).is_some() {
            bail!("Beam map state contains a duplicate key");
        }
    }
    Ok(entries)
}

fn decode_multimap_entries(bytes: &[u8]) -> Result<BTreeMap<Vec<u8>, Vec<Vec<u8>>>> {
    let encoded: Vec<(Vec<u8>, Vec<Vec<u8>>)> = serde_json::from_slice(bytes)?;
    let mut entries = BTreeMap::new();
    for (key, mut values) in encoded {
        values.sort();
        if !values.is_empty() && entries.insert(key, values).is_some() {
            bail!("Beam multimap state contains a duplicate key");
        }
    }
    Ok(entries)
}

impl UserStateAccess<'_> {
    /// Read a map cell, invoke a callback, and replace its entries within the
    /// current bundle. The runner commits or rolls back these requests with
    /// the bundle instruction.
    pub async fn apply_map_state<R>(
        &self,
        state_id: &str,
        callback: impl FnOnce(&[u8]) -> Result<(R, Option<Vec<u8>>)>,
    ) -> Result<R> {
        let scope = self.scope(state_id, |kind| matches!(kind, UserStateKind::Map { .. }))?;
        let mut keys = self.read_map_keys(state_id).await?;
        let mut old = BTreeMap::new();
        while !keys.is_empty() {
            let (key, used) = decode_length_prefix(&keys)?;
            let mut encoded_key = Vec::new();
            encode_length_prefix(key, &mut encoded_key)?;
            let encoded_value = self.read_map_value(state_id, encoded_key).await?;
            let (value, consumed) = decode_length_prefix(&encoded_value)?;
            if consumed != encoded_value.len() || old.insert(key.to_vec(), value.to_vec()).is_some()
            {
                bail!("Beam map state contains a duplicate key or value");
            }
            keys = keys.split_off(used);
        }
        let current = serde_json::to_vec(&old.iter().collect::<Vec<_>>())?;
        let (result, next) = match catch_unwind(AssertUnwindSafe(|| callback(&current))) {
            Ok(result) => result?,
            Err(_) => bail!("Beam map-state callback panicked"),
        };
        let new = next
            .as_deref()
            .map(decode_entries)
            .transpose()?
            .unwrap_or_default();
        if old == new {
            return Ok(result);
        }
        self.client
            .clear(self.instruction_id, scope.multimap_keys())
            .await?;
        for (key, value) in new {
            let mut encoded_key = Vec::new();
            let mut encoded_value = Vec::new();
            encode_length_prefix(&key, &mut encoded_key)?;
            encode_length_prefix(&value, &mut encoded_value)?;
            self.append_map_value(state_id, encoded_key, encoded_value)
                .await?;
        }
        Ok(result)
    }

    /// Read all values per multimap key, invoke a callback, and replace the
    /// changed cell within one runner-owned bundle transaction.
    pub async fn apply_multimap_state<R>(
        &self,
        state_id: &str,
        callback: impl FnOnce(&[u8]) -> Result<(R, Option<Vec<u8>>)>,
    ) -> Result<R> {
        let scope = self.scope(state_id, |kind| {
            matches!(kind, UserStateKind::Multimap { .. })
        })?;
        let mut keys = self.read_map_keys(state_id).await?;
        let mut old = BTreeMap::new();
        while !keys.is_empty() {
            let (key, used) = decode_length_prefix(&keys)?;
            let mut encoded_key = Vec::new();
            encode_length_prefix(key, &mut encoded_key)?;
            let mut encoded_values = self.read_map_value(state_id, encoded_key).await?;
            let mut values = Vec::new();
            while !encoded_values.is_empty() {
                let (value, consumed) = decode_length_prefix(&encoded_values)?;
                values.push(value.to_vec());
                encoded_values = encoded_values.split_off(consumed);
            }
            values.sort();
            if old.insert(key.to_vec(), values).is_some() {
                bail!("Beam multimap state contains a duplicate key");
            }
            keys = keys.split_off(used);
        }
        let current = serde_json::to_vec(&old.iter().collect::<Vec<_>>())?;
        let (result, next) = match catch_unwind(AssertUnwindSafe(|| callback(&current))) {
            Ok(result) => result?,
            Err(_) => bail!("Beam multimap-state callback panicked"),
        };
        let new = next
            .as_deref()
            .map(decode_multimap_entries)
            .transpose()?
            .unwrap_or_default();
        if old == new {
            return Ok(result);
        }
        self.client
            .clear(self.instruction_id, scope.multimap_keys())
            .await?;
        for (key, values) in new {
            let mut encoded_key = Vec::new();
            encode_length_prefix(&key, &mut encoded_key)?;
            for value in values {
                let mut encoded_value = Vec::new();
                encode_length_prefix(&value, &mut encoded_value)?;
                self.append_map_value(state_id, encoded_key.clone(), encoded_value)
                    .await?;
            }
        }
        Ok(result)
    }
}
