//! One declared BagState read, callback, and mutation sequence.

use anyhow::{bail, Result};
use std::panic::{catch_unwind, AssertUnwindSafe};

use super::UserStateAccess;
use ironbeam_core::UserStateKind;

impl UserStateAccess<'_> {
    /// Read one bag, invoke a callback, and replace its encoded contents in
    /// the current bundle. The callback receives concatenated element-coder
    /// bytes and returns new concatenated bytes, or `None` to clear the cell.
    /// Runner commit or rollback remains scoped to the bundle instruction.
    pub async fn apply_bag_state<R>(
        &self,
        state_id: &str,
        callback: impl FnOnce(&[u8]) -> Result<(R, Option<Vec<u8>>)>,
    ) -> Result<R> {
        let key = self
            .scope(state_id, |kind| matches!(kind, UserStateKind::Bag { .. }))?
            .bag();
        let current = self.client.get_all(self.instruction_id, key).await?;
        let (result, next) = match catch_unwind(AssertUnwindSafe(|| callback(&current))) {
            Ok(result) => result?,
            Err(_) => bail!("Beam bag-state callback panicked"),
        };
        if next.as_deref() == Some(current.as_slice()) {
            return Ok(result);
        }
        self.clear_bag(state_id).await?;
        if let Some(next) = next.filter(|bytes| !bytes.is_empty()) {
            self.append_bag(state_id, next).await?;
        }
        Ok(result)
    }
}
