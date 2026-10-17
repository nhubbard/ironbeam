//! One runner-scoped value-state callback and its mutation sequence.

use anyhow::{bail, Result};
use std::panic::{catch_unwind, AssertUnwindSafe};

use super::UserStateAccess;

/// Mutation requested by a keyed value-state callback.
#[derive(Debug, Eq, PartialEq)]
pub enum ValueStateMutation {
    /// Leave the runner-owned cell unchanged.
    Keep,
    /// Remove the current value.
    Clear,
    /// Replace the current value with exactly one coder-encoded value.
    Set(Vec<u8>),
}

impl UserStateAccess<'_> {
    /// Read a declared value cell, invoke a synchronous callback, and submit
    /// its mutation under the current Beam bundle instruction. The callback
    /// receives raw coder bytes; it must use the declared value coder to
    /// distinguish a missing value from any zero-length encoding.
    ///
    /// A callback error or panic sends no mutation. A failed Fn State request
    /// fails the bundle, so the runner can discard its uncommitted state.
    /// The bundle executor must serialize invocations for the same key and
    /// window; this method does not lock a runner-owned cell.
    pub async fn apply_value_state<R>(
        &self,
        state_id: &str,
        callback: impl FnOnce(&[u8]) -> Result<(R, ValueStateMutation)>,
    ) -> Result<R> {
        let state_key = self.scope(state_id, Self::value_kind)?.bag();
        let current = self.client.get_all(self.instruction_id, state_key).await?;
        let (result, mutation) = match catch_unwind(AssertUnwindSafe(|| callback(&current))) {
            Ok(result) => result?,
            Err(_) => bail!("Beam value-state callback panicked"),
        };
        match mutation {
            ValueStateMutation::Keep => {}
            ValueStateMutation::Clear => self.clear_value(state_id).await?,
            ValueStateMutation::Set(encoded) => self.write_value(state_id, encoded).await?,
        }
        Ok(result)
    }
}
