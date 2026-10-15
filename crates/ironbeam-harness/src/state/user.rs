//! Fn State keys for one keyed, windowed user-state cell.

use anyhow::{bail, Result};
use beam_proto::fn_execution_v1::{state_key, OrderedListRange, StateKey};
use ironbeam_core::{StatefulParDoContract, UserStateKind};

use super::StateClient;

mod bag_transition;
mod map_transition;
mod set_transition;
mod value_transition;
pub use value_transition::ValueStateMutation;

#[derive(Clone)]
pub struct UserStateScope {
    transform_id: String,
    state_id: String,
    window: Vec<u8>,
    key: Vec<u8>,
}

impl UserStateScope {
    pub fn new(
        transform_id: impl Into<String>,
        state_id: impl Into<String>,
        window: Vec<u8>,
        key: Vec<u8>,
    ) -> Result<Self> {
        let transform_id = transform_id.into();
        let state_id = state_id.into();
        if transform_id.is_empty() || state_id.is_empty() {
            bail!("Beam user state needs a transform ID and state ID");
        }
        Ok(Self {
            transform_id,
            state_id,
            window,
            key,
        })
    }

    pub fn bag(&self) -> StateKey {
        StateKey {
            r#type: Some(state_key::Type::BagUserState(state_key::BagUserState {
                transform_id: self.transform_id.clone(),
                user_state_id: self.state_id.clone(),
                window: self.window.clone(),
                key: self.key.clone(),
            })),
        }
    }

    pub fn multimap_keys(&self) -> StateKey {
        StateKey {
            r#type: Some(state_key::Type::MultimapKeysUserState(
                state_key::MultimapKeysUserState {
                    transform_id: self.transform_id.clone(),
                    user_state_id: self.state_id.clone(),
                    window: self.window.clone(),
                    key: self.key.clone(),
                },
            )),
        }
    }

    pub fn multimap_entries(&self) -> StateKey {
        StateKey {
            r#type: Some(state_key::Type::MultimapEntriesUserState(
                state_key::MultimapEntriesUserState {
                    transform_id: self.transform_id.clone(),
                    user_state_id: self.state_id.clone(),
                    window: self.window.clone(),
                    key: self.key.clone(),
                },
            )),
        }
    }

    pub fn multimap_value(&self, map_key: Vec<u8>) -> StateKey {
        StateKey {
            r#type: Some(state_key::Type::MultimapUserState(
                state_key::MultimapUserState {
                    transform_id: self.transform_id.clone(),
                    user_state_id: self.state_id.clone(),
                    window: self.window.clone(),
                    key: self.key.clone(),
                    map_key,
                },
            )),
        }
    }

    pub fn ordered_list(&self, start: i64, end: i64) -> Result<StateKey> {
        if start >= end {
            bail!("Beam ordered-list state requires a nonempty sort range");
        }
        Ok(StateKey {
            r#type: Some(state_key::Type::OrderedListUserState(
                state_key::OrderedListUserState {
                    transform_id: self.transform_id.clone(),
                    user_state_id: self.state_id.clone(),
                    window: self.window.clone(),
                    key: self.key.clone(),
                    range: Some(OrderedListRange { start, end }),
                },
            )),
        })
    }

    /// Base ordered-list key used for append. The encoded entries carry their
    /// own sort timestamps; range is only present on read and clear requests.
    pub fn ordered_list_all(&self) -> StateKey {
        StateKey {
            r#type: Some(state_key::Type::OrderedListUserState(
                state_key::OrderedListUserState {
                    transform_id: self.transform_id.clone(),
                    user_state_id: self.state_id.clone(),
                    window: self.window.clone(),
                    key: self.key.clone(),
                    range: None,
                },
            )),
        }
    }
}

/// Runner-backed state cells for one keyed, windowed bundle instruction.
/// The declared contract determines which Fn State key family each method may
/// use. The runner owns commit or rollback of mutations when the bundle ends.
pub struct UserStateAccess<'a> {
    client: &'a StateClient,
    instruction_id: &'a str,
    transform_id: &'a str,
    contract: &'a StatefulParDoContract,
    window: Vec<u8>,
    key: Vec<u8>,
}

impl<'a> UserStateAccess<'a> {
    /// Bind the application declaration to the runner's current bundle scope.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid contract or missing instruction or
    /// transform ID. Empty window and key bytes are valid coder outputs.
    pub fn new(
        client: &'a StateClient,
        instruction_id: &'a str,
        transform_id: &'a str,
        contract: &'a StatefulParDoContract,
        window: Vec<u8>,
        key: Vec<u8>,
    ) -> Result<Self> {
        contract.validate()?;
        if instruction_id.is_empty() || transform_id.is_empty() {
            bail!("Beam user state needs an instruction ID and transform ID");
        }
        Ok(Self {
            client,
            instruction_id,
            transform_id,
            contract,
            window,
            key,
        })
    }

    fn scope(
        &self,
        state_id: &str,
        expected: fn(&UserStateKind) -> bool,
    ) -> Result<UserStateScope> {
        let kind = self
            .contract
            .states
            .get(state_id)
            .ok_or_else(|| anyhow::anyhow!("undeclared Beam user state {state_id}"))?;
        if !expected(kind) {
            bail!("Beam user state {state_id} uses a different protocol");
        }
        UserStateScope::new(
            self.transform_id,
            state_id,
            self.window.clone(),
            self.key.clone(),
        )
    }

    fn bag_kind(kind: &UserStateKind) -> bool {
        matches!(
            kind,
            UserStateKind::Value { .. }
                | UserStateKind::Bag { .. }
                | UserStateKind::Combining { .. }
        )
    }

    fn value_kind(kind: &UserStateKind) -> bool {
        matches!(kind, UserStateKind::Value { .. })
    }

    fn map_only_kind(kind: &UserStateKind) -> bool {
        matches!(kind, UserStateKind::Map { .. })
    }

    fn multimap_kind(kind: &UserStateKind) -> bool {
        matches!(
            kind,
            UserStateKind::Set { .. } | UserStateKind::Map { .. } | UserStateKind::Multimap { .. }
        )
    }

    fn ordered_kind(kind: &UserStateKind) -> bool {
        matches!(kind, UserStateKind::OrderedList { .. })
    }

    /// Read all encoded bag bytes for a value, bag, or combining state cell.
    pub async fn read_bag(&self, state_id: &str) -> Result<Vec<u8>> {
        let key = self.scope(state_id, Self::bag_kind)?.bag();
        self.client.get_all(self.instruction_id, key).await
    }

    /// Append encoded bag bytes in the current bundle.
    pub async fn append_bag(&self, state_id: &str, data: Vec<u8>) -> Result<()> {
        let key = self.scope(state_id, Self::bag_kind)?.bag();
        self.client.append(self.instruction_id, key, data).await
    }

    /// Clear a bag cell in the current bundle.
    pub async fn clear_bag(&self, state_id: &str) -> Result<()> {
        let key = self.scope(state_id, Self::bag_kind)?.bag();
        self.client.clear(self.instruction_id, key).await
    }

    /// Replace one encoded value-state cell in the current bundle.
    ///
    /// Beam represents value state with a bag key. Replacement clears that
    /// key before appending one coder-encoded element. The runner commits or
    /// rolls back both requests with the enclosing bundle.
    pub async fn write_value(&self, state_id: &str, encoded_value: Vec<u8>) -> Result<()> {
        let key = self.scope(state_id, Self::value_kind)?.bag();
        self.client.clear(self.instruction_id, key.clone()).await?;
        self.client
            .append(self.instruction_id, key, encoded_value)
            .await
    }

    /// Clear a declared value-state cell in the current bundle.
    pub async fn clear_value(&self, state_id: &str) -> Result<()> {
        let key = self.scope(state_id, Self::value_kind)?.bag();
        self.client.clear(self.instruction_id, key).await
    }

    /// Read encoded multimap keys for set, map, or multimap state.
    pub async fn read_map_keys(&self, state_id: &str) -> Result<Vec<u8>> {
        let key = self.scope(state_id, Self::multimap_kind)?.multimap_keys();
        self.client.get_all(self.instruction_id, key).await
    }

    /// Read the encoded entries of a set, map, or multimap state cell.
    pub async fn read_map_entries(&self, state_id: &str) -> Result<Vec<u8>> {
        let key = self
            .scope(state_id, Self::multimap_kind)?
            .multimap_entries();
        self.client.get_all(self.instruction_id, key).await
    }

    /// Read encoded values at one encoded multimap key.
    pub async fn read_map_value(&self, state_id: &str, map_key: Vec<u8>) -> Result<Vec<u8>> {
        let key = self
            .scope(state_id, Self::multimap_kind)?
            .multimap_value(map_key);
        self.client.get_all(self.instruction_id, key).await
    }

    /// Append encoded values at one encoded multimap key.
    pub async fn append_map_value(
        &self,
        state_id: &str,
        map_key: Vec<u8>,
        data: Vec<u8>,
    ) -> Result<()> {
        let key = self
            .scope(state_id, Self::multimap_kind)?
            .multimap_value(map_key);
        self.client.append(self.instruction_id, key, data).await
    }

    /// Clear one encoded multimap key.
    pub async fn clear_map_value(&self, state_id: &str, map_key: Vec<u8>) -> Result<()> {
        let key = self
            .scope(state_id, Self::multimap_kind)?
            .multimap_value(map_key);
        self.client.clear(self.instruction_id, key).await
    }

    /// Replace one map entry. Other keys in the same map remain untouched.
    pub async fn write_map_value(
        &self,
        state_id: &str,
        map_key: Vec<u8>,
        encoded_value: Vec<u8>,
    ) -> Result<()> {
        let key = self
            .scope(state_id, Self::map_only_kind)?
            .multimap_value(map_key);
        self.client.clear(self.instruction_id, key.clone()).await?;
        self.client
            .append(self.instruction_id, key, encoded_value)
            .await
    }

    /// Read encoded ordered-list entries in a half-open sort range.
    pub async fn read_ordered(&self, state_id: &str, start: i64, end: i64) -> Result<Vec<u8>> {
        let key = self
            .scope(state_id, Self::ordered_kind)?
            .ordered_list(start, end)?;
        self.client.get_all(self.instruction_id, key).await
    }

    /// Append encoded timestamped entries to an ordered-list state cell.
    /// Each entry carries its own sort timestamp; the StateKey has no range.
    pub async fn append_ordered(&self, state_id: &str, data: Vec<u8>) -> Result<()> {
        let key = self.scope(state_id, Self::ordered_kind)?.ordered_list_all();
        self.client.append(self.instruction_id, key, data).await
    }

    /// Clear an ordered-list range in the current bundle.
    pub async fn clear_ordered(&self, state_id: &str, start: i64, end: i64) -> Result<()> {
        let key = self
            .scope(state_id, Self::ordered_kind)?
            .ordered_list(start, end)?;
        self.client.clear(self.instruction_id, key).await
    }
}
