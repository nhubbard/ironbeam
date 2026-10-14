//! Fn State keys for one keyed, windowed user-state cell.

use anyhow::{bail, Result};
use beam_proto::fn_execution_v1::{state_key, OrderedListRange, StateKey};

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
}
