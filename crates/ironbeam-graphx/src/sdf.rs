//! Versioned restriction payload for a one-split Ironbeam source SDF.
//!
//! This is the body carried by a Beam bytes coder. PairWithRestriction emits
//! `KV<impulse, restriction>`; a residual returns the same restriction shape
//! with the last accepted source cursor and watermark. The process stage must
//! request a runner split before handing residual work back to Prism.

use ironbeam_core::ProgressSignal;
use serde::{Deserialize, Serialize};

pub const RESTRICTION_VERSION: u32 = 1;
pub const MAX_RESTRICTION_BYTES: usize = 1024 * 1024;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SourceRestriction {
    pub version: u32,
    pub source_key: String,
    pub progress: Option<ProgressSignal>,
}

#[derive(Debug, thiserror::Error)]
pub enum RestrictionError {
    #[error("source restriction encoding failed: {0}")]
    Encoding(String),
    #[error("source restriction exceeds 1 MiB")]
    TooLarge,
    #[error("source restriction has a wrong version or source identity")]
    Identity,
    #[error("source restriction has invalid resume progress")]
    Progress,
}

impl SourceRestriction {
    #[must_use]
    pub fn initial(source_key: impl Into<String>) -> Self {
        Self {
            version: RESTRICTION_VERSION,
            source_key: source_key.into(),
            progress: None,
        }
    }

    pub fn resume(progress: ProgressSignal) -> Result<Self, RestrictionError> {
        let restriction = Self {
            version: RESTRICTION_VERSION,
            source_key: progress.source_key.clone(),
            progress: Some(progress),
        };
        restriction.validate(&restriction.source_key)?;
        Ok(restriction)
    }

    pub fn encode(&self) -> Result<Vec<u8>, RestrictionError> {
        self.validate(&self.source_key)?;
        let bytes = serde_json::to_vec(self)
            .map_err(|error| RestrictionError::Encoding(error.to_string()))?;
        if bytes.len() > MAX_RESTRICTION_BYTES {
            return Err(RestrictionError::TooLarge);
        }
        Ok(bytes)
    }

    pub fn decode(source_key: &str, bytes: &[u8]) -> Result<Self, RestrictionError> {
        if bytes.len() > MAX_RESTRICTION_BYTES {
            return Err(RestrictionError::TooLarge);
        }
        let restriction: Self = serde_json::from_slice(bytes)
            .map_err(|error| RestrictionError::Encoding(error.to_string()))?;
        restriction.validate(source_key)?;
        Ok(restriction)
    }

    fn validate(&self, source_key: &str) -> Result<(), RestrictionError> {
        if source_key.is_empty()
            || self.version != RESTRICTION_VERSION
            || self.source_key != source_key
        {
            return Err(RestrictionError::Identity);
        }
        if let Some(progress) = &self.progress {
            if progress.source_key != source_key
                || progress.split_id.is_empty()
                || progress.checkpoint_position.is_none()
                || progress
                    .checkpoint_position
                    .as_ref()
                    .is_some_and(|cursor| cursor.len() > MAX_RESTRICTION_BYTES)
                || progress.completed
            {
                return Err(RestrictionError::Progress);
            }
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "../tests/unit/sdf__tests.rs"]
mod tests;
