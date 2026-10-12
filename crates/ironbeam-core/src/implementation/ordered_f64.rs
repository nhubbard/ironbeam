//! A totally ordered floating-point value for keys and extrema.

use crate::{Element, Error};
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::hash::{Hash, Hasher};

/// An `f64` ordered by IEEE total ordering, including signed zero and NaN payloads.
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize)]
pub struct OrdF64(pub f64);

impl PartialEq for OrdF64 {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}
impl Eq for OrdF64 {}
impl PartialOrd for OrdF64 {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for OrdF64 {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.total_cmp(&other.0)
    }
}
impl Hash for OrdF64 {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.to_bits().hash(state);
    }
}
impl From<f64> for OrdF64 {
    fn from(value: f64) -> Self {
        Self(value)
    }
}
impl From<OrdF64> for f64 {
    fn from(value: OrdF64) -> Self {
        value.0
    }
}
impl Element for OrdF64 {
    const TYPE_ID: &'static str = "ironbeam/ord-f64";
    const CODER_ID: &'static str = "ironbeam/ord-f64-ieee754-le";
    fn supports_coder() -> bool {
        true
    }
    fn encode(&self) -> Result<Vec<u8>, Error> {
        self.0.encode()
    }
    fn encode_reusing(&self, buffer: &mut Vec<u8>) -> Result<(), Error> {
        self.0.encode_reusing(buffer)
    }
    fn encoded_len(&self) -> Result<usize, Error> {
        self.0.encoded_len()
    }
    fn decode(bytes: &[u8]) -> Result<Self, Error> {
        f64::decode(bytes).map(Self)
    }
}
