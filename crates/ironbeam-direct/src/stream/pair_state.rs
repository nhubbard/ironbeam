//! One checkpoint payload for both branches of a streaming fanout.
use crate::multi_output::PairPlan;
use ironbeam_core::{Error, Registry};

const MAGIC: &[u8; 8] = b"IBPAIR01";

pub(super) fn snapshot(
    plan: &PairPlan,
    registry: &Registry,
    budget: usize,
) -> Result<Option<Vec<u8>>, Error> {
    let mut states = Vec::with_capacity(plan.stateful_keys.len());
    for key in &plan.stateful_keys {
        states.push(registry.checkpoint_stateful_value(key)?);
    }
    let state = match states.len() {
        0 => return Ok(None),
        1 => states.remove(0),
        2 => {
            let size = MAGIC
                .len()
                .checked_add(16)
                .and_then(|size| size.checked_add(states[0].len()))
                .and_then(|size| size.checked_add(states[1].len()))
                .ok_or_else(|| Error::resource("stateful-value", "pair state size overflow"))?;
            if size > budget {
                return Err(Error::resource(
                    "stateful-value",
                    "stream value state exceeds memory budget",
                ));
            }
            let mut encoded = Vec::with_capacity(size);
            encoded.extend_from_slice(MAGIC);
            for state in states {
                encoded.extend_from_slice(&(state.len() as u64).to_le_bytes());
                encoded.extend_from_slice(&state);
            }
            encoded
        }
        _ => return Err(Error::graph("stateful-value", "too many pair states")),
    };
    if state.len() > budget {
        return Err(Error::resource(
            "stateful-value",
            "stream value state exceeds memory budget",
        ));
    }
    Ok(Some(state))
}

pub(super) fn restore(plan: &PairPlan, registry: &Registry, bytes: &[u8]) -> Result<(), Error> {
    match plan.stateful_keys.as_slice() {
        [] => Err(Error::incompatible("checkpoint has no stateful pair stage")),
        [key] => registry.restore_stateful_value(key, bytes),
        [left, right] => {
            let mut remaining = bytes
                .strip_prefix(MAGIC)
                .ok_or_else(|| Error::incompatible("pair state checkpoint magic mismatch"))?;
            let mut states = Vec::with_capacity(2);
            for _ in 0..2 {
                let length = remaining
                    .get(..8)
                    .ok_or_else(|| Error::incompatible("truncated pair state length"))?;
                let length = usize::try_from(u64::from_le_bytes(length.try_into().unwrap()))
                    .map_err(|_| Error::incompatible("pair state length overflow"))?;
                remaining = &remaining[8..];
                let state = remaining
                    .get(..length)
                    .ok_or_else(|| Error::incompatible("truncated pair state"))?;
                states.push(state);
                remaining = &remaining[length..];
            }
            if !remaining.is_empty() {
                return Err(Error::incompatible("trailing pair state bytes"));
            }
            registry.restore_stateful_value(left, states[0])?;
            registry.restore_stateful_value(right, states[1])
        }
        _ => Err(Error::graph("stateful-value", "too many pair states")),
    }
}

#[cfg(test)]
#[path = "../../tests/unit/stream__pair_state_tests.rs"]
mod tests;
