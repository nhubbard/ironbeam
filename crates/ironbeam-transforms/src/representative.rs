use ironbeam_core::{CombineFn, Element};

/// Keeps one coded representative while partial keyed accumulators merge.
pub(crate) struct MinimumEncoded;

impl<T: Element> CombineFn<(Vec<u8>, T), Option<(Vec<u8>, T)>, Vec<T>> for MinimumEncoded {
    fn create(&self) -> Option<(Vec<u8>, T)> {
        None
    }

    fn add_input(&self, chosen: &mut Option<(Vec<u8>, T)>, candidate: (Vec<u8>, T)) {
        if chosen
            .as_ref()
            .is_none_or(|(prior, _)| candidate.0 < *prior)
        {
            *chosen = Some(candidate);
        }
    }

    fn merge(&self, chosen: &mut Option<(Vec<u8>, T)>, other: Option<(Vec<u8>, T)>) {
        if let Some(candidate) = other {
            self.add_input(chosen, candidate);
        }
    }

    fn finish(&self, chosen: Option<(Vec<u8>, T)>) -> Vec<T> {
        chosen.into_iter().map(|(_, value)| value).collect()
    }
}

#[cfg(test)]
#[path = "../tests/unit/representative__tests.rs"]
mod tests;
