//! Priority-based reservoir sampling combiner

use crate::RFBound;
use crate::collection::{CombineFn, LiftableCombiner};
use crate::utils::OrdF64;
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::marker::PhantomData;

// ======================================================================
// Reservoir / Priority Sampling (Efraimidis–Spirakis A-ExpJ, unit weight)
// ======================================================================

#[derive(Clone, Copy, Debug)]
struct SplitMix64 {
    state: u64,
}

impl SplitMix64 {
    const fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    #[inline]
    const fn next_u64(&mut self) -> u64 {
        let mut z = {
            self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
            self.state
        };
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    #[inline]
    #[allow(clippy::cast_precision_loss)]
    fn next_f64(&mut self) -> f64 {
        const SCALE: f64 = 1.0 / ((1u64 << 53) as f64);
        ((self.next_u64() >> 11) as f64) * SCALE
    }
}

/// Accumulator for priority reservoir.
/// - `heap`: min-heap by (priority asc, seq asc) with entries (key, seq, idx)
/// - `store`: slot array holding actual items + their (key,seq)
#[derive(Clone, Debug)]
pub struct PRAcc<T> {
    k: usize,
    rng: SplitMix64,
    seq: u64,
    heap: BinaryHeap<Reverse<(OrdF64, u64, usize)>>,
    store: Vec<Option<(OrdF64, u64, T)>>,
    alive: usize,
}

/// Reservoir / Priority Sampling (Efraimidis–Spirakis A-ExpJ, unit weight)
///
/// We sample k items by assigning each element an i.i.d. priority key u ~ U(0,1),
/// and keeping the top-k by key. This is mergeable: to combine two reservoirs,
/// take the k largest keys across both.
///
/// Determinism: we use a tiny `SplitMix64` PRNG in the accumulator seeded from
/// the combiner's seed. Sequential vs. parallel runs produce identical results
/// as long as the input multiset is the same (merge is order/partition-neutral).
///
/// Output order: `finish` returns items sorted by (priority desc, seq asc)
/// for stable deterministic output.
#[derive(Clone, Copy, Debug)]
pub struct PriorityReservoir<T> {
    pub k: usize,
    pub seed: u64,
    _m: PhantomData<T>,
}

impl<T> PriorityReservoir<T> {
    #[must_use]
    pub const fn new(k: usize, seed: u64) -> Self {
        Self {
            k,
            seed,
            _m: PhantomData,
        }
    }
}

impl<T: RFBound> CombineFn<T, PRAcc<T>, Vec<T>> for PriorityReservoir<T> {
    fn create(&self) -> PRAcc<T> {
        PRAcc {
            k: self.k,
            rng: SplitMix64::new(self.seed.wrapping_mul(0xA24B_AED4_0B9C_497C)),
            seq: 0,
            heap: BinaryHeap::new(),
            store: Vec::new(),
            alive: 0,
        }
    }

    fn add_input(&self, acc: &mut PRAcc<T>, v: T) {
        if acc.k == 0 {
            return;
        }
        let mut u = acc.rng.next_f64();
        if u == 0.0 {
            u = f64::from_bits(1);
        } // strictly > 0
        let key = OrdF64(u);
        let seq = {
            let s = acc.seq;
            acc.seq += 1;
            s
        };

        let idx = acc.store.len();
        acc.store.push(Some((key, seq, v)));
        acc.heap.push(Reverse((key, seq, idx)));
        acc.alive += 1;

        // Trim to k real (non-dead) items
        while acc.alive > acc.k {
            if let Some(Reverse((_k, _s, i))) = acc.heap.pop() {
                if let Some(slot) = acc.store.get_mut(i)
                    && slot.is_some()
                {
                    *slot = None;
                    acc.alive -= 1;
                }
            } else {
                break;
            }
        }
    }

    fn merge(&self, acc: &mut PRAcc<T>, mut other: PRAcc<T>) {
        if acc.k == 0 {
            return;
        }
        // align k (keep the larger request in case of mismatch)
        acc.k = acc.k.max(other.k);

        // move other's live items into acc with remapped indices
        let mut map: Vec<Option<usize>> = Vec::with_capacity(other.store.len());
        for slot in other.store {
            if let Some((key, seq, v)) = slot {
                let idx = acc.store.len();
                acc.store.push(Some((key, seq, v)));
                map.push(Some(idx));
                acc.alive += 1;
            } else {
                map.push(None);
            }
        }

        // move heap entries, remapping indices; drop ones pointing to tombstones
        while let Some(Reverse((k, s, i_old))) = other.heap.pop() {
            if let Some(Some(i_new)) = map.get(i_old) {
                acc.heap.push(Reverse((k, s, *i_new)));
            }
        }

        // Trim to k live items
        while acc.alive > acc.k {
            if let Some(Reverse((_k, _s, i))) = acc.heap.pop() {
                if let Some(slot) = acc.store.get_mut(i)
                    && slot.is_some()
                {
                    *slot = None;
                    acc.alive -= 1;
                }
            } else {
                break;
            }
        }
    }

    fn finish(&self, acc: PRAcc<T>) -> Vec<T> {
        if acc.k == 0 || acc.alive == 0 {
            return Vec::new();
        }
        // Collect live items with their (key,seq), sort by (key desc, seq asc)
        let mut items: Vec<(OrdF64, u64, T)> = Vec::with_capacity(acc.alive.min(acc.k));
        for slot in acc.store.into_iter().flatten() {
            items.push(slot);
        }
        items.sort_by(|a, b| b.0.cmp(&a.0).then_with(|| a.1.cmp(&b.1)));
        items.truncate(acc.k);
        items.into_iter().map(|(_, _, v)| v).collect()
    }
}

impl<T: RFBound> LiftableCombiner<T, PRAcc<T>, Vec<T>> for PriorityReservoir<T> {
    fn build_from_group(&self, values: &[T]) -> PRAcc<T> {
        let mut acc = self.create();
        for v in values.iter().cloned() {
            self.add_input(&mut acc, v);
        }
        acc
    }
}
