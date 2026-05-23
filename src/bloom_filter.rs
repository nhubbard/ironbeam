//! In-tree Bloom filter for the Bloom semi-join optimization.
//!
//! This module provides a lightweight, ephemeral Bloom filter designed for
//! one-shot use during [`crate::node::Node::CoGroup`] execution:
//!
//! 1. **Build** from the smaller (or semantically-required) join side's keys.
//! 2. **Query** for each element of the other side before the hash-join phase.
//! 3. **Discard** elements whose key is definitively absent from the filter.
//!
//! Elements eliminated here never enter the hash-map construction step,
//! reducing both peak memory usage and CPU cost for sparse joins.
//!
//! ## Parameters
//!
//! | Parameter | Value | Notes |
//! |-----------|-------|-------|
//! | Hash functions (`k`) | 4 | Each uses a distinct seed via `DefaultHasher` |
//! | Bit-array size (`m`) | `⌈n × 9.59⌉` bits | Targets ~1 % false-positive rate |
//! | Minimum size | 64 bits | Avoids degenerate zero-size filters |
//!
//! The false-positive rate (`p ≈ 0.01`) is acceptable for a semi-join pre-filter:
//! false positives only mean a few extra elements reach the hash-join step;
//! false negatives are impossible, so join correctness is guaranteed.
//!
//! ## Non-persistence guarantee
//!
//! The filter is not serializable and must not be stored across pipeline runs.
//! It is valid only for the duration of a single `exec` call.

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

/// Number of independent hash functions applied to each key.
const K: usize = 4;

/// Minimum number of bits in the filter's bit array.
const MIN_BITS: usize = 64;

/// A simple, ephemeral Bloom filter for join key membership tests.
///
/// Construct with [`BloomFilter::new`], populate with [`BloomFilter::insert`],
/// then query with [`BloomFilter::might_contain`].
///
/// # Correctness
///
/// - [`BloomFilter::might_contain`] returning `false` guarantees the key was **never
///   inserted** — safe to discard the element.
/// - [`BloomFilter::might_contain`] returning `true` means the key *probably* was
///   inserted (false-positive rate ≈ 1 %).
pub struct BloomFilter {
    /// Bit array packed into 64-bit words.
    bits: Vec<u64>,
    /// Total number of addressable bits (`bits.len() * 64`).
    m: usize,
}

impl BloomFilter {
    /// Create a new `BloomFilter` sized for `n` expected insertions at ~1 % FPR.
    ///
    /// Uses `m = ⌈−n ln(0.01) / (ln 2)²⌉ ≈ ⌈n × 9.59⌉`, clamped to at least
    /// 64 bits.
    #[must_use]
    pub fn new(n: usize) -> Self {
        // m ≈ n * 9.59 for p = 0.01, k = 4
        #[allow(
            clippy::cast_precision_loss,
            clippy::cast_sign_loss,
            clippy::cast_possible_truncation
        )]
        let m = ((n as f64 * 9.59_f64).ceil() as usize).max(MIN_BITS);
        let words = m.div_ceil(64);
        Self {
            bits: vec![0u64; words],
            m,
        }
    }

    /// Insert `key` into the filter.
    pub fn insert<H: Hash>(&mut self, key: &H) {
        for pos in bit_positions(key, self.m) {
            self.bits[pos / 64] |= 1u64 << (pos % 64);
        }
    }

    /// Returns `false` if `key` was **definitely never inserted**; `true` if it
    /// *probably* was (subject to the ~1 % false-positive rate).
    #[must_use]
    pub fn might_contain<H: Hash>(&self, key: &H) -> bool {
        bit_positions(key, self.m)
            .iter()
            .all(|&pos| self.bits[pos / 64] & (1u64 << (pos % 64)) != 0)
    }
}

/// Compute `K` independent bit positions for `key` in a bit array of size `m`.
///
/// Uses `k` distinct seeds: `H_i(key) = hash(i ‖ key) mod m`. Seeds are
/// mixed in as a leading `u64` before hashing `key`, ensuring each of the
/// `K` positions is derived from an independent hash function.
fn bit_positions<H: Hash>(key: &H, m: usize) -> [usize; K] {
    std::array::from_fn(|i| {
        let mut h = DefaultHasher::new();
        (i as u64).hash(&mut h);
        key.hash(&mut h);
        #[allow(clippy::cast_possible_truncation)]
        let raw = h.finish() as usize;
        raw % m
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inserted_keys_are_always_found() {
        let keys: Vec<u32> = (0..500).collect();
        let mut f = BloomFilter::new(keys.len());
        for k in &keys {
            f.insert(k);
        }
        for k in &keys {
            assert!(
                f.might_contain(k),
                "false negative for key {k} — Bloom filter must not produce false negatives"
            );
        }
    }

    #[test]
    fn absent_keys_mostly_absent() {
        let mut f = BloomFilter::new(100);
        for i in 0u32..100 {
            f.insert(&i);
        }
        // Keys 1000..2000 were never inserted; FPR ≈ 1 %, so expect < 5 % false positives.
        let false_positives = (1000u32..2000).filter(|k| f.might_contain(k)).count();
        assert!(
            false_positives < 50,
            "FPR too high: {false_positives}/1000 false positives"
        );
    }

    #[test]
    fn empty_filter_contains_nothing() {
        let f = BloomFilter::new(0);
        assert!(!f.might_contain(&42u32));
        assert!(!f.might_contain(&"hello"));
    }

    #[test]
    fn string_keys_work() {
        let mut f = BloomFilter::new(10);
        f.insert(&"apple");
        f.insert(&"banana");
        assert!(f.might_contain(&"apple"));
        assert!(f.might_contain(&"banana"));
        assert!(!f.might_contain(&"cherry"));
    }
}
