//! Distinct value combiners: `DistinctCount`, `DistinctSet`,
//! `KMVApproxDistinctCount`, `HllApproxDistinctCount`.

use crate::RFBound;
use crate::collection::CombineFn;
use hyperloglogplus::{HyperLogLog, HyperLogLogPlus};
use ordered_float::NotNan;
use std::collections::{BinaryHeap, HashSet};
use std::hash::{BuildHasherDefault, DefaultHasher, Hash, Hasher};
use std::marker::PhantomData;

/* ===================== DistinctCount<T> ===================== */

/// Count of **distinct** values per key.
///
/// - Accumulator: `HashSet<T>`
/// - Output: `u64`
///
/// Requires `T: Eq + Hash`.
#[derive(Clone, Copy, Debug, Default)]
pub struct DistinctCount<T>(pub PhantomData<T>);
impl<T> DistinctCount<T> {
    /// Convenience constructor (same as `Default`).
    #[must_use]
    pub const fn new() -> Self {
        Self(PhantomData)
    }
}

impl<T> CombineFn<T, HashSet<T>, u64> for DistinctCount<T>
where
    T: RFBound + Eq + Hash,
{
    fn create(&self) -> HashSet<T> {
        HashSet::new()
    }

    fn add_input(&self, acc: &mut HashSet<T>, v: T) {
        acc.insert(v);
    }

    fn merge(&self, acc: &mut HashSet<T>, other: HashSet<T>) {
        if acc.is_empty() {
            *acc = other;
        } else {
            acc.extend(other);
        }
    }

    fn finish(&self, acc: HashSet<T>) -> u64 {
        acc.len() as u64
    }

    fn is_associative_commutative(&self) -> bool {
        true
    }
}


/* ===================== DistinctSet<T> (exact set) ===================== */

/// Get the distinct elements over a stream: accumulates a `HashSet<T>` and outputs a `Vec<T>`.
///
/// This is intended for use through helpers (e.g., `PCollection<T>::distinct()` or
/// `PCollection<(K,V)>::distinct_per_key()`), which expand the final `Vec<T>` back
/// into an element stream via `flat_map`.
#[derive(Clone, Copy, Debug)]
pub struct DistinctSet<T>(pub PhantomData<T>);
impl<T> DistinctSet<T> {
    #[must_use]
    pub const fn new() -> Self {
        Self(PhantomData)
    }
}
impl<T> Default for DistinctSet<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> CombineFn<T, HashSet<T>, Vec<T>> for DistinctSet<T>
where
    T: RFBound + Eq + Hash,
{
    fn create(&self) -> HashSet<T> {
        HashSet::new()
    }
    fn add_input(&self, acc: &mut HashSet<T>, v: T) {
        acc.insert(v);
    }
    fn merge(&self, acc: &mut HashSet<T>, other: HashSet<T>) {
        if acc.is_empty() {
            *acc = other;
        } else {
            acc.extend(other);
        }
    }
    fn finish(&self, acc: HashSet<T>) -> Vec<T> {
        acc.into_iter().collect()
    }
    fn is_associative_commutative(&self) -> bool {
        true
    }
}


/* ===================== KMVApproxDistinctCount<T> (approximate count) ===================== */

/// Approximate distinct count via the KMV (K-Minimum Values) estimator.
///
/// Keeps the smallest `k` hash ranks in a max-heap; estimate is `(k-1)/r_k` where
/// `r_k` is the largest (i.e., k-th smallest) retained rank. For small `n < k`,
/// returns the exact count (`heap.len()`).
#[derive(Clone, Debug)]
pub struct KMVApproxDistinctCount<T> {
    pub k: usize,
    _m: PhantomData<T>,
}
impl<T> KMVApproxDistinctCount<T> {
    #[must_use]
    pub fn new(k: usize) -> Self {
        Self {
            k: k.max(4),
            _m: PhantomData,
        }
    }
}

/// Accumulator keeps k smallest **distinct** ranks.
#[derive(Default)]
pub struct KMVAcc {
    heap: BinaryHeap<NotNan<f64>>, // max-heap of the kept k smallest
    set: HashSet<NotNan<f64>>,     // membership test to prevent duplicates
    k: usize,
}

#[inline]
#[allow(clippy::cast_precision_loss)]
fn rank_from_value<T: Hash>(v: &T) -> NotNan<f64> {
    let mut h = DefaultHasher::new();
    v.hash(&mut h);
    let u = h.finish();
    // Uniform in [0,1). NotNan is safe (never NaN).
    NotNan::new((u as f64) / ((u64::MAX as f64) + 1.0)).unwrap()
}

impl KMVAcc {
    #[inline]
    fn try_insert(&mut self, r: NotNan<f64>) {
        // Skip exact-duplicate ranks (same element hash)
        if !self.set.insert(r) {
            return;
        }
        if self.heap.len() < self.k {
            self.heap.push(r);
        } else if let Some(&rk) = self.heap.peek() {
            if r < rk {
                // Remove the current threshold from both heap and set
                let old = self.heap.pop().unwrap();
                self.set.remove(&old);
                // Insert the better (smaller) rank
                self.heap.push(r);
            } else {
                // New rank worse than the threshold; forget it from the set
                self.set.remove(&r);
            }
        }
        debug_assert!(self.heap.len() <= self.k && self.set.len() >= self.heap.len());
    }

    #[inline]
    fn merge_from(&mut self, mut other: Self) {
        // Move 'better' (smaller) ranks across, deduping
        while let Some(r) = other.heap.pop() {
            // Remove from other's set as we pop; not strictly needed, but neat
            other.set.remove(&r);
            self.try_insert(r);
        }
        // Any leftovers in other's set that weren't in heap (rare) can be ignored:
        // their ranks are >= other's threshold, and we've already considered the k
        // best from `other`.
    }

    #[inline]
    #[allow(clippy::cast_precision_loss)]
    fn finish(self) -> f64 {
        let m = self.set.len();
        if m == 0 {
            return 0.0;
        }
        if m < self.k {
            // Fewer than k uniques: exact count.
            return m as f64;
        }
        // m >= k: estimator based on the k-th smallest rank (heap root).
        let rk = *self.heap.peek().expect("heap non-empty when m>=k");
        // Classic KMV estimator: (k-1)/R_k. Using (k-1) reduces small-sample bias slightly.
        ((self.k as f64) - 1.0) / rk.into_inner()
    }
}

impl<T> CombineFn<T, KMVAcc, f64> for KMVApproxDistinctCount<T>
where
    T: RFBound + Hash,
{
    fn create(&self) -> KMVAcc {
        KMVAcc {
            heap: BinaryHeap::new(),
            set: HashSet::new(),
            k: self.k,
        }
    }

    fn add_input(&self, acc: &mut KMVAcc, v: T) {
        let r = rank_from_value(&v);
        acc.try_insert(r);
    }

    fn merge(&self, acc: &mut KMVAcc, other: KMVAcc) {
        acc.merge_from(other);
    }

    fn finish(&self, acc: KMVAcc) -> f64 {
        acc.finish()
    }

    fn is_associative_commutative(&self) -> bool {
        true
    }
}


/* ===================== HllApproxDistinctCount<T> (HyperLogLog++) ===================== */

/// `BuildHasher` used by [`HllApproxDistinctCount`] — `BuildHasherDefault`
/// over the std-library `DefaultHasher`, which seeds itself with **fixed
/// zero keys** via `Default::default()`. That determinism is what makes
/// per-partition HLLs mergeable: every partition hashes the same value to
/// the same bits, so register updates agree across the parallel runner's
/// shards and the merge step reproduces the single-pass result exactly.
type DeterministicHasher = BuildHasherDefault<DefaultHasher>;

/// Accumulator type for [`HllApproxDistinctCount`]: a `HyperLogLog`++ sketch
/// indexed by `T` and keyed off the [`DeterministicHasher`].
type Hll<T> = HyperLogLogPlus<T, DeterministicHasher>;

/// Approximate distinct-element count via the `HyperLogLog`++ algorithm
/// (`hyperloglogplus` crate), returning a `u64` cardinality estimate.
///
/// Memory usage is `O(2^precision)` registers regardless of the input
/// cardinality. The relative standard error of the estimate is
/// approximately `1.04 / sqrt(2^precision)`:
///
/// | precision | registers | rel. std. error |
/// |-----------|-----------|-----------------|
/// |        4  |       16  |          ~26 %  |
/// |       10  |     1024  |          ~3.3 % |
/// |       12  |     4096  |          ~1.6 % |
/// |       14  |    16384  |          ~0.8 % |
/// |       16  |    65536  |          ~0.4 % |
/// |       18  |   262144  |          ~0.2 % |
///
/// Use [`HllApproxDistinctCount::new`] for a sensible default
/// (`precision = 12`, ~1.6 % error), or
/// [`HllApproxDistinctCount::with_error`] / [`HllApproxDistinctCount::with_precision`]
/// to tune. Requires `T: RFBound + Hash`.
///
/// # Determinism
///
/// All partitions use the same zero-seeded hasher, so two runs over the
/// same data — and the sequential / parallel runners on the same data —
/// produce the *same* estimate. This is a deliberate trade-off against
/// adversarial-input resilience: the algorithm is meant for batch
/// cardinality reporting where reproducibility matters more than
/// resistance to crafted hash collisions.
#[derive(Clone, Debug)]
pub struct HllApproxDistinctCount<T> {
    precision: u8,
    _phantom: PhantomData<T>,
}

impl<T> HllApproxDistinctCount<T> {
    /// Smallest precision accepted by `hyperloglogplus`.
    const MIN_PRECISION: u8 = 4;
    /// Largest precision accepted by `hyperloglogplus`.
    const MAX_PRECISION: u8 = 18;

    /// Build a combiner with `precision = 12` (relative standard error
    /// ~1.6 %), the canonical Beam / Spark default.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            precision: 12,
            _phantom: PhantomData,
        }
    }

    /// Build a combiner whose precision is chosen so that the relative
    /// standard error is at most `error` (a value in `(0, 1)`).
    ///
    /// Maps to the smallest HLL precision `p` such that
    /// `1.04 / sqrt(2^p) <= error`, clamped to `[4, 18]`. Smaller `error`
    /// values yield larger sketches (memory grows by `2x` per precision
    /// step).
    ///
    /// # Panics
    ///
    /// Panics if `error` is not finite or is outside `(0, 1)`.
    #[must_use]
    pub fn with_error(error: f64) -> Self {
        assert!(
            error.is_finite() && error > 0.0 && error < 1.0,
            "approx_count_distinct error bound must be in (0, 1), got {error}"
        );
        let raw = (2.0 * (1.04_f64 / error).log2()).ceil();
        #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
        let p = (raw as i32).clamp(
            i32::from(Self::MIN_PRECISION),
            i32::from(Self::MAX_PRECISION),
        ) as u8;
        Self {
            precision: p,
            _phantom: PhantomData,
        }
    }

    /// Build a combiner with the exact `precision` requested, clamped to
    /// the `[4, 18]` range accepted by `hyperloglogplus`.
    #[must_use]
    pub const fn with_precision(precision: u8) -> Self {
        // const-clamp via a manual ternary so we can stay `const`.
        let p = if precision < Self::MIN_PRECISION {
            Self::MIN_PRECISION
        } else if precision > Self::MAX_PRECISION {
            Self::MAX_PRECISION
        } else {
            precision
        };
        Self {
            precision: p,
            _phantom: PhantomData,
        }
    }

    /// Return the configured precision (4..=18).
    #[must_use]
    pub const fn precision(&self) -> u8 {
        self.precision
    }
}

impl<T> Default for HllApproxDistinctCount<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> CombineFn<T, Hll<T>, u64> for HllApproxDistinctCount<T>
where
    T: RFBound + Hash,
{
    fn create(&self) -> Hll<T> {
        // Precision is range-checked by every constructor, so `new` always
        // succeeds here; the `expect` documents the invariant.
        Hll::<T>::new(self.precision, DeterministicHasher::default())
            .expect("HLL precision held in [4, 18] by every public constructor")
    }

    fn add_input(&self, acc: &mut Hll<T>, v: T) {
        acc.insert(&v);
    }

    fn merge(&self, acc: &mut Hll<T>, other: Hll<T>) {
        // Both sketches were created by the same combiner instance, so
        // precision matches and the merge cannot return Err in practice.
        acc.merge(&other)
            .expect("matching precision (both sketches built by this combiner)");
    }

    fn finish(&self, mut acc: Hll<T>) -> u64 {
        // `count()` is `&mut self` because HLL++ may lazily transition
        // from sparse to dense representation while estimating. We round
        // the f64 estimate to the nearest integer.
        #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
        let est = acc.count().round() as u64;
        est
    }

    fn is_associative_commutative(&self) -> bool {
        true
    }
}

