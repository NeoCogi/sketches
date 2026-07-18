// MIT License
//
// Copyright (c) 2026 Raja Lehtihet & Wael El Oraiby
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.
//
//! MinHash sketch for approximate Jaccard similarity.
//!
//! This implementation uses the classical `k`-hash signature: it retains one
//! minimum for each of `k` deterministically derived hash functions. This is
//! distinct from [Broder's original single-permutation bottom-`k` sketch][broder].
//! The hash-family configuration is shared by compatible live sketches. Each
//! [`MinHash`] stores a constant-sized handle to that immutable configuration
//! and its own signature rather than retaining a duplicate `k`-element seed
//! vector.
//!
//! Sharing the hash family does not alter the component seeds or signatures
//! produced by the crate's default family, and it keeps component seeds
//! precomputed on the insertion hot path. The concrete hash algorithm behind
//! [`crate::seeded_hash64`] is an implementation detail, however, so signatures
//! should not be treated as a portable persistence format across crate or Rust
//! versions.
//!
//! [broder]: https://www.cs.princeton.edu/courses/archive/spring13/cos598C/broder97resemblance.pdf

use std::collections::HashMap;
use std::hash::Hash;
use std::sync::{Arc, Mutex, OnceLock, Weak};

use crate::jacard::JacardIndex;
use crate::{SketchError, seeded_hash64, splitmix64};

/// Derivation seed for the deterministic default MinHash family.
const DEFAULT_HASH_FAMILY_SEED: u64 = 0xBF58_476D_1CE4_E5B9;

type HashFamilyKey = (u64, usize);
type HashFamilyCache = HashMap<HashFamilyKey, Weak<HashFamily>>;

/// Weak cache of immutable hash families keyed by `(derivation seed, width)`.
///
/// Weak entries ensure the cache does not keep an otherwise unused `O(k)` seed
/// table alive. Access is limited to construction; insertion never locks.
static HASH_FAMILY_CACHE: OnceLock<Mutex<HashFamilyCache>> = OnceLock::new();

#[derive(Debug)]
struct HashFamily {
    derivation_seed: u64,
    component_seeds: Box<[u64]>,
}

/// MinHash signature sketch for estimating set similarity.
///
/// # Example
/// ```rust
/// use sketches::minhash::MinHash;
///
/// let mut left = MinHash::new(256).unwrap();
/// let mut right = MinHash::new(256).unwrap();
///
/// for value in 0_u64..10_000 {
///     left.add(&value);
/// }
/// for value in 5_000_u64..15_000 {
///     right.add(&value);
/// }
///
/// // Exact Jaccard is 5_000 / 15_000 = 0.333...
/// let estimate = left.estimate_jaccard(&right).unwrap();
/// assert!(estimate > 0.20 && estimate < 0.45);
/// ```
///
/// # Representation and complexity
///
/// A sketch with `k` components stores `k` signature words plus a
/// constant-sized shared-family handle. Compatible live sketches reuse one
/// immutable `k`-word component-seed table. Adding an item takes `O(k)` time;
/// comparing or merging sketches requires matching signature widths and hash
/// families.
#[derive(Debug, Clone)]
pub struct MinHash {
    hash_family: Arc<HashFamily>,
    signature: Vec<u64>,
    observed_any: bool,
}

impl MinHash {
    /// Creates a MinHash sketch with `num_hashes` signature components.
    ///
    /// Compatible live sketches reuse one immutable component-seed table. The
    /// cache holds only weak references, so that table is released after its
    /// last sketch is dropped.
    ///
    /// # Errors
    /// Returns [`SketchError::InvalidParameter`] when `num_hashes == 0`.
    pub fn new(num_hashes: usize) -> Result<Self, SketchError> {
        if num_hashes == 0 {
            return Err(SketchError::InvalidParameter(
                "num_hashes must be greater than zero",
            ));
        }

        Ok(Self {
            hash_family: shared_hash_family(DEFAULT_HASH_FAMILY_SEED, num_hashes),
            signature: vec![u64::MAX; num_hashes],
            observed_any: false,
        })
    }

    /// Creates a MinHash sketch from a target standard error.
    ///
    /// Uses the relation `std_error ≈ 1 / sqrt(num_hashes)`, i.e.
    /// `num_hashes = ceil(1 / std_error^2)`.
    ///
    /// # Errors
    /// Returns [`SketchError::InvalidParameter`] for invalid `std_error`.
    pub fn with_error_rate(std_error: f64) -> Result<Self, SketchError> {
        if !std_error.is_finite() || std_error <= 0.0 || std_error >= 1.0 {
            return Err(SketchError::InvalidParameter(
                "std_error must be finite and strictly between 0 and 1",
            ));
        }

        let num_hashes = (1.0 / (std_error * std_error)).ceil() as usize;
        Self::new(num_hashes.max(1))
    }

    /// Returns the number of signature components.
    pub fn num_hashes(&self) -> usize {
        self.signature.len()
    }

    /// Returns the expected standard error approximation.
    pub fn expected_error(&self) -> f64 {
        1.0 / (self.num_hashes() as f64).sqrt()
    }

    /// Returns `true` when no item has been observed yet.
    pub fn is_empty(&self) -> bool {
        !self.observed_any
    }

    /// Returns a read-only view of the signature vector.
    pub fn signature(&self) -> &[u64] {
        &self.signature
    }

    /// Adds one item to the sketch in `O(k)` time, where `k` is
    /// [`Self::num_hashes`].
    ///
    /// Component seeds are read from the immutable shared hash family. This
    /// preserves the previous insertion loop and seed sequence without storing
    /// a duplicate `k`-element vector in every compatible sketch.
    pub fn add<T: Hash>(&mut self, item: &T) {
        for (index, seed) in self.hash_family.component_seeds.iter().enumerate() {
            let hashed = seeded_hash64(item, *seed);
            if hashed < self.signature[index] {
                self.signature[index] = hashed;
            }
        }
        self.observed_any = true;
    }

    /// Estimates Jaccard similarity against another MinHash sketch.
    ///
    /// # Errors
    /// Returns [`SketchError::IncompatibleSketches`] when the signature widths
    /// or hash families differ.
    pub fn estimate_jaccard(&self, other: &Self) -> Result<f64, SketchError> {
        self.estimate_jaccard_signature(
            &other.signature,
            other.observed_any,
            other.hash_family.derivation_seed,
        )
    }

    /// Estimates Jaccard against compact signature state retained by another
    /// crate data structure.
    pub(crate) fn estimate_jaccard_signature(
        &self,
        other_signature: &[u64],
        other_observed_any: bool,
        other_family_seed: u64,
    ) -> Result<f64, SketchError> {
        if self.hash_family.derivation_seed != other_family_seed
            || self.signature.len() != other_signature.len()
        {
            return Err(SketchError::IncompatibleSketches(
                "num_hashes/hash family must match",
            ));
        }

        match (self.observed_any, other_observed_any) {
            (false, false) => return Ok(1.0),
            (false, true) | (true, false) => return Ok(0.0),
            (true, true) => {}
        }

        let matches = self
            .signature
            .iter()
            .zip(other_signature.iter())
            .filter(|(left, right)| left == right)
            .count();
        Ok(matches as f64 / self.signature.len() as f64)
    }

    /// Returns the compact identity of the configured hash family for other
    /// crate data structures that retain MinHash signatures.
    pub(crate) fn hash_family_seed(&self) -> u64 {
        self.hash_family.derivation_seed
    }

    /// Merges another sketch in-place by taking element-wise minima.
    ///
    /// # Errors
    /// Returns [`SketchError::IncompatibleSketches`] when the signature widths
    /// or hash families differ.
    pub fn merge(&mut self, other: &Self) -> Result<(), SketchError> {
        self.ensure_compatible(other, "num_hashes/hash family must match for merge")?;

        for (left, right) in self.signature.iter_mut().zip(other.signature.iter()) {
            *left = (*left).min(*right);
        }
        self.observed_any |= other.observed_any;
        Ok(())
    }

    /// Resets the sketch to the empty state.
    pub fn clear(&mut self) {
        self.signature.fill(u64::MAX);
        self.observed_any = false;
    }

    fn ensure_compatible(&self, other: &Self, message: &'static str) -> Result<(), SketchError> {
        if self.hash_family.derivation_seed != other.hash_family.derivation_seed
            || self.num_hashes() != other.num_hashes()
        {
            return Err(SketchError::IncompatibleSketches(message));
        }
        Ok(())
    }
}

/// Returns a shared, precomputed hash family without retaining it globally
/// after its final sketch is dropped.
fn shared_hash_family(derivation_seed: u64, num_hashes: usize) -> Arc<HashFamily> {
    let cache = HASH_FAMILY_CACHE.get_or_init(|| Mutex::new(HashMap::new()));
    let mut cache = cache
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let key = (derivation_seed, num_hashes);

    if let Some(family) = cache.get(&key).and_then(Weak::upgrade) {
        return family;
    }

    cache.retain(|_, family| family.strong_count() > 0);
    let component_seeds = (0..num_hashes)
        .map(|index| splitmix64((index as u64).wrapping_add(derivation_seed)))
        .collect();
    let family = Arc::new(HashFamily {
        derivation_seed,
        component_seeds,
    });
    cache.insert(key, Arc::downgrade(&family));
    family
}

impl JacardIndex for MinHash {
    fn jaccard_index(&self, other: &Self) -> Result<f64, SketchError> {
        self.estimate_jaccard(other)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::{DEFAULT_HASH_FAMILY_SEED, MinHash, shared_hash_family};

    #[test]
    fn constructor_validates_num_hashes() {
        assert!(MinHash::new(0).is_err());
        assert!(MinHash::new(64).is_ok());
    }

    #[test]
    fn compatible_sketches_share_the_component_seed_table() {
        let left = MinHash::new(256).unwrap();
        let right = MinHash::new(256).unwrap();

        assert!(Arc::ptr_eq(&left.hash_family, &right.hash_family));
        assert_eq!(left.hash_family.component_seeds.len(), 256);
    }

    #[test]
    fn default_signature_matches_the_pre_elision_known_answer() {
        let mut sketch = MinHash::new(8).unwrap();
        for value in 0_u64..10_000 {
            sketch.add(&value);
        }

        assert_eq!(
            sketch.signature(),
            &[
                751_021_725_051_808,
                2_594_915_795_371_041,
                1_524_705_651_004_105,
                2_787_610_102_987,
                3_166_387_023_764_429,
                1_730_634_328_335_802,
                4_346_437_160_029_285,
                304_615_318_525_070,
            ]
        );
    }

    #[test]
    fn jaccard_estimate_is_reasonable_for_overlap() {
        let mut left = MinHash::new(256).unwrap();
        let mut right = MinHash::new(256).unwrap();

        for value in 0_u64..10_000 {
            left.add(&value);
        }
        for value in 5_000_u64..15_000 {
            right.add(&value);
        }

        let estimate = left.estimate_jaccard(&right).unwrap();
        let exact = 5_000.0 / 15_000.0;
        assert!(
            (estimate - exact).abs() < 0.15,
            "estimate={estimate} exact={exact}"
        );
    }

    #[test]
    fn identical_sets_have_high_similarity() {
        let mut left = MinHash::new(128).unwrap();
        let mut right = MinHash::new(128).unwrap();

        for value in 0_u64..5_000 {
            left.add(&value);
            right.add(&value);
        }

        let estimate = left.estimate_jaccard(&right).unwrap();
        assert!(estimate > 0.90, "estimate={estimate}");
    }

    #[test]
    fn empty_semantics_are_supported() {
        let left = MinHash::new(64).unwrap();
        let mut right = MinHash::new(64).unwrap();
        right.add(&"x");

        assert_eq!(left.estimate_jaccard(&left).unwrap(), 1.0);
        assert_eq!(left.estimate_jaccard(&right).unwrap(), 0.0);
    }

    #[test]
    fn merge_uses_elementwise_min() {
        let mut left = MinHash::new(64).unwrap();
        let mut right = MinHash::new(64).unwrap();
        for value in 0_u64..1_000 {
            left.add(&value);
        }
        for value in 500_u64..1_500 {
            right.add(&value);
        }

        let mut merged = left.clone();
        merged.merge(&right).unwrap();

        for index in 0..merged.signature().len() {
            assert_eq!(
                merged.signature()[index],
                left.signature()[index].min(right.signature()[index])
            );
        }
    }

    #[test]
    fn merge_rejects_incompatible_sketches() {
        let mut left = MinHash::new(64).unwrap();
        let right = MinHash::new(65).unwrap();
        assert!(left.merge(&right).is_err());
        assert!(left.estimate_jaccard(&right).is_err());
    }

    #[test]
    fn merge_and_jaccard_reject_a_different_hash_family() {
        let mut left = MinHash::new(64).unwrap();
        let mut right = MinHash::new(64).unwrap();
        right.hash_family = shared_hash_family(DEFAULT_HASH_FAMILY_SEED ^ 1, 64);

        assert!(left.merge(&right).is_err());
        assert!(left.estimate_jaccard(&right).is_err());
    }

    #[test]
    fn clones_retain_hash_family_compatibility_and_signature() {
        let mut original = MinHash::new(64).unwrap();
        for value in 0_u64..1_000 {
            original.add(&value);
        }

        let clone = original.clone();
        assert!(Arc::ptr_eq(&clone.hash_family, &original.hash_family));
        assert_eq!(clone.signature(), original.signature());
        assert_eq!(clone.estimate_jaccard(&original).unwrap(), 1.0);
    }

    #[test]
    fn clear_resets_state() {
        let mut sketch = MinHash::new(64).unwrap();
        sketch.add(&"alpha");
        sketch.clear();
        assert!(sketch.is_empty());
        assert!(sketch.signature().iter().all(|&value| value == u64::MAX));
    }
}
