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
//!
//! Each [`MinHash`] owns its deterministically derived component seeds and its
//! signature. Seeds remain precomputed on the insertion hot path without any
//! global cache or shared mutable state. The concrete hash algorithm behind
//! `crate::seeded_hash64` is an implementation detail, so signatures should
//! not be treated as a portable persistence format across crate or Rust versions.
//!
//! [broder]: https://www.cs.princeton.edu/courses/archive/spring13/cos598C/broder97resemblance.pdf

use std::hash::Hash;

use crate::jaccard::JaccardIndex;
use crate::{SketchError, seeded_hash64, splitmix64};

/// Derivation seed for the deterministic default MinHash family.
const DEFAULT_HASH_FAMILY_SEED: u64 = 0xBF58_476D_1CE4_E5B9;

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
/// A sketch with `k` components owns `k` signature words and `k` component-seed
/// words. Adding an item takes `O(k)` time; comparing or merging sketches
/// requires matching signature widths and hash families.
#[derive(Debug, Clone)]
pub struct MinHash {
    derivation_seed: u64,
    component_seeds: Box<[u64]>,
    signature: Vec<u64>,
    observed_any: bool,
}

impl MinHash {
    /// Creates a MinHash sketch with `num_hashes` signature components.
    ///
    /// # Errors
    /// Returns [`SketchError::InvalidParameter`] when `num_hashes == 0` or the
    /// requested component seeds or signature cannot be allocated.
    pub fn new(num_hashes: usize) -> Result<Self, SketchError> {
        Self::with_derivation_seed(num_hashes, DEFAULT_HASH_FAMILY_SEED)
    }

    fn with_derivation_seed(num_hashes: usize, derivation_seed: u64) -> Result<Self, SketchError> {
        if num_hashes == 0 {
            return Err(SketchError::InvalidParameter(
                "num_hashes must be greater than zero",
            ));
        }

        let mut component_seeds = Vec::new();
        component_seeds
            .try_reserve_exact(num_hashes)
            .map_err(|_| SketchError::InvalidParameter("num_hashes is too large to allocate"))?;
        component_seeds.extend(
            (0..num_hashes).map(|index| splitmix64((index as u64).wrapping_add(derivation_seed))),
        );

        let mut signature = Vec::new();
        signature
            .try_reserve_exact(num_hashes)
            .map_err(|_| SketchError::InvalidParameter("num_hashes is too large to allocate"))?;
        signature.resize(num_hashes, u64::MAX);

        Ok(Self {
            derivation_seed,
            component_seeds: component_seeds.into_boxed_slice(),
            signature,
            observed_any: false,
        })
    }

    /// Creates a MinHash sketch from a target worst-case standard error.
    ///
    /// For `k` independent ideal MinHash components and true Jaccard similarity
    /// `J`, the estimator's standard error is `sqrt(J * (1 - J) / k)`. Its
    /// maximum over `J` is `1 / (2 * sqrt(k))`. This constructor starts from
    /// `ceil((0.5 / max_standard_error)^2)`, then increases the width until
    /// [`Self::worst_case_standard_error`] does not exceed the target.
    ///
    /// Sizing and the reported model error use ordinary `f64` arithmetic. The
    /// result satisfies `sketch.worst_case_standard_error() <= max_standard_error`
    /// without a tolerance; sizing is conservative at rounding boundaries and
    /// does not promise exact mathematical minimality. Finite targets at least
    /// `0.5` select one component. Explicit widths remain available via [`Self::new`].
    ///
    /// # Errors
    /// Returns [`SketchError::InvalidParameter`] when `max_standard_error` is
    /// non-finite, non-positive, or requires an unrepresentable or unallocatable
    /// signature. Width validation finishes before sketch allocation.
    ///
    /// # Example
    /// ```rust
    /// use sketches::minhash::MinHash;
    ///
    /// let target = f64::from_bits(0x3FBE_2B7D_DDFE_FA66);
    /// let sketch = MinHash::with_error_rate(target)?;
    /// assert_eq!(sketch.num_hashes(), 19);
    /// assert!(sketch.worst_case_standard_error() <= target);
    /// # Ok::<(), sketches::SketchError>(())
    /// ```
    pub fn with_error_rate(max_standard_error: f64) -> Result<Self, SketchError> {
        let num_hashes = required_hashes_for_max_standard_error(max_standard_error)?;
        Self::new(num_hashes)
    }

    /// Returns the worst-case standard error under the independent-component
    /// MinHash model.
    ///
    /// Evaluates `0.5 / sqrt(k)` with ordinary `f64` rounding. Error-based
    /// construction validates its target against this same calculation.
    pub fn worst_case_standard_error(&self) -> f64 {
        max_standard_error(self.num_hashes())
    }

    /// Returns the standard error at a specified true Jaccard similarity under
    /// the independent-component MinHash model.
    ///
    /// # Errors
    /// Returns [`SketchError::InvalidParameter`] unless `jaccard` is finite and
    /// in the inclusive range `[0, 1]`.
    pub fn standard_error_at(&self, jaccard: f64) -> Result<f64, SketchError> {
        if !jaccard.is_finite() || !(0.0..=1.0).contains(&jaccard) {
            return Err(SketchError::InvalidParameter(
                "jaccard must be finite and between zero and one",
            ));
        }

        Ok((jaccard * (1.0 - jaccard) / self.num_hashes() as f64).sqrt())
    }

    /// Returns the number of signature components.
    pub fn num_hashes(&self) -> usize {
        self.signature.len()
    }

    /// Returns the worst-case standard error under the independent-component
    /// MinHash model.
    #[deprecated(
        since = "0.1.3",
        note = "use worst_case_standard_error or standard_error_at"
    )]
    pub fn expected_error(&self) -> f64 {
        self.worst_case_standard_error()
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
    /// Component seeds are read from the sketch-owned precomputed seed table.
    pub fn add<T: Hash>(&mut self, item: &T) {
        for (index, seed) in self.component_seeds.iter().enumerate() {
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
        self.estimate_jaccard_signature(&other.signature, other.observed_any, other.derivation_seed)
    }

    /// Estimates Jaccard against compact signature state retained by another
    /// crate data structure.
    pub(crate) fn estimate_jaccard_signature(
        &self,
        other_signature: &[u64],
        other_observed_any: bool,
        other_family_seed: u64,
    ) -> Result<f64, SketchError> {
        if self.derivation_seed != other_family_seed
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
        self.derivation_seed
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
        if self.derivation_seed != other.derivation_seed || self.num_hashes() != other.num_hashes()
        {
            return Err(SketchError::IncompatibleSketches(message));
        }
        Ok(())
    }
}

/// Reports the rounded worst-case model error for a positive signature width.
///
/// Constructors guarantee `num_hashes >= 1`. Keeping one arithmetic owner makes
/// the sizing postcondition agree with the public accessor, including rounding.
fn max_standard_error(num_hashes: usize) -> f64 {
    0.5 / (num_hashes as f64).sqrt()
}

/// Selects a representable width satisfying the reported error target.
///
/// The floating inverse gives an initial candidate; its rounded
/// ceiling can still undersize a boundary, so verify the shared error calculation
/// and use checked increments before allocating. This promises the rounded
/// accessor postcondition, not exact-real minimality or a fixed correction count.
fn required_hashes_for_max_standard_error(target: f64) -> Result<usize, SketchError> {
    if !target.is_finite() || target <= 0.0 {
        return Err(SketchError::InvalidParameter(
            "standard error must be finite and greater than zero",
        ));
    }

    let root = 0.5 / target;
    let required = (root * root).ceil();
    if !required.is_finite() || required >= usize::MAX as f64 {
        return Err(SketchError::InvalidParameter(
            "requested standard error requires too many hashes",
        ));
    }

    let mut num_hashes = (required as usize).max(1);
    while max_standard_error(num_hashes) > target {
        num_hashes = num_hashes
            .checked_add(1)
            .ok_or(SketchError::InvalidParameter(
                "requested standard error requires too many hashes",
            ))?;
    }
    Ok(num_hashes)
}

impl JaccardIndex for MinHash {
    fn jaccard_index(&self, other: &Self) -> Result<f64, SketchError> {
        self.estimate_jaccard(other)
    }
}

#[cfg(test)]
mod tests {
    use super::{
        DEFAULT_HASH_FAMILY_SEED, MinHash, max_standard_error,
        required_hashes_for_max_standard_error,
    };
    use crate::{SketchError, splitmix64};

    fn sketch_for_range(start: u64, end: u64, num_hashes: usize) -> MinHash {
        let mut sketch = MinHash::new(num_hashes).unwrap();
        for value in start..end {
            sketch.add(&value);
        }
        sketch
    }

    fn estimate_for_family(
        num_hashes: usize,
        derivation_seed: u64,
        intersection_size: u64,
        union_size: u64,
    ) -> f64 {
        assert!(intersection_size <= union_size);
        assert_eq!((union_size + intersection_size) % 2, 0);

        let set_size = (union_size + intersection_size) / 2;
        let mut left = MinHash::with_derivation_seed(num_hashes, derivation_seed).unwrap();
        let mut right = MinHash::with_derivation_seed(num_hashes, derivation_seed).unwrap();

        for value in 0..set_size {
            left.add(&value);
        }
        for value in 0..intersection_size {
            right.add(&value);
        }
        for value in set_size..union_size {
            right.add(&value);
        }

        left.estimate_jaccard(&right).unwrap()
    }

    #[test]
    fn constructor_validates_num_hashes() {
        assert!(MinHash::new(0).is_err());
        assert!(MinHash::new(64).is_ok());
        assert!(MinHash::new(usize::MAX).is_err());
    }

    #[test]
    fn error_rate_constructor_validates_and_handles_extreme_inputs() {
        assert!(MinHash::with_error_rate(0.0).is_err());
        assert!(MinHash::with_error_rate(-0.1).is_err());
        assert!(MinHash::with_error_rate(f64::NAN).is_err());
        assert!(MinHash::with_error_rate(f64::INFINITY).is_err());
        assert!(MinHash::with_error_rate(f64::MIN_POSITIVE).is_err());

        assert_eq!(MinHash::with_error_rate(0.5).unwrap().num_hashes(), 1);
        assert_eq!(MinHash::with_error_rate(1.0).unwrap().num_hashes(), 1);
    }

    #[test]
    fn error_rate_constructor_selects_a_conservative_width() {
        for (target, expected_width) in [(0.49, 2), (0.25, 4), (0.1, 25), (0.05, 100), (0.01, 2500)]
        {
            let sketch = MinHash::with_error_rate(target).unwrap();
            let num_hashes = sketch.num_hashes();
            let selected_error = sketch.worst_case_standard_error();
            assert!(
                selected_error <= target,
                "target={target} k={num_hashes} error={selected_error}"
            );
            assert_eq!(num_hashes, expected_width);
        }
    }

    #[test]
    fn error_rate_constructor_honors_the_reported_target_at_width_18() {
        let target = f64::from_bits(0x3FBE_2B7D_DDFE_FA66);
        let sketch = MinHash::with_error_rate(target).unwrap();
        assert!(
            sketch.worst_case_standard_error() <= target,
            "target={target:?} k={} error={:?}",
            sketch.num_hashes(),
            sketch.worst_case_standard_error()
        );
        assert_eq!(sketch.num_hashes(), 19);
    }

    #[test]
    fn error_rate_constructor_checks_every_small_width_boundary() {
        for width in 1..=512 {
            let edge = 0.5 / (width as f64).sqrt();
            let targets = [
                f64::from_bits(edge.to_bits() - 1),
                edge,
                f64::from_bits(edge.to_bits() + 1),
            ];
            for (index, target) in targets.into_iter().enumerate() {
                let sketch = MinHash::with_error_rate(target).unwrap();
                assert!(
                    sketch.worst_case_standard_error() <= target,
                    "width={width}, target={target:?}, selected={}",
                    sketch.num_hashes()
                );
                assert!(sketch.num_hashes() >= width);
                if index == 0 {
                    // The target is strictly below the reported error for this
                    // width; allowing that width would repeat the original bug.
                    assert!(sketch.num_hashes() > width);
                }
                assert_eq!(sketch.component_seeds.len(), sketch.num_hashes());
                assert!(sketch.is_empty());
            }
        }
    }

    #[test]
    fn error_rate_constructor_allows_conservative_rounded_widths() {
        let target = f64::from_bits(0x3FD6_A09E_667F_3BCC);
        let explicit = MinHash::new(2).unwrap();
        assert_eq!(explicit.worst_case_standard_error(), target);

        // The rounded inverse squares to slightly above two. Convenience
        // sizing preserves that ceiling instead of promising a minimal width
        // under either exact-real or rounded-accessor arithmetic.
        let sketch = MinHash::with_error_rate(target).unwrap();
        assert_eq!(sketch.num_hashes(), 3);
        assert!(sketch.worst_case_standard_error() <= target);
    }

    #[test]
    fn error_sizing_covers_every_binary_exponent_without_large_allocations() {
        let invalid =
            SketchError::InvalidParameter("standard error must be finite and greater than zero");
        for target in [
            0.0,
            -0.0,
            -f64::from_bits(1),
            -1.0,
            f64::NAN,
            -f64::NAN,
            f64::INFINITY,
            f64::NEG_INFINITY,
        ] {
            assert_eq!(
                required_hashes_for_max_standard_error(target),
                Err(invalid.clone())
            );
            assert_eq!(MinHash::with_error_rate(target).unwrap_err(), invalid);
        }

        for exponent in -1074_i32..=1023 {
            let target = if exponent < -1022 {
                f64::from_bits(1_u64 << (exponent + 1074))
            } else {
                f64::from_bits(((exponent + 1023) as u64) << 52)
            };
            // For an exact power-of-two target, k = 2^(-2*exponent-2).
            // Derive expected dimensions with integer exponents, not inversion
            // or square roots. Test the pure owner to avoid enormous storage.
            let width_exponent = -2 * exponent - 2;
            let expected = if width_exponent <= 0 {
                Ok(1)
            } else if width_exponent < usize::BITS as i32 {
                Ok(1_usize << width_exponent)
            } else {
                Err(SketchError::InvalidParameter(
                    "requested standard error requires too many hashes",
                ))
            };
            assert_eq!(
                required_hashes_for_max_standard_error(target),
                expected,
                "exponent={exponent}, target={target:?}"
            );
        }

        for target in [0.5, f64::from_bits(0.5_f64.to_bits() + 1), 1.0, f64::MAX] {
            let sketch = MinHash::with_error_rate(target).unwrap();
            assert_eq!(sketch.num_hashes(), 1);
            assert!(sketch.worst_case_standard_error() <= target);
        }
    }

    #[cfg(target_pointer_width = "64")]
    #[test]
    fn error_sizing_repeats_correction_across_large_float_plateaus() {
        let target = f64::from_bits(0x3E1E_2B7D_DDFE_FA66);
        let initial = 81_064_793_292_668_912_usize;
        assert!(max_standard_error(initial) > target);
        assert!(max_standard_error(initial + 1) > target);

        let selected = required_hashes_for_max_standard_error(target).unwrap();
        assert!(selected >= initial + 2);
        assert!(max_standard_error(selected) <= target);
        // This checks representable dimensions only, without constructing a
        // signature whose memory requirement is far above available storage.
    }

    #[test]
    fn corrected_width_preserves_owned_signatures_and_lifecycle() {
        let target = f64::from_bits(0x3FBE_2B7D_DDFE_FA66);
        let mut sized = MinHash::with_error_rate(target).unwrap();
        let mut explicit = MinHash::new(19).unwrap();
        assert_eq!(sized.component_seeds, explicit.component_seeds);
        assert_ne!(
            sized.component_seeds.as_ptr(),
            explicit.component_seeds.as_ptr()
        );
        for value in [0_u64, 1, 7, 19, u64::MAX, 1, 19] {
            sized.add(&value);
            explicit.add(&value);
        }
        assert_eq!(sized.signature(), explicit.signature());
        assert_eq!(sized.estimate_jaccard(&explicit), Ok(1.0));

        let mut donor = MinHash::new(19).unwrap();
        for value in 19_u64..33 {
            donor.add(&value);
            explicit.add(&value);
        }
        let donor_before = donor.clone();
        sized.merge(&donor).unwrap();
        assert_eq!(sized.signature(), explicit.signature());
        assert_eq!(donor.signature(), donor_before.signature());
        assert_eq!(donor.component_seeds, donor_before.component_seeds);

        let before = sized.clone();
        assert!(matches!(
            sized.merge(&MinHash::new(18).unwrap()),
            Err(SketchError::IncompatibleSketches(_))
        ));
        assert_eq!(sized.signature(), before.signature());
        assert_eq!(sized.component_seeds, before.component_seeds);
        assert_eq!(sized.observed_any, before.observed_any);

        sized.clear();
        assert!(sized.is_empty());
        assert_eq!(sized.signature(), &[u64::MAX; 19]);
        assert_eq!(sized.component_seeds, before.component_seeds);
        sized.add(&7_u64);
        let mut fresh = MinHash::new(19).unwrap();
        fresh.add(&7_u64);
        assert_eq!(sized.signature(), fresh.signature());
    }

    #[test]
    fn standard_error_accessors_match_the_binomial_model() {
        let sketch = MinHash::new(100).unwrap();

        assert_eq!(sketch.worst_case_standard_error(), 0.05);
        assert_eq!(sketch.standard_error_at(0.0).unwrap(), 0.0);
        assert_eq!(sketch.standard_error_at(0.5).unwrap(), 0.05);
        assert_eq!(sketch.standard_error_at(1.0).unwrap(), 0.0);
        assert!(sketch.standard_error_at(-f64::EPSILON).is_err());
        assert!(sketch.standard_error_at(1.0 + f64::EPSILON).is_err());
        assert!(sketch.standard_error_at(f64::NAN).is_err());

        #[allow(deprecated)]
        {
            assert_eq!(sketch.expected_error(), sketch.worst_case_standard_error());
        }
    }

    #[test]
    fn compatible_sketches_own_equivalent_component_seed_tables() {
        let left = MinHash::new(256).unwrap();
        let right = MinHash::new(256).unwrap();

        assert_eq!(left.component_seeds, right.component_seeds);
        assert_ne!(
            left.component_seeds.as_ptr(),
            right.component_seeds.as_ptr()
        );
        assert_eq!(left.component_seeds.len(), 256);
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
    fn identical_sets_have_exact_similarity() {
        let mut left = MinHash::new(128).unwrap();
        let mut right = MinHash::new(128).unwrap();

        for value in 0_u64..5_000 {
            left.add(&value);
            right.add(&value);
        }

        let estimate = left.estimate_jaccard(&right).unwrap();
        assert_eq!(estimate, 1.0);
    }

    #[test]
    fn duplicates_and_ingestion_order_do_not_change_the_signature() {
        let mut forward_with_duplicates = MinHash::new(128).unwrap();
        let mut reverse = MinHash::new(128).unwrap();

        for value in 0_u64..1_000 {
            forward_with_duplicates.add(&value);
            forward_with_duplicates.add(&value);
        }
        for value in (0_u64..1_000).rev() {
            reverse.add(&value);
        }

        assert_eq!(forward_with_duplicates.signature(), reverse.signature());
        assert_eq!(
            forward_with_duplicates.estimate_jaccard(&reverse).unwrap(),
            1.0
        );
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
    fn merge_matches_direct_union_ingestion() {
        let mut left = sketch_for_range(0, 1_000, 128);
        let right = sketch_for_range(500, 1_500, 128);
        let direct = sketch_for_range(0, 1_500, 128);

        left.merge(&right).unwrap();

        assert_eq!(left.signature(), direct.signature());
        assert_eq!(left.is_empty(), direct.is_empty());
    }

    #[test]
    fn merge_obeys_union_algebra_and_empty_identity() {
        let first = sketch_for_range(0, 800, 128);
        let second = sketch_for_range(400, 1_200, 128);
        let third = sketch_for_range(1_000, 1_600, 128);
        let empty = MinHash::new(128).unwrap();

        let mut idempotent = first.clone();
        idempotent.merge(&first).unwrap();
        assert_eq!(idempotent.signature(), first.signature());

        let mut with_empty = first.clone();
        with_empty.merge(&empty).unwrap();
        assert_eq!(with_empty.signature(), first.signature());
        assert_eq!(with_empty.is_empty(), first.is_empty());

        let mut forward = first.clone();
        forward.merge(&second).unwrap();
        let mut reverse = second.clone();
        reverse.merge(&first).unwrap();
        assert_eq!(forward.signature(), reverse.signature());

        let mut left_associative = first.clone();
        left_associative.merge(&second).unwrap();
        left_associative.merge(&third).unwrap();

        let mut second_and_third = second.clone();
        second_and_third.merge(&third).unwrap();
        let mut right_associative = first.clone();
        right_associative.merge(&second_and_third).unwrap();

        assert_eq!(left_associative.signature(), right_associative.signature());
        assert_eq!(left_associative.is_empty(), right_associative.is_empty());
    }

    #[test]
    fn estimator_is_calibrated_across_hash_families() {
        const NUM_HASHES: usize = 128;
        const TRIALS: usize = 128;
        const UNION_SIZE: u64 = 200;

        for intersection_size in [20_u64, 100, 180] {
            let exact = intersection_size as f64 / UNION_SIZE as f64;
            let mut sum_error = 0.0;
            let mut sum_squared_error = 0.0;

            for trial in 0..TRIALS {
                let derivation_seed = DEFAULT_HASH_FAMILY_SEED
                    ^ splitmix64((trial as u64).wrapping_add(0xD1B5_4A32_D192_ED03));
                let estimate =
                    estimate_for_family(NUM_HASHES, derivation_seed, intersection_size, UNION_SIZE);
                let error = estimate - exact;
                sum_error += error;
                sum_squared_error += error * error;
            }

            let mean_error = sum_error / TRIALS as f64;
            let mean_squared_error = sum_squared_error / TRIALS as f64;
            let ideal_variance = exact * (1.0 - exact) / NUM_HASHES as f64;
            let mean_tolerance = 5.0 * (ideal_variance / TRIALS as f64).sqrt();
            let variance_ratio = mean_squared_error / ideal_variance;

            assert!(
                mean_error.abs() <= mean_tolerance,
                "J={exact} mean_error={mean_error} tolerance={mean_tolerance}"
            );
            assert!(
                (0.65..=1.35).contains(&variance_ratio),
                "J={exact} MSE={mean_squared_error} ideal={ideal_variance} ratio={variance_ratio}"
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
        let right = MinHash::with_derivation_seed(64, DEFAULT_HASH_FAMILY_SEED ^ 1).unwrap();

        assert!(left.merge(&right).is_err());
        assert!(left.estimate_jaccard(&right).is_err());
    }

    #[test]
    fn clones_copy_component_seeds_and_retain_compatibility() {
        let mut original = MinHash::new(64).unwrap();
        for value in 0_u64..1_000 {
            original.add(&value);
        }

        let clone = original.clone();
        assert_eq!(clone.component_seeds, original.component_seeds);
        assert_ne!(
            clone.component_seeds.as_ptr(),
            original.component_seeds.as_ptr()
        );
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
