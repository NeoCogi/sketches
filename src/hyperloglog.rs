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
//! HyperLogLog cardinality estimator.
//!
//! Cardinality is calculated with the maximum-likelihood estimator described as
//! the second single-sketch estimator in [Ertl 2017]. In the paper this is
//! **Algorithm 8**; its literal Algorithm 2 is the register-wise merge operation.
//! The maximum-likelihood estimator works directly from the register-value
//! multiplicities and avoids the original estimator's empirically chosen
//! small-/large-range transitions.
//!
//! Precision is configurable from 4 through 18. [`HyperLogLog::with_error_rate`]
//! interprets its argument as a target nominal relative standard error and
//! rejects targets below the `0.00203125` achievable at precision 18. The
//! standard error describes the estimator's expected statistical variation;
//! it is not a deterministic bound on every estimate.
//!
//! # Intersection and Jaccard limitations
//!
//! HyperLogLog natively represents unions through register-wise maxima. This
//! module's [`HyperLogLog::intersection_estimate`] and
//! [`HyperLogLog::jaccard_index`] instead use the conventional
//! inclusion-exclusion estimate `|A| + |B| - |A ∪ B|`. As explained by
//! [Ertl 2017], that approach can be quite inaccurate, especially when the true
//! Jaccard index is small. Subtracting three noisy cardinality estimates can
//! leave an error comparable to the sizes of the input sets rather than the
//! much smaller intersection.
//!
//! Clamping the result into its mathematical range only prevents impossible
//! negative or oversized outputs. It does not recover the lost information,
//! make a zero result proof of disjointness, or make a positive result proof of
//! overlap. [`HyperLogLog::expected_relative_error`] describes single-sketch
//! cardinality error only; it is not an error guarantee for intersections or
//! Jaccard values. Prefer [`crate::minhash::MinHash`] when similarity is the
//! primary workload. Ertl's joint maximum-likelihood method is the appropriate
//! HLL-specific alternative when substantially better set-operation estimates
//! are required.
//!
//! [Ertl 2017]: https://arxiv.org/pdf/1702.01284

use std::hash::Hash;

use crate::jacard::{JacardIndex, inclusion_exclusion_estimates};
use crate::{SketchError, seeded_hash64};

const MIN_PRECISION: u8 = 4;
const MAX_PRECISION: u8 = 18;
const RELATIVE_STANDARD_ERROR_FACTOR: f64 = 1.04;
const HASH_SEED: u64 = 0xD6E8_FD93_5E7A_4A6D;
const HASH_BITS: usize = u64::BITS as usize;
const MAX_REGISTER_COUNTS: usize = HASH_BITS + 2;
const MAX_LIKELIHOOD_EPSILON: f64 = 1e-2;

fn relative_standard_error(precision: u8) -> f64 {
    RELATIVE_STANDARD_ERROR_FACTOR / ((1_usize << precision) as f64).sqrt()
}

/// Approximate distinct counter using HyperLogLog registers.
///
/// # Example
/// ```rust
/// use sketches::hyperloglog::HyperLogLog;
///
/// let mut hll = HyperLogLog::new(12).unwrap();
/// for i in 0..10_000_u64 {
///     hll.add(&i);
/// }
///
/// let estimate = hll.count();
/// assert!(estimate > 9_000 && estimate < 11_000);
/// ```
#[derive(Debug, Clone)]
pub struct HyperLogLog {
    precision: u8,
    registers: Vec<u8>,
}

impl HyperLogLog {
    /// Creates a HyperLogLog with precision `p`.
    ///
    /// Register count is `2^p`. Valid range is `[4, 18]`.
    ///
    /// # Errors
    /// Returns [`SketchError::InvalidParameter`] when precision is out of range.
    pub fn new(precision: u8) -> Result<Self, SketchError> {
        if !(MIN_PRECISION..=MAX_PRECISION).contains(&precision) {
            return Err(SketchError::InvalidParameter(
                "precision must be in the inclusive range [4, 18]",
            ));
        }

        let register_count = 1_usize << precision;
        Ok(Self {
            precision,
            registers: vec![0; register_count],
        })
    }

    /// Creates a HyperLogLog from a target nominal relative standard error.
    ///
    /// Selects the smallest supported precision whose nominal relative standard
    /// error, `1.04 / sqrt(2^p)`, is no greater than the target. Supported
    /// precision is `[4, 18]`, so the smallest accepted target is `0.00203125`.
    /// A standard error is an expected measure of statistical variation, not a
    /// deterministic upper bound on every cardinality estimate.
    ///
    /// # Errors
    /// Returns [`SketchError::InvalidParameter`] when the target is not finite
    /// and strictly between zero and one, or when precision 18 cannot meet it.
    pub fn with_error_rate(target_relative_error: f64) -> Result<Self, SketchError> {
        if !target_relative_error.is_finite()
            || target_relative_error <= 0.0
            || target_relative_error >= 1.0
        {
            return Err(SketchError::InvalidParameter(
                "target relative error must be finite and strictly between 0 and 1",
            ));
        }

        let precision = (MIN_PRECISION..=MAX_PRECISION)
            .find(|&precision| relative_standard_error(precision) <= target_relative_error)
            .ok_or(SketchError::InvalidParameter(
                "target relative error is below the minimum supported value of 0.00203125",
            ))?;

        Self::new(precision)
    }

    /// Returns the configured precision.
    pub fn precision(&self) -> u8 {
        self.precision
    }

    /// Returns the number of registers (`2^precision`).
    pub fn register_count(&self) -> usize {
        self.registers.len()
    }

    /// Returns the nominal relative standard error: `1.04 / sqrt(m)`.
    ///
    /// This is the expected statistical variation for the configured register
    /// count, not a deterministic bound on every estimate.
    pub fn expected_relative_error(&self) -> f64 {
        relative_standard_error(self.precision)
    }

    /// Returns `true` if no item has been observed yet.
    pub fn is_empty(&self) -> bool {
        self.registers.iter().all(|&register| register == 0)
    }

    /// Adds one item to the sketch.
    pub fn add<T: Hash>(&mut self, item: &T) {
        let hash = seeded_hash64(item, HASH_SEED);
        let index = (hash >> (64 - self.precision as u32)) as usize;
        let rank = Self::rank(hash, self.precision);

        if rank > self.registers[index] {
            self.registers[index] = rank;
        }
    }

    /// Returns the estimated cardinality as `f64`.
    ///
    /// This uses the maximum-likelihood cardinality estimator presented as
    /// Algorithm 8 in [Ertl 2017], which is the paper's second single-sketch
    /// estimator. (The paper's literal Algorithm 2 describes sketch merging,
    /// not cardinality estimation.)
    ///
    /// [Ertl 2017]: https://arxiv.org/pdf/1702.01284
    pub fn estimate(&self) -> f64 {
        let mut counts = [0_usize; MAX_REGISTER_COUNTS];
        for &register in &self.registers {
            counts[register as usize] += 1;
        }

        let suffix_bits = HASH_BITS - self.precision as usize;
        Self::maximum_likelihood_estimate(&counts[..=suffix_bits + 1], self.register_count())
    }

    /// Returns the estimated cardinality rounded to `u64`.
    pub fn count(&self) -> u64 {
        self.estimate().round() as u64
    }

    /// Resets all registers to zero.
    pub fn clear(&mut self) {
        self.registers.fill(0);
    }

    /// Merges another HyperLogLog into this sketch.
    ///
    /// Register-wise maximum is the native HLL union operation and corresponds
    /// to Algorithm 2 in [Ertl 2017]. Cardinality of the merged state is then
    /// calculated by the Algorithm 8 maximum-likelihood estimator used by
    /// [`Self::estimate`].
    ///
    /// [Ertl 2017]: https://arxiv.org/pdf/1702.01284
    ///
    /// # Errors
    /// Returns [`SketchError::IncompatibleSketches`] when precision differs.
    pub fn merge(&mut self, other: &Self) -> Result<(), SketchError> {
        if self.precision != other.precision {
            return Err(SketchError::IncompatibleSketches(
                "precision must match for merge",
            ));
        }

        for (left, right) in self.registers.iter_mut().zip(other.registers.iter()) {
            *left = (*left).max(*right);
        }
        Ok(())
    }

    /// Returns the estimated union cardinality `|A ∪ B|`.
    ///
    /// This clones `self`, merges `other` into that clone using register-wise
    /// maxima, then estimates the resulting merged sketch.
    ///
    /// # Example
    /// ```rust
    /// use sketches::hyperloglog::HyperLogLog;
    ///
    /// // Both sets contain exactly the same 10_000 values.
    /// let mut left = HyperLogLog::new(14).unwrap();
    /// let mut right = HyperLogLog::new(14).unwrap();
    /// for value in 0_u64..10_000 {
    ///     left.add(&value);
    ///     right.add(&value);
    /// }
    ///
    /// // Union of identical sets should stay near 10_000.
    /// let union = left.union_estimate(&right).unwrap();
    /// assert!(union > 9_000.0 && union < 11_000.0);
    /// ```
    ///
    /// # Errors
    /// Returns [`SketchError::IncompatibleSketches`] when precision differs.
    pub fn union_estimate(&self, other: &Self) -> Result<f64, SketchError> {
        let mut union = self.clone();
        union.merge(other)?;
        Ok(union.estimate())
    }

    /// Returns the estimated intersection cardinality `|A ∩ B|`.
    ///
    /// This uses inclusion-exclusion:
    /// `|A ∩ B| ≈ |A| + |B| - |A ∪ B|`.
    ///
    /// The output is clamped to `[0, min(|A|, |B|)]` because estimator noise
    /// can occasionally push inclusion-exclusion slightly outside that range.
    ///
    /// # Statistical limitations
    ///
    /// This is the conventional inclusion-exclusion approach, not Ertl's joint
    /// maximum-likelihood estimator. [Ertl 2017] shows that inclusion-exclusion
    /// becomes inaccurate in particular for small Jaccard indices: the desired
    /// intersection is obtained by subtracting cardinality estimates whose
    /// individual errors scale with the much larger input sets.
    ///
    /// Clamping does not correct that statistical error. A returned zero does
    /// not prove disjointness, and a positive value does not prove overlap. The
    /// nominal error from [`Self::expected_relative_error`] applies to an HLL
    /// cardinality estimate, not to this derived intersection estimate.
    ///
    /// [Ertl 2017]: https://arxiv.org/pdf/1702.01284
    ///
    /// # Example
    /// ```rust
    /// use sketches::hyperloglog::HyperLogLog;
    ///
    /// // Overlap is exactly [5_000, 10_000), so exact intersection is 5_000.
    /// let mut left = HyperLogLog::new(14).unwrap();
    /// let mut right = HyperLogLog::new(14).unwrap();
    /// for value in 0_u64..10_000 {
    ///     left.add(&value);
    /// }
    /// for value in 5_000_u64..15_000 {
    ///     right.add(&value);
    /// }
    ///
    /// let intersection = left.intersection_estimate(&right).unwrap();
    /// assert!(intersection > 4_000.0 && intersection < 6_000.0);
    /// ```
    ///
    /// # Errors
    /// Returns [`SketchError::IncompatibleSketches`] when precision differs.
    pub fn intersection_estimate(&self, other: &Self) -> Result<f64, SketchError> {
        let union = self.union_estimate(other)?;
        let a = self.estimate();
        let b = other.estimate();
        Ok(inclusion_exclusion_estimates(a, b, union).intersection)
    }

    /// Returns the estimated Jaccard index `|A ∩ B| / |A ∪ B|`.
    ///
    /// Jaccard index is:
    /// - `0.0` when two sets are disjoint,
    /// - `1.0` when two sets are identical.
    ///
    /// For two empty sets, this method returns `1.0` by convention.
    ///
    /// # Statistical limitations
    ///
    /// This method derives the intersection using inclusion-exclusion. It does
    /// not implement the joint maximum-likelihood estimator from [Ertl 2017].
    /// The paper explains that conventional inclusion-exclusion can be quite
    /// inaccurate, especially for small Jaccard indices. In that regime the
    /// result can be dominated by cardinality-estimation noise and clamping.
    /// Consequently, `0.0` is not proof that the sets are disjoint, a positive
    /// result is not proof of overlap, and
    /// [`Self::expected_relative_error`] is not a Jaccard error bound.
    ///
    /// Prefer [`crate::minhash::MinHash`] for similarity-centric workloads. If
    /// the inputs must remain HLL sketches, Ertl's joint maximum-likelihood
    /// estimator is the substantially more accurate alternative.
    ///
    /// [Ertl 2017]: https://arxiv.org/pdf/1702.01284
    ///
    /// # Example
    /// ```rust
    /// use sketches::hyperloglog::HyperLogLog;
    ///
    /// // Construct two sets with a known overlap:
    /// // A = [0, 10_000), B = [5_000, 15_000)
    /// // exact |A ∩ B| = 5_000, exact |A ∪ B| = 15_000.
    /// let mut a = HyperLogLog::new(14).unwrap();
    /// let mut b = HyperLogLog::new(14).unwrap();
    ///
    /// for value in 0_u64..10_000 {
    ///     a.add(&value);
    /// }
    /// for value in 5_000_u64..15_000 {
    ///     b.add(&value);
    /// }
    ///
    /// // Exact Jaccard is 5_000 / 15_000 = 0.333...
    /// let jaccard = a.jaccard_index(&b).unwrap();
    /// assert!(jaccard > 0.25 && jaccard < 0.42);
    /// ```
    ///
    /// # Errors
    /// Returns [`SketchError::IncompatibleSketches`] when precision differs.
    pub fn jaccard_index(&self, other: &Self) -> Result<f64, SketchError> {
        let union = self.union_estimate(other)?;
        let a = self.estimate();
        let b = other.estimate();
        Ok(inclusion_exclusion_estimates(a, b, union).jaccard)
    }

    /// Returns the rank of the first set bit in the suffix (1-indexed).
    fn rank(hash: u64, precision: u8) -> u8 {
        let suffix = hash << precision;
        let max_rank = 64 - precision as u32 + 1;
        let rank = suffix.leading_zeros() + 1;
        rank.min(max_rank) as u8
    }

    /// Implements the maximum-likelihood cardinality estimator from Algorithm 8
    /// of Ertl's "New cardinality estimation algorithms for HyperLogLog sketches".
    /// `counts` is the multiplicity vector `C[0..=q+1]` from the paper.
    fn maximum_likelihood_estimate(counts: &[usize], register_count: usize) -> f64 {
        debug_assert_eq!(counts.iter().sum::<usize>(), register_count);
        let q = counts.len() - 2;
        if counts[q + 1] == register_count {
            return f64::INFINITY;
        }

        let k_min = counts.iter().position(|&count| count != 0).unwrap();
        let k_min_prime = k_min.max(1);
        let k_max = counts.iter().rposition(|&count| count != 0).unwrap();
        let k_max_prime = k_max.min(q);

        let mut z = 0.0;
        if k_min_prime <= k_max_prime {
            for &count in counts[k_min_prime..=k_max_prime].iter().rev() {
                z = 0.5 * z + count as f64;
            }
        }
        z *= 2_f64.powi(-(k_min_prime as i32));

        let mut c_prime = counts[q + 1];
        if q >= 1 {
            c_prime += counts[k_max_prime];
        }

        let a = z + counts[0] as f64;
        let b = z + (counts[q + 1] as f64) * 2_f64.powi(-(q as i32));
        let nonzero_registers = (register_count - counts[0]) as f64;

        let mut x = if b <= 1.5 * a {
            nonzero_registers / (0.5 * b + a)
        } else {
            (nonzero_registers / b) * (b / a).ln_1p()
        };

        let relative_error_limit = MAX_LIKELIHOOD_EPSILON / (register_count as f64).sqrt();
        let mut delta_x = x;
        let mut g_previous = 0.0;

        while delta_x > x * relative_error_limit {
            let kappa_minus_one = Self::frexp_exponent(x);
            let scale_exponent = (k_max_prime as i32 + 1).max(kappa_minus_one + 2);
            let mut x_prime = x * 2_f64.powi(-scale_exponent);
            let x_prime_squared = x_prime * x_prime;
            let mut h = x_prime - x_prime_squared / 3.0
                + (x_prime_squared * x_prime_squared) * (1.0 / 45.0 - x_prime_squared / 472.5);

            for _ in (k_max_prime as i32)..=kappa_minus_one {
                let one_minus_h = 1.0 - h;
                h = (x_prime + h * one_minus_h) / (x_prime + one_minus_h);
                x_prime *= 2.0;
            }

            let mut g = c_prime as f64 * h;
            for k in (k_min_prime..k_max_prime).rev() {
                let one_minus_h = 1.0 - h;
                h = (x_prime + h * one_minus_h) / (x_prime + one_minus_h);
                x_prime *= 2.0;
                g += counts[k] as f64 * h;
            }
            g += x * a;

            if g > g_previous && g <= nonzero_registers {
                delta_x *= (nonzero_registers - g) / (g - g_previous);
            } else {
                delta_x = 0.0;
            }
            x += delta_x;
            g_previous = g;
        }

        x * register_count as f64
    }

    /// Returns the exponent produced by `frexp(x)` for positive finite `x`.
    fn frexp_exponent(x: f64) -> i32 {
        debug_assert!(x.is_finite() && x > 0.0);

        let bits = x.to_bits();
        let biased_exponent = ((bits >> 52) & 0x7ff) as i32;
        if biased_exponent != 0 {
            biased_exponent - 1022
        } else {
            let mantissa = bits & ((1_u64 << 52) - 1);
            let highest_bit = 63 - mantissa.leading_zeros() as i32;
            highest_bit - 1073
        }
    }
}

impl JacardIndex for HyperLogLog {
    fn jaccard_index(&self, other: &Self) -> Result<f64, SketchError> {
        HyperLogLog::jaccard_index(self, other)
    }
}

#[cfg(test)]
mod tests {
    use super::HyperLogLog;

    fn assert_relative_eq(actual: f64, expected: f64, tolerance: f64) {
        let scale = expected.abs().max(1.0);
        assert!(
            (actual - expected).abs() <= tolerance * scale,
            "actual={actual:.17} expected={expected:.17} tolerance={tolerance}"
        );
    }

    #[test]
    fn precision_range_is_enforced() {
        assert!(HyperLogLog::new(3).is_err());
        assert!(HyperLogLog::new(4).is_ok());
        assert!(HyperLogLog::new(18).is_ok());
        assert!(HyperLogLog::new(19).is_err());
    }

    #[test]
    fn error_rate_constructor_validates_input() {
        assert!(HyperLogLog::with_error_rate(0.0).is_err());
        assert!(HyperLogLog::with_error_rate(1.0).is_err());
        assert!(HyperLogLog::with_error_rate(f64::NAN).is_err());
        assert!(HyperLogLog::with_error_rate(0.05).is_ok());
    }

    #[test]
    fn error_rate_constructor_selects_smallest_precision_that_meets_target() {
        for target in [0.9, 0.05, 0.01, 0.005] {
            let hll = HyperLogLog::with_error_rate(target).unwrap();
            assert!(hll.expected_relative_error() <= target);

            if hll.precision() > super::MIN_PRECISION {
                let smaller = HyperLogLog::new(hll.precision() - 1).unwrap();
                assert!(smaller.expected_relative_error() > target);
            }
        }
    }

    #[test]
    fn error_rate_constructor_enforces_supported_boundary() {
        let minimum_supported = HyperLogLog::new(super::MAX_PRECISION)
            .unwrap()
            .expected_relative_error();
        let immediately_smaller = f64::from_bits(minimum_supported.to_bits() - 1);

        let boundary = HyperLogLog::with_error_rate(minimum_supported).unwrap();
        assert_eq!(boundary.precision(), super::MAX_PRECISION);
        assert!(HyperLogLog::with_error_rate(immediately_smaller).is_err());
        assert!(HyperLogLog::with_error_rate(0.001).is_err());
        assert!(HyperLogLog::with_error_rate(0.000001).is_err());

        let largest_valid_target = f64::from_bits(1.0_f64.to_bits() - 1);
        let loosest = HyperLogLog::with_error_rate(largest_valid_target).unwrap();
        assert_eq!(loosest.precision(), super::MIN_PRECISION);
        assert!(loosest.expected_relative_error() <= largest_valid_target);
    }

    #[test]
    fn empty_sketch_estimates_zero() {
        let hll = HyperLogLog::new(12).unwrap();
        assert!(hll.is_empty());
        assert_eq!(hll.count(), 0);
    }

    #[test]
    fn maximum_likelihood_estimator_matches_ertl_reference_results() {
        // These multiplicity vectors use p=8, q=56. Expected values were
        // generated by the MaxLikelihoodEstimator in the reference code
        // accompanying Ertl 2017, Algorithm 8.
        let mut mixed = [0_usize; 58];
        mixed[0] = 80;
        mixed[1] = 70;
        mixed[2] = 50;
        mixed[3] = 30;
        mixed[4] = 20;
        mixed[5] = 6;
        assert_relative_eq(
            HyperLogLog::maximum_likelihood_estimate(&mixed, 256),
            286.866_625_986_763_15,
            1e-13,
        );

        let mut no_zero = [0_usize; 58];
        no_zero[1] = 5;
        no_zero[2] = 40;
        no_zero[3] = 70;
        no_zero[4] = 80;
        no_zero[5] = 50;
        no_zero[8] = 11;
        assert_relative_eq(
            HyperLogLog::maximum_likelihood_estimate(&no_zero, 256),
            1_675.894_405_487_860_2,
            1e-13,
        );

        let mut high_and_saturated = [0_usize; 58];
        high_and_saturated[40] = 5;
        high_and_saturated[45] = 40;
        high_and_saturated[50] = 70;
        high_and_saturated[55] = 80;
        high_and_saturated[56] = 50;
        high_and_saturated[57] = 11;
        assert_relative_eq(
            HyperLogLog::maximum_likelihood_estimate(&high_and_saturated, 256),
            10_290_268_119_670_374.0,
            1e-13,
        );
    }

    #[test]
    fn maximum_likelihood_estimator_handles_boundary_states() {
        let mut empty = [0_usize; 58];
        empty[0] = 256;
        assert_eq!(HyperLogLog::maximum_likelihood_estimate(&empty, 256), 0.0);

        let mut saturated = [0_usize; 58];
        saturated[57] = 256;
        assert!(HyperLogLog::maximum_likelihood_estimate(&saturated, 256).is_infinite());
    }

    #[test]
    fn maximum_likelihood_estimator_avoids_the_old_transition_bias_spike() {
        let precision = 12;
        let register_count = 1_u64 << precision;
        let exact = register_count * 5 / 2;
        let trials = 64_u64;
        let mut relative_error_sum = 0.0;

        for trial in 0..trials {
            let base = (trial << 32) ^ (u64::from(precision) << 56) ^ (5 << 24) ^ 2;
            let mut hll = HyperLogLog::new(precision).unwrap();
            for value in 0..exact {
                hll.add(&crate::splitmix64(base + value));
            }
            relative_error_sum += hll.estimate() / exact as f64 - 1.0;
        }

        let mean_relative_bias = relative_error_sum / trials as f64;
        assert!(
            mean_relative_bias.abs() < 0.01,
            "mean_relative_bias={mean_relative_bias}"
        );
    }

    #[test]
    fn duplicate_insertions_do_not_explode_cardinality() {
        let mut hll = HyperLogLog::new(12).unwrap();
        for _ in 0..1_000 {
            hll.add(&"same-key");
        }
        assert!(hll.count() <= 3);
    }

    #[test]
    fn estimate_is_reasonable_for_medium_cardinality() {
        let mut hll = HyperLogLog::new(12).unwrap();
        let exact = 10_000_u64;

        for value in 0..exact {
            hll.add(&value);
        }

        let estimate = hll.count();
        let relative_error = (estimate as f64 - exact as f64).abs() / exact as f64;
        assert!(
            relative_error <= 0.10,
            "estimate={estimate} exact={exact} rel_error={relative_error}"
        );
    }

    #[test]
    fn merge_combines_observations() {
        let mut left = HyperLogLog::new(12).unwrap();
        let mut right = HyperLogLog::new(12).unwrap();

        for value in 0_u64..7_500 {
            left.add(&value);
        }
        for value in 7_500_u64..15_000 {
            right.add(&value);
        }

        left.merge(&right).unwrap();
        let estimate = left.count();
        let exact = 15_000_u64;
        let relative_error = (estimate as f64 - exact as f64).abs() / exact as f64;
        assert!(
            relative_error <= 0.12,
            "estimate={estimate} exact={exact} rel_error={relative_error}"
        );
    }

    #[test]
    fn merge_rejects_mismatched_precision() {
        let mut left = HyperLogLog::new(10).unwrap();
        let right = HyperLogLog::new(11).unwrap();
        assert!(left.merge(&right).is_err());
    }

    #[test]
    fn jaccard_estimate_is_reasonable_for_partial_overlap() {
        let mut left = HyperLogLog::new(14).unwrap();
        let mut right = HyperLogLog::new(14).unwrap();

        for value in 0_u64..10_000 {
            left.add(&value);
        }
        for value in 5_000_u64..15_000 {
            right.add(&value);
        }

        let estimate = left.jaccard_index(&right).unwrap();
        let exact = 5_000.0 / 15_000.0;
        assert!(
            (estimate - exact).abs() <= 0.12,
            "estimate={estimate} exact={exact}"
        );
    }

    #[test]
    fn empty_sketches_have_jaccard_one() {
        let left = HyperLogLog::new(12).unwrap();
        let right = HyperLogLog::new(12).unwrap();
        assert_eq!(left.jaccard_index(&right).unwrap(), 1.0);
    }

    #[test]
    fn set_relation_helpers_reject_mismatched_precision() {
        let left = HyperLogLog::new(10).unwrap();
        let right = HyperLogLog::new(11).unwrap();
        assert!(left.union_estimate(&right).is_err());
        assert!(left.intersection_estimate(&right).is_err());
        assert!(left.jaccard_index(&right).is_err());
    }

    #[test]
    fn clear_removes_state() {
        let mut hll = HyperLogLog::new(12).unwrap();
        for value in 0..500_u64 {
            hll.add(&value);
        }
        assert!(hll.count() > 0);
        hll.clear();
        assert_eq!(hll.count(), 0);
        assert!(hll.is_empty());
    }

    #[test]
    fn expected_error_matches_register_count() {
        let hll = HyperLogLog::new(10).unwrap();
        let expected = 1.04 / (hll.register_count() as f64).sqrt();
        assert!((hll.expected_relative_error() - expected).abs() < 1e-12);
    }
}
