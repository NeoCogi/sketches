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
//! The implementation follows the classic HyperLogLog estimator with:
//! - small-range linear counting correction,
//! - large-range correction for 64-bit hash space.

use std::hash::Hash;

use crate::{SketchError, seeded_hash64};
use crate::jacard::JacardIndex;

const MIN_PRECISION: u8 = 4;
const MAX_PRECISION: u8 = 18;
const HASH_SEED: u64 = 0xD6E8_FD93_5E7A_4A6D;

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

    /// Creates a HyperLogLog from a target relative error.
    ///
    /// The target must be in `(0, 1)`. Internally this computes:
    /// `p = ceil(log2((1.04 / error)^2))`, clamped to `[4, 18]`.
    ///
    /// # Errors
    /// Returns [`SketchError::InvalidParameter`] when `relative_error` is invalid.
    pub fn with_error_rate(relative_error: f64) -> Result<Self, SketchError> {
        if !relative_error.is_finite() || relative_error <= 0.0 || relative_error >= 1.0 {
            return Err(SketchError::InvalidParameter(
                "relative_error must be finite and strictly between 0 and 1",
            ));
        }

        let required_registers = (1.04 / relative_error).powi(2);
        let raw_precision = required_registers.log2().ceil() as u8;
        let precision = raw_precision.clamp(MIN_PRECISION, MAX_PRECISION);
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

    /// Returns the theoretical relative error: `1.04 / sqrt(m)`.
    pub fn expected_relative_error(&self) -> f64 {
        1.04 / (self.register_count() as f64).sqrt()
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
    pub fn estimate(&self) -> f64 {
        if self.is_empty() {
            return 0.0;
        }

        let m = self.register_count() as f64;
        let alpha = Self::alpha(self.register_count());
        let harmonic_sum = self
            .registers
            .iter()
            .map(|&register| 2_f64.powi(-(register as i32)))
            .sum::<f64>();

        let raw_estimate = alpha * m * m / harmonic_sum;
        let zero_registers = self
            .registers
            .iter()
            .filter(|&&register| register == 0)
            .count() as f64;

        // Small-range correction (linear counting).
        let corrected_small = if raw_estimate <= 2.5 * m && zero_registers > 0.0 {
            m * (m / zero_registers).ln()
        } else {
            raw_estimate
        };

        // Large-range correction in 64-bit hash space.
        let two_to_64 = (u64::MAX as f64) + 1.0;
        if corrected_small > two_to_64 / 30.0 {
            let ratio = (corrected_small / two_to_64).min(1.0 - f64::EPSILON);
            -two_to_64 * (1.0 - ratio).ln()
        } else {
            corrected_small
        }
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

        // Estimator variance can make inclusion-exclusion slightly negative.
        let intersection = (a + b - union).max(0.0).min(a.min(b));
        Ok(intersection)
    }

    /// Returns the estimated Jaccard index `|A ∩ B| / |A ∪ B|`.
    ///
    /// Jaccard index is:
    /// - `0.0` when two sets are disjoint,
    /// - `1.0` when two sets are identical.
    ///
    /// For two empty sets, this method returns `1.0` by convention.
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
        if union == 0.0 {
            return Ok(1.0);
        }

        let intersection = self.intersection_estimate(other)?;
        Ok((intersection / union).clamp(0.0, 1.0))
    }

    /// Returns the rank of the first set bit in the suffix (1-indexed).
    fn rank(hash: u64, precision: u8) -> u8 {
        let suffix = hash << precision;
        let max_rank = 64 - precision as u32 + 1;
        let rank = suffix.leading_zeros() + 1;
        rank.min(max_rank) as u8
    }

    /// Returns the bias-correction constant for register count `m`.
    fn alpha(m: usize) -> f64 {
        match m {
            16 => 0.673,
            32 => 0.697,
            64 => 0.709,
            _ => 0.7213 / (1.0 + 1.079 / m as f64),
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
    fn empty_sketch_estimates_zero() {
        let hll = HyperLogLog::new(12).unwrap();
        assert!(hll.is_empty());
        assert_eq!(hll.count(), 0);
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
