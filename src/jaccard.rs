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
//! Jaccard similarity trait shared by sketch implementations.
//!
//! # Cardinality-sketch caveat
//!
//! [`crate::hyperloglog::HyperLogLog`] and
//! [`crate::ultraloglog::UltraLogLog`] implement this trait using cardinality
//! estimates and the inclusion-exclusion identity. That is useful when only
//! cardinality-sketch state is available, but it has a materially weaker
//! accuracy profile than MinHash similarity: inclusion-exclusion can be quite
//! inaccurate when the true Jaccard index is small. Clamping the result to
//! `[0, 1]` does not correct this statistical error. See the extensive warnings
//! on each implementation and [Ertl 2017].
//! These relations require finite cardinalities for both inputs and their
//! union. A nonfinite required estimate returns [`SketchError::EstimateUnavailable`],
//! including saturated self-comparisons and comparisons with an empty sketch.
//!
//! [Ertl 2017]: https://arxiv.org/pdf/1702.01284

use crate::SketchError;

/// Successful result of a finite-cardinality inclusion-exclusion calculation.
///
/// Keeping both outputs together ensures that cardinality-based sketches use
/// exactly the same clamping and empty-union convention. Nonfinite required
/// cardinalities are rejected before either result is constructed.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct InclusionExclusionEstimates {
    /// Intersection estimate clamped to the feasible cardinality range.
    pub(crate) intersection: f64,
    /// Jaccard estimate clamped to `[0, 1]`.
    pub(crate) jaccard: f64,
}

/// Derives intersection and Jaccard estimates from three cardinality estimates.
///
/// This helper centralizes mechanics only; it does not make inclusion-exclusion
/// statistically reliable for small intersections. The two-empty-set convention
/// is Jaccard `1.0`. Callers supply nonnegative cardinality estimates.
///
/// # Errors
/// Returns [`SketchError::EstimateUnavailable`] if any required cardinality is
/// nonfinite, before arithmetic or empty-union handling. Valid saturation is
/// therefore reported as an unavailable relation instead of a clamped zero.
pub(crate) fn inclusion_exclusion_estimates(
    left: f64,
    right: f64,
    union: f64,
) -> Result<InclusionExclusionEstimates, SketchError> {
    if !left.is_finite() || !right.is_finite() || !union.is_finite() {
        return Err(SketchError::EstimateUnavailable);
    }

    // Estimator noise can produce a negative intersection or one larger than
    // either input, so restrict the subtraction to its mathematically feasible
    // interval.
    let intersection = (left + right - union).max(0.0).min(left.min(right));

    // Jaccard of two empty sets is one by convention; every nonempty union uses
    // the ordinary intersection-to-union ratio.
    let jaccard = if union == 0.0 {
        1.0
    } else {
        (intersection / union).clamp(0.0, 1.0)
    };

    Ok(InclusionExclusionEstimates {
        intersection,
        jaccard,
    })
}

/// Common API for sketches that can estimate Jaccard similarity.
///
/// The returned value is expected to be in `[0, 1]`:
/// - `0.0` means disjoint sets,
/// - `1.0` means identical sets.
///
/// These values describe the exact Jaccard scale, not a guarantee that every
/// approximate implementation can reliably prove disjointness or identity.
/// In particular, consult the implementation-specific limitations before using
/// approximate zero or near-zero results as classification thresholds.
/// Cardinality-based implementations can return
/// [`SketchError::EstimateUnavailable`] for valid saturated states.
///
/// # Example
/// ```rust
/// use sketches::SketchError;
/// use sketches::jaccard::JaccardIndex;
/// use sketches::minhash::MinHash;
///
/// fn compare<S: JaccardIndex>(left: &S, right: &S) -> Result<f64, SketchError> {
///     left.jaccard_index(right)
/// }
///
/// let mut left = MinHash::new(128).unwrap();
/// let mut right = MinHash::new(128).unwrap();
/// for value in 0_u64..5_000 {
///     left.add(&value);
/// }
/// for value in 2_500_u64..7_500 {
///     right.add(&value);
/// }
///
/// let similarity = compare(&left, &right).unwrap();
/// assert!(similarity > 0.20 && similarity < 0.60);
/// ```
pub trait JaccardIndex {
    /// Returns the estimated Jaccard index `|A ∩ B| / |A ∪ B|`.
    ///
    /// # Errors
    /// Implementations return [`SketchError::IncompatibleSketches`] when two
    /// sketches are not compatible for comparison.
    /// Cardinality-based implementations return [`SketchError::EstimateUnavailable`]
    /// when a required cardinality is nonfinite. See implementation-specific
    /// documentation for supported estimate boundaries.
    fn jaccard_index(&self, other: &Self) -> Result<f64, SketchError>;
}

#[cfg(test)]
mod tests {
    use crate::{
        hyperloglog::HyperLogLog,
        jaccard::{JaccardIndex, inclusion_exclusion_estimates},
        minhash::MinHash,
        ultraloglog::UltraLogLog,
    };

    // Verifies the shared helper's empty-set convention and both feasibility
    // clamps independently of a particular sketch implementation.
    #[test]
    fn inclusion_exclusion_helper_clamps_noisy_estimates() {
        let empty = inclusion_exclusion_estimates(0.0, 0.0, 0.0).unwrap();
        assert_eq!(empty.intersection, 0.0);
        assert_eq!(empty.jaccard, 1.0);

        let negative = inclusion_exclusion_estimates(100.0, 100.0, 250.0).unwrap();
        assert_eq!(negative.intersection, 0.0);
        assert_eq!(negative.jaccard, 0.0);

        let oversized = inclusion_exclusion_estimates(40.0, 60.0, 20.0).unwrap();
        assert_eq!(oversized.intersection, 40.0);
        assert_eq!(oversized.jaccard, 1.0);
    }

    #[test]
    fn saturated_ultraloglog_self_comparison_reports_unavailable() {
        let saturated = UltraLogLog::from_state(vec![255; 8]).unwrap();
        assert!(saturated.estimate().is_infinite());
        let before = saturated.clone();
        assert_eq!(
            saturated.intersection_estimate(&saturated),
            Err(crate::SketchError::EstimateUnavailable)
        );
        assert_eq!(
            saturated.jaccard_index(&saturated),
            Err(crate::SketchError::EstimateUnavailable)
        );
        assert_eq!(
            JaccardIndex::jaccard_index(&saturated, &saturated),
            Err(crate::SketchError::EstimateUnavailable)
        );
        assert_eq!(saturated, before);
    }

    #[test]
    fn inclusion_exclusion_rejects_each_nonfinite_cardinality() {
        for unavailable in [f64::INFINITY, f64::NEG_INFINITY, f64::NAN] {
            for estimates in [
                [unavailable, 3.0, 4.0],
                [2.0, unavailable, 4.0],
                [2.0, 3.0, unavailable],
            ] {
                assert_eq!(
                    inclusion_exclusion_estimates(estimates[0], estimates[1], estimates[2]),
                    Err(crate::SketchError::EstimateUnavailable),
                    "estimates={estimates:?}"
                );
            }
        }
    }

    #[test]
    fn inclusion_exclusion_checks_all_nonfinite_class_combinations() {
        // The first three entries are finite. The others exercise both
        // infinities, quiet NaNs with different signs/payloads and a signaling
        // NaN. Classify by fixture index independently of the production guard.
        let values = [
            0.0,
            1.0,
            f64::MAX,
            f64::INFINITY,
            f64::NEG_INFINITY,
            f64::NAN,
            f64::from_bits(0xfff8_0000_0000_0042),
            f64::from_bits(0x7ff0_0000_0000_0001),
        ];
        let mut rejected = 0;
        for (i, &left) in values.iter().enumerate() {
            for (j, &right) in values.iter().enumerate() {
                for (k, &union) in values.iter().enumerate() {
                    let result = inclusion_exclusion_estimates(left, right, union);
                    if i >= 3 || j >= 3 || k >= 3 {
                        assert_eq!(result, Err(crate::SketchError::EstimateUnavailable));
                        rejected += 1;
                    } else {
                        let result = result.unwrap();
                        assert!(result.intersection.is_finite());
                        assert!(0.0 <= result.intersection);
                        assert!(result.intersection <= left.min(right));
                        assert!(result.jaccard.is_finite());
                        assert!((0.0..=1.0).contains(&result.jaccard));
                    }
                }
            }
        }
        assert_eq!(rejected, 485);
    }

    // Exercises HyperLogLog through the shared trait rather than its inherent
    // method, guarding the trait delegation.
    #[test]
    fn trait_api_works_for_hyperloglog() {
        let mut left = HyperLogLog::new(12).unwrap();
        let mut right = HyperLogLog::new(12).unwrap();
        for value in 0_u64..5_000 {
            left.add(&value);
        }
        for value in 2_500_u64..7_500 {
            right.add(&value);
        }

        let similarity = JaccardIndex::jaccard_index(&left, &right).unwrap();
        assert!(similarity > 0.20 && similarity < 0.60);
    }

    // Exercises UltraLogLog through the same trait API used by the other set
    // sketches.
    #[test]
    fn trait_api_works_for_ultraloglog() {
        let mut left = UltraLogLog::new(12).unwrap();
        let mut right = UltraLogLog::new(12).unwrap();
        for value in 0_u64..5_000 {
            left.add(&value);
        }
        for value in 2_500_u64..7_500 {
            right.add(&value);
        }

        let similarity = JaccardIndex::jaccard_index(&left, &right).unwrap();
        assert!(similarity > 0.20 && similarity < 0.60);
    }

    // Retains coverage for MinHash's direct similarity estimator alongside the
    // inclusion-exclusion implementations.
    #[test]
    fn trait_api_works_for_minhash() {
        let mut left = MinHash::new(128).unwrap();
        let mut right = MinHash::new(128).unwrap();
        for value in 0_u64..5_000 {
            left.add(&value);
        }
        for value in 2_500_u64..7_500 {
            right.add(&value);
        }

        let similarity = JaccardIndex::jaccard_index(&left, &right).unwrap();
        assert!(similarity > 0.20 && similarity < 0.60);
    }
}
