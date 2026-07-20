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
//! UltraLogLog approximate distinct counter.
//!
//! This is an independent implementation of the mergeable sketch described in
//! [Ertl 2024]. UltraLogLog retains the largest suffix observation and flags
//! for its two immediately preceding ranks. Its one-byte registers therefore
//! contain more cardinality information than HyperLogLog registers.
//!
//! [`UltraLogLog::estimate`] uses the paper's optimal further-generalized
//! remaining-area (FGRA) estimator. It has asymptotic relative standard error
//! `0.78224 / sqrt(m)` and is the fast default used by the reference
//! implementation. [`UltraLogLog::estimate_mle`] provides the more expensive
//! bias-reduced maximum-likelihood estimate with asymptotic relative standard
//! error `0.76086 / sqrt(m)`.
//!
//! The internal register encoding follows the precision-independent byte
//! mapping used by Hash4j's production implementation. Raw-hash updates and
//! estimator results are cross-checked against that implementation in this
//! module's tests.
//!
//! # Intersection and Jaccard limitations
//!
//! UltraLogLog natively supports union. [`UltraLogLog::intersection_estimate`]
//! and [`UltraLogLog::jaccard_index`] derive other set relations through
//! inclusion-exclusion. [Ertl 2017] shows why this subtraction can be very
//! inaccurate when the true Jaccard index is small. UltraLogLog's lower
//! cardinality variance helps, but it does not eliminate that fundamental
//! instability. Clamping prevents impossible values; it does not make zero a
//! proof of disjointness or make a positive result proof of overlap. Prefer
//! [`crate::minhash::MinHash`] when similarity is the primary workload.
//!
//! [Ertl 2024]: https://arxiv.org/abs/2308.16862
//! [Ertl 2017]: https://arxiv.org/pdf/1702.01284
//! [Hash4j]: https://github.com/dynatrace-oss/hash4j

use std::hash::Hash;
use std::sync::OnceLock;

use crate::jacard::{InclusionExclusionEstimates, JacardIndex, inclusion_exclusion_estimates};
use crate::{SketchError, seeded_hash64};

/// Smallest precision supported by the byte encoding and merge bit tricks.
const MIN_PRECISION: u8 = 3;
/// Largest precision supported while retaining enough suffix bits in `u64`.
const MAX_PRECISION: u8 = 26;
/// Fixed seed separating UltraLogLog's item hashes from the other sketches.
const HASH_SEED: u64 = 0xA076_1D64_78BD_642F;

/// Optimal FGRA power parameter from Ertl's numerical minimization.
const FGRA_TAU: f64 = 0.819_491_137_591_089_7;
/// Asymptotic variance factor of the optimal FGRA estimator.
const FGRA_VARIANCE_FACTOR: f64 = 0.611_893_149_697_843_7;
/// Four register-state weights used by the optimal FGRA estimator.
const FGRA_ETA: [f64; 4] = [
    4.663_135_422_063_788,
    2.137_850_213_795_852_4,
    2.781_144_650_979_996,
    0.982_408_254_515_371_5,
];
/// Number of nonsaturated, nonsmall encoded values needing table entries.
const FGRA_REGISTER_CONTRIBUTION_COUNT: usize = 236;

/// Inverse square root of the per-register Fisher information for ULL.
const MLE_RELATIVE_ERROR_FACTOR: f64 = 0.760_862_100_272_518_2;
/// First-order finite-register bias correction for the MLE.
const MLE_BIAS_CORRECTION: f64 = 0.481_473_765_277_200_65;
/// Solver tolerance set to 0.1% of the MLE's theoretical sampling error.
const MLE_SOLVER_EPSILON: f64 = 0.001 * MLE_RELATIVE_ERROR_FACTOR;

/// Cardinality estimators supported by [`UltraLogLog`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum UltraLogLogEstimator {
    /// Optimal FGRA estimator: fast and the reference implementation's default.
    #[default]
    OptimalFgra,
    /// Bias-reduced maximum-likelihood estimator: slower but more precise.
    MaximumLikelihood,
}

impl UltraLogLogEstimator {
    /// Returns the estimator's asymptotic error factor before division by
    /// `sqrt(register_count)`.
    fn relative_standard_error_factor(self) -> f64 {
        match self {
            Self::OptimalFgra => FGRA_VARIANCE_FACTOR.sqrt(),
            Self::MaximumLikelihood => MLE_RELATIVE_ERROR_FACTOR,
        }
    }
}

/// Validates that a precision can be represented by this implementation.
fn validate_precision(precision: u8) -> Result<(), SketchError> {
    if !(MIN_PRECISION..=MAX_PRECISION).contains(&precision) {
        return Err(SketchError::InvalidParameter(
            "precision must be in the inclusive range [3, 26]",
        ));
    }
    Ok(())
}

/// Calculates an estimator's asymptotic relative standard error at a precision.
fn relative_standard_error(precision: u8, estimator: UltraLogLogEstimator) -> f64 {
    estimator.relative_standard_error_factor() / ((1_usize << precision) as f64).sqrt()
}

/// Approximate distinct counter using UltraLogLog's one-byte registers.
///
/// # Example
///
/// ```rust
/// use sketches::ultraloglog::UltraLogLog;
///
/// let mut ull = UltraLogLog::new(12).unwrap();
/// for value in 0_u64..10_000 {
///     ull.add(&value);
/// }
///
/// assert!((9_000..11_000).contains(&ull.count()));
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UltraLogLog {
    precision: u8,
    registers: Vec<u8>,
}

impl UltraLogLog {
    /// Creates an empty sketch with `2^precision` one-byte registers.
    ///
    /// Valid precision is `[3, 26]`. Precision 26 allocates 64 MiB, so callers
    /// should normally select the smallest precision that meets their error
    /// requirement.
    ///
    /// # Errors
    ///
    /// Returns [`SketchError::InvalidParameter`] when precision is out of range.
    pub fn new(precision: u8) -> Result<Self, SketchError> {
        validate_precision(precision)?;
        Ok(Self {
            precision,
            registers: vec![0; 1_usize << precision],
        })
    }

    /// Creates a sketch for a target asymptotic relative standard error.
    ///
    /// This selects precision for the default optimal-FGRA estimator. A
    /// standard error is a statistical measure, not a deterministic error
    /// bound for every estimate.
    ///
    /// # Errors
    ///
    /// Returns [`SketchError::InvalidParameter`] for a non-finite target, a
    /// target outside `(0, 1)`, or one that precision 26 cannot meet.
    pub fn with_error_rate(target_relative_error: f64) -> Result<Self, SketchError> {
        Self::with_error_rate_and_estimator(
            target_relative_error,
            UltraLogLogEstimator::OptimalFgra,
        )
    }

    /// Creates a sketch for a target error and a selected estimator.
    ///
    /// # Errors
    ///
    /// Returns [`SketchError::InvalidParameter`] for a non-finite target, a
    /// target outside `(0, 1)`, or one that precision 26 cannot meet.
    pub fn with_error_rate_and_estimator(
        target_relative_error: f64,
        estimator: UltraLogLogEstimator,
    ) -> Result<Self, SketchError> {
        if !target_relative_error.is_finite()
            || target_relative_error <= 0.0
            || target_relative_error >= 1.0
        {
            return Err(SketchError::InvalidParameter(
                "target relative error must be finite and strictly between 0 and 1",
            ));
        }

        // Select the smallest state satisfying the requested statistical error
        // instead of silently overallocating registers.
        let precision = (MIN_PRECISION..=MAX_PRECISION)
            .find(|&p| relative_standard_error(p, estimator) <= target_relative_error)
            .ok_or(SketchError::InvalidParameter(
                "target relative error is below the supported precision range",
            ))?;
        Self::new(precision)
    }

    /// Restores a sketch from its precision-independent register bytes.
    ///
    /// The state length must be a power of two corresponding to precision
    /// `[3, 26]`. Nonzero registers are validated against the encoding's
    /// precision-dependent minimum.
    ///
    /// # Errors
    ///
    /// Returns [`SketchError::InvalidParameter`] for an invalid length or byte.
    pub fn from_state(registers: Vec<u8>) -> Result<Self, SketchError> {
        // State length encodes precision, so it must be an admissible power of
        // two before trailing zeros can be interpreted as `p`.
        if !registers.len().is_power_of_two() {
            return Err(SketchError::InvalidParameter(
                "state length must be a power of two",
            ));
        }

        let precision = registers.len().trailing_zeros() as u8;
        validate_precision(precision)?;
        // The precision-independent mapping reserves low byte values that
        // cannot arise at this precision; rejecting them prevents later table
        // indexing and shift invariants from being violated.
        let minimum_nonzero_register = (precision << 2) - 4;
        if registers
            .iter()
            .any(|&register| register != 0 && register < minimum_nonzero_register)
        {
            return Err(SketchError::InvalidParameter(
                "state contains a register that is invalid for its precision",
            ));
        }

        Ok(Self {
            precision,
            registers,
        })
    }

    /// Returns the precision parameter.
    pub fn precision(&self) -> u8 {
        self.precision
    }

    /// Returns the number of one-byte registers (`2^precision`).
    pub fn register_count(&self) -> usize {
        self.registers.len()
    }

    /// Returns the serialized register state.
    pub fn state(&self) -> &[u8] {
        &self.registers
    }

    /// Consumes the sketch and returns its serialized register state.
    pub fn into_state(self) -> Vec<u8> {
        self.registers
    }

    /// Returns the default estimator's asymptotic relative standard error.
    pub fn expected_relative_error(&self) -> f64 {
        self.expected_relative_error_with(UltraLogLogEstimator::OptimalFgra)
    }

    /// Returns the selected estimator's asymptotic relative standard error.
    ///
    /// Small-cardinality error is typically lower; small precisions can have
    /// empirical root-mean-square error slightly above this asymptotic value.
    pub fn expected_relative_error_with(&self, estimator: UltraLogLogEstimator) -> f64 {
        relative_standard_error(self.precision, estimator)
    }

    /// Returns `true` when the sketch is in its initial empty state.
    pub fn is_empty(&self) -> bool {
        self.registers.iter().all(|&register| register == 0)
    }

    /// Adds an item after hashing it to 64 bits with the crate's fixed seed.
    pub fn add<T: Hash>(&mut self, item: &T) {
        self.add_hash(seeded_hash64(item, HASH_SEED));
    }

    /// Adds an item already represented by a uniformly distributed 64-bit hash.
    ///
    /// Using a weak or correlated hash function invalidates the estimator's
    /// statistical assumptions. This method is also useful for interoperating
    /// with other UltraLogLog implementations.
    pub fn add_hash(&mut self, hash: u64) {
        let precision = u32::from(self.precision);

        // The high `p` bits choose the register. Complementing before and after
        // the shift counts zeros only in the remaining suffix, including the
        // all-zero suffix as the maximum observation.
        let index = (hash >> (u64::BITS - precision)) as usize;
        let suffix_leading_zeros = (!((!hash) << precision)).leading_zeros();
        let observation_bit = suffix_leading_zeros + precision - 1;

        // Expanding, OR-ing, and repacking makes updates commutative and
        // idempotent while retaining the largest rank and two predecessor bits.
        let hash_prefix = Self::unpack(self.registers[index]) | (1_u64 << observation_bit);
        self.registers[index] = Self::pack(hash_prefix);
    }

    /// Estimates cardinality with the optimal FGRA estimator.
    pub fn estimate(&self) -> f64 {
        self.estimate_with(UltraLogLogEstimator::OptimalFgra)
    }

    /// Estimates cardinality with the bias-reduced maximum-likelihood estimator.
    pub fn estimate_mle(&self) -> f64 {
        self.estimate_with(UltraLogLogEstimator::MaximumLikelihood)
    }

    /// Estimates cardinality using the selected estimator.
    pub fn estimate_with(&self, estimator: UltraLogLogEstimator) -> f64 {
        match estimator {
            UltraLogLogEstimator::OptimalFgra => self.estimate_fgra(),
            UltraLogLogEstimator::MaximumLikelihood => self.estimate_maximum_likelihood(),
        }
    }

    /// Returns the default FGRA estimate rounded to `u64`.
    pub fn count(&self) -> u64 {
        self.estimate().round() as u64
    }

    /// Returns the selected estimate rounded to `u64`.
    pub fn count_with(&self, estimator: UltraLogLogEstimator) -> u64 {
        self.estimate_with(estimator).round() as u64
    }

    /// Resets all registers to the empty state.
    pub fn clear(&mut self) {
        self.registers.fill(0);
    }

    /// Returns a copy reduced to at most `precision`.
    ///
    /// Passing a precision at least as large as the current precision returns
    /// an unchanged clone; downsizing never invents precision.
    ///
    /// # Errors
    ///
    /// Returns [`SketchError::InvalidParameter`] when precision is out of range.
    pub fn downsize(&self, precision: u8) -> Result<Self, SketchError> {
        validate_precision(precision)?;
        if precision >= self.precision {
            return Ok(self.clone());
        }

        // Merging into an empty lower-precision target applies the exact
        // reduction rule without maintaining a second implementation.
        let mut result = Self::new(precision)?;
        result.merge(self)?;
        Ok(result)
    }

    /// Merges `other` into this sketch.
    ///
    /// Equal precisions merge by reconstructing and OR-ing the retained hash
    /// prefixes. A higher-precision `other` is reduced exactly while merging.
    /// The receiver is never silently downsized.
    ///
    /// # Errors
    ///
    /// Returns [`SketchError::IncompatibleSketches`] when `other` has lower
    /// precision. Use [`Self::merged`] when the result should automatically use
    /// the smaller of two precisions.
    pub fn merge(&mut self, other: &Self) -> Result<(), SketchError> {
        if other.precision < self.precision {
            return Err(SketchError::IncompatibleSketches(
                "source precision must be at least the receiver precision",
            ));
        }

        if other.precision == self.precision {
            // Equal partitions can combine their retained prefix flags
            // register by register.
            for (left, &right) in self.registers.iter_mut().zip(&other.registers) {
                if right != 0 {
                    *left = Self::pack(Self::unpack(*left) | Self::unpack(right));
                }
            }
            return Ok(());
        }

        // Each lower-precision register corresponds to one contiguous group in
        // the source. Group zero retains its source prefix; nonzero group
        // addresses themselves encode observations after reduction.
        let precision_difference = other.precision - self.precision;
        let group_size = 1_usize << precision_difference;
        let other_precision_minus_one = u32::from(other.precision - 1);

        for (index, register) in self.registers.iter_mut().enumerate() {
            let group_start = index * group_size;
            let mut hash_prefix =
                Self::unpack(*register) | Self::unpack(other.registers[group_start]);

            // A nonempty nonzero subregister contributes the rank implied by
            // the first set bit in its discarded address suffix. Its internal
            // rank is irrelevant because the address difference appears first.
            for offset in 1..group_size {
                if other.registers[group_start + offset] != 0 {
                    let observation_bit =
                        (offset as u64).leading_zeros() + other_precision_minus_one;
                    // The paper/reference formula is expressed with Java's
                    // masked long shifts; preserve its modulo-64 bit index.
                    hash_prefix |= 1_u64 << (observation_bit & 63);
                }
            }

            // Preserve the canonical zero byte for a group with no observations.
            if hash_prefix != 0 {
                *register = Self::pack(hash_prefix);
            }
        }
        Ok(())
    }

    /// Returns a merged sketch at the smaller input precision.
    pub fn merged(&self, other: &Self) -> Self {
        // Cloning the smaller sketch lets the instance merge rule consume the
        // other operand without ever silently reducing the receiver.
        if self.precision <= other.precision {
            let mut result = self.clone();
            result
                .merge(other)
                .expect("higher precision source must be merge-compatible");
            result
        } else {
            let mut result = other.clone();
            result
                .merge(self)
                .expect("higher precision source must be merge-compatible");
            result
        }
    }

    /// Returns the estimated union cardinality at the smaller input precision.
    pub fn union_estimate(&self, other: &Self) -> f64 {
        self.merged(other).estimate()
    }

    /// Returns the estimated intersection cardinality `|A ∩ B|`.
    ///
    /// Both sketches are evaluated at their smaller precision, and the result
    /// is derived by inclusion-exclusion:
    /// `|A ∩ B| ≈ |A| + |B| - |A ∪ B|`.
    ///
    /// # Statistical limitations
    ///
    /// This is not a joint estimator designed specifically for UltraLogLog.
    /// Inclusion-exclusion subtracts three noisy cardinality estimates and can
    /// therefore be highly inaccurate when the true intersection is small
    /// relative to either input. The result is clamped to
    /// `[0, min(|A|, |B|)]`, but clamping does not correct the statistical
    /// error. Zero does not prove disjointness, and a positive estimate does
    /// not prove overlap. See [Ertl 2017].
    ///
    /// [Ertl 2017]: https://arxiv.org/pdf/1702.01284
    pub fn intersection_estimate(&self, other: &Self) -> f64 {
        self.relation_estimates(other).intersection
    }

    /// Returns the estimated Jaccard index `|A ∩ B| / |A ∪ B|`.
    ///
    /// Two empty sketches return `1.0` by convention. Sketches with different
    /// precisions are compared at the smaller precision.
    ///
    /// # Statistical limitations
    ///
    /// This method derives its intersection through inclusion-exclusion. It is
    /// not a published UltraLogLog joint estimator. Although UltraLogLog has
    /// lower single-cardinality variance than HyperLogLog, subtraction remains
    /// unstable for small Jaccard indices. Consequently, `0.0` is not proof of
    /// disjointness, a positive result is not proof of overlap, and
    /// [`Self::expected_relative_error`] is not a Jaccard error bound. Prefer
    /// [`crate::minhash::MinHash`] for similarity-centric workloads. See
    /// [Ertl 2017].
    ///
    /// # Errors
    ///
    /// UltraLogLog states created by this crate are always comparable, so this
    /// implementation currently returns `Ok`. The `Result` preserves the shared
    /// [`JacardIndex`] trait contract.
    ///
    /// [Ertl 2017]: https://arxiv.org/pdf/1702.01284
    pub fn jaccard_index(&self, other: &Self) -> Result<f64, SketchError> {
        Ok(self.relation_estimates(other).jaccard)
    }

    /// Computes shared inclusion-exclusion outputs at a common precision.
    ///
    /// Reducing a higher-precision operand before estimating its cardinality
    /// keeps all three estimates on the same register partition and avoids
    /// combining a high-resolution operand estimate with a lower-resolution
    /// union estimate.
    fn relation_estimates(&self, other: &Self) -> InclusionExclusionEstimates {
        // Reduce the higher-precision side exactly once, then keep the complete
        // relation calculation on that shared partition.
        if self.precision == other.precision {
            Self::relation_estimates_at_common_precision(self, other)
        } else if self.precision < other.precision {
            let reduced_other = other
                .downsize(self.precision)
                .expect("the lower input precision is always a valid reduction target");
            Self::relation_estimates_at_common_precision(self, &reduced_other)
        } else {
            let reduced_self = self
                .downsize(other.precision)
                .expect("the lower input precision is always a valid reduction target");
            Self::relation_estimates_at_common_precision(&reduced_self, other)
        }
    }

    /// Calculates cardinality relations for two already aligned register
    /// partitions.
    fn relation_estimates_at_common_precision(
        left: &Self,
        right: &Self,
    ) -> InclusionExclusionEstimates {
        debug_assert_eq!(left.precision, right.precision);

        // Estimating all three cardinalities from aligned partitions preserves
        // their useful covariance before shared mechanics apply feasibility
        // clamps and the two-empty-set convention.
        let union = left.merged(right).estimate();
        inclusion_exclusion_estimates(left.estimate(), right.estimate(), union)
    }

    /// Expands one encoded register into its retained hash-prefix bits.
    fn unpack(register: u8) -> u64 {
        // Values below eight represent the canonical empty prefix under every
        // supported precision.
        if register < 8 {
            0
        } else {
            let leading_pattern = 4_u64 | u64::from(register & 3);
            leading_pattern << (u32::from(register >> 2) - 2)
        }
    }

    /// Compresses retained hash-prefix bits into one register byte.
    fn pack(hash_prefix: u64) -> u8 {
        if hash_prefix == 0 {
            return 0;
        }

        // The high six bits encode the largest retained rank; the low two bits
        // record whether each of its two predecessor ranks has been observed.
        let leading_zeros_plus_one = hash_prefix.leading_zeros() + 1;
        let recent_bits = hash_prefix.wrapping_shl(leading_zeros_plus_one) >> 62;
        (0_u8.wrapping_sub((leading_zeros_plus_one as u8).wrapping_mul(4))) | recent_bits as u8
    }

    /// Returns the lazily initialized contribution table for ordinary FGRA
    /// register values.
    fn fgra_register_contributions() -> &'static [f64; FGRA_REGISTER_CONTRIBUTION_COUNT] {
        static CONTRIBUTIONS: OnceLock<[f64; FGRA_REGISTER_CONTRIBUTION_COUNT]> = OnceLock::new();
        CONTRIBUTIONS.get_or_init(|| {
            // This closure evaluates the closed-form contribution once per
            // process, keeping every later estimate free of per-register
            // exponentiation.
            let mut contributions = [0.0; FGRA_REGISTER_CONTRIBUTION_COUNT];
            for (register, contribution) in contributions.iter_mut().enumerate() {
                let exponent = ((register + 12) >> 2) as f64;
                *contribution = FGRA_ETA[register & 3] * 2_f64.powf(-FGRA_TAU * exponent);
            }
            contributions
        })
    }

    /// Implements the paper's optimal further-generalized remaining-area
    /// cardinality estimator.
    fn estimate_fgra(&self) -> f64 {
        let register_count = self.register_count() as u64;
        let offset = i32::from(self.precision << 2) + 4;

        let mut small_counts = [0_u64; 4];
        let mut saturated_counts = [0_u64; 4];
        let mut sum = 0.0;
        let contributions = Self::fgra_register_contributions();

        // Classify the 256 possible bytes once. Ordinary registers use a table;
        // boundary registers are deferred to the analytical range corrections.
        for (register, count) in self.register_histogram().into_iter().enumerate() {
            if count == 0 {
                continue;
            }
            let register = register as u8;
            let shifted = i32::from(register) - offset;
            if shifted < 0 {
                match shifted {
                    value if value < -8 => small_counts[0] += count,
                    -8 => small_counts[1] += count,
                    -4 => small_counts[2] += count,
                    -2 => small_counts[3] += count,
                    _ => {}
                }
            } else if register < 252 {
                sum += count as f64 * contributions[shifted as usize];
            } else {
                saturated_counts[(register - 252) as usize] += count;
            }
        }

        // Empty and near-empty register states need a small-range correction
        // derived from the likelihood's lower boundary rather than an empirical
        // cardinality switch.
        if small_counts.iter().any(|&count| count != 0) {
            let z = Self::fgra_small_range_z(small_counts, register_count);
            if small_counts[0] != 0 {
                sum += small_counts[0] as f64 * Self::fgra_sigma(z);
            }
            if small_counts[1] != 0 {
                sum += small_counts[1] as f64
                    * Self::fgra_pow_2_minus_tau()
                    * Self::fgra_eta_x()
                    * Self::fgra_psi_prime(z, z * z);
            }
            if small_counts[2] != 0 {
                sum += small_counts[2] as f64
                    * Self::fgra_pow_4_minus_tau()
                    * (z * (FGRA_ETA[0] - FGRA_ETA[1]) + FGRA_ETA[1]);
            }
            if small_counts[3] != 0 {
                sum += small_counts[3] as f64
                    * Self::fgra_pow_4_minus_tau()
                    * (z * (FGRA_ETA[2] - FGRA_ETA[3]) + FGRA_ETA[3]);
            }
        }

        // Registers at byte values 252..=255 have reached the finite 64-bit
        // hash boundary and require the matching upper-tail correction.
        if saturated_counts.iter().any(|&count| count != 0) {
            sum += Self::fgra_large_range_contribution(
                saturated_counts,
                register_count,
                65 - i32::from(self.precision),
            );
        }

        // Apply the finite-register bias factor before transforming the summed
        // remaining area into a cardinality.
        let m = register_count as f64;
        let bias_correction = 1.0 / (1.0 + FGRA_VARIANCE_FACTOR * (1.0 + FGRA_TAU) / (2.0 * m));
        let estimation_factor = bias_correction * m * m.powf(1.0 / FGRA_TAU);
        estimation_factor * sum.powf(-1.0 / FGRA_TAU)
    }

    /// Returns the alternating combination of eta weights used to normalize
    /// the FGRA correction polynomials.
    fn fgra_eta_x() -> f64 {
        FGRA_ETA[0] - FGRA_ETA[1] - FGRA_ETA[2] + FGRA_ETA[3]
    }

    /// Returns `2^tau`, used by the lower-tail recurrence.
    fn fgra_pow_2_tau() -> f64 {
        2_f64.powf(FGRA_TAU)
    }

    /// Returns `2^-tau`, used by the upper-tail recurrence and scaling.
    fn fgra_pow_2_minus_tau() -> f64 {
        2_f64.powf(-FGRA_TAU)
    }

    /// Returns `4^-tau`, used by small-range state contributions.
    fn fgra_pow_4_minus_tau() -> f64 {
        4_f64.powf(-FGRA_TAU)
    }

    /// Solves the closed-form small-range boundary approximation for `z`.
    fn fgra_small_range_z(counts: [u64; 4], register_count: u64) -> f64 {
        let [c0, c4, c8, c10] = counts;
        let alpha = register_count + 3 * (c0 + c4 + c8 + c10);
        let beta = register_count - c0 - c4;
        let gamma = 4 * c0 + 2 * c4 + 3 * c8 + c10;
        // Solve the quadratic in the fourth root of z, then raise that root
        // back to the fourth power.
        let fourth_root = ((beta as f64).mul_add(beta as f64, 4.0 * alpha as f64 * gamma as f64))
            .sqrt()
            - beta as f64;
        let fourth_root = fourth_root / (2.0 * alpha as f64);
        fourth_root.powi(4)
    }

    /// Solves the closed-form saturation-boundary approximation for `z`.
    fn fgra_large_range_z(counts: [u64; 4], register_count: u64) -> f64 {
        let [c0, c1, c2, c3] = counts;
        let alpha = register_count + 3 * (c0 + c1 + c2 + c3);
        let beta = c0 + c1 + 2 * (c2 + c3);
        let gamma = register_count + 2 * c0 + c2 - c3;
        // The positive quadratic root is square-rooted once more because the
        // boundary equations are expressed in powers of `sqrt(z)`.
        let square = ((beta as f64).mul_add(beta as f64, 4.0 * alpha as f64 * gamma as f64)).sqrt()
            - beta as f64;
        (square / (2.0 * alpha as f64)).sqrt()
    }

    /// Evaluates the paper's normalized `psi` polynomial.
    fn fgra_psi_prime(z: f64, z_squared: f64) -> f64 {
        let eta_x = Self::fgra_eta_x();
        let eta23x = (FGRA_ETA[2] - FGRA_ETA[3]) / eta_x;
        let eta13x = (FGRA_ETA[1] - FGRA_ETA[3]) / eta_x;
        let eta3012xx = (FGRA_ETA[3] * FGRA_ETA[0] - FGRA_ETA[1] * FGRA_ETA[2]) / (eta_x * eta_x);
        (z + eta23x) * (z_squared + eta13x) + eta3012xx
    }

    /// Evaluates the convergent lower-bound correction series `sigma(z)`.
    fn fgra_sigma(z: f64) -> f64 {
        if z <= 0.0 {
            return FGRA_ETA[3];
        }
        if z >= 1.0 {
            return f64::INFINITY;
        }

        let mut power_z = z;
        let mut next_power_z = power_z * power_z;
        let mut sum = 0.0;
        let mut power_tau = Self::fgra_eta_x();
        // Repeated squaring rapidly drives powers of z to zero. Stop when the
        // next positive term can no longer change the floating-point sum.
        loop {
            let old_sum = sum;
            let next_next_power_z = next_power_z * next_power_z;
            sum += power_tau
                * (power_z - next_power_z)
                * Self::fgra_psi_prime(next_power_z, next_next_power_z);
            if sum <= old_sum {
                return sum / z;
            }
            power_z = next_power_z;
            next_power_z = next_next_power_z;
            power_tau *= Self::fgra_pow_2_tau();
        }
    }

    /// Evaluates the convergent upper-bound correction series `phi(z)`.
    fn fgra_phi(z: f64, z_squared: f64) -> f64 {
        if z <= 0.0 {
            return 0.0;
        }
        let power_2_tau = Self::fgra_pow_2_tau();
        if z >= 1.0 {
            return FGRA_ETA[0] / (power_2_tau * (2.0 * power_2_tau - 1.0));
        }

        let power_2_minus_tau = Self::fgra_pow_2_minus_tau();
        let mut previous_power_z = z_squared;
        let mut power_z = z;
        let mut next_power_z = power_z.sqrt();
        let mut p = Self::fgra_eta_x() * (Self::fgra_pow_4_minus_tau() / (2.0 - power_2_minus_tau))
            / (1.0 + next_power_z);
        let mut psi = Self::fgra_psi_prime(power_z, previous_power_z);
        let mut sum = next_power_z * (psi + psi) * p;

        // Repeated square roots approach one while the geometric tau factor
        // shrinks each term. Floating-point stagnation is the convergence test.
        loop {
            previous_power_z = power_z;
            power_z = next_power_z;
            let old_sum = sum;
            next_power_z = power_z.sqrt();
            let next_psi = Self::fgra_psi_prime(power_z, previous_power_z);
            p *= power_2_minus_tau / (1.0 + next_power_z);
            sum += next_power_z * ((next_psi + next_psi) - (power_z + next_power_z) * psi) * p;
            if sum <= old_sum {
                return sum;
            }
            psi = next_psi;
        }
    }

    /// Combines saturated-register counts into the FGRA upper-tail contribution.
    fn fgra_large_range_contribution(counts: [u64; 4], register_count: u64, w: i32) -> f64 {
        let [c0, c1, c2, c3] = counts.map(|count| count as f64);
        let z = Self::fgra_large_range_z(counts, register_count);
        let root_z = z.sqrt();
        let mut sum = Self::fgra_phi(root_z, z) * (c0 + c1 + c2 + c3);
        sum += z
            * (1.0 + root_z)
            * (c0 * FGRA_ETA[0] + c1 * FGRA_ETA[1] + c2 * FGRA_ETA[2] + c3 * FGRA_ETA[3]);

        // Add contributions for the four possible retained predecessor-bit
        // patterns before rescaling by the remaining hash width `w`.
        let power_2_minus_tau = Self::fgra_pow_2_minus_tau();
        sum += root_z
            * ((c0 + c1)
                * (z * power_2_minus_tau * (FGRA_ETA[0] - FGRA_ETA[2])
                    + power_2_minus_tau * FGRA_ETA[2])
                + (c2 + c3)
                    * (z * power_2_minus_tau * (FGRA_ETA[1] - FGRA_ETA[3])
                        + power_2_minus_tau * FGRA_ETA[3]));

        sum * power_2_minus_tau.powi(w) / ((1.0 + root_z) * (1.0 + z))
    }

    /// Implements UltraLogLog's bias-reduced maximum-likelihood estimator.
    fn estimate_maximum_likelihood(&self) -> f64 {
        let mut sum = 0_u64;
        let mut b = [0_u64; 65];

        // Convert byte multiplicities into the sufficient statistics of the
        // Poisson likelihood. Integer accumulation preserves the reference
        // implementation's modulo-2^64 scaling exactly.
        for (register, count) in self.register_histogram().into_iter().enumerate() {
            if count != 0 {
                sum = sum.wrapping_add(Self::mle_contribution(
                    register as u8,
                    count,
                    &mut b,
                    self.precision,
                ));
            }
        }

        // A zero scaled sum has exactly two valid causes: every register is
        // empty or every register is saturated. One byte distinguishes them.
        if sum == 0 {
            return if self.registers[0] == 0 {
                0.0
            } else {
                f64::INFINITY
            };
        }

        // Fold the unreachable terminal bucket into the last observable one,
        // then solve the normalized likelihood equation and undo its scaling.
        let q = 64 - usize::from(self.precision);
        b[q - 1] += b[q];
        let register_count = self.register_count() as f64;
        let factor = 2.0 * register_count;
        let a = sum as f64 * factor * 2_f64.powi(-64);
        let root = Self::solve_maximum_likelihood_equation(
            a,
            &b,
            q - 1,
            MLE_SOLVER_EPSILON / register_count.sqrt(),
        );
        factor * root / (1.0 + MLE_BIAS_CORRECTION / register_count)
    }

    /// Counts all possible byte values in one tight scan of the sketch state.
    fn register_histogram(&self) -> [u64; 256] {
        let mut histogram = [0_u64; 256];
        for &register in &self.registers {
            histogram[register as usize] += 1;
        }
        histogram
    }

    /// Adds one register byte's multiplicity to the MLE sufficient statistics
    /// and returns its scaled alpha contribution.
    fn mle_contribution(register: u8, count: u64, b: &mut [u64; 65], precision: u8) -> u64 {
        let register = i32::from(register);
        let shifted = register - (i32::from(precision) << 2) - 4;
        if shifted < 0 {
            // Small-range encodings contribute to the first two likelihood
            // buckets and to alpha through a precision-scaled integer weight.
            let mut contribution = 4_u64;
            if shifted == -2 || shifted == -8 {
                b[0] += count;
                contribution -= 2;
            }
            if shifted == -2 || shifted == -4 {
                b[1] += count;
                contribution -= 1;
            }
            (contribution << (62 - u32::from(precision))).wrapping_mul(count)
        } else {
            // Ordinary and saturated encodings expose their two predecessor
            // flags as adjacent likelihood buckets plus the mandatory maximum.
            let index = (shifted >> 2) as usize;
            let bit0 = (register & 1) as u64;
            let bit1 = ((register >> 1) & 1) as u64;
            let mut contribution = 0xE000_0000_0000_0000_u64;
            contribution = contribution.wrapping_sub(bit0 << 63);
            contribution = contribution.wrapping_sub(bit1 << 62);
            b[index] += bit0 * count;
            b[index + 1] += bit1 * count;
            b[index + 2] += count;
            (contribution >> (index + usize::from(precision))).wrapping_mul(count)
        }
    }

    /// Solves the concave one-dimensional Poisson likelihood equation using
    /// Ertl's stable recurrence and secant updates.
    fn solve_maximum_likelihood_equation(
        a: f64,
        b: &[u64; 65],
        maximum_index: usize,
        relative_error_limit: f64,
    ) -> f64 {
        if a == 0.0 {
            return f64::INFINITY;
        }

        // Trim zero tails so every subsequent recurrence step covers only the
        // likelihood's nonempty support.
        let Some(maximum_nonzero) = (0..=maximum_index).rev().find(|&index| b[index] != 0) else {
            return 0.0;
        };

        let minimum_nonzero = (0..=maximum_nonzero)
            .find(|&index| b[index] != 0)
            .expect("maximum nonzero index implies a minimum");
        // `sum_counts` and its power-of-two weighted counterpart provide a
        // rigorous positive lower-bound initialization for the root.
        let sum_counts = b[minimum_nonzero..=maximum_nonzero]
            .iter()
            .copied()
            .sum::<u64>() as f64;
        let weighted_sum = b[minimum_nonzero..=maximum_nonzero]
            .iter()
            .enumerate()
            .map(|(offset, &count)| (count as f64) * 2_f64.powi((minimum_nonzero + offset) as i32))
            .sum::<f64>();

        // Choose the algebraically stable form of the lower bound depending on
        // whether log1p would improve the dynamic range.
        let mut x = if weighted_sum <= 1.5 * a {
            sum_counts / (0.5 * weighted_sum + a)
        } else {
            (weighted_sum / a).ln_1p() * (sum_counts / weighted_sum)
        };
        let mut delta_x = x;
        let mut previous_g = 0.0;

        // Each iteration evaluates the monotone score function and advances a
        // one-sided secant step, so x never overshoots the unique root.
        while delta_x > x * relative_error_limit {
            let kappa = Self::floor_binary_exponent(x) + 2;
            let scale = (maximum_nonzero as i32).max(kappa) + 1;
            let mut x_prime = x * 2_f64.powi(-scale);
            let x_prime_squared = x_prime * x_prime;
            let mut h = x_prime
                + x_prime_squared
                    * (-1.0 / 3.0 + x_prime_squared * (1.0 / 45.0 - x_prime_squared / 472.5));

            // Lift the small-argument polynomial to the largest occupied bucket
            // using h(2x)'s stable rational recurrence.
            for _ in maximum_nonzero as i32..kappa {
                let one_minus_h = 1.0 - h;
                h = (x_prime + h * one_minus_h) / (x_prime + one_minus_h);
                x_prime += x_prime;
            }

            // Walk back through every occupied scale while accumulating the
            // likelihood score at the current candidate root.
            let mut g = b[maximum_nonzero] as f64 * h;
            for index in (minimum_nonzero..maximum_nonzero).rev() {
                let one_minus_h = 1.0 - h;
                h = (x_prime + h * one_minus_h) / (x_prime + one_minus_h);
                x_prime += x_prime;
                g += b[index] as f64 * h;
            }
            g += x * a;

            // The score must increase while remaining below its target. If
            // floating-point rounding breaks that invariant, the current x is
            // already as accurate as this representation permits.
            if previous_g < g && g <= sum_counts {
                delta_x *= (g - sum_counts) / (previous_g - g);
            } else {
                delta_x = 0.0;
            }
            x += delta_x;
            previous_g = g;
        }
        x
    }

    /// Returns `floor(log2(value))` without transcendental operations.
    fn floor_binary_exponent(value: f64) -> i32 {
        debug_assert!(value.is_finite() && value > 0.0);
        let bits = value.to_bits();
        let biased_exponent = ((bits >> 52) & 0x7ff) as i32;
        if biased_exponent != 0 {
            // Normal values store the exponent directly with IEEE-754 bias.
            biased_exponent - 1023
        } else {
            // Subnormal values derive the exponent from their highest mantissa
            // bit because their stored exponent field is zero.
            let mantissa = bits & ((1_u64 << 52) - 1);
            63 - mantissa.leading_zeros() as i32 - 1074
        }
    }
}

impl JacardIndex for UltraLogLog {
    /// Delegates the shared trait API to UltraLogLog's documented inherent
    /// inclusion-exclusion implementation.
    fn jaccard_index(&self, other: &Self) -> Result<f64, SketchError> {
        UltraLogLog::jaccard_index(self, other)
    }
}

#[cfg(test)]
mod tests {
    use super::{UltraLogLog, UltraLogLogEstimator};

    /// Asserts a scale-aware floating-point tolerance and reports full values
    /// on failure.
    fn assert_relative_eq(actual: f64, expected: f64, tolerance: f64) {
        let scale = expected.abs().max(1.0);
        assert!(
            (actual - expected).abs() <= tolerance * scale,
            "actual={actual:.17} expected={expected:.17} tolerance={tolerance}"
        );
    }

    /// Produces the deterministic raw hashes used by Hash4j cross-check vectors
    /// and merge property tests.
    fn reference_hashes(count: usize) -> impl Iterator<Item = u64> {
        (0..count).map(|index| crate::splitmix64(0x0123_4567_89AB_CDEF + index as u64))
    }

    // Covers both constructor bounds and minimal precision selection for an
    // error-rate request.
    #[test]
    fn precision_and_error_rate_are_validated() {
        assert!(UltraLogLog::new(2).is_err());
        assert!(UltraLogLog::new(3).is_ok());
        assert!(UltraLogLog::new(18).is_ok());
        assert!(UltraLogLog::new(27).is_err());

        assert!(UltraLogLog::with_error_rate(0.0).is_err());
        assert!(UltraLogLog::with_error_rate(1.0).is_err());
        assert!(UltraLogLog::with_error_rate(f64::NAN).is_err());

        let sketch = UltraLogLog::with_error_rate(0.01).unwrap();
        assert!(sketch.expected_relative_error() <= 0.01);
        let smaller = UltraLogLog::new(sketch.precision() - 1).unwrap();
        assert!(smaller.expected_relative_error() > 0.01);
    }

    // Cross-checks the byte mapping's important boundaries and round-trip
    // invariant against the reference implementation.
    #[test]
    fn packing_matches_reference_vectors() {
        let vectors = [
            (0_u64, 0_u8),
            (4, 8),
            (5, 9),
            (6, 10),
            (7, 11),
            (8, 12),
            (9, 12),
            (10, 13),
            (11, 13),
            (12, 14),
            (1_u64 << 11, 44),
            (1_u64 << 12, 48),
            ((1_u64 << 11) | (1_u64 << 12), 50),
            (u64::MAX, 255),
        ];
        for (prefix, expected) in vectors {
            assert_eq!(UltraLogLog::pack(prefix), expected);
        }

        assert_eq!(UltraLogLog::unpack(0), 0);
        assert_eq!(UltraLogLog::unpack(8), 4);
        assert_eq!(UltraLogLog::unpack(13), 10);
        assert_eq!(UltraLogLog::unpack(44), 1_u64 << 11);
        assert_eq!(UltraLogLog::unpack(255), 0xE000_0000_0000_0000);

        for register in 8_u8..=u8::MAX {
            assert_eq!(UltraLogLog::pack(UltraLogLog::unpack(register)), register);
        }
    }

    // Checks complete states and both estimator outputs generated by Hash4j
    // 0.30.0 for deterministic raw hashes.
    #[test]
    fn state_and_estimators_match_hash4j_known_answers() {
        let cases: &[(u8, usize, &[u8], f64, f64)] = &[
            (
                3,
                20,
                &[0x0e, 0x08, 0x15, 0x11, 0x14, 0x13, 0x0e, 0x00],
                19.767_093_769_354_43,
                20.790_678_259_670_717,
            ),
            (
                6,
                1_000,
                &[
                    0x23, 0x25, 0x21, 0x2c, 0x28, 0x1f, 0x22, 0x25, 0x27, 0x2b, 0x31, 0x27, 0x23,
                    0x30, 0x23, 0x2c, 0x2c, 0x27, 0x23, 0x21, 0x1f, 0x29, 0x2a, 0x23, 0x2b, 0x24,
                    0x23, 0x2c, 0x1f, 0x21, 0x23, 0x25, 0x23, 0x27, 0x31, 0x2c, 0x25, 0x2c, 0x30,
                    0x2c, 0x29, 0x27, 0x26, 0x38, 0x2e, 0x44, 0x25, 0x23, 0x2d, 0x28, 0x2f, 0x25,
                    0x30, 0x2f, 0x28, 0x23, 0x27, 0x23, 0x30, 0x2a, 0x27, 0x21, 0x38, 0x25,
                ],
                1_053.027_533_050_589_7,
                1_039.531_620_024_757_6,
            ),
        ];

        for &(precision, count, expected_state, expected_fgra, expected_mle) in cases {
            let mut sketch = UltraLogLog::new(precision).unwrap();
            for hash in reference_hashes(count) {
                sketch.add_hash(hash);
            }
            assert_eq!(sketch.state(), expected_state);
            assert_relative_eq(sketch.estimate(), expected_fgra, 2e-15);
            assert_relative_eq(sketch.estimate_mle(), expected_mle, 2e-15);
        }
    }

    // Verifies initial-state detection, both zero estimates, and clear reuse.
    #[test]
    fn empty_and_clear_have_zero_estimates() {
        let mut sketch = UltraLogLog::new(8).unwrap();
        assert!(sketch.is_empty());
        assert_eq!(sketch.estimate(), 0.0);
        assert_eq!(sketch.estimate_mle(), 0.0);

        for hash in reference_hashes(1_000) {
            sketch.add_hash(hash);
        }
        assert!(!sketch.is_empty());
        sketch.clear();
        assert!(sketch.is_empty());
        assert_eq!(sketch.count(), 0);
    }

    // Exercises the opposite state boundary, including saturating float-to-u64
    // count conversion.
    #[test]
    fn saturated_state_has_infinite_estimates() {
        let sketch = UltraLogLog::from_state(vec![u8::MAX; 1 << 3]).unwrap();
        assert!(sketch.estimate().is_infinite());
        assert!(sketch.estimate_mle().is_infinite());
        assert_eq!(sketch.count(), u64::MAX);
    }

    // Ensures repeated insertion of one raw hash cannot mutate state after the
    // first observation.
    #[test]
    fn duplicates_are_idempotent() {
        let mut sketch = UltraLogLog::new(10).unwrap();
        sketch.add_hash(0x1234_5678_9ABC_DEF0);
        let state = sketch.state().to_vec();
        for _ in 0..100 {
            sketch.add_hash(0x1234_5678_9ABC_DEF0);
        }
        assert_eq!(sketch.state(), state);
    }

    // Confirms equal-precision register merging is identical to ingesting the
    // combined stream directly.
    #[test]
    fn same_precision_merge_matches_direct_ingestion() {
        let mut left = UltraLogLog::new(10).unwrap();
        let mut right = UltraLogLog::new(10).unwrap();
        let mut direct = UltraLogLog::new(10).unwrap();

        for (index, hash) in reference_hashes(20_000).enumerate() {
            if index % 2 == 0 {
                left.add_hash(hash);
            } else {
                right.add_hash(hash);
            }
            direct.add_hash(hash);
        }

        left.merge(&right).unwrap();
        assert_eq!(left, direct);
    }

    // Confirms exact reduction while merging and the receiver-precision safety
    // check in the unsupported direction.
    #[test]
    fn mixed_precision_merge_and_downsize_match_direct_ingestion() {
        let mut high = UltraLogLog::new(12).unwrap();
        let mut low = UltraLogLog::new(8).unwrap();
        let mut direct = UltraLogLog::new(8).unwrap();

        for (index, hash) in reference_hashes(20_000).enumerate() {
            if index < 10_000 {
                high.add_hash(hash);
            } else {
                low.add_hash(hash);
            }
            direct.add_hash(hash);
        }

        low.merge(&high).unwrap();
        assert_eq!(low, direct);
        assert_eq!(high.downsize(8).unwrap().precision(), 8);
        assert!(high.merge(&direct).is_err());
    }

    // Guards the symmetric convenience merge and its documented minimum-
    // precision result.
    #[test]
    fn merged_is_commutative_across_precision() {
        let mut low = UltraLogLog::new(8).unwrap();
        let mut high = UltraLogLog::new(11).unwrap();
        for (index, hash) in reference_hashes(10_000).enumerate() {
            if index % 2 == 0 {
                low.add_hash(hash);
            } else {
                high.add_hash(hash);
            }
        }

        assert_eq!(low.merged(&high), high.merged(&low));
        assert_eq!(low.merged(&high).precision(), 8);
    }

    // Verifies empty-set Jaccard conventions and the exact containment result
    // when only one input has observations.
    #[test]
    fn jaccard_handles_empty_and_identical_sketches() {
        let empty = UltraLogLog::new(12).unwrap();
        assert_eq!(empty.jaccard_index(&empty).unwrap(), 1.0);
        assert_eq!(empty.intersection_estimate(&empty), 0.0);

        let mut populated = UltraLogLog::new(12).unwrap();
        for value in 0_u64..10_000 {
            populated.add(&value);
        }
        assert_eq!(empty.jaccard_index(&populated).unwrap(), 0.0);
        assert_eq!(empty.intersection_estimate(&populated), 0.0);
        assert_eq!(populated.jaccard_index(&populated).unwrap(), 1.0);
    }

    // Checks inclusion-exclusion against a moderate, known overlap where the
    // estimator is expected to be useful.
    #[test]
    fn jaccard_and_intersection_are_reasonable_for_partial_overlap() {
        let mut left = UltraLogLog::new(14).unwrap();
        let mut right = UltraLogLog::new(14).unwrap();
        for value in 0_u64..10_000 {
            left.add(&value);
        }
        for value in 5_000_u64..15_000 {
            right.add(&value);
        }

        let intersection = left.intersection_estimate(&right);
        let jaccard = left.jaccard_index(&right).unwrap();
        assert!((4_000.0..6_000.0).contains(&intersection));
        assert!((0.25..0.42).contains(&jaccard));
    }

    // Ensures comparisons reduce both operands to a common precision and stay
    // symmetric when their order is reversed.
    #[test]
    fn jaccard_supports_mixed_precision() {
        let mut low = UltraLogLog::new(10).unwrap();
        let mut high = UltraLogLog::new(12).unwrap();
        for value in 0_u64..10_000 {
            low.add(&value);
        }
        for value in 5_000_u64..15_000 {
            high.add(&value);
        }

        let forward = low.jaccard_index(&high).unwrap();
        let reverse = high.jaccard_index(&low).unwrap();
        assert_eq!(forward, reverse);
        assert!((0.15..0.55).contains(&forward));
    }

    // Covers state serialization round-tripping and rejects malformed length
    // and register encodings.
    #[test]
    fn state_roundtrip_validates_encoding() {
        let mut sketch = UltraLogLog::new(6).unwrap();
        for hash in reference_hashes(1_000) {
            sketch.add_hash(hash);
        }
        assert_eq!(
            UltraLogLog::from_state(sketch.clone().into_state()).unwrap(),
            sketch
        );

        assert!(UltraLogLog::from_state(vec![0; 7]).is_err());
        let mut invalid = vec![0; 1 << 6];
        invalid[0] = 1;
        assert!(UltraLogLog::from_state(invalid).is_err());
    }

    // Pins the theoretical asymptotic error constants for both estimators.
    #[test]
    fn estimator_error_constants_match_the_paper() {
        let sketch = UltraLogLog::new(12).unwrap();
        assert_relative_eq(
            sketch.expected_relative_error(),
            0.012_222_437_400_144_462,
            1e-15,
        );
        assert_relative_eq(
            sketch.expected_relative_error_with(UltraLogLogEstimator::MaximumLikelihood),
            0.011_888_470_316_758_097,
            1e-15,
        );
    }

    // Provides a broad end-to-end accuracy smoke test through the item-hashing
    // API rather than raw reference hashes.
    #[test]
    fn estimate_is_reasonable_for_medium_cardinality() {
        let mut sketch = UltraLogLog::new(12).unwrap();
        for value in 0_u64..100_000 {
            sketch.add(&value);
        }
        assert!((95_000.0..105_000.0).contains(&sketch.estimate()));
        assert!((95_000.0..105_000.0).contains(&sketch.estimate_mle()));
    }
}
