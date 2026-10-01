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

//! Online mean and covariance using a multivariate extension of Welford's algorithm.
//!
//! This keeps streaming moments without sketch approximation in `O(d²)` space
//! for `d` dimensions. Means and moments use ordinary `f64` arithmetic. Diagonal
//! moments remain nonnegative while the calculations remain finite; underflow
//! can round a positive variance to zero. Rounding does not guarantee that the
//! full covariance matrix is positive semidefinite. Very large finite values can
//! overflow intermediate calculations, eventually producing infinity or NaN.
//!
//! Covariance updates use the old-mean deviations on both sides of a symmetric
//! rank-one correction, independently of the rounded updated means. Addition is
//! the singleton case of the pairwise covariance formula in Philippe Pébay's
//! [SAND2008-6212, equations (3.1) and (3.12)](https://digital.library.unt.edu/ark:/67531/metadc837537/m2/1/high_res_d/1028931.pdf#page=13).

use crate::SketchError;

/// Streaming mean, per-coordinate variance, and full covariance matrix.
///
/// # Example
/// ```rust
/// use sketches::vector_welford::VectorWelford;
///
/// let mut stats = VectorWelford::new(2).unwrap();
/// stats.add(&[1.0, 2.0]).unwrap();
/// stats.add(&[3.0, 4.0]).unwrap();
/// assert_eq!(stats.mean(), Some(&[2.0, 3.0][..]));
/// assert_eq!(stats.sample_covariance().unwrap(), vec![vec![2.0, 2.0], vec![2.0, 2.0]]);
/// ```
#[derive(Debug, Clone)]
pub struct VectorWelford {
    /// Fixed coordinate count shared by the mean and each matrix row.
    dimension: usize,
    /// Exact observation count, checked before any update mutates the state.
    count: u64,
    /// Owned coordinate means; zeroed and unavailable when the count is zero.
    mean: Vec<f64>,
    /// Owned row-major centered cross-product sums with mirrored triangles.
    /// Finite diagonal entries are nonnegative; full matrix PSD is not promised.
    m2: Vec<f64>,
}

impl VectorWelford {
    /// Creates an empty accumulator for vectors with `dimension` coordinates.
    ///
    /// Returns an error if the dimension is zero, the covariance matrix
    /// cannot be indexed with `usize`, or storage cannot be allocated.
    pub fn new(dimension: usize) -> Result<Self, SketchError> {
        let cells = dimension
            .checked_mul(dimension)
            .filter(|_| dimension > 0)
            .ok_or(SketchError::InvalidParameter(
                "dimension must be positive and its square must fit usize",
            ))?;
        let mut mean = Vec::new();
        mean.try_reserve_exact(dimension)
            .map_err(|_| SketchError::InvalidParameter("mean vector is too large to allocate"))?;
        mean.resize(dimension, 0.0);
        let mut m2 = Vec::new();
        m2.try_reserve_exact(cells).map_err(|_| {
            SketchError::InvalidParameter("covariance matrix is too large to allocate")
        })?;
        m2.resize(cells, 0.0);
        Ok(Self {
            dimension,
            count: 0,
            mean,
            m2,
        })
    }

    /// Returns the number of coordinates in each observation.
    pub fn dimension(&self) -> usize {
        self.dimension
    }

    /// Returns the number of observations.
    pub fn count(&self) -> u64 {
        self.count
    }

    /// Returns whether no observations have been added.
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// Returns the coordinate means, or `None` for an empty stream.
    pub fn mean(&self) -> Option<&[f64]> {
        (self.count > 0).then_some(&self.mean)
    }

    /// Adds one finite vector. A rejected observation leaves the state intact.
    ///
    /// Uses `delta = value - old_mean` and adds
    /// `(old_count / new_count) * delta * deltaᵀ` to the centered moment matrix.
    /// The means are updated separately, so mean rounding does not select one
    /// coordinate's residual for a covariance correction. Finite input alone
    /// does not guarantee finite intermediate calculations.
    pub fn add(&mut self, value: &[f64]) -> Result<(), SketchError> {
        if value.len() != self.dimension {
            return Err(SketchError::InvalidParameter("vector dimension must match"));
        }
        if value.iter().any(|coordinate| !coordinate.is_finite()) {
            return Err(SketchError::InvalidParameter(
                "vector coordinates must be finite",
            ));
        }
        let next_count = self
            .count
            .checked_add(1)
            .ok_or(SketchError::ObservationCountOverflow)?;
        if self.count == 0 {
            self.mean.copy_from_slice(value);
            self.count = next_count;
            return Ok(());
        }

        let delta: Vec<_> = value
            .iter()
            .zip(&self.mean)
            .map(|(x, mean)| x - mean)
            .collect();
        let n = next_count as f64;
        let correction = self.count as f64 / n;
        for (mean, difference) in self.mean.iter_mut().zip(&delta) {
            *mean += difference / n;
        }
        for i in 0..self.dimension {
            for j in i..self.dimension {
                let cross_product = weighted_cross_product(delta[i], delta[j], correction);
                self.m2[i * self.dimension + j] += cross_product;
                if i != j {
                    self.m2[j * self.dimension + i] = self.m2[i * self.dimension + j];
                }
            }
        }
        self.count = next_count;
        Ok(())
    }

    /// Combines independent batches with the same vector dimension.
    /// A failed merge leaves the receiver unchanged.
    ///
    /// Adds both centered moment matrices and the symmetric correction
    /// `(left_count * right_count / total_count) * delta * deltaᵀ`, where
    /// `delta` is the difference between the original means. The same weighted
    /// product ordering is used by [`Self::add`]; arbitrary batch partitions can
    /// still differ through ordinary floating-point rounding.
    pub fn merge(&mut self, other: &Self) -> Result<(), SketchError> {
        if self.dimension != other.dimension {
            return Err(SketchError::IncompatibleSketches(
                "vector dimensions must match for merge",
            ));
        }
        let total = self
            .count
            .checked_add(other.count)
            .ok_or(SketchError::ObservationCountOverflow)?;
        if other.is_empty() {
            return Ok(());
        }
        if self.is_empty() {
            self.mean.clone_from(&other.mean);
            self.m2.clone_from(&other.m2);
            self.count = other.count;
            return Ok(());
        }

        let delta: Vec<_> = other
            .mean
            .iter()
            .zip(&self.mean)
            .map(|(b, a)| b - a)
            .collect();
        let weight = other.count as f64 / total as f64;
        let correction = (self.count as f64 * other.count as f64) / total as f64;
        for (mean, difference) in self.mean.iter_mut().zip(&delta) {
            *mean += difference * weight;
        }
        for i in 0..self.dimension {
            for j in i..self.dimension {
                let index = i * self.dimension + j;
                self.m2[index] +=
                    other.m2[index] + weighted_cross_product(delta[i], delta[j], correction);
                if i != j {
                    self.m2[j * self.dimension + i] = self.m2[index];
                }
            }
        }
        self.count = total;
        Ok(())
    }

    /// Returns variances divided by `n`, or `None` for an empty stream.
    pub fn population_variance(&self) -> Option<Vec<f64>> {
        self.variance(self.count)
    }

    /// Returns unbiased sample variances divided by `n - 1`, or `None` if `n < 2`.
    pub fn sample_variance(&self) -> Option<Vec<f64>> {
        self.variance(self.count.saturating_sub(1))
    }

    /// Returns covariance divided by `n`, or `None` for an empty stream.
    pub fn population_covariance(&self) -> Option<Vec<Vec<f64>>> {
        self.covariance(self.count)
    }

    /// Returns covariance divided by `n - 1`, or `None` if `n < 2`.
    pub fn sample_covariance(&self) -> Option<Vec<Vec<f64>>> {
        self.covariance(self.count.saturating_sub(1))
    }

    /// Removes all observations while retaining the vector dimension.
    pub fn clear(&mut self) {
        self.count = 0;
        self.mean.fill(0.0);
        self.m2.fill(0.0);
    }

    fn variance(&self, denominator: u64) -> Option<Vec<f64>> {
        (denominator > 0).then(|| {
            (0..self.dimension)
                .map(|i| self.m2[i * self.dimension + i] / denominator as f64)
                .collect()
        })
    }

    fn covariance(&self, denominator: u64) -> Option<Vec<Vec<f64>>> {
        (denominator > 0).then(|| {
            self.m2
                .chunks_exact(self.dimension)
                .map(|row| row.iter().map(|value| value / denominator as f64).collect())
                .collect()
        })
    }
}

/// Evaluates a positively weighted cross product with coordinate-independent order.
///
/// Callers supply a positive finite weight derived from nonempty batch counts.
/// A reducing weight scales the larger-magnitude operand first to avoid raw
/// product overflow without prematurely underflowing the smaller operand.
/// An increasing weight scales the smaller operand first to protect tiny raw
/// products from premature underflow and avoid unnecessarily growing the larger
/// operand. Equal-magnitude operands are interchangeable for finite products.
/// This preserves nonnegative diagonal corrections, but does not eliminate
/// rounding or overflow, including nonfinite deviations supplied by callers.
fn weighted_cross_product(left: f64, right: f64, weight: f64) -> f64 {
    let (larger, smaller) = if left.abs() >= right.abs() {
        (left, right)
    } else {
        (right, left)
    };
    if weight <= 1.0 {
        (larger * weight) * smaller
    } else {
        larger * (smaller * weight)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn near_constant_pair_matches_singleton_merge_and_coordinate_permutation() {
        let e = f64::EPSILON;
        let expected = vec![vec![0.5, e / 2.0], vec![e / 2.0, e * e / 2.0]];
        let mut direct = VectorWelford::new(2).unwrap();
        let mut swapped = VectorWelford::new(2).unwrap();
        let mut merged = VectorWelford::new(2).unwrap();
        for value in [[0.0, 1.0], [1.0, 1.0 + e]] {
            direct.add(&value).unwrap();
            swapped.add(&[value[1], value[0]]).unwrap();
            let mut singleton = VectorWelford::new(2).unwrap();
            singleton.add(&value).unwrap();
            merged.merge(&singleton).unwrap();
        }

        let covariance = direct.sample_covariance().unwrap();
        assert_eq!(covariance, expected);
        assert_eq!(merged.sample_covariance().unwrap(), expected);
        let permuted = swapped.sample_covariance().unwrap();
        for i in 0..2 {
            for j in 0..2 {
                assert_eq!(permuted[1 - i][1 - j], expected[i][j]);
            }
        }
        let correlation = covariance[0][1] / (covariance[0][0] * covariance[1][1]).sqrt();
        assert_eq!(correlation, 1.0);
    }

    #[test]
    fn large_pair_correction_remains_finite_in_add_and_merge() {
        let difference = 1.5e154_f64;
        // Squaring first overflows, although the weighted centered moment fits.
        assert!(!(difference * difference).is_finite());
        let expected = (difference * 0.5) * difference;
        assert!(expected.is_finite());

        let mut direct = VectorWelford::new(1).unwrap();
        direct.add(&[0.0]).unwrap();
        direct.add(&[difference]).unwrap();
        let mut merged = VectorWelford::new(1).unwrap();
        merged.add(&[0.0]).unwrap();
        let mut singleton = VectorWelford::new(1).unwrap();
        singleton.add(&[difference]).unwrap();
        merged.merge(&singleton).unwrap();

        assert_eq!(direct.sample_variance().unwrap(), vec![expected]);
        assert_eq!(merged.sample_variance().unwrap(), vec![expected]);
        assert_eq!(merged.mean(), direct.mean());
    }

    #[test]
    fn dyadic_pairs_preserve_covariance_across_scales_signs_and_permutations() {
        let permutations = [
            [0, 1, 2],
            [0, 2, 1],
            [1, 0, 2],
            [1, 2, 0],
            [2, 0, 1],
            [2, 1, 0],
        ];
        for first_exponent in [-300, 0, 300] {
            for second_exponent in [-300, 0, 300] {
                let first_scale = 2.0_f64.powi(first_exponent);
                let second_scale = 2.0_f64.powi(second_exponent);
                for first_origin in [0.0, 2.0] {
                    for second_origin in [1.0_f64, 7.0] {
                        let step = f64::from_bits(second_origin.to_bits() + 1) - second_origin;
                        for first_sign in [-1.0, 1.0] {
                            for second_sign in [-1.0, 1.0] {
                                let first = [
                                    first_origin * first_scale,
                                    second_origin * second_scale,
                                    -4.0,
                                ];
                                let second = [
                                    (first_origin + first_sign) * first_scale,
                                    (second_origin + second_sign * step) * second_scale,
                                    -4.0,
                                ];
                                // All deviations and products are dyadic and exactly
                                // representable here. The closed-form two-point
                                // sample covariance is delta_i * delta_j / 2.
                                let differences = [
                                    first_sign * first_scale,
                                    second_sign * step * second_scale,
                                    0.0,
                                ];
                                for permutation in permutations {
                                    let a = permutation.map(|i| first[i]);
                                    let b = permutation.map(|i| second[i]);
                                    let mut direct = VectorWelford::new(3).unwrap();
                                    direct.add(&a).unwrap();
                                    direct.add(&b).unwrap();
                                    let mut merged = VectorWelford::new(3).unwrap();
                                    merged.add(&a).unwrap();
                                    let mut singleton = VectorWelford::new(3).unwrap();
                                    singleton.add(&b).unwrap();
                                    merged.merge(&singleton).unwrap();
                                    let sample = direct.sample_covariance().unwrap();
                                    let population = direct.population_covariance().unwrap();
                                    for i in 0..3 {
                                        for j in 0..3 {
                                            let expected = differences[permutation[i]]
                                                * differences[permutation[j]]
                                                / 2.0;
                                            assert_eq!(sample[i][j], expected);
                                            assert_eq!(population[i][j], expected / 2.0);
                                        }
                                    }
                                    assert_eq!(merged.sample_covariance().unwrap(), sample);
                                    assert_eq!(merged.mean(), direct.mean());
                                    assert_eq!(
                                        direct.sample_variance().unwrap(),
                                        (0..3).map(|i| sample[i][i]).collect::<Vec<_>>()
                                    );
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn batch_partitions_match_an_independent_pairwise_reference() {
        let offsets = [
            [-5.0, -3.0],
            [-2.0, 4.0],
            [0.0, 1.0],
            [1.0, -2.0],
            [4.0, 6.0],
            [8.0, 5.0],
        ];
        for (first_exponent, second_exponent) in [(0, 0), (200, -200), (-200, 200)] {
            let first_scale = 2.0_f64.powi(first_exponent);
            let second_scale = 2.0_f64.powi(second_exponent);
            let data =
                offsets.map(|[a, b]| [(32.0 + a) * first_scale, (7.0 + b) * second_scale, -3.0]);
            // Sum over unordered observation pairs, divided by n(n-1),
            // independently obtains sample covariance without rounded means
            // or the streaming/merge recurrence. These dyadic products and
            // their sums are exact before the final division.
            let mut expected = [[0.0; 3]; 3];
            for a in 0..data.len() {
                for b in (a + 1)..data.len() {
                    for i in 0..3 {
                        for j in 0..3 {
                            expected[i][j] += (data[a][i] - data[b][i]) * (data[a][j] - data[b][j]);
                        }
                    }
                }
            }
            for row in &mut expected {
                for entry in row {
                    *entry /= (data.len() * (data.len() - 1)) as f64;
                }
            }
            let mut direct = VectorWelford::new(3).unwrap();
            for value in &data {
                direct.add(value).unwrap();
            }
            assert_covariance_close(&direct, &expected);
            // Include empty batches, unequal weights and both receiver orders.
            for split in 0..=data.len() {
                let mut left = VectorWelford::new(3).unwrap();
                let mut right = VectorWelford::new(3).unwrap();
                for value in &data[..split] {
                    left.add(value).unwrap();
                }
                for value in &data[split..] {
                    right.add(value).unwrap();
                }
                let mut reverse = right.clone();
                reverse.merge(&left).unwrap();
                left.merge(&right).unwrap();
                assert_eq!(left.count(), data.len() as u64);
                assert_covariance_close(&left, &expected);
                assert_covariance_close(&reverse, &expected);
            }
        }
    }

    /// Checks finite moments against an independent reference with relative
    /// tolerance, retaining sensitivity to very small nonzero covariances.
    fn assert_covariance_close(stats: &VectorWelford, expected: &[[f64; 3]; 3]) {
        let actual = stats.sample_covariance().unwrap();
        let variances = stats.sample_variance().unwrap();
        for i in 0..3 {
            assert!(variances[i].is_finite() && variances[i] >= 0.0);
            assert_eq!(variances[i], actual[i][i]);
            for j in 0..3 {
                assert!(actual[i][j].is_finite());
                assert_eq!(actual[i][j], actual[j][i]);
                let tolerance = 64.0 * f64::EPSILON * expected[i][j].abs();
                assert!(
                    (actual[i][j] - expected[i][j]).abs() <= tolerance,
                    "covariance[{i}][{j}]: actual {}, expected {}",
                    actual[i][j],
                    expected[i][j]
                );
            }
        }
    }

    #[test]
    fn finite_variances_remain_nonnegative_including_zero_and_underflow() {
        let data = [
            [-0.0, 0.0, -3.0],
            [0.0, 1e-200, 1.0],
            [-0.0, -1e-200, 2.0],
            [0.0, 2e-200, -1.0],
        ];
        let mut direct = VectorWelford::new(3).unwrap();
        let mut merged = VectorWelford::new(3).unwrap();
        for value in data {
            direct.add(&value).unwrap();
            let mut singleton = VectorWelford::new(3).unwrap();
            singleton.add(&value).unwrap();
            merged.merge(&singleton).unwrap();
            for stats in [&direct, &merged] {
                let population = stats.population_variance().unwrap();
                assert_eq!(population[0], 0.0); // Constant coordinate.
                assert_eq!(population[1], 0.0); // Squared deviations underflow.
                assert!(population.iter().all(|v| v.is_finite() && *v >= 0.0));
                if let Some(sample) = stats.sample_variance() {
                    assert!(sample.iter().all(|v| v.is_finite() && *v >= 0.0));
                }
            }
        }
    }

    #[test]
    fn weighted_products_preserve_range_and_sign_at_scaling_boundaries() {
        let cases = [
            (2.0_f64.powi(600), 2.0_f64.powi(-600), 0.5, 0.5),
            (
                2.0_f64.powi(550),
                2.0_f64.powi(474),
                0.5,
                2.0_f64.powi(1023),
            ),
            (
                2.0_f64.powi(-600),
                2.0_f64.powi(-475),
                2.0,
                f64::from_bits(1),
            ),
            (
                2.0_f64.powi(600),
                2.0_f64.powi(-550),
                2.0_f64.powi(50),
                2.0_f64.powi(100),
            ),
            (
                2.0_f64.powi(-300),
                2.0_f64.powi(-300),
                1.0,
                2.0_f64.powi(-600),
            ),
        ];
        for (left, right, weight, expected) in cases {
            for left_sign in [-1.0, 1.0] {
                for right_sign in [-1.0, 1.0] {
                    let a = left * left_sign;
                    let b = right * right_sign;
                    let product = weighted_cross_product(a, b, weight);
                    assert_eq!(product, expected * left_sign * right_sign);
                    assert_eq!(
                        product.to_bits(),
                        weighted_cross_product(b, a, weight).to_bits()
                    );
                }
            }
        }
    }

    #[test]
    fn moments_match_a_known_two_dimensional_dataset() {
        let mut stats = VectorWelford::new(2).unwrap();
        assert!(stats.is_empty());
        assert_eq!(stats.mean(), None);
        assert_eq!(stats.population_covariance(), None);
        assert_eq!(stats.sample_variance(), None);

        for observation in [[1.0, 2.0], [2.0, 4.0], [3.0, 6.0]] {
            stats.add(&observation).unwrap();
        }
        assert_eq!(stats.count(), 3);
        assert_eq!(stats.mean(), Some(&[2.0, 4.0][..]));
        assert_eq!(
            stats.population_variance(),
            Some(vec![2.0 / 3.0, 8.0 / 3.0])
        );
        assert_eq!(stats.sample_variance(), Some(vec![1.0, 4.0]));
        assert_eq!(
            stats.sample_covariance(),
            Some(vec![vec![1.0, 2.0], vec![2.0, 4.0]])
        );
        assert_eq!(
            stats.population_covariance(),
            Some(vec![vec![2.0 / 3.0, 4.0 / 3.0], vec![4.0 / 3.0, 8.0 / 3.0]])
        );
    }

    #[test]
    fn singleton_and_rejected_values_preserve_state() {
        let mut stats = VectorWelford::new(2).unwrap();
        stats.add(&[5.0, -2.0]).unwrap();
        assert_eq!(stats.population_variance(), Some(vec![0.0, 0.0]));
        assert_eq!(stats.sample_covariance(), None);
        stats.add(&[7.0, -4.0]).unwrap();
        let before = stats.clone();
        for rejected in [
            vec![1.0],
            vec![f64::NAN, 3.0],
            vec![3.0, f64::INFINITY],
            vec![f64::NEG_INFINITY, 3.0],
        ] {
            assert!(stats.add(&rejected).is_err());
            assert_eq!(stats.count, before.count);
            assert_eq!(stats.mean, before.mean);
            assert_eq!(stats.m2, before.m2);
        }
        assert!(VectorWelford::new(0).is_err());
        assert!(VectorWelford::new(usize::MAX).is_err());
    }

    #[test]
    fn merged_batches_match_one_pass_and_clear_resets() {
        let data = [[2.0, 10.0], [4.0, 8.0], [6.0, 6.0], [8.0, 4.0]];
        let mut direct = VectorWelford::new(2).unwrap();
        let mut left = VectorWelford::new(2).unwrap();
        let mut right = VectorWelford::new(2).unwrap();
        for (index, value) in data.iter().enumerate() {
            direct.add(value).unwrap();
            if index < 2 {
                left.add(value).unwrap()
            } else {
                right.add(value).unwrap()
            }
        }
        left.merge(&right).unwrap();
        assert_eq!(left.mean(), direct.mean());
        assert_eq!(left.sample_covariance(), direct.sample_covariance());

        let before = left.clone();
        assert!(left.merge(&VectorWelford::new(3).unwrap()).is_err());
        assert_eq!(left.count(), before.count());
        assert_eq!(left.mean(), before.mean());
        assert_eq!(left.m2, before.m2);

        left.clear();
        assert_eq!(left.dimension(), 2);
        assert!(left.is_empty());
        assert_eq!(left.mean(), None);
        assert_eq!(left.population_variance(), None);
        left.merge(&right).unwrap();
        assert_eq!(left.mean(), right.mean());
    }

    #[test]
    fn count_overflow_does_not_change_state() {
        let mut stats = VectorWelford::new(2).unwrap();
        stats.add(&[0.0, 1.0]).unwrap();
        stats.add(&[1.0, 1.0 + f64::EPSILON]).unwrap();
        stats.count = u64::MAX;
        let before = stats.clone();
        assert_eq!(
            stats.add(&[4.0, -2.0]),
            Err(SketchError::ObservationCountOverflow)
        );
        assert_eq!(stats.mean, before.mean);
        assert_eq!(stats.m2, before.m2);
        assert_eq!(stats.count, before.count);

        let mut other = VectorWelford::new(2).unwrap();
        other.add(&[5.0, -2.0]).unwrap();
        assert_eq!(
            stats.merge(&other),
            Err(SketchError::ObservationCountOverflow)
        );
        assert_eq!(stats.mean, before.mean);
        assert_eq!(stats.m2, before.m2);
        assert_eq!(stats.count, before.count);
    }
}
