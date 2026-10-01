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

//! Online mean and covariance of fixed-dimension vectors using Welford's algorithm.
//!
//! This keeps exact streaming moments in `O(d²)` space for `d` dimensions.
//! The estimates are subject only to floating-point rounding, unlike the
//! approximate sketches elsewhere in this crate. Very large finite values can
//! overflow `f64` intermediate calculations.

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
    dimension: usize,
    count: u64,
    mean: Vec<f64>,
    // Row-major sum of centered cross products.
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
        for (mean, difference) in self.mean.iter_mut().zip(&delta) {
            *mean += difference / n;
        }
        for i in 0..self.dimension {
            for j in i..self.dimension {
                let cross_product = delta[i] * (value[j] - self.mean[j]);
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
                self.m2[index] += other.m2[index] + delta[i] * delta[j] * correction;
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

#[cfg(test)]
mod tests {
    use super::*;

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
        assert!(stats.add(&[1.0]).is_err());
        assert!(stats.add(&[f64::NAN, 3.0]).is_err());
        assert!(stats.add(&[3.0, f64::INFINITY]).is_err());
        assert_eq!(stats.count(), 1);
        assert_eq!(stats.mean(), Some(&[5.0, -2.0][..]));
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
        let mut stats = VectorWelford::new(1).unwrap();
        stats.add(&[3.0]).unwrap();
        stats.count = u64::MAX;
        assert_eq!(
            stats.add(&[4.0]),
            Err(SketchError::ObservationCountOverflow)
        );
        assert_eq!(stats.mean(), Some(&[3.0][..]));
        assert_eq!(stats.count(), u64::MAX);

        let mut other = VectorWelford::new(1).unwrap();
        other.add(&[5.0]).unwrap();
        assert_eq!(
            stats.merge(&other),
            Err(SketchError::ObservationCountOverflow)
        );
        assert_eq!(stats.mean(), Some(&[3.0][..]));
        assert_eq!(stats.count(), u64::MAX);
    }
}
