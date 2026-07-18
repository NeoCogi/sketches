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
//! t-digest for approximate quantiles, especially in distribution tails.
//!
//! Quantile interpolation follows Section 2.9 of Dunning and Ertl's
//! [t-digest paper] and the singleton-aware behavior of the reference
//! `MergingDigest`. Exact samples use zero-based rank
//! `min(floor(q * N), N - 1)`. Centroids containing multiple samples are
//! interpolated at their midpoint ranks, while singleton centroids remain
//! exact steps. The exact observed minimum and maximum are retained separately.
//!
//! [t-digest paper]: https://arxiv.org/pdf/1902.04023

use crate::SketchError;

#[derive(Debug, Clone, Copy)]
struct Centroid {
    mean: f64,
    weight: f64,
}

/// Approximate quantile sketch based on t-digest centroids.
///
/// # Example
/// ```rust
/// use sketches::tdigest::TDigest;
///
/// let mut digest = TDigest::new(100.0).unwrap();
/// for value in 0_u64..10_000 {
///     digest.add(value as f64);
/// }
///
/// let p95 = digest.quantile(0.95).unwrap();
/// assert!(p95 > 9_000.0 && p95 < 10_000.0);
/// ```
#[derive(Debug, Clone)]
pub struct TDigest {
    compression: f64,
    centroids: Vec<Centroid>,
    total_weight: f64,
    min: f64,
    max: f64,
}

impl TDigest {
    /// Creates a digest with the given compression parameter.
    ///
    /// Higher compression generally improves quantile accuracy at the cost of
    /// more centroids in memory.
    ///
    /// # Errors
    /// Returns [`SketchError::InvalidParameter`] for non-finite or too-small
    /// compression values.
    pub fn new(compression: f64) -> Result<Self, SketchError> {
        if !compression.is_finite() || compression < 10.0 {
            return Err(SketchError::InvalidParameter(
                "compression must be finite and greater than or equal to 10",
            ));
        }

        Ok(Self {
            compression,
            centroids: Vec::new(),
            total_weight: 0.0,
            min: f64::INFINITY,
            max: f64::NEG_INFINITY,
        })
    }

    /// Creates a digest from a target quantile error heuristic.
    ///
    /// Internally uses: `compression = ceil(10 / error)`.
    ///
    /// # Errors
    /// Returns [`SketchError::InvalidParameter`] when `quantile_error` is invalid.
    pub fn with_error_rate(quantile_error: f64) -> Result<Self, SketchError> {
        if !quantile_error.is_finite() || quantile_error <= 0.0 || quantile_error >= 1.0 {
            return Err(SketchError::InvalidParameter(
                "quantile_error must be finite and strictly between 0 and 1",
            ));
        }

        let compression = (10.0 / quantile_error).ceil().max(10.0);
        Self::new(compression)
    }

    /// Returns the configured compression parameter.
    pub fn compression(&self) -> f64 {
        self.compression
    }

    /// Returns the number of centroids currently tracked.
    pub fn centroid_count(&self) -> usize {
        self.centroids.len()
    }

    /// Returns the total observed weight rounded to `u64`.
    pub fn count(&self) -> u64 {
        self.total_weight.round() as u64
    }

    /// Returns `true` when no values were added.
    pub fn is_empty(&self) -> bool {
        self.total_weight == 0.0
    }

    /// Adds one value to the digest.
    ///
    /// Non-finite values are ignored.
    pub fn add(&mut self, value: f64) {
        if !value.is_finite() {
            return;
        }

        self.min = self.min.min(value);
        self.max = self.max.max(value);
        self.add_weighted(value, 1.0);
    }

    /// Returns the approximate quantile for `q` in `[0, 1]`.
    ///
    /// For exact, uncompressed samples, `q` selects zero-based rank
    /// `min(floor(q * N), N - 1)`. Thus the median of `[0, 10]` is `10`.
    /// This is the same empirical inverse-CDF convention used by
    /// [`crate::kll::KllSketch`].
    ///
    /// For compressed centroids, interpolation follows the [t-digest paper]:
    /// centroids containing multiple samples are positioned at their midpoint
    /// ranks, singleton centroids remain exact samples, and terminal
    /// interpolation uses the separately retained observed minimum and maximum.
    ///
    /// # Errors
    /// Returns [`SketchError::InvalidParameter`] for invalid `q` or empty
    /// digests.
    ///
    /// [t-digest paper]: https://arxiv.org/pdf/1902.04023
    pub fn quantile(&self, q: f64) -> Result<f64, SketchError> {
        if !q.is_finite() || !(0.0..=1.0).contains(&q) {
            return Err(SketchError::InvalidParameter(
                "q must be finite and in [0, 1]",
            ));
        }
        if self.centroids.is_empty() {
            return Err(SketchError::InvalidParameter(
                "quantile is undefined for an empty digest",
            ));
        }

        let mut centroids = self.centroids.clone();
        centroids.sort_unstable_by(|left, right| left.mean.total_cmp(&right.mean));

        if q == 0.0 {
            return Ok(self.min);
        }
        if q == 1.0 {
            return Ok(self.max);
        }
        if centroids.len() == 1 {
            return Ok(centroids[0].mean);
        }

        let index = q * self.total_weight;
        if index < 1.0 {
            return Ok(self.min);
        }

        let first = centroids[0];
        if first.weight > 1.0 && index < first.weight * 0.5 {
            let interior_weight = first.weight * 0.5 - 1.0;
            if interior_weight > 0.0 {
                let fraction = ((index - 1.0) / interior_weight).clamp(0.0, 1.0);
                return Ok(self.min + fraction * (first.mean - self.min));
            }
        }

        if index > self.total_weight - 1.0 {
            return Ok(self.max);
        }

        let last = centroids[centroids.len() - 1];
        let weight_from_right = self.total_weight - index;
        if last.weight > 1.0 && weight_from_right <= last.weight * 0.5 {
            let interior_weight = last.weight * 0.5 - 1.0;
            if interior_weight > 0.0 {
                let fraction = ((weight_from_right - 1.0) / interior_weight).clamp(0.0, 1.0);
                return Ok(self.max - fraction * (self.max - last.mean));
            }
        }

        let mut weight_so_far = first.weight * 0.5;
        for pair in centroids.windows(2) {
            let left = pair[0];
            let right = pair[1];
            let interval_weight = (left.weight + right.weight) * 0.5;

            if weight_so_far + interval_weight > index {
                let mut left_singleton_weight = 0.0;
                if left.weight == 1.0 {
                    if index - weight_so_far < 0.5 {
                        return Ok(left.mean);
                    }
                    left_singleton_weight = 0.5;
                }

                let mut right_singleton_weight = 0.0;
                if right.weight == 1.0 {
                    if weight_so_far + interval_weight - index <= 0.5 {
                        return Ok(right.mean);
                    }
                    right_singleton_weight = 0.5;
                }

                let right_weight = index - weight_so_far - left_singleton_weight;
                let left_weight = weight_so_far + interval_weight - index - right_singleton_weight;
                return Ok(weighted_average(
                    left.mean,
                    left_weight,
                    right.mean,
                    right_weight,
                ));
            }

            weight_so_far += interval_weight;
        }

        Ok(self.max)
    }

    /// Merges another digest into this one.
    ///
    /// Centroids are recompressed and the exact observed minimum and maximum
    /// are combined independently so endpoint queries remain exact.
    ///
    /// # Errors
    /// Returns [`SketchError::IncompatibleSketches`] when compression differs.
    pub fn merge(&mut self, other: &Self) -> Result<(), SketchError> {
        if (self.compression - other.compression).abs() > f64::EPSILON {
            return Err(SketchError::IncompatibleSketches(
                "compression must match for merge",
            ));
        }

        if !other.is_empty() {
            self.min = self.min.min(other.min);
            self.max = self.max.max(other.max);
        }

        for centroid in &other.centroids {
            self.add_weighted(centroid.mean, centroid.weight);
        }
        self.compress();
        Ok(())
    }

    /// Clears all centroids and observed weight.
    pub fn clear(&mut self) {
        self.centroids.clear();
        self.total_weight = 0.0;
        self.min = f64::INFINITY;
        self.max = f64::NEG_INFINITY;
    }

    fn add_weighted(&mut self, value: f64, weight: f64) {
        if !value.is_finite() || !weight.is_finite() || weight <= 0.0 {
            return;
        }

        if self.centroids.is_empty() {
            self.centroids.push(Centroid {
                mean: value,
                weight,
            });
            self.total_weight += weight;
            return;
        }

        self.centroids
            .sort_unstable_by(|left, right| left.mean.total_cmp(&right.mean));

        let mut nearest_index = 0usize;
        let mut nearest_distance = (self.centroids[0].mean - value).abs();
        for (index, centroid) in self.centroids.iter().enumerate().skip(1) {
            let distance = (centroid.mean - value).abs();
            if distance < nearest_distance {
                nearest_distance = distance;
                nearest_index = index;
            }
        }

        let q = self.centroid_quantile(nearest_index);
        let max_weight = self.max_centroid_weight(q);

        if self.centroids[nearest_index].weight + weight <= max_weight {
            let centroid = &mut self.centroids[nearest_index];
            let updated_weight = centroid.weight + weight;
            centroid.mean += (value - centroid.mean) * (weight / updated_weight);
            centroid.weight = updated_weight;
        } else {
            let insert_index = match self
                .centroids
                .binary_search_by(|centroid| centroid.mean.total_cmp(&value))
            {
                Ok(index) | Err(index) => index,
            };
            self.centroids.insert(
                insert_index,
                Centroid {
                    mean: value,
                    weight,
                },
            );
        }

        self.total_weight += weight;
        if self.centroids.len() > (self.compression * 8.0) as usize {
            self.compress();
        }
    }

    fn centroid_quantile(&self, index: usize) -> f64 {
        let cumulative_before = self.centroids[..index]
            .iter()
            .map(|centroid| centroid.weight)
            .sum::<f64>();
        let centered = cumulative_before + self.centroids[index].weight * 0.5;
        (centered / self.total_weight.max(1.0)).clamp(0.0, 1.0)
    }

    fn max_centroid_weight(&self, q: f64) -> f64 {
        let scaled = (4.0 * self.total_weight / self.compression) * q * (1.0 - q);
        scaled.max(1.0)
    }

    fn compress(&mut self) {
        if self.centroids.len() <= 1 {
            return;
        }

        self.centroids
            .sort_unstable_by(|left, right| left.mean.total_cmp(&right.mean));

        let old = std::mem::take(&mut self.centroids);
        let mut merged: Vec<Centroid> = Vec::with_capacity(old.len());
        let mut cumulative = 0.0;

        for centroid in old {
            if let Some(last) = merged.last_mut() {
                let q =
                    ((cumulative + 0.5 * last.weight) / self.total_weight.max(1.0)).clamp(0.0, 1.0);
                let max_weight = self.max_centroid_weight(q);

                if last.weight + centroid.weight <= max_weight {
                    let updated_weight = last.weight + centroid.weight;
                    last.mean += (centroid.mean - last.mean) * (centroid.weight / updated_weight);
                    last.weight = updated_weight;
                    continue;
                }

                cumulative += last.weight;
            }

            merged.push(centroid);
        }

        self.centroids = merged;
    }
}

fn weighted_average(left: f64, left_weight: f64, right: f64, right_weight: f64) -> f64 {
    let total_weight = left_weight + right_weight;
    if total_weight <= 0.0 {
        return (left + right) * 0.5;
    }

    let value = (left * left_weight + right * right_weight) / total_weight;
    value.clamp(left.min(right), left.max(right))
}

#[cfg(test)]
mod tests {
    use super::{Centroid, TDigest};

    fn assert_close(actual: f64, expected: f64) {
        assert!(
            (actual - expected).abs() <= 1e-12,
            "actual={actual} expected={expected}"
        );
    }

    #[test]
    fn constructor_validates_compression() {
        assert!(TDigest::new(5.0).is_err());
        assert!(TDigest::new(50.0).is_ok());
    }

    #[test]
    fn quantile_rejects_invalid_input() {
        let digest = TDigest::new(100.0).unwrap();
        assert!(digest.quantile(0.5).is_err());
        assert!(digest.quantile(-0.1).is_err());
        assert!(digest.quantile(1.1).is_err());
    }

    #[test]
    fn two_singletons_step_at_the_empirical_rank_boundary() {
        let mut digest = TDigest::new(100.0).unwrap();
        digest.add(0.0);
        digest.add(10.0);

        let below = f64::from_bits(0.5_f64.to_bits() - 1);
        let above = f64::from_bits(0.5_f64.to_bits() + 1);
        assert_eq!(digest.quantile(below).unwrap(), 0.0);
        assert_eq!(digest.quantile(0.5).unwrap(), 10.0);
        assert_eq!(digest.quantile(above).unwrap(), 10.0);
    }

    #[test]
    fn multi_sample_centroids_interpolate_at_midpoint_ranks_and_extrema() {
        let digest = TDigest {
            compression: 100.0,
            centroids: vec![
                Centroid {
                    mean: 0.0,
                    weight: 4.0,
                },
                Centroid {
                    mean: 10.0,
                    weight: 4.0,
                },
            ],
            total_weight: 8.0,
            min: -2.0,
            max: 12.0,
        };

        for (q, expected) in [
            (0.0, -2.0),
            (0.125, -2.0),
            (0.1875, -1.0),
            (0.25, 0.0),
            (0.5, 5.0),
            (0.75, 10.0),
            (0.8125, 11.0),
            (0.875, 12.0),
            (1.0, 12.0),
        ] {
            assert_close(digest.quantile(q).unwrap(), expected);
        }
    }

    #[test]
    fn endpoint_queries_do_not_depend_on_terminal_centroid_means() {
        let digest = TDigest {
            compression: 100.0,
            centroids: vec![Centroid {
                mean: 5.0,
                weight: 8.0,
            }],
            total_weight: 8.0,
            min: 0.0,
            max: 10.0,
        };

        assert_eq!(digest.quantile(0.0).unwrap(), 0.0);
        assert_eq!(digest.quantile(1.0).unwrap(), 10.0);
    }

    #[test]
    fn adjacent_singletons_are_not_interpolated() {
        let digest = TDigest {
            compression: 100.0,
            centroids: vec![
                Centroid {
                    mean: 0.0,
                    weight: 4.0,
                },
                Centroid {
                    mean: 10.0,
                    weight: 1.0,
                },
                Centroid {
                    mean: 20.0,
                    weight: 1.0,
                },
            ],
            total_weight: 6.0,
            min: -2.0,
            max: 20.0,
        };

        assert_eq!(digest.quantile(5.0 / 6.0 - 1e-12).unwrap(), 10.0);
        assert_eq!(digest.quantile(5.0 / 6.0).unwrap(), 20.0);
        assert_eq!(digest.quantile(5.0 / 6.0 + 1e-12).unwrap(), 20.0);
    }

    #[test]
    fn exact_extrema_survive_compression_and_merge() {
        let mut left = TDigest::new(20.0).unwrap();
        left.add(-123.5);
        for value in 0_u64..5_000 {
            left.add(value as f64);
        }

        let mut right = TDigest::new(20.0).unwrap();
        for value in 5_000_u64..10_000 {
            right.add(value as f64);
        }
        right.add(12_345.5);

        left.merge(&right).unwrap();

        assert_eq!(left.quantile(0.0).unwrap(), -123.5);
        assert_eq!(left.quantile(1.0).unwrap(), 12_345.5);
    }

    #[test]
    fn quantiles_are_monotonic_across_centroid_boundaries() {
        let mut digest = TDigest::new(40.0).unwrap();
        for value in 0_u64..20_000 {
            digest.add((value % 1_003) as f64);
        }

        let mut previous = digest.quantile(0.0).unwrap();
        for step in 1..=10_000 {
            let current = digest.quantile(step as f64 / 10_000.0).unwrap();
            assert!(
                previous <= current,
                "step={step} previous={previous} current={current}"
            );
            previous = current;
        }
    }

    #[test]
    fn median_estimate_is_reasonable() {
        let mut digest = TDigest::new(120.0).unwrap();
        for value in 0_u64..10_000 {
            digest.add(value as f64);
        }

        let p50 = digest.quantile(0.5).unwrap();
        assert!(p50 > 4_400.0 && p50 < 5_600.0, "p50={p50}");
    }

    #[test]
    fn high_quantile_tracks_tail() {
        let mut digest = TDigest::new(120.0).unwrap();
        for value in 0_u64..10_000 {
            digest.add(value as f64);
        }

        let p95 = digest.quantile(0.95).unwrap();
        let p99 = digest.quantile(0.99).unwrap();
        assert!(p95 <= p99, "p95={p95} p99={p99}");
        assert!(p99 > 9_000.0);
    }

    #[test]
    fn merge_combines_streams() {
        let mut left = TDigest::new(80.0).unwrap();
        let mut right = TDigest::new(80.0).unwrap();

        for value in 0_u64..5_000 {
            left.add(value as f64);
        }
        for value in 5_000_u64..10_000 {
            right.add(value as f64);
        }

        left.merge(&right).unwrap();
        let p90 = left.quantile(0.9).unwrap();
        assert!(p90 > 8_000.0);
    }

    #[test]
    fn merge_rejects_mismatched_compression() {
        let mut left = TDigest::new(80.0).unwrap();
        let right = TDigest::new(81.0).unwrap();
        assert!(left.merge(&right).is_err());
    }

    #[test]
    fn clear_resets_state() {
        let mut digest = TDigest::new(50.0).unwrap();
        digest.add(1.0);
        digest.add(2.0);
        digest.clear();
        assert!(digest.is_empty());
        assert!(digest.quantile(0.5).is_err());

        digest.add(9.0);
        assert_eq!(digest.quantile(0.0).unwrap(), 9.0);
        assert_eq!(digest.quantile(1.0).unwrap(), 9.0);
    }
}
