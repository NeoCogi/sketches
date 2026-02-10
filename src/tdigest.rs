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
        self.add_weighted(value, 1.0);
    }

    /// Returns the approximate quantile for `q` in `[0, 1]`.
    ///
    /// # Errors
    /// Returns [`SketchError::InvalidParameter`] for invalid `q` or empty
    /// digests.
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

        if q <= 0.0 {
            return Ok(centroids[0].mean);
        }
        if q >= 1.0 {
            return Ok(centroids[centroids.len() - 1].mean);
        }

        let target = q * self.total_weight;
        let mut cumulative = 0.0;
        for index in 0..centroids.len() {
            let current = centroids[index];
            let next_cumulative = cumulative + current.weight;
            if target <= next_cumulative {
                if index == 0 {
                    return Ok(current.mean);
                }

                let previous = centroids[index - 1];
                let left_rank = cumulative - previous.weight * 0.5;
                let right_rank = cumulative + current.weight * 0.5;
                if right_rank <= left_rank + f64::EPSILON {
                    return Ok(current.mean);
                }

                let t = ((target - left_rank) / (right_rank - left_rank)).clamp(0.0, 1.0);
                return Ok(previous.mean + t * (current.mean - previous.mean));
            }
            cumulative = next_cumulative;
        }

        Ok(centroids[centroids.len() - 1].mean)
    }

    /// Merges another digest into this one.
    ///
    /// # Errors
    /// Returns [`SketchError::IncompatibleSketches`] when compression differs.
    pub fn merge(&mut self, other: &Self) -> Result<(), SketchError> {
        if (self.compression - other.compression).abs() > f64::EPSILON {
            return Err(SketchError::IncompatibleSketches(
                "compression must match for merge",
            ));
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
        let cumulative_before = self.centroids[..index].iter().map(|centroid| centroid.weight).sum::<f64>();
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
                let q = ((cumulative + 0.5 * last.weight) / self.total_weight.max(1.0)).clamp(0.0, 1.0);
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

#[cfg(test)]
mod tests {
    use super::TDigest;

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
    }
}
