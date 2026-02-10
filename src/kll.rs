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
//! KLL sketch for approximate quantile queries.
//!
//! This is a compact, mergeable quantile sketch based on level compaction.

use crate::{SketchError, splitmix64};

/// Approximate quantile sketch using KLL-style compaction.
///
/// # Example
/// ```rust
/// use sketches::kll::KllSketch;
///
/// let mut kll = KllSketch::new(200).unwrap();
/// for value in 0_u64..10_000 {
///     kll.add(value as f64);
/// }
///
/// let p50 = kll.quantile(0.50).unwrap();
/// assert!(p50 > 4_000.0 && p50 < 6_000.0);
/// ```
#[derive(Debug, Clone)]
pub struct KllSketch {
    k: usize,
    levels: Vec<Vec<f64>>,
    count: u64,
    rng_state: u64,
}

impl KllSketch {
    /// Creates a sketch with compaction parameter `k`.
    ///
    /// Higher `k` increases accuracy at the cost of memory.
    ///
    /// # Errors
    /// Returns [`SketchError::InvalidParameter`] when `k < 2`.
    pub fn new(k: usize) -> Result<Self, SketchError> {
        if k < 2 {
            return Err(SketchError::InvalidParameter(
                "k must be greater than or equal to 2",
            ));
        }

        Ok(Self {
            k,
            levels: vec![Vec::new()],
            count: 0,
            rng_state: 0xD1B5_4A32_C192_ED03,
        })
    }

    /// Creates a sketch from a target rank error.
    ///
    /// This uses a simple heuristic `k = ceil(2 / rank_error)`.
    ///
    /// # Errors
    /// Returns [`SketchError::InvalidParameter`] for invalid `rank_error`.
    pub fn with_error_rate(rank_error: f64) -> Result<Self, SketchError> {
        if !rank_error.is_finite() || rank_error <= 0.0 || rank_error >= 1.0 {
            return Err(SketchError::InvalidParameter(
                "rank_error must be finite and strictly between 0 and 1",
            ));
        }

        let k = (2.0 / rank_error).ceil() as usize;
        Self::new(k.max(2))
    }

    /// Returns the configured compaction parameter.
    pub fn k(&self) -> usize {
        self.k
    }

    /// Returns the number of observed values.
    pub fn count(&self) -> u64 {
        self.count
    }

    /// Returns `true` when no values have been added.
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// Adds one value to the sketch.
    ///
    /// Non-finite values are ignored.
    pub fn add(&mut self, value: f64) {
        if !value.is_finite() {
            return;
        }

        self.levels[0].push(value);
        self.count = self.count.saturating_add(1);
        self.compact_all_levels();
    }

    /// Returns the approximate quantile at `q` where `q` is in `[0, 1]`.
    ///
    /// # Errors
    /// Returns [`SketchError::InvalidParameter`] for invalid `q` or empty
    /// sketches.
    pub fn quantile(&self, q: f64) -> Result<f64, SketchError> {
        if !q.is_finite() || !(0.0..=1.0).contains(&q) {
            return Err(SketchError::InvalidParameter(
                "q must be finite and in [0, 1]",
            ));
        }
        if self.count == 0 {
            return Err(SketchError::InvalidParameter(
                "quantile is undefined for an empty sketch",
            ));
        }

        let mut weighted_values = Vec::new();
        for (level, values) in self.levels.iter().enumerate() {
            let weight = 1_u64.checked_shl(level as u32).unwrap_or(u64::MAX);
            for &value in values {
                weighted_values.push((value, weight));
            }
        }

        weighted_values.sort_unstable_by(|left, right| left.0.total_cmp(&right.0));

        let total_weight: u128 = weighted_values
            .iter()
            .map(|(_, weight)| *weight as u128)
            .sum();
        let target_rank = ((total_weight.saturating_sub(1)) as f64 * q).round() as u128;

        let mut cumulative = 0_u128;
        for (value, weight) in weighted_values {
            cumulative = cumulative.saturating_add(weight as u128);
            if cumulative > target_rank {
                return Ok(value);
            }
        }

        Err(SketchError::InvalidParameter(
            "unable to compute quantile from current state",
        ))
    }

    /// Merges another sketch into this one.
    ///
    /// # Errors
    /// Returns [`SketchError::IncompatibleSketches`] when `k` differs.
    pub fn merge(&mut self, other: &Self) -> Result<(), SketchError> {
        if self.k != other.k {
            return Err(SketchError::IncompatibleSketches(
                "k must match for merge",
            ));
        }

        if self.levels.len() < other.levels.len() {
            self.levels.resize_with(other.levels.len(), Vec::new);
        }
        for (level, values) in other.levels.iter().enumerate() {
            self.levels[level].extend(values.iter().copied());
        }
        self.count = self.count.saturating_add(other.count);
        self.compact_all_levels();
        Ok(())
    }

    /// Clears all retained state.
    pub fn clear(&mut self) {
        self.levels.clear();
        self.levels.push(Vec::new());
        self.count = 0;
    }

    fn level_capacity(&self, level: usize) -> usize {
        let decay = 0.75_f64.powi(level as i32);
        (self.k as f64 * decay).ceil().max(2.0) as usize
    }

    fn compact_all_levels(&mut self) {
        let mut level = 0;
        while level < self.levels.len() {
            let capacity = self.level_capacity(level);
            if self.levels[level].len() > capacity {
                self.compact_level(level);
            }
            level += 1;
        }
    }

    fn compact_level(&mut self, level: usize) {
        if level + 1 == self.levels.len() {
            self.levels.push(Vec::new());
        }

        let mut values = std::mem::take(&mut self.levels[level]);
        values.sort_unstable_by(f64::total_cmp);

        // Keep one value at this level if the level length is odd.
        let carry = if values.len() % 2 == 1 {
            values.pop()
        } else {
            None
        };

        let offset = self.next_u64() as usize & 1;
        for index in (offset..values.len()).step_by(2) {
            self.levels[level + 1].push(values[index]);
        }

        if let Some(value) = carry {
            self.levels[level].push(value);
        }
    }

    fn next_u64(&mut self) -> u64 {
        self.rng_state = splitmix64(self.rng_state.wrapping_add(0x9E37_79B9_7F4A_7C15));
        self.rng_state
    }
}

#[cfg(test)]
mod tests {
    use super::KllSketch;

    #[test]
    fn constructor_validates_k() {
        assert!(KllSketch::new(1).is_err());
        assert!(KllSketch::new(2).is_ok());
    }

    #[test]
    fn quantile_rejects_empty_sketch() {
        let kll = KllSketch::new(64).unwrap();
        assert!(kll.quantile(0.5).is_err());
    }

    #[test]
    fn median_estimate_is_reasonable() {
        let mut kll = KllSketch::new(200).unwrap();
        for value in 0_u64..10_000 {
            kll.add(value as f64);
        }

        let p50 = kll.quantile(0.5).unwrap();
        assert!(p50 > 4_300.0 && p50 < 5_700.0, "p50={p50}");
    }

    #[test]
    fn quantiles_are_monotonic() {
        let mut kll = KllSketch::new(128).unwrap();
        for value in 0_u64..20_000 {
            kll.add(value as f64);
        }

        let p50 = kll.quantile(0.5).unwrap();
        let p90 = kll.quantile(0.9).unwrap();
        assert!(p50 <= p90, "p50={p50} p90={p90}");
    }

    #[test]
    fn merge_combines_streams() {
        let mut left = KllSketch::new(160).unwrap();
        let mut right = KllSketch::new(160).unwrap();

        for value in 0_u64..10_000 {
            left.add(value as f64);
        }
        for value in 10_000_u64..20_000 {
            right.add(value as f64);
        }

        left.merge(&right).unwrap();
        let p95 = left.quantile(0.95).unwrap();
        assert!(p95 > 17_000.0 && p95 < 20_000.0, "p95={p95}");
    }

    #[test]
    fn merge_rejects_different_k() {
        let mut left = KllSketch::new(100).unwrap();
        let right = KllSketch::new(101).unwrap();
        assert!(left.merge(&right).is_err());
    }

    #[test]
    fn clear_resets_state() {
        let mut kll = KllSketch::new(128).unwrap();
        kll.add(1.0);
        kll.add(2.0);
        kll.clear();
        assert!(kll.is_empty());
        assert!(kll.quantile(0.5).is_err());
    }
}
