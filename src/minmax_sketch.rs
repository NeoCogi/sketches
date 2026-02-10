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
//! MinMax sketch for approximate frequency counting.
//!
//! The implementation uses count-min style hashing with conservative updates.
//! Querying returns both:
//! - the minimum counter (tightest estimate),
//! - the maximum counter (loosest estimate across rows).

use std::hash::Hash;

use crate::{SketchError, seeded_hash64, splitmix64};

/// Approximate frequency sketch with min/max counter views.
///
/// `MinMaxSketch` is useful for high-volume frequency counting when exact
/// maps would be too memory-heavy.
///
/// # Example
/// ```rust
/// use sketches::minmax_sketch::MinMaxSketch;
///
/// let mut sketch = MinMaxSketch::new(0.01, 0.01).unwrap();
/// sketch.add(&"cat", 2);
/// sketch.increment(&"cat");
/// let (min_estimate, max_estimate) = sketch.estimate_interval(&"cat");
/// assert!(min_estimate >= 3);
/// assert!(max_estimate >= min_estimate);
/// ```
#[derive(Debug, Clone)]
pub struct MinMaxSketch {
    width: usize,
    depth: usize,
    counters: Vec<u64>,
    seeds: Vec<u64>,
    total_count: u64,
}

impl MinMaxSketch {
    /// Builds a sketch from error bounds.
    ///
    /// `epsilon` controls additive error and `delta` controls confidence.
    /// Both must be in `(0, 1)`.
    ///
    /// Width is `ceil(e / epsilon)` and depth is `ceil(ln(1 / delta))`.
    ///
    /// # Errors
    /// Returns [`SketchError::InvalidParameter`] when bounds are invalid.
    pub fn new(epsilon: f64, delta: f64) -> Result<Self, SketchError> {
        if !epsilon.is_finite() || epsilon <= 0.0 || epsilon >= 1.0 {
            return Err(SketchError::InvalidParameter(
                "epsilon must be finite and strictly between 0 and 1",
            ));
        }
        if !delta.is_finite() || delta <= 0.0 || delta >= 1.0 {
            return Err(SketchError::InvalidParameter(
                "delta must be finite and strictly between 0 and 1",
            ));
        }

        let width = (std::f64::consts::E / epsilon).ceil() as usize;
        let depth = (1.0 / delta).ln().ceil() as usize;
        Self::with_dimensions(width.max(1), depth.max(1))
    }

    /// Builds a sketch from explicit dimensions.
    ///
    /// # Errors
    /// Returns [`SketchError::InvalidParameter`] when `width` or `depth` is
    /// zero, or when `width * depth` overflows.
    pub fn with_dimensions(width: usize, depth: usize) -> Result<Self, SketchError> {
        if width == 0 {
            return Err(SketchError::InvalidParameter(
                "width must be greater than zero",
            ));
        }
        if depth == 0 {
            return Err(SketchError::InvalidParameter(
                "depth must be greater than zero",
            ));
        }
        let table_len = width
            .checked_mul(depth)
            .ok_or(SketchError::InvalidParameter(
                "width * depth overflows usize",
            ))?;

        let seeds = (0..depth)
            .map(|idx| splitmix64((idx as u64).wrapping_add(0xA076_1D64_78BD_642F)))
            .collect();

        Ok(Self {
            width,
            depth,
            counters: vec![0; table_len],
            seeds,
            total_count: 0,
        })
    }

    /// Returns the number of columns per hash row.
    pub fn width(&self) -> usize {
        self.width
    }

    /// Returns the number of hash rows.
    pub fn depth(&self) -> usize {
        self.depth
    }

    /// Returns the total inserted count (saturating).
    pub fn total_count(&self) -> u64 {
        self.total_count
    }

    /// Returns `true` when no positive count has been inserted.
    pub fn is_empty(&self) -> bool {
        self.total_count == 0
    }

    /// Adds `count` occurrences of an item.
    ///
    /// Conservative update is used to reduce overestimation.
    pub fn add<T: Hash>(&mut self, item: &T, count: u64) {
        if count == 0 {
            return;
        }

        let mut min_counter = u64::MAX;
        for row in 0..self.depth {
            let idx = self.counter_index(row, item);
            min_counter = min_counter.min(self.counters[idx]);
        }

        let target = min_counter.saturating_add(count);
        for row in 0..self.depth {
            let idx = self.counter_index(row, item);
            if self.counters[idx] < target {
                self.counters[idx] = target;
            }
        }

        self.total_count = self.total_count.saturating_add(count);
    }

    /// Adds exactly one occurrence of an item.
    pub fn increment<T: Hash>(&mut self, item: &T) {
        self.add(item, 1);
    }

    /// Returns the tightest estimate for an item count.
    ///
    /// This is the minimum counter across all rows.
    pub fn estimate<T: Hash>(&self, item: &T) -> u64 {
        let mut min_counter = u64::MAX;
        for row in 0..self.depth {
            let idx = self.counter_index(row, item);
            min_counter = min_counter.min(self.counters[idx]);
        }
        min_counter
    }

    /// Returns the loosest estimate for an item count.
    ///
    /// This is the maximum counter across all rows.
    pub fn max_estimate<T: Hash>(&self, item: &T) -> u64 {
        let mut max_counter = 0_u64;
        for row in 0..self.depth {
            let idx = self.counter_index(row, item);
            max_counter = max_counter.max(self.counters[idx]);
        }
        max_counter
    }

    /// Returns `(min_estimate, max_estimate)` for an item.
    pub fn estimate_interval<T: Hash>(&self, item: &T) -> (u64, u64) {
        (self.estimate(item), self.max_estimate(item))
    }

    /// Resets all counters and total count to zero.
    pub fn clear(&mut self) {
        self.counters.fill(0);
        self.total_count = 0;
    }

    /// Merges another sketch into this sketch in-place.
    ///
    /// Both sketches must have the same dimensions and seeds.
    ///
    /// # Errors
    /// Returns [`SketchError::IncompatibleSketches`] for mismatched shape/seed.
    pub fn merge(&mut self, other: &Self) -> Result<(), SketchError> {
        if self.width != other.width || self.depth != other.depth {
            return Err(SketchError::IncompatibleSketches(
                "width/depth must match for merge",
            ));
        }
        if self.seeds != other.seeds {
            return Err(SketchError::IncompatibleSketches(
                "hash seeds must match for merge",
            ));
        }

        for (left, right) in self.counters.iter_mut().zip(other.counters.iter()) {
            *left = left.saturating_add(*right);
        }
        self.total_count = self.total_count.saturating_add(other.total_count);
        Ok(())
    }

    /// Computes the flattened counter index for a `(row, item)` pair.
    fn counter_index<T: Hash>(&self, row: usize, item: &T) -> usize {
        let column = (seeded_hash64(item, self.seeds[row]) as usize) % self.width;
        row * self.width + column
    }
}

#[cfg(test)]
mod tests {
    use super::MinMaxSketch;

    #[test]
    fn constructor_from_error_bounds_creates_non_zero_dimensions() {
        let sketch = MinMaxSketch::new(0.01, 0.01).expect("valid bounds");
        assert!(sketch.width() > 0);
        assert!(sketch.depth() > 0);
    }

    #[test]
    fn constructor_rejects_invalid_error_bounds() {
        assert!(MinMaxSketch::new(0.0, 0.1).is_err());
        assert!(MinMaxSketch::new(0.1, 0.0).is_err());
        assert!(MinMaxSketch::new(1.0, 0.1).is_err());
        assert!(MinMaxSketch::new(0.1, 1.0).is_err());
        assert!(MinMaxSketch::new(f64::NAN, 0.1).is_err());
    }

    #[test]
    fn with_dimensions_rejects_zero_sizes() {
        assert!(MinMaxSketch::with_dimensions(0, 1).is_err());
        assert!(MinMaxSketch::with_dimensions(1, 0).is_err());
    }

    #[test]
    fn estimate_is_monotonic_after_updates() {
        let mut sketch = MinMaxSketch::with_dimensions(128, 5).unwrap();
        let mut previous = 0_u64;
        for _ in 0..50 {
            sketch.increment(&"user:42");
            let current = sketch.estimate(&"user:42");
            assert!(current >= previous);
            previous = current;
        }
    }

    #[test]
    fn estimate_interval_is_ordered() {
        let mut sketch = MinMaxSketch::with_dimensions(128, 4).unwrap();
        sketch.add(&"alpha", 7);
        let (min_estimate, max_estimate) = sketch.estimate_interval(&"alpha");
        assert!(min_estimate <= max_estimate);
    }

    #[test]
    fn estimate_is_never_below_exact_count_in_small_stream() {
        let mut sketch = MinMaxSketch::with_dimensions(512, 6).unwrap();
        for _ in 0..30 {
            sketch.increment(&"a");
        }
        for _ in 0..7 {
            sketch.increment(&"b");
        }
        for _ in 0..13 {
            sketch.increment(&"c");
        }

        assert!(sketch.estimate(&"a") >= 30);
        assert!(sketch.estimate(&"b") >= 7);
        assert!(sketch.estimate(&"c") >= 13);
    }

    #[test]
    fn clear_resets_all_state() {
        let mut sketch = MinMaxSketch::with_dimensions(64, 4).unwrap();
        sketch.add(&"key", 10);
        assert!(!sketch.is_empty());
        sketch.clear();
        assert_eq!(sketch.total_count(), 0);
        assert_eq!(sketch.estimate(&"key"), 0);
        assert!(sketch.is_empty());
    }

    #[test]
    fn merge_combines_sketches() {
        let mut left = MinMaxSketch::with_dimensions(256, 5).unwrap();
        let mut right = MinMaxSketch::with_dimensions(256, 5).unwrap();

        left.add(&"hot", 10);
        right.add(&"hot", 15);
        right.add(&"cold", 4);

        left.merge(&right).expect("compatible sketches");

        assert!(left.estimate(&"hot") >= 25);
        assert!(left.estimate(&"cold") >= 4);
        assert!(left.total_count() >= 29);
    }

    #[test]
    fn merge_rejects_mismatched_dimensions() {
        let mut left = MinMaxSketch::with_dimensions(64, 4).unwrap();
        let right = MinMaxSketch::with_dimensions(65, 4).unwrap();
        assert!(left.merge(&right).is_err());
    }

    #[test]
    fn saturating_addition_avoids_overflow() {
        let mut sketch = MinMaxSketch::with_dimensions(32, 4).unwrap();
        sketch.add(&"overflow", u64::MAX);
        sketch.increment(&"overflow");

        assert_eq!(sketch.estimate(&"overflow"), u64::MAX);
        assert_eq!(sketch.total_count(), u64::MAX);
    }
}
