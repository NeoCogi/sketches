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
//! Count Sketch for approximate frequency estimation.
//!
//! Count Sketch uses two hash families per row:
//! - one for selecting a counter index,
//! - one for selecting a sign in `{+1, -1}`.
//!
//! Point queries are estimated by taking the median across rows.

use std::hash::Hash;

use crate::{SketchError, seeded_hash64, splitmix64};

/// Approximate frequency sketch with support for signed updates.
///
/// This sketch is useful when a stream can include both increment and decrement
/// operations and exact maps are too expensive.
///
/// # Example
/// ```rust
/// use sketches::count_sketch::CountSketch;
///
/// let mut sketch = CountSketch::new(0.05, 0.01).unwrap();
/// sketch.add(&"cat", 5);
/// sketch.decrement(&"cat");
///
/// let estimate = sketch.estimate(&"cat");
/// assert!(estimate >= 3 && estimate <= 6);
/// ```
#[derive(Debug, Clone)]
pub struct CountSketch {
    width: usize,
    depth: usize,
    counters: Vec<i64>,
    index_seeds: Vec<u64>,
    sign_seeds: Vec<u64>,
    total_update_magnitude: u64,
}

impl CountSketch {
    /// Builds a sketch from error-style parameters.
    ///
    /// `epsilon` and `delta` must be finite values in `(0, 1)`.
    ///
    /// This constructor chooses:
    /// - `width = ceil(3 / epsilon^2)`,
    /// - `depth = ceil(ln(1 / delta))`.
    ///
    /// # Errors
    /// Returns [`SketchError::InvalidParameter`] when parameters are invalid.
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

        let width = (3.0 / (epsilon * epsilon)).ceil() as usize;
        let depth = (1.0 / delta).ln().ceil() as usize;
        Self::with_dimensions(width.max(1), depth.max(1))
    }

    /// Builds a sketch from explicit dimensions.
    ///
    /// # Errors
    /// Returns [`SketchError::InvalidParameter`] when `width`/`depth` is zero
    /// or when `width * depth` overflows.
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

        let index_seeds = (0..depth)
            .map(|idx| splitmix64((idx as u64).wrapping_add(0x0D6E_8FD9_3A5E_4C31)))
            .collect();
        let sign_seeds = (0..depth)
            .map(|idx| splitmix64((idx as u64).wrapping_add(0xA076_1D64_78BD_642F)))
            .collect();

        Ok(Self {
            width,
            depth,
            counters: vec![0; table_len],
            index_seeds,
            sign_seeds,
            total_update_magnitude: 0,
        })
    }

    /// Returns the number of columns per row.
    pub fn width(&self) -> usize {
        self.width
    }

    /// Returns the number of rows.
    pub fn depth(&self) -> usize {
        self.depth
    }

    /// Returns the total absolute update magnitude inserted so far.
    pub fn total_update_magnitude(&self) -> u64 {
        self.total_update_magnitude
    }

    /// Returns `true` if no non-zero update has been observed.
    pub fn is_empty(&self) -> bool {
        self.total_update_magnitude == 0
    }

    /// Adds a signed update for an item.
    pub fn add<T: Hash>(&mut self, item: &T, delta: i64) {
        if delta == 0 {
            return;
        }

        for row in 0..self.depth {
            let idx = self.counter_index(row, item);
            let signed_delta = if self.sign(row, item) == 1 {
                delta
            } else {
                delta.saturating_neg()
            };
            self.counters[idx] = self.counters[idx].saturating_add(signed_delta);
        }

        self.total_update_magnitude = self
            .total_update_magnitude
            .saturating_add(delta.unsigned_abs());
    }

    /// Adds one occurrence of an item.
    pub fn increment<T: Hash>(&mut self, item: &T) {
        self.add(item, 1);
    }

    /// Removes one occurrence of an item.
    pub fn decrement<T: Hash>(&mut self, item: &T) {
        self.add(item, -1);
    }

    /// Returns the median estimate for the item's signed count.
    pub fn estimate<T: Hash>(&self, item: &T) -> i64 {
        let mut estimates = Vec::with_capacity(self.depth);
        for row in 0..self.depth {
            let idx = self.counter_index(row, item);
            let signed_counter = if self.sign(row, item) == 1 {
                self.counters[idx]
            } else {
                self.counters[idx].saturating_neg()
            };
            estimates.push(signed_counter);
        }

        estimates.sort_unstable();
        let mid = estimates.len() / 2;
        if estimates.len() % 2 == 1 {
            estimates[mid]
        } else {
            let left = estimates[mid - 1] as i128;
            let right = estimates[mid] as i128;
            ((left + right) / 2) as i64
        }
    }

    /// Clears all counters and update metadata.
    pub fn clear(&mut self) {
        self.counters.fill(0);
        self.total_update_magnitude = 0;
    }

    /// Merges another sketch into this sketch in-place.
    ///
    /// # Errors
    /// Returns [`SketchError::IncompatibleSketches`] for shape/seed mismatch.
    pub fn merge(&mut self, other: &Self) -> Result<(), SketchError> {
        if self.width != other.width || self.depth != other.depth {
            return Err(SketchError::IncompatibleSketches(
                "width/depth must match for merge",
            ));
        }
        if self.index_seeds != other.index_seeds || self.sign_seeds != other.sign_seeds {
            return Err(SketchError::IncompatibleSketches(
                "hash seeds must match for merge",
            ));
        }

        for (left, right) in self.counters.iter_mut().zip(other.counters.iter()) {
            *left = left.saturating_add(*right);
        }
        self.total_update_magnitude = self
            .total_update_magnitude
            .saturating_add(other.total_update_magnitude);
        Ok(())
    }

    fn counter_index<T: Hash>(&self, row: usize, item: &T) -> usize {
        let column = (seeded_hash64(item, self.index_seeds[row]) as usize) % self.width;
        row * self.width + column
    }

    fn sign<T: Hash>(&self, row: usize, item: &T) -> i64 {
        if (seeded_hash64(item, self.sign_seeds[row]) & 1) == 0 {
            1
        } else {
            -1
        }
    }
}

#[cfg(test)]
mod tests {
    use super::CountSketch;

    #[test]
    fn constructor_from_error_bounds_creates_non_zero_dimensions() {
        let sketch = CountSketch::new(0.05, 0.01).expect("valid bounds");
        assert!(sketch.width() > 0);
        assert!(sketch.depth() > 0);
    }

    #[test]
    fn constructor_rejects_invalid_parameters() {
        assert!(CountSketch::new(0.0, 0.1).is_err());
        assert!(CountSketch::new(0.1, 0.0).is_err());
        assert!(CountSketch::new(1.0, 0.1).is_err());
        assert!(CountSketch::new(0.1, 1.0).is_err());
        assert!(CountSketch::new(f64::NAN, 0.1).is_err());
        assert!(CountSketch::with_dimensions(0, 2).is_err());
        assert!(CountSketch::with_dimensions(2, 0).is_err());
    }

    #[test]
    fn estimate_is_reasonable_with_noise() {
        let mut sketch = CountSketch::with_dimensions(2_048, 7).unwrap();

        for _ in 0..5_000 {
            sketch.increment(&"hot-key");
        }
        for value in 0_u64..50_000 {
            sketch.increment(&value);
        }

        let estimate = sketch.estimate(&"hot-key");
        assert!(estimate > 3_500 && estimate < 6_500, "estimate={estimate}");
    }

    #[test]
    fn signed_updates_are_supported() {
        let mut sketch = CountSketch::with_dimensions(1_024, 7).unwrap();
        sketch.add(&"x", 10);
        sketch.add(&"x", -3);
        let estimate = sketch.estimate(&"x");
        assert!(estimate >= 5 && estimate <= 9, "estimate={estimate}");
    }

    #[test]
    fn merge_combines_two_sketches() {
        let mut left = CountSketch::with_dimensions(512, 5).unwrap();
        let mut right = CountSketch::with_dimensions(512, 5).unwrap();

        left.add(&"alpha", 100);
        right.add(&"alpha", 50);
        left.merge(&right).unwrap();
        assert_eq!(left.estimate(&"alpha"), 150);
    }

    #[test]
    fn merge_rejects_mismatched_shape() {
        let mut left = CountSketch::with_dimensions(256, 4).unwrap();
        let right = CountSketch::with_dimensions(128, 4).unwrap();
        assert!(left.merge(&right).is_err());
    }

    #[test]
    fn clear_resets_state() {
        let mut sketch = CountSketch::with_dimensions(128, 4).unwrap();
        sketch.add(&"item", 7);
        assert!(!sketch.is_empty());
        sketch.clear();
        assert_eq!(sketch.estimate(&"item"), 0);
        assert!(sketch.is_empty());
    }
}
