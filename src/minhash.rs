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
//! MinHash sketch for approximate Jaccard similarity.

use std::hash::Hash;

use crate::{SketchError, seeded_hash64, splitmix64};
use crate::jacard::JacardIndex;

/// MinHash signature sketch for estimating set similarity.
///
/// # Example
/// ```rust
/// use sketches::minhash::MinHash;
///
/// let mut left = MinHash::new(256).unwrap();
/// let mut right = MinHash::new(256).unwrap();
///
/// for value in 0_u64..10_000 {
///     left.add(&value);
/// }
/// for value in 5_000_u64..15_000 {
///     right.add(&value);
/// }
///
/// // Exact Jaccard is 5_000 / 15_000 = 0.333...
/// let estimate = left.estimate_jaccard(&right).unwrap();
/// assert!(estimate > 0.20 && estimate < 0.45);
/// ```
#[derive(Debug, Clone)]
pub struct MinHash {
    seeds: Vec<u64>,
    signature: Vec<u64>,
    observed_any: bool,
}

impl MinHash {
    /// Creates a MinHash sketch with `num_hashes` signature components.
    ///
    /// # Errors
    /// Returns [`SketchError::InvalidParameter`] when `num_hashes == 0`.
    pub fn new(num_hashes: usize) -> Result<Self, SketchError> {
        if num_hashes == 0 {
            return Err(SketchError::InvalidParameter(
                "num_hashes must be greater than zero",
            ));
        }

        let seeds = (0..num_hashes)
            .map(|index| splitmix64((index as u64).wrapping_add(0xBF58_476D_1CE4_E5B9)))
            .collect();

        Ok(Self {
            seeds,
            signature: vec![u64::MAX; num_hashes],
            observed_any: false,
        })
    }

    /// Creates a MinHash sketch from a target standard error.
    ///
    /// Uses the relation `std_error ≈ 1 / sqrt(num_hashes)`, i.e.
    /// `num_hashes = ceil(1 / std_error^2)`.
    ///
    /// # Errors
    /// Returns [`SketchError::InvalidParameter`] for invalid `std_error`.
    pub fn with_error_rate(std_error: f64) -> Result<Self, SketchError> {
        if !std_error.is_finite() || std_error <= 0.0 || std_error >= 1.0 {
            return Err(SketchError::InvalidParameter(
                "std_error must be finite and strictly between 0 and 1",
            ));
        }

        let num_hashes = (1.0 / (std_error * std_error)).ceil() as usize;
        Self::new(num_hashes.max(1))
    }

    /// Returns the number of signature components.
    pub fn num_hashes(&self) -> usize {
        self.signature.len()
    }

    /// Returns the expected standard error approximation.
    pub fn expected_error(&self) -> f64 {
        1.0 / (self.num_hashes() as f64).sqrt()
    }

    /// Returns `true` when no item has been observed yet.
    pub fn is_empty(&self) -> bool {
        !self.observed_any
    }

    /// Returns a read-only view of the signature vector.
    pub fn signature(&self) -> &[u64] {
        &self.signature
    }

    /// Adds one item to the sketch.
    pub fn add<T: Hash>(&mut self, item: &T) {
        for (index, seed) in self.seeds.iter().enumerate() {
            let hashed = seeded_hash64(item, *seed);
            if hashed < self.signature[index] {
                self.signature[index] = hashed;
            }
        }
        self.observed_any = true;
    }

    /// Estimates Jaccard similarity against another MinHash sketch.
    ///
    /// # Errors
    /// Returns [`SketchError::IncompatibleSketches`] when the hash families
    /// differ.
    pub fn estimate_jaccard(&self, other: &Self) -> Result<f64, SketchError> {
        if self.seeds != other.seeds {
            return Err(SketchError::IncompatibleSketches(
                "num_hashes/hash seeds must match",
            ));
        }

        match (self.observed_any, other.observed_any) {
            (false, false) => return Ok(1.0),
            (false, true) | (true, false) => return Ok(0.0),
            (true, true) => {}
        }

        let matches = self
            .signature
            .iter()
            .zip(other.signature.iter())
            .filter(|(left, right)| left == right)
            .count();
        Ok(matches as f64 / self.signature.len() as f64)
    }

    /// Merges another sketch in-place by taking element-wise minima.
    ///
    /// # Errors
    /// Returns [`SketchError::IncompatibleSketches`] for incompatible seeds.
    pub fn merge(&mut self, other: &Self) -> Result<(), SketchError> {
        if self.seeds != other.seeds {
            return Err(SketchError::IncompatibleSketches(
                "num_hashes/hash seeds must match for merge",
            ));
        }

        for (left, right) in self.signature.iter_mut().zip(other.signature.iter()) {
            *left = (*left).min(*right);
        }
        self.observed_any |= other.observed_any;
        Ok(())
    }

    /// Resets the sketch to the empty state.
    pub fn clear(&mut self) {
        self.signature.fill(u64::MAX);
        self.observed_any = false;
    }
}

impl JacardIndex for MinHash {
    fn jaccard_index(&self, other: &Self) -> Result<f64, SketchError> {
        self.estimate_jaccard(other)
    }
}

#[cfg(test)]
mod tests {
    use super::MinHash;

    #[test]
    fn constructor_validates_num_hashes() {
        assert!(MinHash::new(0).is_err());
        assert!(MinHash::new(64).is_ok());
    }

    #[test]
    fn jaccard_estimate_is_reasonable_for_overlap() {
        let mut left = MinHash::new(256).unwrap();
        let mut right = MinHash::new(256).unwrap();

        for value in 0_u64..10_000 {
            left.add(&value);
        }
        for value in 5_000_u64..15_000 {
            right.add(&value);
        }

        let estimate = left.estimate_jaccard(&right).unwrap();
        let exact = 5_000.0 / 15_000.0;
        assert!((estimate - exact).abs() < 0.15, "estimate={estimate} exact={exact}");
    }

    #[test]
    fn identical_sets_have_high_similarity() {
        let mut left = MinHash::new(128).unwrap();
        let mut right = MinHash::new(128).unwrap();

        for value in 0_u64..5_000 {
            left.add(&value);
            right.add(&value);
        }

        let estimate = left.estimate_jaccard(&right).unwrap();
        assert!(estimate > 0.90, "estimate={estimate}");
    }

    #[test]
    fn empty_semantics_are_supported() {
        let left = MinHash::new(64).unwrap();
        let mut right = MinHash::new(64).unwrap();
        right.add(&"x");

        assert_eq!(left.estimate_jaccard(&left).unwrap(), 1.0);
        assert_eq!(left.estimate_jaccard(&right).unwrap(), 0.0);
    }

    #[test]
    fn merge_uses_elementwise_min() {
        let mut left = MinHash::new(64).unwrap();
        let mut right = MinHash::new(64).unwrap();
        for value in 0_u64..1_000 {
            left.add(&value);
        }
        for value in 500_u64..1_500 {
            right.add(&value);
        }

        let mut merged = left.clone();
        merged.merge(&right).unwrap();

        for index in 0..merged.signature().len() {
            assert_eq!(
                merged.signature()[index],
                left.signature()[index].min(right.signature()[index])
            );
        }
    }

    #[test]
    fn merge_rejects_incompatible_sketches() {
        let mut left = MinHash::new(64).unwrap();
        let right = MinHash::new(65).unwrap();
        assert!(left.merge(&right).is_err());
        assert!(left.estimate_jaccard(&right).is_err());
    }

    #[test]
    fn clear_resets_state() {
        let mut sketch = MinHash::new(64).unwrap();
        sketch.add(&"alpha");
        sketch.clear();
        assert!(sketch.is_empty());
        assert!(sketch.signature().iter().all(|&value| value == u64::MAX));
    }
}
