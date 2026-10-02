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
//! Bloom filter for approximate set membership.
//!
//! A Bloom filter can return false positives, but never false negatives.
//! The target false-positive rate passed to [`BloomFilter::new`] is used only
//! to size the filter for the expected number of distinct items. The filter
//! does not report a runtime false-positive rate: that probability also
//! depends on assumptions about hashing and the distribution of absent
//! queries, which the bitmap does not retain.

use std::hash::Hash;

use crate::{SketchError, seeded_hash64};

const HASH_SEED_A: u64 = 0x243F_6A88_85A3_08D3;
const HASH_SEED_B: u64 = 0x1319_8A2E_0370_7344;

/// Probabilistic set-membership filter.
///
/// # Example
/// ```rust
/// use sketches::bloom_filter::BloomFilter;
///
/// let mut filter = BloomFilter::new(1_000, 0.01).unwrap();
/// filter.insert(&"alice");
/// assert!(filter.contains(&"alice"));
/// ```
#[derive(Debug, Clone)]
pub struct BloomFilter {
    bit_len: usize,
    words: Vec<u64>,
    num_hashes: u32,
    inserted_items: u64,
}

impl BloomFilter {
    /// Creates a Bloom filter from an expected number of distinct items and a
    /// target false-positive rate.
    ///
    /// # Errors
    /// Returns [`SketchError::InvalidParameter`] for invalid input values,
    /// unrepresentable recommended dimensions, or a failed bitmap reservation.
    pub fn new(expected_items: usize, false_positive_rate: f64) -> Result<Self, SketchError> {
        let bit_len = Self::optimal_bit_len(expected_items, false_positive_rate)?;
        let num_hashes = Self::optimal_num_hashes(bit_len, expected_items)?;
        Self::with_size(bit_len, num_hashes)
    }

    /// Creates a Bloom filter from explicit bit length and hash count.
    ///
    /// # Errors
    /// Returns [`SketchError::InvalidParameter`] when values are zero or the
    /// bitmap storage cannot be represented or reserved.
    pub fn with_size(bit_len: usize, num_hashes: u32) -> Result<Self, SketchError> {
        if bit_len == 0 {
            return Err(SketchError::InvalidParameter(
                "bit_len must be greater than zero",
            ));
        }
        if num_hashes == 0 {
            return Err(SketchError::InvalidParameter(
                "num_hashes must be greater than zero",
            ));
        }

        let word_len = bit_len.div_ceil(64);
        let mut words = Vec::new();
        words.try_reserve_exact(word_len).map_err(|_| {
            SketchError::InvalidParameter("bitmap storage cannot be represented or reserved")
        })?;
        // The complete reservation precedes initialization, so filling the
        // bitmap cannot require another allocation.
        words.resize(word_len, 0);
        Ok(Self {
            bit_len,
            words,
            num_hashes,
            inserted_items: 0,
        })
    }

    /// Returns the recommended bit length for an expected number of distinct
    /// items and a target rate.
    ///
    /// Formula: `m = -n * ln(p) / (ln(2)^2)`, rounded upward using ordinary
    /// `f64` arithmetic. This recommendation does not allocate or guarantee
    /// that its bitmap fits available memory.
    ///
    /// # Errors
    /// Returns [`SketchError::InvalidParameter`] for invalid parameters or a
    /// rounded recommendation outside the `usize` range.
    pub fn optimal_bit_len(
        expected_items: usize,
        false_positive_rate: f64,
    ) -> Result<usize, SketchError> {
        if expected_items == 0 {
            return Err(SketchError::InvalidParameter(
                "expected_items must be greater than zero",
            ));
        }
        if !false_positive_rate.is_finite()
            || false_positive_rate <= 0.0
            || false_positive_rate >= 1.0
        {
            return Err(SketchError::InvalidParameter(
                "false_positive_rate must be finite and strictly between 0 and 1",
            ));
        }

        let n = expected_items as f64;
        let numerator = -n * false_positive_rate.ln();
        let denominator = std::f64::consts::LN_2.powi(2);
        let bits = (numerator / denominator).ceil();
        // Use the exclusive power-of-two bound: usize::MAX rounds to 2^64
        // when converted to f64 on a 64-bit target.
        let exclusive_limit = 2.0_f64.powi(usize::BITS as i32);
        if !bits.is_finite() || bits >= exclusive_limit {
            return Err(SketchError::InvalidParameter(
                "recommended bit length exceeds usize",
            ));
        }
        Ok((bits as usize).max(1))
    }

    /// Returns the recommended number of hash functions for a bit length and
    /// expected number of distinct items.
    ///
    /// Formula: `k = (m / n) * ln(2)`, rounded to the nearest integer using
    /// ordinary `f64` arithmetic, with a minimum of one hash probe.
    ///
    /// # Errors
    /// Returns [`SketchError::InvalidParameter`] for invalid parameters or a
    /// rounded recommendation outside the `u32` range.
    pub fn optimal_num_hashes(bit_len: usize, expected_items: usize) -> Result<u32, SketchError> {
        if bit_len == 0 {
            return Err(SketchError::InvalidParameter(
                "bit_len must be greater than zero",
            ));
        }
        if expected_items == 0 {
            return Err(SketchError::InvalidParameter(
                "expected_items must be greater than zero",
            ));
        }

        let k = ((bit_len as f64 / expected_items as f64) * std::f64::consts::LN_2).round();
        if !k.is_finite() || k > f64::from(u32::MAX) {
            return Err(SketchError::InvalidParameter(
                "recommended hash count exceeds u32",
            ));
        }
        Ok((k as u32).max(1))
    }

    /// Returns the number of addressable bits.
    pub fn bit_len(&self) -> usize {
        self.bit_len
    }

    /// Returns the configured number of hash probes per inserted key.
    pub fn num_hashes(&self) -> u32 {
        self.num_hashes
    }

    /// Returns the number of `insert` operations applied, including duplicate
    /// items, as a saturating counter.
    ///
    /// This is operational telemetry, not a distinct-item count or a measure
    /// of bitmap load. Merging sums the operation counters from both filters,
    /// even when their inserted items overlap.
    pub fn inserted_items(&self) -> u64 {
        self.inserted_items
    }

    /// Returns `true` if no item has been inserted.
    pub fn is_empty(&self) -> bool {
        self.inserted_items == 0
    }

    /// Inserts an item into the filter.
    pub fn insert<T: Hash>(&mut self, item: &T) {
        let (h1, h2) = self.hash_pair(item);

        let mut probe = h1;
        for _ in 0..self.num_hashes {
            let bit_index = (probe as usize) % self.bit_len;
            self.set_bit(bit_index);
            probe = probe.wrapping_add(h2);
        }

        self.inserted_items = self.inserted_items.saturating_add(1);
    }

    /// Returns `true` if the item is possibly in the set.
    ///
    /// `false` means definitely not present.
    pub fn contains<T: Hash>(&self, item: &T) -> bool {
        let (h1, h2) = self.hash_pair(item);

        let mut probe = h1;
        for _ in 0..self.num_hashes {
            let bit_index = (probe as usize) % self.bit_len;
            if !self.is_bit_set(bit_index) {
                return false;
            }
            probe = probe.wrapping_add(h2);
        }
        true
    }

    /// Clears all bits and resets the insert counter.
    pub fn clear(&mut self) {
        self.words.fill(0);
        self.inserted_items = 0;
    }

    /// Merges another filter into this one by bitwise OR.
    ///
    /// # Errors
    /// Returns [`SketchError::IncompatibleSketches`] when dimensions mismatch.
    pub fn merge(&mut self, other: &Self) -> Result<(), SketchError> {
        if self.bit_len != other.bit_len || self.num_hashes != other.num_hashes {
            return Err(SketchError::IncompatibleSketches(
                "bit_len and num_hashes must match for merge",
            ));
        }

        for (left, right) in self.words.iter_mut().zip(other.words.iter()) {
            *left |= *right;
        }
        self.inserted_items = self.inserted_items.saturating_add(other.inserted_items);
        Ok(())
    }

    /// Returns two independent hashes for Kirsch-Mitzenmacher double hashing.
    fn hash_pair<T: Hash>(&self, item: &T) -> (u64, u64) {
        let first = seeded_hash64(item, HASH_SEED_A);
        let second = seeded_hash64(item, HASH_SEED_B) | 1;
        (first, second)
    }

    /// Sets one bit in the backing bitmap.
    fn set_bit(&mut self, bit_index: usize) {
        let word_index = bit_index / 64;
        let bit_offset = bit_index % 64;
        self.words[word_index] |= 1_u64 << bit_offset;
    }

    /// Checks whether one bit is set in the backing bitmap.
    fn is_bit_set(&self, bit_index: usize) -> bool {
        let word_index = bit_index / 64;
        let bit_offset = bit_index % 64;
        (self.words[word_index] & (1_u64 << bit_offset)) != 0
    }
}

#[cfg(test)]
mod tests {
    use super::BloomFilter;
    use crate::SketchError;

    #[test]
    fn oversized_bit_recommendation_is_rejected_before_construction() {
        for result in [
            BloomFilter::optimal_bit_len(usize::MAX, 0.01),
            BloomFilter::new(usize::MAX, 0.01).map(|filter| filter.bit_len()),
        ] {
            assert_eq!(
                result,
                Err(SketchError::InvalidParameter(
                    "recommended bit length exceeds usize"
                ))
            );
        }
    }

    #[test]
    fn bit_recommendations_respect_integer_range_across_binary_rates() {
        // Enumerate every positive binary power below one, including all
        // subnormals. A wider integer compares the rounded formula with the
        // actual usize maximum, independently of a rounded float bound.
        for exponent in 1..=1074 {
            let rate = if exponent <= 1022 {
                f64::from_bits((1023 - exponent) << 52)
            } else {
                f64::from_bits(1 << (1074 - exponent))
            };
            for count in [1, 2, 1024, usize::MAX / 2, usize::MAX] {
                let rounded =
                    (-(count as f64) * rate.ln() / std::f64::consts::LN_2.powi(2)).ceil() as u128;
                let result = BloomFilter::optimal_bit_len(count, rate);
                if rounded > usize::MAX as u128 {
                    assert!(matches!(result, Err(SketchError::InvalidParameter(_))));
                } else {
                    assert_eq!(result.unwrap() as u128, rounded.max(1));
                }
            }
        }
    }

    #[test]
    fn bit_recommendations_cover_both_sides_of_the_usize_boundary() {
        let boundary_rate = (-std::f64::consts::LN_2.powi(2)).exp();
        let mut accepted = 0;
        let mut rejected = 0;
        for bits in boundary_rate.to_bits() - 8..=boundary_rate.to_bits() + 8 {
            let rate = f64::from_bits(bits);
            let rounded =
                (-(usize::MAX as f64) * rate.ln() / std::f64::consts::LN_2.powi(2)).ceil() as u128;
            let result = BloomFilter::optimal_bit_len(usize::MAX, rate);
            if rounded > usize::MAX as u128 {
                assert!(matches!(result, Err(SketchError::InvalidParameter(_))));
                rejected += 1;
            } else {
                assert_eq!(result.unwrap() as u128, rounded);
                accepted += 1;
            }
        }
        assert!(accepted > 0 && rejected > 0);
    }

    #[test]
    fn sizing_preserves_small_dimensions_and_rejects_nonfinite_rates() {
        let near_one = f64::from_bits(1.0_f64.to_bits() - 1);
        assert_eq!(BloomFilter::optimal_bit_len(1, near_one), Ok(1));
        assert_eq!(BloomFilter::optimal_num_hashes(1, usize::MAX), Ok(1));
        for rate in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY, -0.1] {
            assert!(matches!(
                BloomFilter::optimal_bit_len(1, rate),
                Err(SketchError::InvalidParameter(_))
            ));
        }
        let filter = BloomFilter::new(1, f64::from_bits(1)).unwrap();
        assert!(filter.bit_len() > 1000);
        assert!(filter.num_hashes() > 1000);
    }

    #[test]
    #[cfg(target_pointer_width = "64")]
    fn hash_recommendations_cover_both_sides_of_the_u32_boundary() {
        let boundary = ((f64::from(u32::MAX) + 0.5) / std::f64::consts::LN_2).ceil() as usize;
        let mut accepted = 0;
        let mut rejected = 0;
        for bit_len in boundary - 3..=boundary + 3 {
            let rounded = (bit_len as f64 * std::f64::consts::LN_2).round() as u64;
            let result = BloomFilter::optimal_num_hashes(bit_len, 1);
            if rounded > u64::from(u32::MAX) {
                assert!(matches!(result, Err(SketchError::InvalidParameter(_))));
                rejected += 1;
            } else {
                assert_eq!(u64::from(result.unwrap()), rounded);
                accepted += 1;
            }
        }
        assert!(accepted > 0 && rejected > 0);
        assert!(matches!(
            BloomFilter::optimal_num_hashes(usize::MAX, 1),
            Err(SketchError::InvalidParameter(_))
        ));
    }

    #[test]
    fn explicit_bitmaps_preserve_word_boundaries_and_reuse() {
        for bit_len in [1, 63, 64, 65, 127, 128, 129] {
            let mut filter = BloomFilter::with_size(bit_len, 1).unwrap();
            assert_eq!(filter.words.len(), bit_len.div_ceil(64));
            assert!(filter.words.iter().all(|&word| word == 0));
            filter.insert(&7_u64);
            assert!(filter.contains(&7_u64));
            filter.clear();
            assert!(filter.words.iter().all(|&word| word == 0));
            filter.insert(&11_u64);
            assert!(filter.contains(&11_u64));
        }
    }

    #[test]
    fn constructor_from_rate_creates_positive_shape() {
        let filter = BloomFilter::new(1_000, 0.01).unwrap();
        assert!(filter.bit_len() > 0);
        assert!(filter.num_hashes() > 0);
    }

    #[test]
    fn constructors_validate_parameters() {
        assert!(BloomFilter::new(0, 0.01).is_err());
        assert!(BloomFilter::new(100, 0.0).is_err());
        assert!(BloomFilter::new(100, 1.0).is_err());
        assert!(BloomFilter::with_size(0, 2).is_err());
        assert!(BloomFilter::with_size(64, 0).is_err());
    }

    #[test]
    fn helper_parameter_functions_validate_inputs() {
        assert!(BloomFilter::optimal_bit_len(0, 0.01).is_err());
        assert!(BloomFilter::optimal_bit_len(100, 0.0).is_err());
        assert!(BloomFilter::optimal_num_hashes(0, 100).is_err());
        assert!(BloomFilter::optimal_num_hashes(100, 0).is_err());
    }

    #[test]
    fn inserted_elements_are_always_reported_present() {
        let mut filter = BloomFilter::new(5_000, 0.01).unwrap();
        for value in 0_u64..5_000 {
            filter.insert(&value);
        }
        for value in 0_u64..5_000 {
            assert!(filter.contains(&value));
        }
    }

    #[test]
    fn empirical_false_positive_rate_is_reasonable() {
        let mut filter = BloomFilter::new(4_000, 0.01).unwrap();
        for value in 0_u64..4_000 {
            filter.insert(&value);
        }

        let mut false_positives = 0_u64;
        let test_queries = 4_000_u64;
        for value in 10_000_u64..10_000 + test_queries {
            if filter.contains(&value) {
                false_positives += 1;
            }
        }

        let observed_rate = false_positives as f64 / test_queries as f64;
        assert!(
            observed_rate <= 0.03,
            "observed false-positive rate too high: {observed_rate}"
        );
    }

    #[test]
    fn clear_resets_filter_state() {
        let mut filter = BloomFilter::new(1_000, 0.01).unwrap();
        filter.insert(&"k1");
        filter.insert(&"k2");
        assert!(filter.contains(&"k1"));
        assert!(!filter.is_empty());

        filter.clear();

        assert_eq!(filter.inserted_items(), 0);
        assert!(!filter.contains(&"k1"));
        assert!(filter.is_empty());
    }

    #[test]
    fn merge_combines_two_filters() {
        let mut left = BloomFilter::new(2_000, 0.01).unwrap();
        let mut right = BloomFilter::new(2_000, 0.01).unwrap();

        left.insert(&"left-only");
        right.insert(&"right-only");

        left.merge(&right).unwrap();
        assert!(left.contains(&"left-only"));
        assert!(left.contains(&"right-only"));
        assert_eq!(left.inserted_items(), 2);
    }

    #[test]
    fn merge_rejects_incompatible_filters() {
        let mut left = BloomFilter::with_size(256, 3).unwrap();
        let right = BloomFilter::with_size(512, 3).unwrap();
        assert!(left.merge(&right).is_err());
    }

    #[test]
    fn insert_counter_tracks_operations() {
        let mut filter = BloomFilter::new(100, 0.01).unwrap();
        filter.insert(&"same");
        filter.insert(&"same");
        assert_eq!(filter.inserted_items(), 2);
    }
}
