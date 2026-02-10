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
//! Cuckoo filter for approximate set membership with deletions.
//!
//! Compared to Bloom filters, cuckoo filters support deletion while keeping a
//! compact in-memory representation.

use std::hash::Hash;

use crate::{SketchError, seeded_hash64, splitmix64};

const BUCKET_SIZE: usize = 4;
const DEFAULT_MAX_KICKS: usize = 500;
const INDEX_SEED: u64 = 0x243F_6A88_85A3_08D3;
const FINGERPRINT_SEED: u64 = 0x1319_8A2E_0370_7344;
const ALT_INDEX_SEED: u64 = 0xA409_3822_299F_31D0;

/// Approximate set-membership filter with support for deletion.
///
/// # Example
/// ```rust
/// use sketches::cuckoo_filter::CuckooFilter;
///
/// let mut filter = CuckooFilter::new(10_000, 0.01).unwrap();
/// assert!(filter.insert(&"alice"));
/// assert!(filter.contains(&"alice"));
/// assert!(filter.delete(&"alice"));
/// assert!(!filter.contains(&"alice"));
/// ```
#[derive(Debug, Clone)]
pub struct CuckooFilter {
    buckets: Vec<[u16; BUCKET_SIZE]>,
    fingerprint_bits: u8,
    max_kicks: usize,
    inserted_items: u64,
    rng_state: u64,
}

impl CuckooFilter {
    /// Creates a filter from expected inserts and target false-positive rate.
    ///
    /// # Errors
    /// Returns [`SketchError::InvalidParameter`] for invalid inputs.
    pub fn new(expected_items: usize, false_positive_rate: f64) -> Result<Self, SketchError> {
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

        let fingerprint_bits =
            (((1.0 / false_positive_rate).log2().ceil() as i32) + 1).clamp(4, 16) as u8;
        let buckets = (((expected_items as f64 / BUCKET_SIZE as f64) / 0.90).ceil() as usize)
            .max(2)
            .next_power_of_two();

        Self::with_parameters(buckets, fingerprint_bits, DEFAULT_MAX_KICKS)
    }

    /// Creates a filter from explicit parameters.
    ///
    /// `bucket_count` must be a non-zero power of two.
    ///
    /// # Errors
    /// Returns [`SketchError::InvalidParameter`] for invalid values.
    pub fn with_parameters(
        bucket_count: usize,
        fingerprint_bits: u8,
        max_kicks: usize,
    ) -> Result<Self, SketchError> {
        if bucket_count == 0 || !bucket_count.is_power_of_two() {
            return Err(SketchError::InvalidParameter(
                "bucket_count must be a non-zero power of two",
            ));
        }
        if fingerprint_bits == 0 || fingerprint_bits > 16 {
            return Err(SketchError::InvalidParameter(
                "fingerprint_bits must be in the inclusive range [1, 16]",
            ));
        }
        if max_kicks == 0 {
            return Err(SketchError::InvalidParameter(
                "max_kicks must be greater than zero",
            ));
        }

        Ok(Self {
            buckets: vec![[0; BUCKET_SIZE]; bucket_count],
            fingerprint_bits,
            max_kicks,
            inserted_items: 0,
            rng_state: 0xD6E8_FD93_5E7A_4A6D,
        })
    }

    /// Returns the number of buckets.
    pub fn bucket_count(&self) -> usize {
        self.buckets.len()
    }

    /// Returns the fingerprint width in bits.
    pub fn fingerprint_bits(&self) -> u8 {
        self.fingerprint_bits
    }

    /// Returns the total number of successful insertions minus deletions.
    pub fn inserted_items(&self) -> u64 {
        self.inserted_items
    }

    /// Returns `true` when no items have been inserted.
    pub fn is_empty(&self) -> bool {
        self.inserted_items == 0
    }

    /// Returns current slot utilization in `[0, 1]`.
    pub fn load_factor(&self) -> f64 {
        let capacity = (self.buckets.len() * BUCKET_SIZE) as f64;
        if capacity == 0.0 {
            return 0.0;
        }
        self.inserted_items as f64 / capacity
    }

    /// Returns a simple expected false-positive-rate approximation.
    pub fn expected_false_positive_rate(&self) -> f64 {
        let denominator = (1_u64 << self.fingerprint_bits) as f64;
        ((2.0 * BUCKET_SIZE as f64) / denominator).min(1.0)
    }

    /// Inserts one item into the filter.
    ///
    /// Returns `false` when insertion fails after relocation attempts.
    pub fn insert<T: Hash>(&mut self, item: &T) -> bool {
        let mut fingerprint = self.fingerprint(item);
        let (index_a, index_b) = self.bucket_indexes(item, fingerprint);

        if self.insert_into_bucket(index_a, fingerprint) || self.insert_into_bucket(index_b, fingerprint) {
            self.inserted_items = self.inserted_items.saturating_add(1);
            return true;
        }

        let mut bucket = if (self.next_u64() & 1) == 0 {
            index_a
        } else {
            index_b
        };

        for _ in 0..self.max_kicks {
            let slot = (self.next_u64() as usize) % BUCKET_SIZE;
            std::mem::swap(&mut fingerprint, &mut self.buckets[bucket][slot]);
            bucket = self.alternate_index(bucket, fingerprint);

            if self.insert_into_bucket(bucket, fingerprint) {
                self.inserted_items = self.inserted_items.saturating_add(1);
                return true;
            }
        }

        false
    }

    /// Returns `true` if the item is possibly in the set.
    pub fn contains<T: Hash>(&self, item: &T) -> bool {
        let fingerprint = self.fingerprint(item);
        let (index_a, index_b) = self.bucket_indexes(item, fingerprint);
        self.bucket_contains(index_a, fingerprint) || self.bucket_contains(index_b, fingerprint)
    }

    /// Deletes one item instance.
    ///
    /// Returns `true` if a matching fingerprint was removed.
    pub fn delete<T: Hash>(&mut self, item: &T) -> bool {
        let fingerprint = self.fingerprint(item);
        let (index_a, index_b) = self.bucket_indexes(item, fingerprint);

        if self.remove_from_bucket(index_a, fingerprint) || self.remove_from_bucket(index_b, fingerprint) {
            self.inserted_items = self.inserted_items.saturating_sub(1);
            return true;
        }
        false
    }

    /// Clears all buckets and resets counters.
    pub fn clear(&mut self) {
        for bucket in &mut self.buckets {
            *bucket = [0; BUCKET_SIZE];
        }
        self.inserted_items = 0;
    }

    fn insert_into_bucket(&mut self, bucket_index: usize, fingerprint: u16) -> bool {
        for slot in &mut self.buckets[bucket_index] {
            if *slot == 0 {
                *slot = fingerprint;
                return true;
            }
        }
        false
    }

    fn remove_from_bucket(&mut self, bucket_index: usize, fingerprint: u16) -> bool {
        for slot in &mut self.buckets[bucket_index] {
            if *slot == fingerprint {
                *slot = 0;
                return true;
            }
        }
        false
    }

    fn bucket_contains(&self, bucket_index: usize, fingerprint: u16) -> bool {
        self.buckets[bucket_index].contains(&fingerprint)
    }

    fn primary_index<T: Hash>(&self, item: &T) -> usize {
        (seeded_hash64(item, INDEX_SEED) as usize) & (self.buckets.len() - 1)
    }

    fn bucket_indexes<T: Hash>(&self, item: &T, fingerprint: u16) -> (usize, usize) {
        let index_a = self.primary_index(item);
        let index_b = self.alternate_index(index_a, fingerprint);
        (index_a, index_b)
    }

    fn alternate_index(&self, index: usize, fingerprint: u16) -> usize {
        let hashed_fingerprint = seeded_hash64(&fingerprint, ALT_INDEX_SEED) as usize;
        (index ^ hashed_fingerprint) & (self.buckets.len() - 1)
    }

    fn fingerprint<T: Hash>(&self, item: &T) -> u16 {
        let hash = seeded_hash64(item, FINGERPRINT_SEED);
        let mask = if self.fingerprint_bits == 16 {
            u64::from(u16::MAX)
        } else {
            (1_u64 << self.fingerprint_bits) - 1
        };

        let fingerprint = (hash & mask) as u16;
        fingerprint.max(1)
    }

    fn next_u64(&mut self) -> u64 {
        self.rng_state = splitmix64(self.rng_state.wrapping_add(0x9E37_79B9_7F4A_7C15));
        self.rng_state
    }
}

#[cfg(test)]
mod tests {
    use super::CuckooFilter;

    #[test]
    fn constructor_validates_parameters() {
        assert!(CuckooFilter::new(0, 0.01).is_err());
        assert!(CuckooFilter::new(100, 0.0).is_err());
        assert!(CuckooFilter::new(100, 1.0).is_err());
        assert!(CuckooFilter::with_parameters(3, 8, 100).is_err());
        assert!(CuckooFilter::with_parameters(8, 0, 100).is_err());
        assert!(CuckooFilter::with_parameters(8, 8, 0).is_err());
    }

    #[test]
    fn insert_contains_delete_roundtrip() {
        let mut filter = CuckooFilter::new(1_000, 0.01).unwrap();
        assert!(filter.insert(&"alice"));
        assert!(filter.contains(&"alice"));
        assert!(filter.delete(&"alice"));
        assert!(!filter.contains(&"alice"));
    }

    #[test]
    fn load_factor_increases_with_inserts() {
        let mut filter = CuckooFilter::new(1_000, 0.01).unwrap();
        let before = filter.load_factor();
        for value in 0_u64..300 {
            assert!(filter.insert(&value));
        }
        let after = filter.load_factor();
        assert!(after > before);
    }

    #[test]
    fn tiny_filter_eventually_refuses_insert() {
        let mut filter = CuckooFilter::with_parameters(2, 4, 50).unwrap();
        let mut accepted = 0;
        for value in 0_u64..100 {
            if filter.insert(&value) {
                accepted += 1;
            }
        }
        assert!(accepted < 100);
    }

    #[test]
    fn empirical_false_positive_rate_is_reasonable() {
        let mut filter = CuckooFilter::new(2_000, 0.01).unwrap();
        for value in 0_u64..2_000 {
            assert!(filter.insert(&value));
        }

        let mut false_positives = 0_u64;
        let trials = 2_000_u64;
        for value in 20_000_u64..(20_000 + trials) {
            if filter.contains(&value) {
                false_positives += 1;
            }
        }

        let rate = false_positives as f64 / trials as f64;
        assert!(rate < 0.10, "rate={rate}");
    }

    #[test]
    fn deleting_unknown_item_returns_false() {
        let mut filter = CuckooFilter::new(100, 0.01).unwrap();
        assert!(!filter.delete(&"ghost"));
    }
}
