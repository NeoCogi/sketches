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
//!
//! # Safe deletion precondition
//!
//! [`CuckooFilter::delete`] may be called safely only for an item instance that
//! the caller knows was previously inserted successfully and has not already
//! been deleted. A positive [`CuckooFilter::contains`] result is not sufficient
//! evidence because it may be a false positive. Deleting a non-member that
//! collides with a stored fingerprint can remove a different real member and
//! introduce a false negative. This is the deletion precondition described in
//! Section 3.3 of the [paper]. Safe deletion of arbitrary keys requires exact
//! membership information outside the filter.
//!
//! # Difference from the original insertion algorithm
//!
//! [Algorithm 1 in the original Cuckoo Filter paper][paper] returns failure
//! after exhausting its bounded random kick loop and considers the table full;
//! it does not reverse the swaps already made by that attempt. This
//! implementation provides a stronger failure guarantee: it records each
//! swapped slot and, if all kicks fail, reverses the complete chain and restores
//! the random-number-generator state. Therefore, [`CuckooFilter::insert`]
//! returning `false` leaves the stored fingerprints, item count, load factor,
//! and future random-kick sequence unchanged.
//!
//! Both algorithms take `O(max_kicks)` time in the worst case. Atomic failure
//! adds an `O(max_kicks)` reverse pass after an unsuccessful insertion and an
//! `O(max_kicks)` reusable rollback log per filter. Successful relocation adds
//! one sequential log write per kick; insertions into an empty candidate slot
//! do not use the log.
//!
//! # Parameter choices relative to the paper
//!
//! Section 4 of the [paper] reports that four-entry buckets sustain about 95%
//! occupancy in large practical filters once fingerprints are at least six
//! bits wide, while shorter fingerprints can fail earlier because partial-key
//! cuckoo hashing offers too few distinct bucket pairs. Both constructors
//! therefore reject fingerprint widths outside `6..=16`.
//!
//! The paper and its reference implementation use 500 as `MaxNumKicks`, which
//! is also the default used by [`CuckooFilter::new`]. Applications that prefer
//! more relocation work in exchange for fewer early failures near capacity can
//! opt into a larger limit through [`CuckooFilter::with_parameters`].
//!
//! # False-positive-rate sizing
//!
//! Equation 6 in the [paper] sizes a fingerprint for two full candidate
//! buckets as `ceil(log2(2 * bucket_size / false_positive_rate))`. This
//! implementation uses the corresponding conservative comparison bound, with
//! one adjustment: fingerprint value zero marks an empty slot, so a zero hash
//! fingerprint is remapped to one. For `q = 2^fingerprint_bits`, that mapping
//! makes the collision probability of two fingerprints `(q + 2) / q^2`
//! instead of `1 / q`. [`CuckooFilter::new`] chooses the smallest automatic
//! width (at least six bits) whose full-bucket bound meets the requested rate
//! and rejects rates that would require more than 16 bits.
//!
//! [paper]: https://www.cs.cmu.edu/~dga/papers/cuckoo-conext2014.pdf

use std::hash::Hash;

use crate::{SketchError, seeded_hash64, splitmix64};

const BUCKET_SIZE: usize = 4;
const DEFAULT_MAX_KICKS: usize = 500;
const MAX_TARGET_LOAD_FACTOR: f64 = 0.96;
const MIN_FINGERPRINT_BITS: u8 = 6;
const MAX_FINGERPRINT_BITS: u8 = 16;
const INDEX_SEED: u64 = 0x243F_6A88_85A3_08D3;
const FINGERPRINT_SEED: u64 = 0x1319_8A2E_0370_7344;
const ALT_INDEX_SEED: u64 = 0xA409_3822_299F_31D0;

/// Probability that two independently hashed fingerprints collide after the
/// reserved zero value is remapped to one.
fn fingerprint_collision_probability(fingerprint_bits: u8) -> f64 {
    let possibilities = (1_u64 << fingerprint_bits) as f64;
    (possibilities + 2.0) / possibilities.powi(2)
}

/// Union bound for matching any entry across two completely full buckets.
fn full_bucket_false_positive_rate_bound(fingerprint_bits: u8) -> f64 {
    (2.0 * BUCKET_SIZE as f64 * fingerprint_collision_probability(fingerprint_bits)).min(1.0)
}

/// Chooses the smallest power-of-two bucket count whose target occupancy does
/// not exceed the 96% threshold used by the reference implementation.
fn bucket_count_for_expected_items(expected_items: usize) -> Result<usize, SketchError> {
    debug_assert!(expected_items > 0);

    let minimum_buckets = expected_items.div_ceil(BUCKET_SIZE).max(2);
    let mut buckets =
        minimum_buckets
            .checked_next_power_of_two()
            .ok_or(SketchError::InvalidParameter(
                "expected_items requires too many buckets",
            ))?;
    let target_load = expected_items as f64 / (buckets as f64 * BUCKET_SIZE as f64);

    if target_load > MAX_TARGET_LOAD_FACTOR {
        buckets = buckets.checked_mul(2).ok_or(SketchError::InvalidParameter(
            "expected_items requires too many buckets",
        ))?;
    }
    Ok(buckets)
}

/// Byte-aligned storage for buckets of four packed fingerprints.
///
/// A bucket uses `ceil(BUCKET_SIZE * fingerprint_bits / 8)` bytes. Keeping
/// buckets byte-aligned makes each lookup touch one contiguous byte range while
/// wasting at most four padding bits per bucket for odd fingerprint widths.
#[derive(Debug, Clone, PartialEq, Eq)]
struct PackedBuckets {
    bytes: Vec<u8>,
    bucket_count: usize,
    bytes_per_bucket: usize,
    fingerprint_bits: u8,
}

impl PackedBuckets {
    fn new(bucket_count: usize, fingerprint_bits: u8) -> Result<Self, SketchError> {
        let bits_per_bucket = BUCKET_SIZE * usize::from(fingerprint_bits);
        let bytes_per_bucket = bits_per_bucket.div_ceil(8);
        let storage_len =
            bucket_count
                .checked_mul(bytes_per_bucket)
                .ok_or(SketchError::InvalidParameter(
                    "packed bucket storage size overflows usize",
                ))?;
        // A small zeroed suffix lets every bucket be decoded with one safe
        // eight-byte load, including the final bucket and widths below 16.
        let allocation_len = storage_len
            .checked_add(std::mem::size_of::<u64>() - 1)
            .ok_or(SketchError::InvalidParameter(
                "packed bucket storage size overflows usize",
            ))?;

        Ok(Self {
            bytes: vec![0; allocation_len],
            bucket_count,
            bytes_per_bucket,
            fingerprint_bits,
        })
    }

    fn len(&self) -> usize {
        self.bucket_count
    }

    #[cfg(test)]
    fn storage_len(&self) -> usize {
        self.bucket_count * self.bytes_per_bucket
    }

    fn clear(&mut self) {
        self.bytes.fill(0);
    }

    fn contains(&self, bucket: usize, fingerprint: u16) -> bool {
        let word = self.read_bucket(bucket);
        let mask = self.fingerprint_mask();

        (0..BUCKET_SIZE)
            .any(|slot| ((word >> self.slot_shift(slot)) & mask) == u64::from(fingerprint))
    }

    #[cfg(test)]
    fn has_empty(&self, bucket: usize) -> bool {
        self.contains(bucket, 0)
    }

    fn insert(&mut self, bucket: usize, fingerprint: u16) -> bool {
        debug_assert_ne!(fingerprint, 0);
        debug_assert!(u64::from(fingerprint) <= self.fingerprint_mask());

        let mut word = self.read_bucket(bucket);
        let mask = self.fingerprint_mask();

        for slot in 0..BUCKET_SIZE {
            let shift = self.slot_shift(slot);
            if ((word >> shift) & mask) == 0 {
                word |= u64::from(fingerprint) << shift;
                self.write_bucket(bucket, word);
                return true;
            }
        }
        false
    }

    fn remove(&mut self, bucket: usize, fingerprint: u16) -> bool {
        let mut word = self.read_bucket(bucket);
        let mask = self.fingerprint_mask();

        for slot in 0..BUCKET_SIZE {
            let shift = self.slot_shift(slot);
            if ((word >> shift) & mask) == u64::from(fingerprint) {
                word &= !(mask << shift);
                self.write_bucket(bucket, word);
                return true;
            }
        }
        false
    }

    fn swap_slot(&mut self, bucket: usize, slot: usize, fingerprint: &mut u16) {
        debug_assert!(slot < BUCKET_SIZE);
        debug_assert!(u64::from(*fingerprint) <= self.fingerprint_mask());

        let mut word = self.read_bucket(bucket);
        let mask = self.fingerprint_mask();
        let shift = self.slot_shift(slot);
        let previous = ((word >> shift) & mask) as u16;

        word = (word & !(mask << shift)) | (u64::from(*fingerprint) << shift);
        self.write_bucket(bucket, word);
        *fingerprint = previous;
    }

    #[cfg(test)]
    fn read_slot(&self, bucket: usize, slot: usize) -> u16 {
        debug_assert!(slot < BUCKET_SIZE);
        ((self.read_bucket(bucket) >> self.slot_shift(slot)) & self.fingerprint_mask()) as u16
    }

    fn fingerprint_mask(&self) -> u64 {
        (1_u64 << self.fingerprint_bits) - 1
    }

    fn slot_shift(&self, slot: usize) -> usize {
        slot * usize::from(self.fingerprint_bits)
    }

    fn read_bucket(&self, bucket: usize) -> u64 {
        debug_assert!(bucket < self.bucket_count);
        let start = bucket * self.bytes_per_bucket;
        let bytes = self.bytes[start..start + std::mem::size_of::<u64>()]
            .try_into()
            .expect("packed bucket storage always has read padding");
        u64::from_le_bytes(bytes)
    }

    fn write_bucket(&mut self, bucket: usize, word: u64) {
        debug_assert!(bucket < self.bucket_count);
        let start = bucket * self.bytes_per_bucket;
        let destination = &mut self.bytes[start..start + self.bytes_per_bucket];
        destination.copy_from_slice(&word.to_le_bytes()[..destination.len()]);
    }
}

/// Approximate set-membership filter with support for deletion.
///
/// Each four-entry bucket stores fingerprints in a byte-aligned packed field,
/// using `ceil(4 * fingerprint_bits / 8)` bytes rather than four fixed-width
/// integers.
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
    buckets: PackedBuckets,
    max_kicks: usize,
    inserted_items: u64,
    rng_state: u64,
    /// Reusable flattened slot indexes for reversing a failed kick chain.
    relocation_log: Vec<usize>,
}

impl CuckooFilter {
    /// Creates a filter from expected inserts and target false-positive rate.
    ///
    /// The fingerprint width is the smallest value in the automatic range
    /// `6..=16` whose conservative full-bucket false-positive-rate bound meets
    /// `false_positive_rate`. The calculation follows Equation 6 of the
    /// original Cuckoo Filter paper and accounts for this implementation's
    /// remapping of the reserved zero fingerprint to one.
    /// Six bits is the minimum because shorter partial-key fingerprints may not
    /// sustain high occupancy in large tables.
    ///
    /// The bucket count is the smallest supported power of two whose target
    /// load does not exceed 96%, matching the reference implementation's
    /// sizing threshold. `expected_items` is a sizing target rather than a
    /// successful-insertion guarantee: the randomized 500-kick insertion can
    /// still fail before that count, especially near the target load.
    ///
    /// # Errors
    /// Returns [`SketchError::InvalidParameter`] for invalid inputs or when the
    /// requested false-positive rate would require fingerprints wider than 16
    /// bits.
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

        let fingerprint_bits = (MIN_FINGERPRINT_BITS..=MAX_FINGERPRINT_BITS)
            .find(|&bits| full_bucket_false_positive_rate_bound(bits) <= false_positive_rate)
            .ok_or(SketchError::InvalidParameter(
                "false_positive_rate requires fingerprints wider than 16 bits",
            ))?;
        let buckets = bucket_count_for_expected_items(expected_items)?;

        Self::with_parameters(buckets, fingerprint_bits, DEFAULT_MAX_KICKS)
    }

    /// Creates a filter from explicit parameters.
    ///
    /// `bucket_count` must be a non-zero power of two.
    /// `fingerprint_bits` must be in `6..=16`, enforcing the practical minimum
    /// reported for four-entry buckets in Section 4 of the paper.
    /// `max_kicks = 500` selects the paper/reference limit used by the automatic
    /// constructor. Larger values trade additional worst-case insertion and
    /// rollback work for fewer early failures near capacity.
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
        if !(MIN_FINGERPRINT_BITS..=MAX_FINGERPRINT_BITS).contains(&fingerprint_bits) {
            return Err(SketchError::InvalidParameter(
                "fingerprint_bits must be in the inclusive range [6, 16]",
            ));
        }
        if max_kicks == 0 {
            return Err(SketchError::InvalidParameter(
                "max_kicks must be greater than zero",
            ));
        }

        Ok(Self {
            buckets: PackedBuckets::new(bucket_count, fingerprint_bits)?,
            max_kicks,
            inserted_items: 0,
            rng_state: 0xD6E8_FD93_5E7A_4A6D,
            relocation_log: Vec::new(),
        })
    }

    /// Returns the number of buckets.
    pub fn bucket_count(&self) -> usize {
        self.buckets.len()
    }

    /// Returns the fingerprint width in bits.
    pub fn fingerprint_bits(&self) -> u8 {
        self.buckets.fingerprint_bits
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

    /// Returns a conservative false-positive-rate bound for two full buckets.
    ///
    /// This is the union bound across all eight possible fingerprint
    /// comparisons. It accounts for remapping the reserved zero fingerprint to
    /// one and is therefore slightly higher than the paper's `8 / 2^f`
    /// approximation. The value is not load-aware; partially filled filters
    /// normally have a lower false-positive rate.
    pub fn expected_false_positive_rate(&self) -> f64 {
        full_bucket_false_positive_rate_bound(self.fingerprint_bits())
    }

    /// Inserts one item into the filter.
    ///
    /// Returns `false` when no empty slot is found within `max_kicks` random
    /// relocations. A failed insertion reverses every relocation and leaves
    /// all membership state unchanged.
    ///
    /// Unlike Algorithm 1 in the original paper, this method is failure-atomic:
    /// its rollback log uses `O(max_kicks)` retained memory and adds an
    /// `O(max_kicks)` reverse pass only when insertion fails. The bounded
    /// worst-case insertion time remains `O(max_kicks)`.
    pub fn insert<T: Hash>(&mut self, item: &T) -> bool {
        let mut fingerprint = self.fingerprint(item);
        let original_fingerprint = fingerprint;
        let (index_a, index_b) = self.bucket_indexes(item, fingerprint);

        if self.insert_into_bucket(index_a, fingerprint)
            || self.insert_into_bucket(index_b, fingerprint)
        {
            self.inserted_items = self.inserted_items.saturating_add(1);
            return true;
        }

        self.relocation_log.clear();
        if self.relocation_log.try_reserve(self.max_kicks).is_err() {
            return false;
        }

        let rng_state_before = self.rng_state;
        let mut bucket = if (self.next_u64() & 1) == 0 {
            index_a
        } else {
            index_b
        };

        for _ in 0..self.max_kicks {
            let slot = (self.next_u64() as usize) % BUCKET_SIZE;
            self.relocation_log.push(bucket * BUCKET_SIZE + slot);
            self.buckets.swap_slot(bucket, slot, &mut fingerprint);
            bucket = self.alternate_index(bucket, fingerprint);

            if self.insert_into_bucket(bucket, fingerprint) {
                self.inserted_items = self.inserted_items.saturating_add(1);
                self.relocation_log.clear();
                return true;
            }
        }

        self.rollback_relocations(&mut fingerprint);
        self.rng_state = rng_state_before;
        self.relocation_log.clear();
        debug_assert_eq!(fingerprint, original_fingerprint);
        false
    }

    /// Returns `true` if the item is possibly in the set.
    pub fn contains<T: Hash>(&self, item: &T) -> bool {
        let fingerprint = self.fingerprint(item);
        let (index_a, index_b) = self.bucket_indexes(item, fingerprint);
        self.bucket_contains(index_a, fingerprint) || self.bucket_contains(index_b, fingerprint)
    }

    /// Deletes one known-present item instance.
    ///
    /// Call this method only when the caller knows that this item instance was
    /// previously inserted successfully and has not already been deleted. A
    /// positive [`Self::contains`] result does not establish that precondition:
    /// it may be a false positive. Deleting a non-member can remove a different
    /// real item with a colliding fingerprint and introduce a false negative.
    /// Safe deletion of arbitrary keys requires exact membership tracking
    /// outside the filter.
    ///
    /// Returns `true` if a matching fingerprint was removed. Because the filter
    /// stores fingerprints rather than complete items, `true` does not prove
    /// that the fingerprint belonged uniquely to `item`.
    pub fn delete<T: Hash>(&mut self, item: &T) -> bool {
        let fingerprint = self.fingerprint(item);
        let (index_a, index_b) = self.bucket_indexes(item, fingerprint);

        // Exact identity is unavailable here; callers must uphold the
        // known-present precondition documented above.
        if self.remove_from_bucket(index_a, fingerprint)
            || self.remove_from_bucket(index_b, fingerprint)
        {
            self.inserted_items = self.inserted_items.saturating_sub(1);
            return true;
        }
        false
    }

    /// Clears all buckets and resets counters.
    pub fn clear(&mut self) {
        self.buckets.clear();
        self.inserted_items = 0;
        self.relocation_log.clear();
    }

    fn insert_into_bucket(&mut self, bucket_index: usize, fingerprint: u16) -> bool {
        self.buckets.insert(bucket_index, fingerprint)
    }

    fn remove_from_bucket(&mut self, bucket_index: usize, fingerprint: u16) -> bool {
        self.buckets.remove(bucket_index, fingerprint)
    }

    fn bucket_contains(&self, bucket_index: usize, fingerprint: u16) -> bool {
        self.buckets.contains(bucket_index, fingerprint)
    }

    /// Reverses the paper-style swap chain after exhausting `max_kicks`.
    fn rollback_relocations(&mut self, fingerprint: &mut u16) {
        for &location in self.relocation_log.iter().rev() {
            let bucket = location / BUCKET_SIZE;
            let slot = location % BUCKET_SIZE;
            self.buckets.swap_slot(bucket, slot, fingerprint);
        }
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
        let fingerprint_bits = self.fingerprint_bits();
        let mask = if fingerprint_bits == 16 {
            u64::from(u16::MAX)
        } else {
            (1_u64 << fingerprint_bits) - 1
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
    use super::{
        BUCKET_SIZE, CuckooFilter, MAX_FINGERPRINT_BITS, MAX_TARGET_LOAD_FACTOR,
        MIN_FINGERPRINT_BITS, PackedBuckets, bucket_count_for_expected_items,
        fingerprint_collision_probability, full_bucket_false_positive_rate_bound,
    };

    #[test]
    fn packed_buckets_roundtrip_every_encodable_width() {
        let bucket_count = 3;

        for fingerprint_bits in 1..=MAX_FINGERPRINT_BITS {
            let mut buckets = PackedBuckets::new(bucket_count, fingerprint_bits).unwrap();
            let expected_bytes =
                bucket_count * (BUCKET_SIZE * usize::from(fingerprint_bits)).div_ceil(8);
            let mask = (1_u64 << fingerprint_bits) - 1;
            let mut expected = [[0_u16; BUCKET_SIZE]; 3];

            assert_eq!(buckets.storage_len(), expected_bytes);

            for (bucket, expected_bucket) in expected.iter_mut().enumerate() {
                for (slot, expected_slot) in expected_bucket.iter_mut().enumerate() {
                    let value = ((((bucket * BUCKET_SIZE + slot + 1) as u64) & mask).max(1)) as u16;
                    let mut incoming = value;
                    buckets.swap_slot(bucket, slot, &mut incoming);
                    assert_eq!(incoming, 0);
                    *expected_slot = value;
                }
            }

            for (bucket, expected_bucket) in expected.iter().enumerate() {
                assert!(!buckets.has_empty(bucket));
                for (slot, &value) in expected_bucket.iter().enumerate() {
                    assert_eq!(buckets.read_slot(bucket, slot), value);
                    assert!(buckets.contains(bucket, value));
                }
            }

            buckets.clear();
            for bucket in 0..bucket_count {
                assert!(buckets.has_empty(bucket));
                for slot in 0..BUCKET_SIZE {
                    assert_eq!(buckets.read_slot(bucket, slot), 0);
                }
            }

            for (bucket, expected_bucket) in expected.iter().enumerate() {
                for &value in expected_bucket {
                    assert!(buckets.insert(bucket, value));
                }
                assert!(!buckets.insert(bucket, 1));

                for &value in expected_bucket {
                    assert!(buckets.remove(bucket, value));
                }
                assert!(!buckets.remove(bucket, 1));
                assert!(buckets.has_empty(bucket));
            }
        }
    }

    #[test]
    fn constructor_validates_parameters() {
        assert!(CuckooFilter::new(0, 0.01).is_err());
        assert!(CuckooFilter::new(100, 0.0).is_err());
        assert!(CuckooFilter::new(100, 1.0).is_err());
        assert!(CuckooFilter::with_parameters(3, 8, 100).is_err());
        assert!(CuckooFilter::with_parameters(8, 0, 100).is_err());
        assert!(CuckooFilter::with_parameters(8, 5, 100).is_err());
        assert!(CuckooFilter::with_parameters(8, 6, 100).is_ok());
        assert!(CuckooFilter::with_parameters(8, 8, 0).is_err());
    }

    #[test]
    fn automatic_sizing_uses_reference_load_threshold() {
        let expected_items = 1_000_000;
        let filter = CuckooFilter::new(expected_items, 0.01).unwrap();

        assert_eq!(filter.bucket_count(), 262_144);
        assert_eq!(filter.fingerprint_bits(), 10);
        assert_eq!(filter.max_kicks, 500);
        assert_eq!(filter.buckets.storage_len(), 262_144 * 5);
        assert!(
            expected_items as f64 / (filter.bucket_count() * BUCKET_SIZE) as f64
                <= MAX_TARGET_LOAD_FACTOR
        );

        let capacity = 262_144 * BUCKET_SIZE;
        let last_item_below_threshold = (capacity as f64 * MAX_TARGET_LOAD_FACTOR).floor() as usize;
        assert_eq!(
            bucket_count_for_expected_items(last_item_below_threshold).unwrap(),
            262_144
        );
        assert_eq!(
            bucket_count_for_expected_items(last_item_below_threshold + 1).unwrap(),
            524_288
        );
    }

    #[test]
    fn automatic_sizing_is_minimal_across_power_of_two_boundaries() {
        for expected_items in [
            1, 8, 9, 100, 1_000, 10_000, 100_000, 1_000_000, 1_006_632, 1_006_633,
        ] {
            let buckets = bucket_count_for_expected_items(expected_items).unwrap();
            let load = expected_items as f64 / (buckets as f64 * BUCKET_SIZE as f64);

            assert!(buckets.is_power_of_two());
            assert!(buckets >= 2);
            assert!(load <= MAX_TARGET_LOAD_FACTOR);

            if buckets > 2 {
                let previous_load =
                    expected_items as f64 / ((buckets / 2) as f64 * BUCKET_SIZE as f64);
                assert!(previous_load > MAX_TARGET_LOAD_FACTOR);
            }
        }
    }

    #[test]
    fn constructor_selects_smallest_width_meeting_requested_rate() {
        for (target_rate, expected_bits) in
            [(0.9, 6), (0.03, 9), (0.01, 10), (0.001, 13), (0.00013, 16)]
        {
            let filter = CuckooFilter::new(1_000, target_rate).unwrap();

            assert_eq!(filter.fingerprint_bits(), expected_bits);
            assert!(filter.expected_false_positive_rate() <= target_rate);
            assert!(
                expected_bits == MIN_FINGERPRINT_BITS
                    || full_bucket_false_positive_rate_bound(expected_bits - 1) > target_rate
            );
        }
    }

    #[test]
    fn constructor_rejects_rate_below_sixteen_bit_bound() {
        let minimum_supported_rate = full_bucket_false_positive_rate_bound(MAX_FINGERPRINT_BITS);

        assert!(CuckooFilter::new(1_000, minimum_supported_rate).is_ok());
        assert!(CuckooFilter::new(1_000, minimum_supported_rate * 0.99).is_err());
    }

    #[test]
    fn expected_false_positive_rate_is_full_bucket_remapping_bound() {
        let filter = CuckooFilter::with_parameters(8, 8, 100).unwrap();
        let paper_uniform_approximation = 2.0 * BUCKET_SIZE as f64 / 2_f64.powi(8);

        assert_eq!(
            filter.expected_false_positive_rate(),
            full_bucket_false_positive_rate_bound(8)
        );
        assert!(filter.expected_false_positive_rate() > paper_uniform_approximation);
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
        let mut filter = CuckooFilter::with_parameters(2, 6, 50).unwrap();
        let mut accepted = 0;
        for value in 0_u64..100 {
            if filter.insert(&value) {
                accepted += 1;
            }
        }
        assert!(accepted < 100);
    }

    #[test]
    fn failed_insert_preserves_membership_state() {
        // More than one kick exercises reversal of the complete chain,
        // including any slot revisited by the random walk.
        let mut filter = CuckooFilter::with_parameters(2, 16, 17).unwrap();
        let mut accepted = Vec::new();
        let mut observed_failure = false;

        for value in 0_u64..100 {
            let buckets_before = filter.buckets.clone();
            let count_before = filter.inserted_items();
            let load_before = filter.load_factor();
            let rng_state_before = filter.rng_state;

            if filter.insert(&value) {
                accepted.push(value);
                continue;
            }

            observed_failure = true;
            assert_eq!(filter.buckets, buckets_before);
            assert_eq!(filter.inserted_items(), count_before);
            assert_eq!(filter.load_factor(), load_before);
            assert_eq!(filter.rng_state, rng_state_before);
            assert!(filter.relocation_log.is_empty());
            for previous in &accepted {
                assert!(
                    filter.contains(previous),
                    "failed insertion lost previously accepted item {previous}"
                );
            }
            break;
        }

        assert!(observed_failure);
    }

    #[test]
    fn successful_random_relocation_preserves_membership() {
        let mut filter = CuckooFilter::with_parameters(8, 16, 500).unwrap();
        let mut accepted = Vec::new();
        let mut observed_relocation = false;

        for value in 0_u64..10_000 {
            let fingerprint = filter.fingerprint(&value);
            let (index_a, index_b) = filter.bucket_indexes(&value, fingerprint);
            let needs_relocation =
                !filter.buckets.has_empty(index_a) && !filter.buckets.has_empty(index_b);
            let count_before = filter.inserted_items();

            if !filter.insert(&value) {
                break;
            }
            accepted.push(value);

            if needs_relocation {
                observed_relocation = true;
                assert_eq!(filter.inserted_items(), count_before + 1);
                assert!(filter.contains(&value));
                for previous in &accepted {
                    assert!(
                        filter.contains(previous),
                        "relocation lost previously accepted item {previous}"
                    );
                }
                assert!(filter.relocation_log.is_empty());
                break;
            }
        }

        assert!(observed_relocation);
    }

    #[test]
    fn empirical_false_positive_rate_meets_requested_bound() {
        let target_rate = 0.01;
        let mut filter = CuckooFilter::new(3_600, target_rate).unwrap();
        for value in 0_u64..3_600 {
            assert!(filter.insert(&value));
        }

        let mut false_positives = 0_u64;
        let trials = 200_000_u64;
        for value in 1_000_000_u64..(1_000_000 + trials) {
            if filter.contains(&value) {
                false_positives += 1;
            }
        }

        let observed_rate = false_positives as f64 / trials as f64;
        let target_standard_error = (target_rate * (1.0 - target_rate) / trials as f64).sqrt();
        assert!(
            observed_rate <= target_rate + 6.0 * target_standard_error,
            "observed rate {observed_rate} exceeds target {target_rate} beyond six standard errors"
        );

        let per_slot_match =
            filter.load_factor() * fingerprint_collision_probability(filter.fingerprint_bits());
        let load_aware_prediction = 1.0 - (1.0 - per_slot_match).powi((2 * BUCKET_SIZE) as i32);
        let prediction_standard_error =
            (load_aware_prediction * (1.0 - load_aware_prediction) / trials as f64).sqrt();
        assert!(
            (observed_rate - load_aware_prediction).abs() <= 6.0 * prediction_standard_error,
            "observed rate {observed_rate} differs from load-aware prediction \
             {load_aware_prediction} beyond six standard errors"
        );
    }

    #[test]
    fn deleting_from_an_empty_filter_returns_false() {
        let mut filter = CuckooFilter::new(100, 0.01).unwrap();
        assert!(!filter.delete(&"ghost"));
    }

    #[test]
    fn deleting_a_colliding_non_member_can_remove_an_inserted_member() {
        let mut filter = CuckooFilter::with_parameters(2, 6, 50).unwrap();
        let inserted = 0_u64;
        assert!(filter.insert(&inserted));

        let colliding_non_member = (1_u64..100_000)
            .find(|candidate| filter.contains(candidate))
            .expect("small fingerprints should yield a false-positive fixture");

        // Section 3.3 of the Cuckoo Filter paper requires callers to delete
        // only items known to have been inserted. A false-positive lookup is
        // not proof of insertion: deleting it removes the sole matching
        // fingerprint, which actually belongs to `inserted`.
        assert!(filter.delete(&colliding_non_member));
        assert!(!filter.contains(&inserted));
    }
}
