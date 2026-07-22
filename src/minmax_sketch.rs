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
//! MinMax sketch for approximate ordered-value lookup.
//!
//! [`MinMaxSketch`] is the value-compression sketch introduced by
//! [SketchML][paper]. It approximates a mapping from keys to small ordered
//! values, such as quantile-bucket indices. Insertion takes the minimum at one
//! cell in each row; lookup takes the maximum of those row values. For every
//! inserted `(key, value)` pair, the estimate is therefore never greater than
//! `value`. It is exact when at least one selected cell has not collided with a
//! smaller value.
//!
//! This is not a frequency sketch. Use [`crate::mincount_sketch::MinCountSketch`]
//! for non-negative frequency estimation.
//!
//! # Keys and values
//!
//! Values may be any compact [`Copy`] type with a total [`Ord`]. The default
//! value type is `u8`, matching the paper's quantile-bucket use case. Repeated
//! insertion of one key retains the smallest supplied value; an existing value
//! cannot be raised without clearing and rebuilding the sketch. The ordering is
//! semantic: codes should increase away from the conservative value. In the
//! paper's signed-gradient use case, positive and negative values are encoded
//! separately so a numerically smaller negative value cannot increase a
//! gradient's magnitude.
//!
//! Generic [`Hash`] keys are fingerprinted once with seed-keyed SipHash before
//! applying the inexpensive row functions. [`MinMaxSketch::insert_u64`] and
//! [`MinMaxSketch::estimate_u64`] bypass that fingerprinting layer for callers
//! that already have stable, distinct `u64` identifiers.
//!
//! # Unknown keys
//!
//! Lookup returns [`None`] if any selected cell is empty, which proves the key
//! was not inserted. Once every selected cell has been occupied by other keys,
//! an unknown key can return a false-positive [`Some`] value. MinMax sketches
//! are normally queried only for the separately retained keys of the compressed
//! mapping; they are not membership filters.
//!
//! # Seeds and merging
//!
//! The caller-owned seed selects the fingerprint and row-hash families. Two
//! sketches can merge only when their width, depth, and seed match. A merge
//! takes cell-wise minima and exactly reproduces the state obtained by inserting
//! both collections into one empty sketch.
//!
//! [paper]: https://doi.org/10.1145/3183713.3196894

use std::hash::{Hash, Hasher};

use siphasher::sip::SipHasher13;

use crate::{SketchError, splitmix64};

const SPLITMIX_INCREMENT: u64 = 0x9E37_79B9_7F4A_7C15;
const FINGERPRINT_DOMAIN_A: u64 = 0x6A09_E667_F3BC_C908;
const FINGERPRINT_DOMAIN_B: u64 = 0xBB67_AE85_84CA_A73B;
const ROW_DOMAIN: u64 = 0x3C6E_F372_FE94_F82B;
const OCCUPANCY_WORD_BITS: usize = u64::BITS as usize;

/// Approximate mapping from keys to compact ordered values.
///
/// # Example
///
/// ```rust
/// use sketches::minmax_sketch::MinMaxSketch;
///
/// // Width one deliberately forces a collision to show how Ord is used.
/// // Production sketches should use wider rows.
/// let mut sketch = MinMaxSketch::<u8>::new(1, 3, 0x3C6E_F372_FE94_F82B).unwrap();
/// sketch.insert(&"large", 17);
/// sketch.insert(&"small", 4);
///
/// // Every selected cell retains min(17, 4), so the estimate is lowered.
/// assert_eq!(sketch.estimate(&"large"), Some(4));
/// ```
///
/// # Representation and complexity
///
/// A sketch owns `width * depth` values plus a one-bit occupancy marker per
/// cell. Insertion and lookup take `O(depth)` time. The value type defaults to
/// `u8`; choosing a wider type increases the main table proportionally.
#[derive(Debug, Clone)]
pub struct MinMaxSketch<V = u8> {
    width: usize,
    values: Vec<V>,
    occupied: Vec<u64>,
    occupied_cells: usize,
    row_seeds: Box<[u64]>,
    family_seed: u64,
    fingerprint_keys: (u64, u64),
}

impl<V: Copy + Default + Ord> MinMaxSketch<V> {
    /// Creates a seeded sketch with explicit dimensions.
    ///
    /// `width` is the number of cells in each row and `depth` is the number of
    /// independently seeded rows. Both must be greater than zero. Unlike
    /// power-of-two frequency-sketch constructors, arbitrary widths are useful
    /// here because the SketchML configuration is commonly expressed as a
    /// fraction of the number of mapped keys.
    ///
    /// The seed selects the complete hash family. Use the same seed for shards
    /// that may later be merged and independently selected seeds for unrelated
    /// sketches.
    ///
    /// # Errors
    ///
    /// Returns [`SketchError::InvalidParameter`] when a dimension is zero, the
    /// table length overflows, or the requested storage cannot be allocated.
    pub fn new(width: usize, depth: usize, seed: u64) -> Result<Self, SketchError> {
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

        // Rows occupy consecutive ranges in the value table. Checked
        // multiplication turns impossible dimensions into an ordinary error.
        let table_len = width
            .checked_mul(depth)
            .ok_or(SketchError::InvalidParameter(
                "width * depth overflows usize",
            ))?;
        let occupancy_words = table_len.div_ceil(OCCUPANCY_WORD_BITS);

        // Reserve each allocation explicitly so capacity overflow and memory
        // pressure are reported rather than panicking inside vec![...].
        let mut values = Vec::new();
        values
            .try_reserve_exact(table_len)
            .map_err(|_| SketchError::InvalidParameter("value table is too large to allocate"))?;
        values.resize(table_len, V::default());

        let mut occupied = Vec::new();
        occupied.try_reserve_exact(occupancy_words).map_err(|_| {
            SketchError::InvalidParameter("occupancy table is too large to allocate")
        })?;
        occupied.resize(occupancy_words, 0);

        // SplitMix expands the caller seed into one deterministic seed per row.
        // Row mixing operates on the compact fingerprint, not the original key.
        let mut seed_stream = SeedStream::new(seed ^ ROW_DOMAIN);
        let mut row_seeds = Vec::new();
        row_seeds
            .try_reserve_exact(depth)
            .map_err(|_| SketchError::InvalidParameter("depth is too large to allocate"))?;
        row_seeds.extend((0..depth).map(|_| seed_stream.next_u64()));

        Ok(Self {
            width,
            values,
            occupied,
            occupied_cells: 0,
            row_seeds: row_seeds.into_boxed_slice(),
            family_seed: seed,
            fingerprint_keys: (
                splitmix64(seed ^ FINGERPRINT_DOMAIN_A),
                splitmix64(seed ^ FINGERPRINT_DOMAIN_B),
            ),
        })
    }

    /// Returns the number of value cells per row.
    pub fn width(&self) -> usize {
        self.width
    }

    /// Returns the number of independently seeded rows.
    pub fn depth(&self) -> usize {
        self.row_seeds.len()
    }

    /// Returns the caller-provided hash-family seed.
    pub fn seed(&self) -> u64 {
        self.family_seed
    }

    /// Returns the number of occupied cells across all rows.
    ///
    /// This is insertion telemetry, not an estimate of distinct keys. One key
    /// can occupy up to `depth` cells, while collisions reuse occupied cells.
    pub fn occupied_cells(&self) -> usize {
        self.occupied_cells
    }

    /// Returns `true` when no key/value pair has been inserted.
    pub fn is_empty(&self) -> bool {
        self.occupied_cells == 0
    }

    /// Inserts a key and ordered value after fingerprinting the key once.
    ///
    /// Each selected cell retains the smaller of its current value and the new
    /// value. Repeated insertion of one key can therefore lower its estimate
    /// but cannot raise it.
    pub fn insert<T: Hash + ?Sized>(&mut self, key: &T, value: V) {
        self.insert_u64(self.fingerprint(key), value);
    }

    /// Inserts a value for a stable 64-bit key identifier.
    ///
    /// This bypasses generic fingerprinting. Distinct logical keys must have
    /// distinct identifiers.
    pub fn insert_u64(&mut self, key_id: u64, value: V) {
        for row in 0..self.depth() {
            let index = self.location(row, key_id);
            if self.is_occupied(index) {
                self.values[index] = self.values[index].min(value);
            } else {
                // An empty cell represents positive infinity. Store the first
                // finite value separately from its occupancy bit so every V,
                // including u8::MAX, remains representable.
                self.values[index] = value;
                self.mark_occupied(index);
                self.occupied_cells += 1;
            }
        }
    }

    /// Estimates the ordered value associated with a key.
    ///
    /// For an inserted pair, the returned value is never greater than the
    /// smallest value inserted for that key. [`None`] proves the key was not
    /// inserted; [`Some`] does not prove membership because an unknown key can
    /// collide with occupied cells in every row.
    pub fn estimate<T: Hash + ?Sized>(&self, key: &T) -> Option<V> {
        self.estimate_u64(self.fingerprint(key))
    }

    /// Estimates the value for a stable 64-bit key identifier.
    ///
    /// This bypasses generic fingerprinting and has the same one-sided and
    /// unknown-key semantics as [`Self::estimate`].
    pub fn estimate_u64(&self, key_id: u64) -> Option<V> {
        let mut maximum: Option<V> = None;
        for row in 0..self.depth() {
            let index = self.location(row, key_id);
            if !self.is_occupied(index) {
                return None;
            }
            maximum = Some(match maximum {
                Some(current) => current.max(self.values[index]),
                None => self.values[index],
            });
        }
        maximum
    }

    /// Clears all entries while retaining the allocation and hash family.
    pub fn clear(&mut self) {
        self.values.fill(V::default());
        self.occupied.fill(0);
        self.occupied_cells = 0;
    }

    /// Merges another compatible sketch into this sketch.
    ///
    /// Cell-wise minima are equivalent to inserting both sketches' input pairs
    /// into one empty sketch. Compatibility requires the same dimensions and
    /// hash-family seed.
    ///
    /// # Errors
    ///
    /// Returns [`SketchError::IncompatibleSketches`] for a dimension or seed
    /// mismatch. An error leaves this sketch unchanged.
    pub fn merge(&mut self, other: &Self) -> Result<(), SketchError> {
        if self.width != other.width || self.depth() != other.depth() {
            return Err(SketchError::IncompatibleSketches(
                "width/depth must match for merge",
            ));
        }
        if self.family_seed != other.family_seed {
            return Err(SketchError::IncompatibleSketches(
                "hash-family seeds must match for merge",
            ));
        }

        for index in 0..self.values.len() {
            if !other.is_occupied(index) {
                continue;
            }

            if self.is_occupied(index) {
                self.values[index] = self.values[index].min(other.values[index]);
            } else {
                self.values[index] = other.values[index];
                self.mark_occupied(index);
                self.occupied_cells += 1;
            }
        }
        Ok(())
    }

    fn fingerprint<T: Hash + ?Sized>(&self, key: &T) -> u64 {
        // Keyed SipHash reduces an arbitrary Hash implementation to one stable
        // identifier for this sketch family. Each row then mixes only that ID.
        let mut hasher =
            SipHasher13::new_with_keys(self.fingerprint_keys.0, self.fingerprint_keys.1);
        key.hash(&mut hasher);
        hasher.finish()
    }

    fn location(&self, row: usize, key_id: u64) -> usize {
        // SplitMix provides a deterministic pseudorandom row mapping.
        // Multiply-high reduction maps its 64-bit result to an arbitrary width
        // without depending only on the hash's low bits.
        let hash = splitmix64(key_id ^ self.row_seeds[row]);
        let column = ((u128::from(hash) * self.width as u128) >> 64) as usize;
        row * self.width + column
    }

    fn is_occupied(&self, index: usize) -> bool {
        let word = index / OCCUPANCY_WORD_BITS;
        let bit = index % OCCUPANCY_WORD_BITS;
        self.occupied[word] & (1_u64 << bit) != 0
    }

    fn mark_occupied(&mut self, index: usize) {
        let word = index / OCCUPANCY_WORD_BITS;
        let bit = index % OCCUPANCY_WORD_BITS;
        self.occupied[word] |= 1_u64 << bit;
    }
}

struct SeedStream {
    state: u64,
}

impl SeedStream {
    fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    fn next_u64(&mut self) -> u64 {
        let value = splitmix64(self.state);
        self.state = self.state.wrapping_add(SPLITMIX_INCREMENT);
        value
    }
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::cmp::Reverse;
    use std::fmt::Debug;
    use std::hash::{Hash, Hasher};

    use super::MinMaxSketch;
    use crate::SketchError;

    const SEED: u64 = 0x3C6E_F372_FE94_F82B;

    fn assert_same_state<V>(left: &MinMaxSketch<V>, right: &MinMaxSketch<V>)
    where
        V: Copy + Debug + Default + Ord + PartialEq,
    {
        assert_eq!(left.width, right.width);
        assert_eq!(left.values, right.values);
        assert_eq!(left.occupied, right.occupied);
        assert_eq!(left.occupied_cells, right.occupied_cells);
        assert_eq!(left.row_seeds, right.row_seeds);
        assert_eq!(left.family_seed, right.family_seed);
        assert_eq!(left.fingerprint_keys, right.fingerprint_keys);
    }

    #[test]
    fn constructor_retains_arbitrary_dimensions_and_seed() {
        let sketch = MinMaxSketch::<u8>::new(13, 5, SEED).unwrap();
        assert_eq!(sketch.width(), 13);
        assert_eq!(sketch.depth(), 5);
        assert_eq!(sketch.seed(), SEED);
        assert_eq!(sketch.occupied_cells(), 0);
        assert!(sketch.is_empty());
    }

    #[test]
    fn constructor_rejects_invalid_or_unallocatable_dimensions() {
        assert!(MinMaxSketch::<u8>::new(0, 1, SEED).is_err());
        assert!(MinMaxSketch::<u8>::new(1, 0, SEED).is_err());
        assert!(MinMaxSketch::<u8>::new(usize::MAX, 2, SEED).is_err());
        assert!(MinMaxSketch::<u8>::new(usize::MAX, 1, SEED).is_err());
    }

    #[test]
    fn one_key_is_exact_and_the_full_u8_domain_is_available() {
        let mut sketch = MinMaxSketch::<u8>::new(16, 3, SEED).unwrap();
        assert_eq!(sketch.estimate_u64(42), None);

        sketch.insert_u64(42, u8::MAX);

        assert_eq!(sketch.estimate_u64(42), Some(u8::MAX));
        assert_eq!(sketch.occupied_cells(), sketch.depth());
        assert!(!sketch.is_empty());
    }

    #[test]
    fn collisions_can_only_lower_inserted_values() {
        let mut sketch = MinMaxSketch::<u16>::new(3, 4, SEED).unwrap();
        let mut exact_minima = [u16::MAX; 128];

        for operation in 0..20_000_u64 {
            let key = operation.wrapping_mul(104_729) % exact_minima.len() as u64;
            let value = operation.wrapping_mul(65_537) as u16;
            exact_minima[key as usize] = exact_minima[key as usize].min(value);
            sketch.insert_u64(key, value);
        }

        for (key, &exact) in exact_minima.iter().enumerate() {
            assert!(sketch.estimate_u64(key as u64).unwrap() <= exact);
        }
    }

    #[test]
    fn table_and_query_semantics_match_a_reference_across_configurations() {
        const WIDTHS: [usize; 8] = [1, 2, 3, 7, 8, 31, 64, 65];
        const DEPTHS: [usize; 3] = [1, 2, 5];
        const SEEDS: [u64; 4] = [0, 1, SEED, u64::MAX];
        const KEY_COUNT: usize = 41;

        for width in WIDTHS {
            for depth in DEPTHS {
                for seed in SEEDS {
                    let mut sketch = MinMaxSketch::<u8>::new(width, depth, seed).unwrap();
                    let mut reference = vec![None; width * depth];
                    let mut exact_minima: [Option<u8>; KEY_COUNT] = [None; KEY_COUNT];

                    // Every key is revisited with values in a non-monotonic
                    // order, exercising both replacing and no-op insertions.
                    for operation in 0..512_u64 {
                        let key = operation % KEY_COUNT as u64;
                        let value = operation
                            .wrapping_mul(73)
                            .wrapping_add(key.wrapping_mul(19))
                            as u8;

                        for row in 0..depth {
                            let index = sketch.location(row, key);
                            let row_start = row * width;
                            assert!(
                                (row_start..row_start + width).contains(&index),
                                "width={width} depth={depth} seed={seed} row={row} key={key}"
                            );
                            reference[index] = Some(
                                reference[index].map_or(value, |current: u8| current.min(value)),
                            );
                        }
                        exact_minima[key as usize] = Some(
                            exact_minima[key as usize].map_or(value, |current| current.min(value)),
                        );
                        sketch.insert_u64(key, value);
                    }

                    for (index, expected) in reference.iter().copied().enumerate() {
                        assert_eq!(sketch.is_occupied(index), expected.is_some());
                        if let Some(expected) = expected {
                            assert_eq!(sketch.values[index], expected);
                        }
                    }
                    assert_eq!(
                        sketch.occupied_cells(),
                        reference.iter().filter(|value| value.is_some()).count()
                    );

                    for (key, exact) in exact_minima.iter().copied().enumerate() {
                        let expected = (0..depth)
                            .map(|row| reference[sketch.location(row, key as u64)].unwrap())
                            .max();
                        assert_eq!(sketch.estimate_u64(key as u64), expected);
                        assert!(expected.unwrap() <= exact.unwrap());
                    }
                }
            }
        }
    }

    #[test]
    fn custom_ord_controls_which_colliding_value_wins() {
        let mut natural = MinMaxSketch::<u8>::new(1, 3, SEED).unwrap();
        natural.insert_u64(1, 7);
        natural.insert_u64(2, 200);
        assert_eq!(natural.estimate_u64(1), Some(7));

        // Reverse changes only the value ordering. Under Reverse, raw 200 is
        // smaller than raw 7, so the same collision retains 200 instead.
        let mut reversed = MinMaxSketch::<Reverse<u8>>::new(1, 3, SEED).unwrap();
        reversed.insert_u64(1, Reverse(7));
        reversed.insert_u64(2, Reverse(200));
        assert_eq!(reversed.estimate_u64(1), Some(Reverse(200)));
    }

    #[test]
    fn occupancy_bitmap_handles_word_boundaries() {
        let mut sketch = MinMaxSketch::<u8>::new(65, 2, SEED).unwrap();
        assert_eq!(sketch.values.len(), 130);
        assert_eq!(sketch.occupied.len(), 3);

        for index in 0..sketch.values.len() {
            assert!(!sketch.is_occupied(index));
        }
        // Mark alternating indices first so both sides of the 63/64 and
        // 127/128 word boundaries are exercised in a non-sequential pattern.
        for index in (0..sketch.values.len())
            .step_by(2)
            .chain((1..sketch.values.len()).step_by(2))
        {
            sketch.mark_occupied(index);
            assert!(sketch.is_occupied(index));
        }
        assert!((0..sketch.values.len()).all(|index| sketch.is_occupied(index)));
    }

    #[test]
    fn max_query_recovers_a_value_when_one_row_avoids_a_lower_collision() {
        let mut sketch = MinMaxSketch::<u8>::new(16, 3, SEED).unwrap();
        let key = 0_u64;
        let key_locations: Vec<_> = (0..sketch.depth())
            .map(|row| sketch.location(row, key))
            .collect();
        let collider = (1_u64..1_000_000)
            .find(|&candidate| {
                sketch.location(0, candidate) == key_locations[0]
                    && sketch.location(1, candidate) != key_locations[1]
            })
            .expect("a partial collision should be easy to find");

        sketch.insert_u64(key, 200);
        sketch.insert_u64(collider, 7);

        assert_eq!(sketch.values[key_locations[0]], 7);
        assert_eq!(sketch.values[key_locations[1]], 200);
        assert_eq!(sketch.estimate_u64(key), Some(200));
    }

    #[test]
    fn complete_collisions_return_the_smallest_mapped_value() {
        let mut sketch = MinMaxSketch::<u8>::new(1, 4, SEED).unwrap();
        sketch.insert_u64(1, 200);
        sketch.insert_u64(2, 7);

        assert_eq!(sketch.estimate_u64(1), Some(7));
        assert_eq!(sketch.estimate_u64(2), Some(7));
        // Unknown keys can be false positives once every selected cell is full.
        assert_eq!(sketch.estimate_u64(3), Some(7));
    }

    #[test]
    fn repeated_insertion_retains_the_smallest_value() {
        let mut sketch = MinMaxSketch::<u32>::new(32, 5, SEED).unwrap();
        sketch.insert_u64(7, 20);
        sketch.insert_u64(7, 30);
        assert_eq!(sketch.estimate_u64(7), Some(20));

        sketch.insert_u64(7, 4);
        assert_eq!(sketch.estimate_u64(7), Some(4));
    }

    #[test]
    fn generic_operations_hash_a_key_once() {
        struct CountedHash<'a> {
            calls: &'a Cell<usize>,
        }

        impl Hash for CountedHash<'_> {
            fn hash<H: Hasher>(&self, state: &mut H) {
                self.calls.set(self.calls.get() + 1);
                42_u64.hash(state);
            }
        }

        let calls = Cell::new(0);
        let key = CountedHash { calls: &calls };
        let mut sketch = MinMaxSketch::<u8>::new(32, 7, SEED).unwrap();
        sketch.insert(&key, 12);
        assert_eq!(calls.get(), 1);
        assert_eq!(sketch.estimate(&key), Some(12));
        assert_eq!(calls.get(), 2);
    }

    #[test]
    fn seed_selects_reproducible_hash_families() {
        let first = MinMaxSketch::<u8>::new(64, 5, SEED).unwrap();
        let second = MinMaxSketch::<u8>::new(64, 5, SEED).unwrap();
        let different = MinMaxSketch::<u8>::new(64, 5, SEED + 1).unwrap();

        assert_eq!(first.row_seeds, second.row_seeds);
        assert_eq!(first.fingerprint_keys, second.fingerprint_keys);
        assert_ne!(first.row_seeds, different.row_seeds);
        assert_ne!(first.fingerprint_keys, different.fingerprint_keys);
    }

    #[test]
    fn merge_matches_direct_insertion_and_checks_configuration() {
        let mut left = MinMaxSketch::<u8>::new(17, 5, SEED).unwrap();
        let mut right = MinMaxSketch::<u8>::new(17, 5, SEED).unwrap();
        let mut direct = MinMaxSketch::<u8>::new(17, 5, SEED).unwrap();

        for (key, value) in [(1, 90), (2, 40), (3, 210)] {
            left.insert_u64(key, value);
            direct.insert_u64(key, value);
        }
        for (key, value) in [(2, 10), (4, 70), (5, 255)] {
            right.insert_u64(key, value);
            direct.insert_u64(key, value);
        }

        left.merge(&right).unwrap();
        assert_eq!(left.values, direct.values);
        assert_eq!(left.occupied, direct.occupied);
        assert_eq!(left.occupied_cells, direct.occupied_cells);

        let before_error = left.clone();
        let different_width = MinMaxSketch::<u8>::new(18, 5, SEED).unwrap();
        assert_eq!(
            left.merge(&different_width),
            Err(SketchError::IncompatibleSketches(
                "width/depth must match for merge"
            ))
        );
        assert_same_state(&left, &before_error);

        let different_seed = MinMaxSketch::<u8>::new(17, 5, SEED + 1).unwrap();
        assert_eq!(
            left.merge(&different_seed),
            Err(SketchError::IncompatibleSketches(
                "hash-family seeds must match for merge"
            ))
        );
        assert_same_state(&left, &before_error);
    }

    #[test]
    fn merge_is_commutative_associative_idempotent_and_has_an_empty_identity() {
        let mut sketches: Vec<_> = (0_u64..3)
            .map(|_| MinMaxSketch::<u16>::new(19, 4, SEED).unwrap())
            .collect();
        for (shard, sketch) in sketches.iter_mut().enumerate() {
            for key in 0_u64..96 {
                if key as usize % 3 == shard || key % 5 == 0 {
                    let value = key
                        .wrapping_mul(257)
                        .wrapping_add((shard as u64).wrapping_mul(101))
                        as u16;
                    sketch.insert_u64(key, value);
                }
            }
        }
        let [first, second, third]: [MinMaxSketch<u16>; 3] =
            sketches.try_into().expect("exactly three sketches");

        let mut first_second = first.clone();
        first_second.merge(&second).unwrap();
        let mut second_first = second.clone();
        second_first.merge(&first).unwrap();
        assert_same_state(&first_second, &second_first);

        let mut left_associative = first_second.clone();
        left_associative.merge(&third).unwrap();
        let mut second_third = second.clone();
        second_third.merge(&third).unwrap();
        let mut right_associative = first.clone();
        right_associative.merge(&second_third).unwrap();
        assert_same_state(&left_associative, &right_associative);

        let mut idempotent = first.clone();
        idempotent.merge(&first).unwrap();
        assert_same_state(&idempotent, &first);

        let empty = MinMaxSketch::<u16>::new(19, 4, SEED).unwrap();
        let mut right_identity = first.clone();
        right_identity.merge(&empty).unwrap();
        assert_same_state(&right_identity, &first);

        let mut left_identity = empty;
        left_identity.merge(&first).unwrap();
        assert_same_state(&left_identity, &first);
    }

    #[test]
    fn clear_resets_values_and_occupancy_but_retains_configuration() {
        let mut sketch = MinMaxSketch::<u8>::new(13, 5, SEED).unwrap();
        sketch.insert_u64(7, 19);
        sketch.clear();

        assert!(sketch.is_empty());
        assert_eq!(sketch.occupied_cells(), 0);
        assert_eq!(sketch.estimate_u64(7), None);
        assert!(sketch.values.iter().all(|&value| value == 0));
        assert_eq!(sketch.width(), 13);
        assert_eq!(sketch.depth(), 5);
        assert_eq!(sketch.seed(), SEED);
    }
}
