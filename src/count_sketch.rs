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
//! Count Sketch for signed point-frequency estimation.
//!
//! Each row uses an independent bucket hash and sign hash. Updates add the
//! signed weight to one counter per row, and point queries return the median of
//! the corrected row estimates. This is the point-query component of the
//! original [Count Sketch][count-sketch-paper], not its candidate-tracking
//! heavy-hitter algorithm.
//!
//! # Error guarantee
//!
//! [`CountSketch::new`] sizes the table for one fixed, non-adaptive query. Under
//! the standard independent-hashing model, for frequency vector `f` and queried
//! item `x`, it provides
//!
//! `Pr[|estimate(x) - f[x]| > epsilon * ||f without x||_2] <= delta`.
//!
//! Width is the next power of two at least `8 / epsilon^2`, so Chebyshev's
//! inequality bounds one row's failure probability by `1/8`. Depth is the next
//! odd integer at least `2 * ln(1 / delta) / ln(16 / 7)`, obtained by applying
//! the Chernoff/KL majority bound to the median. A simultaneous guarantee for
//! `q` predetermined queries requires constructing with `delta / q`.
//!
//! The integer row functions use Thorup's [strongly universal
//! multiply-shift][multiply-shift] construction. The caller-owned seed is
//! expanded deterministically into the row coefficients, so sketches are
//! reproducible and there is no global random state or lock. The probability
//! statement uses the conventional pseudorandom-hashing model: choose the seed
//! independently of the stream. This is not an adversarial or cryptographic
//! guarantee.
//!
//! Generic [`Hash`] items are fingerprinted once with seed-keyed SipHash before
//! applying the row functions. [`CountSketch::add_u64`] and
//! [`CountSketch::estimate_u64`] avoid that extra fingerprinting layer when the
//! application already has stable 64-bit item identifiers.
//!
//! # Seeds and merging
//!
//! A seed selects the complete hash family. Independently populated sketches
//! must use the same seed and dimensions to merge. Unrelated sketches should
//! use independently generated seeds so an unlucky collision pattern is not
//! repeated across applications. Fixed seeds are useful for tests and
//! reproducible pipelines; they are not secret keys.
//!
//! # Arithmetic
//!
//! Count Sketch is a linear sketch, so counters are never clamped. Every update
//! and merge first checks all affected counters, then either commits exactly or
//! returns [`SketchError::CounterOverflow`] without mutation. `i64::MIN` is
//! excluded because its sign correction is not representable.
//!
//! [count-sketch-paper]: https://www.cs.yale.edu/homes/el327/datamining2011aFiles/FindingFrequentItemsInDataStreams.pdf
//! [multiply-shift]: https://arxiv.org/abs/1504.06804

use std::hash::{Hash, Hasher};

use siphasher::sip::SipHasher13;

use crate::{SketchError, splitmix64};

const WIDTH_NUMERATOR: f64 = 8.0;
const DEPTH_DENOMINATOR: f64 = 0.826_678_573_184_467_9; // ln(16 / 7)
const SPLITMIX_INCREMENT: u64 = 0x9E37_79B9_7F4A_7C15;
const FINGERPRINT_DOMAIN_A: u64 = 0x243F_6A88_85A3_08D3;
const FINGERPRINT_DOMAIN_B: u64 = 0x1319_8A2E_0370_7344;
const ROW_DOMAIN: u64 = 0xA409_3822_299F_31D0;

#[derive(Debug, Clone, PartialEq, Eq)]
struct RowHash {
    index_multiplier: u128,
    index_offset: u128,
    sign_multiplier: u64,
    sign_offset: u64,
}

/// Approximate signed frequency sketch for turnstile streams.
///
/// # Example
///
/// ```rust
/// use sketches::count_sketch::CountSketch;
///
/// // A fixed seed makes this example reproducible. Production code should
/// // draw a seed independently of the stream being summarized.
/// let seed = 0xA409_3822_299F_31D0;
/// let mut sketch = CountSketch::new(0.05, 0.01, seed).unwrap();
/// sketch.add(&"cat", 5).unwrap();
/// sketch.decrement(&"cat").unwrap();
///
/// // A stream containing only one distinct item has no collision noise.
/// assert_eq!(sketch.estimate(&"cat"), 4);
/// ```
#[derive(Debug, Clone)]
pub struct CountSketch {
    width: usize,
    counters: Vec<i64>,
    rows: Box<[RowHash]>,
    family_seed: u64,
    fingerprint_keys: (u64, u64),
}

impl CountSketch {
    /// Builds a seeded sketch for a fixed-query error bound.
    ///
    /// `epsilon` and `delta` must be finite and strictly between zero and one.
    /// The selected width is a power of two and the selected depth is odd, as
    /// required by the strongly universal multiply-shift family and an
    /// unambiguous median majority.
    ///
    /// The seed selects the fingerprint and row-hash families. Choose it
    /// independently of the input. Use the same seed for shards that will be
    /// merged, different seeds for unrelated sketches, and a fixed documented
    /// seed when reproducibility is more important than independent trials.
    /// No global random generator or lock is used.
    ///
    /// # Errors
    ///
    /// Returns [`SketchError::InvalidParameter`] when the parameters are
    /// invalid, their dimensions are unrepresentable, or storage cannot be
    /// allocated.
    pub fn new(epsilon: f64, delta: f64, seed: u64) -> Result<Self, SketchError> {
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

        let minimum_width = (WIDTH_NUMERATOR / (epsilon * epsilon)).ceil();
        if !minimum_width.is_finite() || minimum_width > usize::MAX as f64 {
            return Err(SketchError::InvalidParameter(
                "epsilon requires an unrepresentable width",
            ));
        }
        let width = (minimum_width as usize).checked_next_power_of_two().ok_or(
            SketchError::InvalidParameter("epsilon requires an unrepresentable width"),
        )?;

        let minimum_depth = 2.0 * (1.0 / delta).ln() / DEPTH_DENOMINATOR;
        if !minimum_depth.is_finite() || minimum_depth > usize::MAX as f64 {
            return Err(SketchError::InvalidParameter(
                "delta requires an unrepresentable depth",
            ));
        }
        let mut depth = minimum_depth.ceil() as usize;
        if depth.is_multiple_of(2) {
            depth = depth.checked_add(1).ok_or(SketchError::InvalidParameter(
                "delta requires an unrepresentable depth",
            ))?;
        }
        while (-(depth as f64) * DEPTH_DENOMINATOR / 2.0).exp() > delta {
            depth = depth.checked_add(2).ok_or(SketchError::InvalidParameter(
                "delta requires an unrepresentable depth",
            ))?;
        }

        Self::with_dimensions(width, depth, seed)
    }

    /// Builds a seeded sketch from explicit dimensions.
    ///
    /// `width` must be a non-zero power of two because the row family returns
    /// uniformly distributed bit prefixes. `depth` must be non-zero and odd so
    /// the median represents a strict majority. Explicit dimensions do not by
    /// themselves imply an `(epsilon, delta)` guarantee.
    ///
    /// # Errors
    ///
    /// Returns [`SketchError::InvalidParameter`] for invalid dimensions,
    /// unrepresentable storage, or allocation failure.
    pub fn with_dimensions(width: usize, depth: usize, seed: u64) -> Result<Self, SketchError> {
        if !width.is_power_of_two() {
            return Err(SketchError::InvalidParameter(
                "width must be a non-zero power of two",
            ));
        }
        if depth == 0 || depth.is_multiple_of(2) {
            return Err(SketchError::InvalidParameter(
                "depth must be non-zero and odd",
            ));
        }

        let table_len = width
            .checked_mul(depth)
            .ok_or(SketchError::InvalidParameter(
                "width * depth overflows usize",
            ))?;

        let mut counters = Vec::new();
        counters
            .try_reserve_exact(table_len)
            .map_err(|_| SketchError::InvalidParameter("counter table is too large to allocate"))?;
        counters.resize(table_len, 0);

        let index_bits = width.trailing_zeros();
        let arithmetic_bits = 64 + index_bits.saturating_sub(1);
        let index_mask = low_bits_mask(arithmetic_bits);
        let mut seed_stream = SeedStream::new(seed ^ ROW_DOMAIN);
        let mut rows = Vec::new();
        rows.try_reserve_exact(depth)
            .map_err(|_| SketchError::InvalidParameter("depth is too large to allocate"))?;
        rows.extend((0..depth).map(|_| RowHash {
            index_multiplier: seed_stream.next_u128() & index_mask,
            index_offset: seed_stream.next_u128() & index_mask,
            sign_multiplier: seed_stream.next_u64(),
            sign_offset: seed_stream.next_u64(),
        }));

        Ok(Self {
            width,
            counters,
            rows: rows.into_boxed_slice(),
            family_seed: seed,
            fingerprint_keys: (
                splitmix64(seed ^ FINGERPRINT_DOMAIN_A),
                splitmix64(seed ^ FINGERPRINT_DOMAIN_B),
            ),
        })
    }

    /// Returns the number of counters per row.
    pub fn width(&self) -> usize {
        self.width
    }

    /// Returns the number of independent row estimates.
    pub fn depth(&self) -> usize {
        self.rows.len()
    }

    /// Returns the caller-provided hash-family seed.
    pub fn seed(&self) -> u64 {
        self.family_seed
    }

    /// Adds a signed update after fingerprinting an item once with keyed
    /// SipHash-1-3.
    ///
    /// # Errors
    ///
    /// Returns [`SketchError::CounterOverflow`] without changing the sketch if
    /// the signed update or any resulting counter is not exactly representable.
    pub fn add<T: Hash + ?Sized>(&mut self, item: &T, delta: i64) -> Result<(), SketchError> {
        let item_id = self.fingerprint(item);
        self.add_u64(item_id, delta)
    }

    /// Adds a signed update for a stable 64-bit item identifier.
    ///
    /// This bypasses generic fingerprinting and feeds the identifier directly
    /// into the strongly universal integer row functions. Distinct logical
    /// items must have distinct identifiers.
    ///
    /// # Errors
    ///
    /// Returns [`SketchError::CounterOverflow`] without changing the sketch if
    /// the signed update or any resulting counter is not exactly representable.
    pub fn add_u64(&mut self, item_id: u64, delta: i64) -> Result<(), SketchError> {
        if delta == 0 {
            return Ok(());
        }

        if delta == i64::MIN {
            return Err(SketchError::CounterOverflow);
        }

        // Rows occupy disjoint counter ranges. Check every destination before
        // mutating any of them so an error cannot leave a partial update.
        for row in 0..self.depth() {
            let (index, sign_is_positive) = self.location(row, item_id);
            let signed_delta = if sign_is_positive { delta } else { -delta };
            self.counters[index]
                .checked_add(signed_delta)
                .filter(|&counter| counter != i64::MIN)
                .ok_or(SketchError::CounterOverflow)?;
        }
        for row in 0..self.depth() {
            let (index, sign_is_positive) = self.location(row, item_id);
            let signed_delta = if sign_is_positive { delta } else { -delta };
            self.counters[index] = self.counters[index]
                .checked_add(signed_delta)
                .expect("preflight must prove that the counter update is representable");
        }
        Ok(())
    }

    /// Adds one occurrence of an item.
    ///
    /// # Errors
    /// Returns [`SketchError::CounterOverflow`] without changing the sketch if
    /// a resulting counter is not exactly representable.
    pub fn increment<T: Hash + ?Sized>(&mut self, item: &T) -> Result<(), SketchError> {
        self.add(item, 1)
    }

    /// Removes one occurrence of an item.
    ///
    /// # Errors
    /// Returns [`SketchError::CounterOverflow`] without changing the sketch if
    /// a resulting counter is not exactly representable.
    pub fn decrement<T: Hash + ?Sized>(&mut self, item: &T) -> Result<(), SketchError> {
        self.add(item, -1)
    }

    /// Returns the median signed-frequency estimate for an item.
    pub fn estimate<T: Hash + ?Sized>(&self, item: &T) -> i64 {
        self.estimate_u64(self.fingerprint(item))
    }

    /// Returns the median estimate for a stable 64-bit item identifier.
    pub fn estimate_u64(&self, item_id: u64) -> i64 {
        let mut estimates = Vec::with_capacity(self.depth());
        for row in 0..self.depth() {
            let (index, sign_is_positive) = self.location(row, item_id);
            let counter = self.counters[index];
            estimates.push(if sign_is_positive { counter } else { -counter });
        }

        let middle = estimates.len() / 2;
        *estimates.select_nth_unstable(middle).1
    }

    /// Clears all counters while retaining the hash family and allocated table.
    pub fn clear(&mut self) {
        self.counters.fill(0);
    }

    /// Adds another compatible sketch into this sketch.
    ///
    /// Compatibility requires equal dimensions and the same seed. The check is
    /// necessary because merging counters built by different hash families is
    /// not a Count Sketch of the combined stream.
    ///
    /// # Errors
    ///
    /// Returns [`SketchError::IncompatibleSketches`] for dimension or seed
    /// mismatch. Returns [`SketchError::CounterOverflow`] without mutation if
    /// any combined counter is not exactly representable.
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

        for (left, right) in self.counters.iter().zip(other.counters.iter()) {
            left.checked_add(*right)
                .filter(|&counter| counter != i64::MIN)
                .ok_or(SketchError::CounterOverflow)?;
        }
        for (left, right) in self.counters.iter_mut().zip(other.counters.iter()) {
            *left = left
                .checked_add(*right)
                .expect("preflight must prove that the merged counter is representable");
        }
        Ok(())
    }

    fn fingerprint<T: Hash + ?Sized>(&self, item: &T) -> u64 {
        let mut hasher =
            SipHasher13::new_with_keys(self.fingerprint_keys.0, self.fingerprint_keys.1);
        item.hash(&mut hasher);
        hasher.finish()
    }

    fn location(&self, row: usize, item_id: u64) -> (usize, bool) {
        let row_hash = &self.rows[row];
        let index_bits = self.width.trailing_zeros();
        let column = if index_bits == 0 {
            0
        } else {
            let arithmetic_bits = 64 + index_bits - 1;
            let mixed = row_hash
                .index_multiplier
                .wrapping_mul(item_id as u128)
                .wrapping_add(row_hash.index_offset)
                & low_bits_mask(arithmetic_bits);
            (mixed >> (arithmetic_bits - index_bits)) as usize
        };
        let sign_is_positive = row_hash
            .sign_multiplier
            .wrapping_mul(item_id)
            .wrapping_add(row_hash.sign_offset)
            >> 63
            == 0;
        (row * self.width + column, sign_is_positive)
    }
}

fn low_bits_mask(bits: u32) -> u128 {
    match bits {
        0 => 0,
        128 => u128::MAX,
        _ => (1_u128 << bits) - 1,
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

    fn next_u128(&mut self) -> u128 {
        (u128::from(self.next_u64()) << 64) | u128::from(self.next_u64())
    }
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::hash::{Hash, Hasher};

    use super::{CountSketch, DEPTH_DENOMINATOR};
    use crate::SketchError;

    const SEED: u64 = 0xA409_3822_299F_31D0;

    #[test]
    fn constructor_uses_documented_point_query_bound() {
        let sketch = CountSketch::new(0.05, 0.01, SEED).unwrap();
        assert_eq!(sketch.width(), 4_096);
        assert_eq!(sketch.depth(), 13);
        assert!(sketch.depth() % 2 == 1);

        let failure_bound = (-(sketch.depth() as f64) * DEPTH_DENOMINATOR / 2.0).exp();
        assert!(failure_bound <= 0.01, "bound={failure_bound}");
    }

    #[test]
    fn constructors_reject_invalid_or_unallocatable_parameters() {
        assert!(CountSketch::new(0.0, 0.1, SEED).is_err());
        assert!(CountSketch::new(0.1, 0.0, SEED).is_err());
        assert!(CountSketch::new(1.0, 0.1, SEED).is_err());
        assert!(CountSketch::new(0.1, 1.0, SEED).is_err());
        assert!(CountSketch::new(f64::NAN, 0.1, SEED).is_err());
        assert!(CountSketch::new(f64::MIN_POSITIVE, 0.5, SEED).is_err());
        assert!(CountSketch::with_dimensions(0, 3, SEED).is_err());
        assert!(CountSketch::with_dimensions(3, 3, SEED).is_err());
        assert!(CountSketch::with_dimensions(4, 0, SEED).is_err());
        assert!(CountSketch::with_dimensions(4, 2, SEED).is_err());
        assert!(CountSketch::with_dimensions(usize::MAX, 1, SEED).is_err());
    }

    #[test]
    fn one_item_is_exact_for_positive_and_negative_updates() {
        let mut sketch = CountSketch::with_dimensions(128, 7, SEED).unwrap();
        sketch.add(&"x", 10).unwrap();
        sketch.add(&"x", -3).unwrap();
        assert_eq!(sketch.estimate(&"x"), 7);

        sketch.add_u64(42, -20).unwrap();
        sketch.add_u64(42, 4).unwrap();
        assert_eq!(sketch.estimate_u64(42), -16);
    }

    #[test]
    fn estimate_is_reasonable_with_noise() {
        let mut sketch = CountSketch::with_dimensions(2_048, 7, SEED).unwrap();
        sketch.add(&"hot-key", 5_000).unwrap();
        for value in 0_u64..50_000 {
            sketch.increment(&value).unwrap();
        }

        let estimate = sketch.estimate(&"hot-key");
        assert!((4_500..=5_500).contains(&estimate), "estimate={estimate}");
    }

    #[test]
    fn overflow_is_reported_without_mutation() {
        let mut sketch = CountSketch::with_dimensions(16, 3, SEED).unwrap();
        sketch.add_u64(7, i64::MAX).unwrap();
        let counters_before = sketch.counters.clone();

        assert_eq!(sketch.add_u64(7, 1), Err(SketchError::CounterOverflow));
        assert_eq!(sketch.counters, counters_before);
        assert_eq!(sketch.estimate_u64(7), i64::MAX);

        let mut fresh = CountSketch::with_dimensions(16, 3, SEED).unwrap();
        assert_eq!(
            fresh.add_u64(7, i64::MIN),
            Err(SketchError::CounterOverflow)
        );
        assert!(fresh.counters.iter().all(|&counter| counter == 0));
    }

    #[test]
    fn merge_is_linear_and_requires_the_same_seed() {
        let mut left = CountSketch::with_dimensions(512, 5, SEED).unwrap();
        let mut right = CountSketch::with_dimensions(512, 5, SEED).unwrap();
        let mut direct = CountSketch::with_dimensions(512, 5, SEED).unwrap();

        left.add(&"alpha", 100).unwrap();
        right.add(&"alpha", 50).unwrap();
        direct.add(&"alpha", 150).unwrap();
        left.merge(&right).unwrap();

        assert_eq!(left.counters, direct.counters);
        assert_eq!(left.estimate(&"alpha"), 150);

        let different_seed = CountSketch::with_dimensions(512, 5, SEED + 1).unwrap();
        assert_eq!(
            left.merge(&different_seed),
            Err(SketchError::IncompatibleSketches(
                "hash-family seeds must match for merge"
            ))
        );
    }

    #[test]
    fn merge_overflow_is_reported_without_mutation() {
        let mut left = CountSketch::with_dimensions(16, 3, SEED).unwrap();
        let mut right = CountSketch::with_dimensions(16, 3, SEED).unwrap();
        left.add_u64(1, i64::MAX).unwrap();
        right.add_u64(1, 1).unwrap();
        let counters_before = left.counters.clone();

        assert_eq!(left.merge(&right), Err(SketchError::CounterOverflow));
        assert_eq!(left.counters, counters_before);
    }

    #[test]
    fn cancellation_restores_counters_without_consuming_an_update_budget() {
        let mut sketch = CountSketch::with_dimensions(128, 3, SEED).unwrap();
        sketch.add(&"item", 7).unwrap();
        sketch.add(&"item", -7).unwrap();
        assert_eq!(sketch.estimate(&"item"), 0);
        assert!(sketch.counters.iter().all(|&counter| counter == 0));

        sketch.add(&"item", i64::MAX).unwrap();
        assert_eq!(sketch.estimate(&"item"), i64::MAX);
        sketch.add(&"item", -i64::MAX).unwrap();
        assert_eq!(sketch.estimate(&"item"), 0);

        sketch.clear();
        assert!(sketch.counters.iter().all(|&counter| counter == 0));
    }

    #[test]
    fn generic_operations_hash_an_item_once() {
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
        let item = CountedHash { calls: &calls };
        let mut sketch = CountSketch::with_dimensions(128, 7, SEED).unwrap();
        sketch.increment(&item).unwrap();
        assert_eq!(calls.get(), 1);
        assert_eq!(sketch.estimate(&item), 1);
        assert_eq!(calls.get(), 2);
    }

    #[test]
    fn seed_selects_reproducible_hash_families() {
        let first = CountSketch::with_dimensions(128, 7, SEED).unwrap();
        let second = CountSketch::with_dimensions(128, 7, SEED).unwrap();
        let different = CountSketch::with_dimensions(128, 7, SEED + 1).unwrap();

        assert_eq!(first.seed(), SEED);
        assert_eq!(first.rows, second.rows);
        assert_ne!(first.rows, different.rows);
    }
}
