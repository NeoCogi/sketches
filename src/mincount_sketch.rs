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
//! Count-Min frequency sketch with conservative updates.
//!
//! [`MinCountSketch`] summarizes a non-negative frequency vector. A query
//! returns the minimum of the counters selected by the queried item, so the
//! result is a one-sided upper estimate of its frequency. Updates use the
//! conservative rule: mapped counters are raised only as far as needed to make
//! the new estimate reflect the update.
//!
//! # Error guarantee
//!
//! [`MinCountSketch::new`] sizes the table for one fixed, non-adaptive point
//! query. Under the standard independent-hashing model, before counters
//! saturate, an item's true frequency `f[x]` and estimate `estimate(x)` satisfy
//!
//! `f[x] <= estimate(x)` and
//! `Pr[estimate(x) - f[x] > epsilon * ||f||_1] <= delta`.
//!
//! This is the point-query guarantee from the original [Count-Min paper].
//! Conservative updates cannot increase the error relative to ordinary
//! Count-Min updates. The width is the smallest power of two at least
//! `ceil(e / epsilon)`, and the depth is `ceil(ln(1 / delta))`.
//!
//! Integer item identifiers use strongly universal multiply-shift row
//! functions. A caller-owned seed is expanded deterministically into the row
//! coefficients, so sketches are reproducible without global random state. The
//! probability statement uses the conventional pseudorandom-hashing model:
//! choose the seed independently of the stream. It is not an adversarial or
//! cryptographic guarantee.
//!
//! Generic [`Hash`] items are fingerprinted once with seed-keyed SipHash before
//! applying the row functions. [`MinCountSketch::add_u64`] and
//! [`MinCountSketch::estimate_u64`] avoid that layer when the application
//! already has stable, distinct 64-bit item identifiers.
//!
//! # Seeds and merging
//!
//! Independently populated sketches must have the same seed and dimensions to
//! merge. Counter-wise addition produces a valid upper-bound summary of the
//! combined stream, but it need not match direct conservative ingestion and can
//! lose some of conservative update's accuracy advantage.
//!
//! # Arithmetic
//!
//! Counts saturate at [`u64::MAX`] rather than wrapping. Once either an item
//! count or total stream weight exceeds that range, the mathematical error
//! guarantee no longer applies.
//!
//! [Count-Min paper]: https://dimacs.rutgers.edu/~graham/pubs/papers/cm-full.pdf

use std::hash::{Hash, Hasher};

use siphasher::sip::SipHasher13;

use crate::{SketchError, splitmix64};

const SPLITMIX_INCREMENT: u64 = 0x9E37_79B9_7F4A_7C15;
const FINGERPRINT_DOMAIN_A: u64 = 0x3C6E_F372_FE94_F82B;
const FINGERPRINT_DOMAIN_B: u64 = 0xA54F_F53A_5F1D_36F1;
const ROW_DOMAIN: u64 = 0x510E_527F_ADE6_82D1;

#[derive(Debug, Clone, PartialEq, Eq)]
struct RowHash {
    multiplier: u128,
    offset: u128,
}

/// Approximate non-negative frequency sketch using conservative updates.
///
/// # Example
///
/// ```rust
/// use sketches::mincount_sketch::MinCountSketch;
///
/// // Choose production seeds independently of the stream. A fixed seed keeps
/// // this example reproducible.
/// let mut sketch = MinCountSketch::new(0.01, 0.01, 0x510E_527F_ADE6_82D1).unwrap();
/// sketch.add(&"cat", 2);
/// sketch.increment(&"cat");
/// assert!(sketch.estimate(&"cat") >= 3);
/// ```
#[derive(Debug, Clone)]
pub struct MinCountSketch {
    width: usize,
    counters: Vec<u64>,
    rows: Box<[RowHash]>,
    family_seed: u64,
    fingerprint_keys: (u64, u64),
    total_count: u64,
}

impl MinCountSketch {
    /// Builds a seeded sketch from point-query error parameters.
    ///
    /// `epsilon` and `delta` must be finite and strictly between zero and one.
    /// The seed selects the fingerprint and row-hash families. Choose it
    /// independently of the input, and use the same seed for shards that will
    /// later be merged.
    ///
    /// # Errors
    ///
    /// Returns [`SketchError::InvalidParameter`] when parameters are invalid,
    /// their dimensions are unrepresentable, or storage cannot be allocated.
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

        // The Count-Min proof needs at least e/epsilon counters per row.
        let minimum_width = (std::f64::consts::E / epsilon).ceil();
        if !minimum_width.is_finite() || minimum_width > usize::MAX as f64 {
            return Err(SketchError::InvalidParameter(
                "epsilon requires an unrepresentable width",
            ));
        }
        // Multiply-shift selects bit prefixes, so round up to a power of two.
        // Rounding up only strengthens the requested error bound.
        let width = (minimum_width as usize).checked_next_power_of_two().ok_or(
            SketchError::InvalidParameter("epsilon requires an unrepresentable width"),
        )?;

        // Computing -ln(delta) avoids overflowing the reciprocal for tiny,
        // positive subnormal values of delta.
        let minimum_depth = -delta.ln();
        if !minimum_depth.is_finite() || minimum_depth > usize::MAX as f64 {
            return Err(SketchError::InvalidParameter(
                "delta requires an unrepresentable depth",
            ));
        }
        let depth = (minimum_depth.ceil() as usize).max(1);

        Self::with_dimensions(width, depth, seed)
    }

    /// Builds a seeded sketch from explicit dimensions.
    ///
    /// `width` must be a non-zero power of two because the row family returns
    /// uniformly distributed bit prefixes. `depth` must be non-zero. Explicit
    /// dimensions do not by themselves state a caller-requested
    /// `(epsilon, delta)` contract.
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
        if depth == 0 {
            return Err(SketchError::InvalidParameter(
                "depth must be greater than zero",
            ));
        }

        // Rows are stored consecutively in one allocation. A cell at
        // (row, column) therefore lives at row * width + column.
        let table_len = width
            .checked_mul(depth)
            .ok_or(SketchError::InvalidParameter(
                "width * depth overflows usize",
            ))?;

        // Reserve explicitly so impossible or unavailable allocations become
        // InvalidParameter errors rather than capacity-overflow panics.
        let mut counters = Vec::new();
        counters
            .try_reserve_exact(table_len)
            .map_err(|_| SketchError::InvalidParameter("counter table is too large to allocate"))?;
        counters.resize(table_len, 0);

        // Build one independent multiply-shift function per row. Only the low
        // arithmetic_bits bits participate in the modular arithmetic used by
        // location(), so coefficients are masked to that same domain.
        let index_bits = width.trailing_zeros();
        let arithmetic_bits = 64 + index_bits.saturating_sub(1);
        let index_mask = low_bits_mask(arithmetic_bits);
        let mut seed_stream = SeedStream::new(seed ^ ROW_DOMAIN);
        let mut rows = Vec::new();
        rows.try_reserve_exact(depth)
            .map_err(|_| SketchError::InvalidParameter("depth is too large to allocate"))?;
        rows.extend((0..depth).map(|_| RowHash {
            multiplier: seed_stream.next_u128() & index_mask,
            offset: seed_stream.next_u128() & index_mask,
        }));

        // The generic-item fingerprint uses a separate domain from the row
        // functions. This prevents their random streams from overlapping.
        Ok(Self {
            width,
            counters,
            rows: rows.into_boxed_slice(),
            family_seed: seed,
            fingerprint_keys: (
                splitmix64(seed ^ FINGERPRINT_DOMAIN_A),
                splitmix64(seed ^ FINGERPRINT_DOMAIN_B),
            ),
            total_count: 0,
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

    /// Returns the total positive weight added, saturating at [`u64::MAX`].
    pub fn total_count(&self) -> u64 {
        self.total_count
    }

    /// Returns `true` when no positive weight has been added.
    pub fn is_empty(&self) -> bool {
        self.total_count == 0
    }

    /// Conservatively adds `count` occurrences after hashing the item once.
    pub fn add<T: Hash + ?Sized>(&mut self, item: &T, count: u64) {
        if count == 0 {
            return;
        }

        // Hash an arbitrary-sized item once; every row then hashes this compact
        // identifier with inexpensive integer arithmetic.
        self.add_u64(self.fingerprint(item), count);
    }

    /// Conservatively adds `count` occurrences of a stable 64-bit item ID.
    ///
    /// This bypasses generic fingerprinting. Distinct logical items must have
    /// distinct identifiers.
    pub fn add_u64(&mut self, item_id: u64, count: u64) {
        if count == 0 {
            return;
        }

        // First pass: querying a Count-Min sketch means taking the smallest
        // mapped counter. This is the item's current upper estimate.
        let mut minimum = u64::MAX;
        for row in 0..self.depth() {
            minimum = minimum.min(self.counters[self.location(row, item_id)]);
        }

        // Second pass: raise only counters below the new estimate. Counters
        // already above target contain collision noise and need not grow.
        // This is the conservative-update rule.
        let target = minimum.saturating_add(count);
        for row in 0..self.depth() {
            let index = self.location(row, item_id);
            self.counters[index] = self.counters[index].max(target);
        }
        // Track stream weight once, independently of how many row counters
        // changed during the conservative update.
        self.total_count = self.total_count.saturating_add(count);
    }

    /// Adds exactly one occurrence after hashing the item once.
    pub fn increment<T: Hash + ?Sized>(&mut self, item: &T) {
        self.add(item, 1);
    }

    /// Adds exactly one occurrence of a stable 64-bit item ID.
    pub fn increment_u64(&mut self, item_id: u64) {
        self.add_u64(item_id, 1);
    }

    /// Returns the one-sided upper frequency estimate for an item.
    pub fn estimate<T: Hash + ?Sized>(&self, item: &T) -> u64 {
        self.estimate_u64(self.fingerprint(item))
    }

    /// Returns the one-sided upper estimate for a stable 64-bit item ID.
    pub fn estimate_u64(&self, item_id: u64) -> u64 {
        // Every selected counter contains the item's count plus possible
        // collision noise. The minimum is therefore the tightest upper view.
        let mut minimum = u64::MAX;
        for row in 0..self.depth() {
            minimum = minimum.min(self.counters[self.location(row, item_id)]);
        }
        minimum
    }

    /// Resets all counts while retaining the allocation and hash family.
    pub fn clear(&mut self) {
        self.counters.fill(0);
        self.total_count = 0;
    }

    /// Adds another compatible sketch into this sketch.
    ///
    /// Compatibility requires equal dimensions and the same family seed.
    /// Counter-wise addition preserves the one-sided upper-bound property, but
    /// the result need not equal direct conservative ingestion of both streams.
    ///
    /// # Errors
    ///
    /// Returns [`SketchError::IncompatibleSketches`] for a dimension or seed
    /// mismatch.
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

        // Corresponding cells describe the same hash buckets, so addition
        // combines their stream weights. This remains an upper-bound sketch,
        // though it is not identical to replaying both streams conservatively.
        for (left, right) in self.counters.iter_mut().zip(other.counters.iter()) {
            *left = left.saturating_add(*right);
        }
        self.total_count = self.total_count.saturating_add(other.total_count);
        Ok(())
    }

    fn fingerprint<T: Hash + ?Sized>(&self, item: &T) -> u64 {
        // SipHash turns an arbitrary Hash implementation into one stable ID for
        // this sketch family. Row selection never hashes the original item
        // again, which matters for strings and other large keys.
        let mut hasher =
            SipHasher13::new_with_keys(self.fingerprint_keys.0, self.fingerprint_keys.1);
        item.hash(&mut hasher);
        hasher.finish()
    }

    fn location(&self, row: usize, item_id: u64) -> usize {
        // For width = 2^l, multiply-shift evaluates
        //     (a * item_id + b) mod 2^(64 + l - 1)
        // and uses its highest l bits as a uniformly distributed column.
        let index_bits = self.width.trailing_zeros();
        let column = if index_bits == 0 {
            // A width-one row has only column zero and needs no arithmetic.
            0
        } else {
            let arithmetic_bits = 64 + index_bits - 1;
            let row_hash = &self.rows[row];
            let mixed = row_hash
                .multiplier
                .wrapping_mul(item_id as u128)
                .wrapping_add(row_hash.offset)
                & low_bits_mask(arithmetic_bits);
            (mixed >> (arithmetic_bits - index_bits)) as usize
        };
        // Convert the two-dimensional row/column location into the flat table.
        row * self.width + column
    }
}

fn low_bits_mask(bits: u32) -> u128 {
    // Avoid shifting by the integer width, which Rust deliberately rejects.
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
        // SplitMix expands one caller seed into a reproducible stream of row
        // coefficients; advancing the state keeps successive outputs distinct.
        let value = splitmix64(self.state);
        self.state = self.state.wrapping_add(SPLITMIX_INCREMENT);
        value
    }

    fn next_u128(&mut self) -> u128 {
        // Multiply-shift works in a domain wider than the 64-bit item ID.
        (u128::from(self.next_u64()) << 64) | u128::from(self.next_u64())
    }
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::hash::{Hash, Hasher};

    use super::MinCountSketch;
    use crate::SketchError;

    const SEED: u64 = 0x510E_527F_ADE6_82D1;

    #[test]
    fn constructor_uses_documented_point_query_bound() {
        let sketch = MinCountSketch::new(0.01, 0.01, SEED).unwrap();
        assert_eq!(sketch.width(), 512);
        assert_eq!(sketch.depth(), 5);
        assert!(std::f64::consts::E / sketch.width() as f64 <= 0.01);
        assert!((-(sketch.depth() as f64)).exp() <= 0.01);
    }

    #[test]
    fn constructors_reject_invalid_or_unallocatable_parameters() {
        assert!(MinCountSketch::new(0.0, 0.1, SEED).is_err());
        assert!(MinCountSketch::new(0.1, 0.0, SEED).is_err());
        assert!(MinCountSketch::new(1.0, 0.1, SEED).is_err());
        assert!(MinCountSketch::new(0.1, 1.0, SEED).is_err());
        assert!(MinCountSketch::new(f64::NAN, 0.1, SEED).is_err());
        assert!(MinCountSketch::new(f64::MIN_POSITIVE, 0.5, SEED).is_err());
        assert!(MinCountSketch::with_dimensions(0, 3, SEED).is_err());
        assert!(MinCountSketch::with_dimensions(3, 3, SEED).is_err());
        assert!(MinCountSketch::with_dimensions(4, 0, SEED).is_err());
        assert!(MinCountSketch::with_dimensions(4, usize::MAX, SEED).is_err());
        assert!(MinCountSketch::with_dimensions(1_usize << (usize::BITS - 1), 1, SEED).is_err());
    }

    #[test]
    fn constructor_handles_tiny_positive_delta_without_reciprocal_overflow() {
        let sketch = MinCountSketch::new(0.9, f64::from_bits(1), SEED).unwrap();
        assert_eq!(sketch.width(), 4);
        assert_eq!(sketch.depth(), 745);
    }

    #[test]
    fn one_item_stream_is_exact() {
        let mut sketch = MinCountSketch::with_dimensions(128, 5, SEED).unwrap();
        sketch.add(&"generic", 17);
        sketch.add_u64(42, 23);
        assert_eq!(sketch.estimate(&"generic"), 17);
        assert_eq!(sketch.estimate_u64(42), 23);
        assert_eq!(sketch.total_count(), 40);
    }

    #[test]
    fn batched_update_matches_repeated_consecutive_updates() {
        let mut batched = MinCountSketch::with_dimensions(32, 5, SEED).unwrap();
        let mut repeated = MinCountSketch::with_dimensions(32, 5, SEED).unwrap();

        batched.add_u64(7, 100);
        for _ in 0..100 {
            repeated.increment_u64(7);
        }

        assert_eq!(batched.counters, repeated.counters);
        assert_eq!(batched.total_count(), repeated.total_count());
    }

    #[test]
    fn estimates_never_fall_below_exact_counts_under_collisions() {
        for seed in 0..16 {
            let mut sketch = MinCountSketch::with_dimensions(32, 5, seed).unwrap();
            let mut exact = [0_u64; 128];
            for operation in 0..20_000_u64 {
                let item = operation.wrapping_mul(104_729) % exact.len() as u64;
                let count = operation % 5 + 1;
                sketch.add_u64(item, count);
                exact[item as usize] += count;
            }
            for (item, &count) in exact.iter().enumerate() {
                assert!(sketch.estimate_u64(item as u64) >= count);
            }
        }
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
        let mut sketch = MinCountSketch::with_dimensions(128, 7, SEED).unwrap();
        sketch.increment(&item);
        assert_eq!(calls.get(), 1);
        assert_eq!(sketch.estimate(&item), 1);
        assert_eq!(calls.get(), 2);
    }

    #[test]
    fn seed_selects_reproducible_hash_families() {
        let first = MinCountSketch::with_dimensions(128, 7, SEED).unwrap();
        let second = MinCountSketch::with_dimensions(128, 7, SEED).unwrap();
        let different = MinCountSketch::with_dimensions(128, 7, SEED + 1).unwrap();

        assert_eq!(first.seed(), SEED);
        assert_eq!(first.rows, second.rows);
        assert_ne!(first.rows, different.rows);
    }

    #[test]
    fn clear_resets_counts_but_retains_configuration() {
        let mut sketch = MinCountSketch::with_dimensions(64, 5, SEED).unwrap();
        sketch.add_u64(7, 10);
        sketch.clear();

        assert!(sketch.is_empty());
        assert_eq!(sketch.total_count(), 0);
        assert_eq!(sketch.estimate_u64(7), 0);
        assert_eq!(sketch.seed(), SEED);
        assert_eq!(sketch.width(), 64);
        assert_eq!(sketch.depth(), 5);
    }

    #[test]
    fn merge_preserves_upper_bounds_and_checks_configuration() {
        let mut left = MinCountSketch::with_dimensions(64, 5, SEED).unwrap();
        let mut right = MinCountSketch::with_dimensions(64, 5, SEED).unwrap();
        left.add_u64(7, 10);
        right.add_u64(7, 15);
        right.add_u64(8, 4);

        left.merge(&right).unwrap();
        assert!(left.estimate_u64(7) >= 25);
        assert!(left.estimate_u64(8) >= 4);
        assert_eq!(left.total_count(), 29);

        let different_width = MinCountSketch::with_dimensions(128, 5, SEED).unwrap();
        assert_eq!(
            left.merge(&different_width),
            Err(SketchError::IncompatibleSketches(
                "width/depth must match for merge"
            ))
        );

        let different_seed = MinCountSketch::with_dimensions(64, 5, SEED + 1).unwrap();
        assert_eq!(
            left.merge(&different_seed),
            Err(SketchError::IncompatibleSketches(
                "hash-family seeds must match for merge"
            ))
        );
    }

    #[test]
    fn arithmetic_saturates_instead_of_wrapping() {
        let mut sketch = MinCountSketch::with_dimensions(32, 5, SEED).unwrap();
        sketch.add_u64(7, u64::MAX);
        sketch.increment_u64(7);

        assert_eq!(sketch.estimate_u64(7), u64::MAX);
        assert_eq!(sketch.total_count(), u64::MAX);
    }
}
