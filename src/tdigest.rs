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
//! t-digest for approximate quantiles, especially in distribution tails.
//!
//! Quantile interpolation follows Section 2.9 of Dunning and Ertl's
//! [t-digest paper] and the singleton-aware behavior of the reference
//! `MergingDigest`. Exact samples use zero-based rank
//! `min(floor(q * N), N - 1)`. Centroids containing multiple samples are
//! interpolated at their midpoint ranks, while singleton centroids remain
//! exact steps. The exact observed minimum and maximum are retained separately.
//! The paper states its mean and interpolation formulas over real numbers; this
//! implementation evaluates the equivalent convex combinations in a
//! sign-aware form so intermediate `f64` arithmetic stays finite for every
//! finite sample value.
//! Observation and centroid counts are exact `u64` integers. Adding or merging
//! beyond `u64::MAX` observations returns [`SketchError::ObservationCountOverflow`]
//! before changing the digest. Counts are converted to `f64` only for centroid
//! means, compression and query interpolation; those calculations still round,
//! and adjacent integer ranks above `2^53` need not remain distinguishable.
//! Ingestion follows the paper's progressive-merge design: additions collect
//! in an ordered buffer and are periodically merged with the already ordered
//! centroid array. Keeping the buffer ordered costs `O(log B)` per addition,
//! where `B` is bounded by roughly `10 * compression`, but lets read-only
//! quantile queries traverse all current data without cloning or sorting.
//!
//! [t-digest paper]: https://arxiv.org/pdf/1902.04023

use std::cmp::Ordering;
use std::collections::BTreeMap;

use crate::SketchError;

const BUFFER_MULTIPLIER: f64 = 10.0;

/// A finite mean representing a positive, exact number of observations.
#[derive(Debug, Clone, Copy)]
struct Centroid {
    /// Convex combination of the observations represented by this centroid.
    mean: f64,
    /// Exact mass; all merged and buffered weights sum to `TDigest::count`.
    weight: u64,
}

#[derive(Debug, Clone, Copy)]
struct OrderedValue(f64);

impl PartialEq for OrderedValue {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for OrderedValue {}

impl PartialOrd for OrderedValue {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for OrderedValue {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.total_cmp(&other.0)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct BufferedKey {
    mean: OrderedValue,
    sequence: u64,
}

/// Approximate quantile sketch based on t-digest centroids.
///
/// # Example
/// ```rust
/// use sketches::tdigest::TDigest;
///
/// let mut digest = TDigest::new(100.0).unwrap();
/// for value in 0_u64..10_000 {
///     digest.add(value as f64).unwrap();
/// }
///
/// let p95 = digest.quantile(0.95).unwrap();
/// assert!(p95 > 9_000.0 && p95 < 10_000.0);
/// ```
#[derive(Debug, Clone)]
pub struct TDigest {
    /// Finite compression parameter, at least ten.
    compression: f64,
    /// Fully merged centroids, always ordered by mean.
    centroids: Vec<Centroid>,
    /// Pending positive integer weights, ordered for read-only queries.
    buffered: BTreeMap<BufferedKey, u64>,
    /// Next tie-breaking key, reset when the pending buffer is compressed.
    next_sequence: u64,
    /// Authoritative exact count, equal to the sum of all centroid weights.
    count: u64,
    /// Exact observed minimum, or positive infinity when empty.
    min: f64,
    /// Exact observed maximum, or negative infinity when empty.
    max: f64,
}

impl TDigest {
    /// Creates a digest with the given compression parameter.
    ///
    /// Higher compression generally improves quantile accuracy at the cost of
    /// more centroids in memory.
    ///
    /// # Errors
    /// Returns [`SketchError::InvalidParameter`] for non-finite or too-small
    /// compression values.
    pub fn new(compression: f64) -> Result<Self, SketchError> {
        if !compression.is_finite() || compression < 10.0 {
            return Err(SketchError::InvalidParameter(
                "compression must be finite and greater than or equal to 10",
            ));
        }

        Ok(Self {
            compression,
            centroids: Vec::new(),
            buffered: BTreeMap::new(),
            next_sequence: 0,
            count: 0,
            min: f64::INFINITY,
            max: f64::NEG_INFINITY,
        })
    }

    /// Creates a digest from a target quantile error heuristic.
    ///
    /// Internally uses: `compression = ceil(10 / error)`.
    ///
    /// # Errors
    /// Returns [`SketchError::InvalidParameter`] when `quantile_error` is invalid.
    pub fn with_error_rate(quantile_error: f64) -> Result<Self, SketchError> {
        if !quantile_error.is_finite() || quantile_error <= 0.0 || quantile_error >= 1.0 {
            return Err(SketchError::InvalidParameter(
                "quantile_error must be finite and strictly between 0 and 1",
            ));
        }

        let compression = (10.0 / quantile_error).ceil().max(10.0);
        Self::new(compression)
    }

    /// Returns the configured compression parameter.
    pub fn compression(&self) -> f64 {
        self.compression
    }

    /// Returns the number of merged and buffered centroids currently tracked.
    pub fn centroid_count(&self) -> usize {
        self.centroids.len() + self.buffered.len()
    }

    /// Returns the exact number of finite observations, including merged data.
    pub fn count(&self) -> u64 {
        self.count
    }

    /// Returns `true` when no values were added.
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// Adds one value to the digest.
    ///
    /// Every finite `f64`, including values at either finite extreme, is
    /// supported. Non-finite values are ignored and return `Ok(())`, even at
    /// the count limit.
    ///
    /// # Errors
    /// Returns [`SketchError::ObservationCountOverflow`] at `u64::MAX` finite
    /// observations. On error, the entire digest is unchanged.
    pub fn add(&mut self, value: f64) -> Result<(), SketchError> {
        if !value.is_finite() {
            return Ok(());
        }

        // Validate the observation owner before extrema, buffer or sequence
        // changes. The private ingestion step can then update mass infallibly.
        self.count
            .checked_add(1)
            .ok_or(SketchError::ObservationCountOverflow)?;

        self.min = self.min.min(value);
        self.max = self.max.max(value);
        self.add_centroid(value, 1);
        Ok(())
    }

    /// Returns the approximate quantile for `q` in `[0, 1]`.
    ///
    /// For exact, uncompressed samples, `q` selects zero-based rank
    /// `min(floor(q * N), N - 1)`. Thus the median of `[0, 10]` is `10`.
    /// This is the same empirical inverse-CDF convention used by
    /// [`crate::kll::KllSketch`].
    ///
    /// For compressed centroids, interpolation follows the [t-digest paper]:
    /// centroids containing multiple samples are positioned at their midpoint
    /// ranks, singleton centroids remain exact samples, and terminal
    /// interpolation uses the separately retained observed minimum and maximum.
    /// Rank calculations use rounded `f64` counts, so adjacent integer ranks
    /// above `2^53` are not guaranteed to be distinguishable.
    ///
    /// # Errors
    /// Returns [`SketchError::InvalidParameter`] for invalid `q` or empty
    /// digests.
    ///
    /// [t-digest paper]: https://arxiv.org/pdf/1902.04023
    pub fn quantile(&self, q: f64) -> Result<f64, SketchError> {
        if !q.is_finite() || !(0.0..=1.0).contains(&q) {
            return Err(SketchError::InvalidParameter(
                "q must be finite and in [0, 1]",
            ));
        }
        if self.centroid_count() == 0 {
            return Err(SketchError::InvalidParameter(
                "quantile is undefined for an empty digest",
            ));
        }

        if q == 0.0 {
            return Ok(self.min);
        }
        if q == 1.0 {
            return Ok(self.max);
        }

        let mut centroids = self.ordered_centroids();
        let first = centroids.next().expect("non-empty digest has a centroid");
        if self.centroid_count() == 1 {
            return Ok(first.mean);
        }

        let total_weight = self.count as f64;
        let index = q * total_weight;
        if index < 1.0 {
            return Ok(self.min);
        }

        if first.weight > 1 && index < first.weight as f64 * 0.5 {
            let interior_weight = first.weight as f64 * 0.5 - 1.0;
            if interior_weight > 0.0 {
                let fraction = ((index - 1.0) / interior_weight).clamp(0.0, 1.0);
                return Ok(finite_lerp(self.min, first.mean, fraction));
            }
        }

        if index > total_weight - 1.0 {
            return Ok(self.max);
        }

        let last = self
            .last_ordered_centroid()
            .expect("non-empty digest has a centroid");
        let weight_from_right = total_weight - index;
        if last.weight > 1 && weight_from_right <= last.weight as f64 * 0.5 {
            let interior_weight = last.weight as f64 * 0.5 - 1.0;
            if interior_weight > 0.0 {
                let fraction = ((weight_from_right - 1.0) / interior_weight).clamp(0.0, 1.0);
                return Ok(finite_lerp(self.max, last.mean, fraction));
            }
        }

        let mut weight_so_far = first.weight as f64 * 0.5;
        let mut left = first;
        for right in centroids {
            // These disjoint centroid weights sum to at most the exact count.
            let interval_weight = (left.weight + right.weight) as f64 * 0.5;

            if weight_so_far + interval_weight > index {
                let mut left_singleton_weight = 0.0;
                if left.weight == 1 {
                    if index - weight_so_far < 0.5 {
                        return Ok(left.mean);
                    }
                    left_singleton_weight = 0.5;
                }

                let mut right_singleton_weight = 0.0;
                if right.weight == 1 {
                    if weight_so_far + interval_weight - index <= 0.5 {
                        return Ok(right.mean);
                    }
                    right_singleton_weight = 0.5;
                }

                let right_weight = index - weight_so_far - left_singleton_weight;
                let left_weight = weight_so_far + interval_weight - index - right_singleton_weight;
                return Ok(weighted_average(
                    left.mean,
                    left_weight,
                    right.mean,
                    right_weight,
                ));
            }

            weight_so_far += interval_weight;
            left = right;
        }

        Ok(self.max)
    }

    /// Merges another digest into this one.
    ///
    /// Centroids are recompressed and the exact observed minimum and maximum
    /// are combined independently so endpoint queries remain exact.
    ///
    /// # Errors
    /// Returns [`SketchError::IncompatibleSketches`] when compression differs.
    /// For compatible digests, returns [`SketchError::ObservationCountOverflow`]
    /// if the combined count exceeds `u64::MAX`. Either error leaves the entire
    /// destination unchanged.
    pub fn merge(&mut self, other: &Self) -> Result<(), SketchError> {
        if (self.compression - other.compression).abs() > f64::EPSILON {
            return Err(SketchError::IncompatibleSketches(
                "compression must match for merge",
            ));
        }

        // Preflight the complete merge before changing any owned state. Each
        // partial ingestion is then bounded by this validated final count.
        self.count
            .checked_add(other.count)
            .ok_or(SketchError::ObservationCountOverflow)?;

        if !other.is_empty() {
            self.min = self.min.min(other.min);
            self.max = self.max.max(other.max);
        }

        for centroid in other.ordered_centroids() {
            self.add_centroid(centroid.mean, centroid.weight);
        }
        self.compress();
        Ok(())
    }

    /// Clears centroids, buffered observations, count and extrema for reuse.
    pub fn clear(&mut self) {
        self.centroids.clear();
        self.buffered.clear();
        self.next_sequence = 0;
        self.count = 0;
        self.min = f64::INFINITY;
        self.max = f64::NEG_INFINITY;
    }

    /// Ingests a valid centroid after the public owner checks count capacity.
    ///
    /// The caller supplies a finite mean and positive weight. During a merge,
    /// the incoming weights sum to `other.count`, so every partial count is
    /// bounded by the preflighted total. Compression sees the exact mass of the
    /// data ingested so far, preserving the progressive-merge behavior.
    fn add_centroid(&mut self, value: f64, weight: u64) {
        debug_assert!(value.is_finite() && weight > 0);

        if self.next_sequence == u64::MAX {
            self.compress();
        }

        let key = BufferedKey {
            mean: OrderedValue(value),
            sequence: self.next_sequence,
        };
        self.next_sequence += 1;
        let replaced = self.buffered.insert(key, weight);
        debug_assert!(replaced.is_none());

        self.count += weight;
        if self.buffered.len() >= self.buffer_limit() {
            self.compress();
        }
    }

    fn buffer_limit(&self) -> usize {
        (self.compression * BUFFER_MULTIPLIER).ceil() as usize
    }

    fn ordered_centroids(&self) -> OrderedCentroids<'_> {
        OrderedCentroids {
            merged: self.centroids.iter().peekable(),
            buffered: self.buffered.iter().peekable(),
        }
    }

    fn last_ordered_centroid(&self) -> Option<Centroid> {
        let merged = self.centroids.last().copied();
        let buffered = self
            .buffered
            .last_key_value()
            .map(|(key, &weight)| Centroid {
                mean: key.mean.0,
                weight,
            });

        match (merged, buffered) {
            (None, None) => None,
            (Some(centroid), None) | (None, Some(centroid)) => Some(centroid),
            (Some(merged), Some(buffered)) => {
                if merged.mean.total_cmp(&buffered.mean) == Ordering::Greater {
                    Some(merged)
                } else {
                    Some(buffered)
                }
            }
        }
    }

    /// Evaluates the scale-function limit using the finite exact count owner.
    fn max_centroid_weight(&self, q: f64) -> f64 {
        let scaled = (self.count as f64 / self.compression) * 4.0 * q * (1.0 - q);
        scaled.max(1.0)
    }

    /// Combines ordered centroids without changing their total integer mass.
    ///
    /// Disjoint weights and completed mass sum to at most `self.count`, so the
    /// integer additions cannot overflow. Only ratios and means use `f64`.
    fn compress(&mut self) {
        if self.buffered.is_empty() && self.centroids.len() <= 1 {
            return;
        }

        let old = std::mem::take(&mut self.centroids);
        let buffered = std::mem::take(&mut self.buffered);
        let capacity = old.len() + buffered.len();
        let mut old = old.into_iter().peekable();
        let mut buffered = buffered.into_iter().peekable();
        let mut merged: Vec<Centroid> = Vec::with_capacity(capacity);
        let mut cumulative = 0_u64;

        loop {
            let take_buffered = match (old.peek(), buffered.peek()) {
                (None, None) => break,
                (None, Some(_)) => true,
                (Some(_), None) => false,
                (Some(left), Some((right, _))) => {
                    left.mean.total_cmp(&right.mean.0) == Ordering::Greater
                }
            };
            let centroid = if take_buffered {
                let (key, weight) = buffered.next().expect("buffered centroid is available");
                Centroid {
                    mean: key.mean.0,
                    weight,
                }
            } else {
                old.next().expect("merged centroid is available")
            };

            if let Some(last) = merged.last_mut() {
                let q = ((cumulative as f64 + 0.5 * last.weight as f64) / self.count.max(1) as f64)
                    .clamp(0.0, 1.0);
                let max_weight = self.max_centroid_weight(q);

                let updated_weight = last.weight + centroid.weight;
                if updated_weight as f64 <= max_weight {
                    last.mean = weighted_average(
                        last.mean,
                        last.weight as f64,
                        centroid.mean,
                        centroid.weight as f64,
                    );
                    last.weight = updated_weight;
                    continue;
                }

                cumulative += last.weight;
            }

            merged.push(centroid);
        }

        self.centroids = merged;
        self.next_sequence = 0;
    }
}

/// Borrowed merge of the compressed array and pending integer-weight buffer.
struct OrderedCentroids<'a> {
    /// Borrowed compressed means with exact integer weights.
    merged: std::iter::Peekable<std::slice::Iter<'a, Centroid>>,
    /// Borrowed pending means and positive integer weights.
    buffered: std::iter::Peekable<std::collections::btree_map::Iter<'a, BufferedKey, u64>>,
}

impl Iterator for OrderedCentroids<'_> {
    type Item = Centroid;

    fn next(&mut self) -> Option<Self::Item> {
        let take_buffered = match (self.merged.peek(), self.buffered.peek()) {
            (None, None) => return None,
            (None, Some(_)) => true,
            (Some(_), None) => false,
            (Some(left), Some((right, _))) => {
                left.mean.total_cmp(&right.mean.0) == Ordering::Greater
            }
        };

        if take_buffered {
            self.buffered.next().map(|(key, &weight)| Centroid {
                mean: key.mean.0,
                weight,
            })
        } else {
            self.merged.next().copied()
        }
    }
}

/// Evaluates a convex combination without overflowing for finite endpoints.
///
/// Same-sign endpoints use a bounded difference. Opposite-sign endpoints use
/// scaled terms whose addition cannot overflow because their signs differ.
fn finite_lerp(left: f64, right: f64, right_fraction: f64) -> f64 {
    debug_assert!(left.is_finite());
    debug_assert!(right.is_finite());
    debug_assert!((0.0..=1.0).contains(&right_fraction));

    if right_fraction <= 0.0 {
        return left;
    }
    if right_fraction >= 1.0 {
        return right;
    }

    let value = if left.is_sign_negative() == right.is_sign_negative() {
        left + (right - left) * right_fraction
    } else {
        left * (1.0 - right_fraction) + right * right_fraction
    };

    value.clamp(left.min(right), left.max(right))
}

/// Computes `right_weight / (left_weight + right_weight)` without forming the
/// potentially overflowing sum.
fn normalized_right_weight(left_weight: f64, right_weight: f64) -> f64 {
    debug_assert!(left_weight.is_finite() && left_weight >= 0.0);
    debug_assert!(right_weight.is_finite() && right_weight >= 0.0);

    match (left_weight > 0.0, right_weight > 0.0) {
        (false, false) => 0.5,
        (false, true) => 1.0,
        (true, false) => 0.0,
        (true, true) if left_weight >= right_weight => {
            let ratio = right_weight / left_weight;
            ratio / (1.0 + ratio)
        }
        (true, true) => {
            let ratio = left_weight / right_weight;
            1.0 / (1.0 + ratio)
        }
    }
}

fn weighted_average(left: f64, left_weight: f64, right: f64, right_weight: f64) -> f64 {
    finite_lerp(
        left,
        right,
        normalized_right_weight(left_weight, right_weight),
    )
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use super::{Centroid, TDigest, finite_lerp, weighted_average};
    use crate::SketchError;

    fn assert_close(actual: f64, expected: f64) {
        assert!(
            (actual - expected).abs() <= 1e-12,
            "actual={actual} expected={expected}"
        );
    }

    /// Builds an exact count through public unit additions and binary merging.
    fn repeated_value(value: f64, count: u64, compression: f64) -> TDigest {
        let mut digest = TDigest::new(compression).unwrap();
        let mut power = TDigest::new(compression).unwrap();
        power.add(value).unwrap();
        let mut remaining = count;
        while remaining != 0 {
            if remaining & 1 != 0 {
                digest.merge(&power).unwrap();
            }
            remaining >>= 1;
            if remaining != 0 {
                let copy = power.clone();
                power.merge(&copy).unwrap();
            }
        }
        assert_eq!(digest.count(), count);
        digest
    }

    /// Checks mass independently in a wider integer, without float conversion.
    fn assert_mass_invariant(digest: &TDigest) {
        let mass: u128 = digest
            .centroids
            .iter()
            .map(|centroid| u128::from(centroid.weight))
            .chain(digest.buffered.values().map(|&weight| u128::from(weight)))
            .sum();
        assert_eq!(mass, u128::from(digest.count()));
        let mut previous = f64::NEG_INFINITY;
        for centroid in digest.ordered_centroids() {
            assert!(centroid.weight > 0);
            assert!(centroid.mean.is_finite());
            assert!(previous <= centroid.mean);
            assert!(digest.min <= centroid.mean && centroid.mean <= digest.max);
            previous = centroid.mean;
        }
        assert_eq!(digest.is_empty(), digest.centroid_count() == 0);
        if digest.is_empty() {
            assert_eq!(digest.min, f64::INFINITY);
            assert_eq!(digest.max, f64::NEG_INFINITY);
        }
    }

    /// Compares every owned field and query bits, including the pending keys.
    fn assert_same_state(actual: &TDigest, expected: &TDigest) {
        assert_eq!(actual.compression.to_bits(), expected.compression.to_bits());
        assert_eq!(actual.count, expected.count);
        assert_eq!(actual.min.to_bits(), expected.min.to_bits());
        assert_eq!(actual.max.to_bits(), expected.max.to_bits());
        assert_eq!(actual.next_sequence, expected.next_sequence);
        let centroids = |digest: &TDigest| {
            digest
                .centroids
                .iter()
                .map(|centroid| (centroid.mean.to_bits(), centroid.weight))
                .collect::<Vec<_>>()
        };
        let buffered = |digest: &TDigest| {
            digest
                .buffered
                .iter()
                .map(|(key, &weight)| (key.mean.0.to_bits(), key.sequence, weight))
                .collect::<Vec<_>>()
        };
        assert_eq!(centroids(actual), centroids(expected));
        assert_eq!(buffered(actual), buffered(expected));
        for q in [0.0, 0.01, 0.25, 0.5, 0.75, 0.99, 1.0] {
            assert_eq!(
                actual.quantile(q).map(f64::to_bits),
                expected.quantile(q).map(f64::to_bits)
            );
        }
    }

    /// Tests query finiteness and order without imposing exact large ranks.
    fn assert_usable_quantiles(digest: &TDigest) {
        let mut previous = digest.min;
        for step in 0..=1_000 {
            let q = step as f64 / 1_000.0;
            let value = digest.quantile(q).unwrap();
            assert!(value.is_finite(), "q={q} count={}", digest.count());
            assert!(digest.min <= value && value <= digest.max);
            assert!(previous <= value, "q={q} previous={previous} value={value}");
            previous = value;
        }
        assert_eq!(
            digest.quantile(0.0).unwrap().to_bits(),
            digest.min.to_bits()
        );
        assert_eq!(
            digest.quantile(1.0).unwrap().to_bits(),
            digest.max.to_bits()
        );
    }

    #[test]
    fn constructor_validates_compression() {
        assert!(TDigest::new(5.0).is_err());
        assert!(TDigest::new(50.0).is_ok());
    }

    #[test]
    fn clone_merge_rejects_observation_count_overflow_before_mutation() {
        let mut digest = TDigest::new(10.0).unwrap();
        digest.add(-1.0).unwrap();
        digest.add(1.0).unwrap();
        for _ in 0..62 {
            let copy = digest.clone();
            digest.merge(&copy).unwrap();
            assert_mass_invariant(&digest);
        }
        assert!(digest.quantile(0.25).unwrap() < 0.0);

        let before = digest.clone();
        let copy = digest.clone();
        assert_eq!(
            digest.merge(&copy),
            Err(SketchError::ObservationCountOverflow)
        );
        assert_eq!(digest.count(), 1_u64 << 63);
        assert_same_state(&digest, &before);
        assert_usable_quantiles(&digest);
    }

    #[test]
    fn add_rejects_full_count_and_still_ignores_nonfinite_values() {
        let mut digest = repeated_value(3.0, u64::MAX, 10.0);
        let before = digest.clone();
        for value in [-f64::MAX, 0.0, f64::MAX] {
            assert_eq!(
                digest.add(value),
                Err(SketchError::ObservationCountOverflow)
            );
            assert_same_state(&digest, &before);
        }
        for value in [f64::NAN, f64::NEG_INFINITY, f64::INFINITY] {
            assert_eq!(digest.add(value), Ok(()));
            assert_same_state(&digest, &before);
        }
        for q in [0.0, 0.25, 0.5, 0.75, 1.0] {
            assert_eq!(digest.quantile(q).unwrap(), 3.0);
        }
    }

    #[test]
    fn exact_mass_survives_all_small_counts_and_every_binary_boundary() {
        let mut counts: BTreeSet<u64> = (0..=255).collect();
        for bit in 8..64 {
            let power = 1_u64 << bit;
            counts.extend([power - 1, power, power + 1]);
        }
        counts.extend([u64::MAX - 3, u64::MAX - 2, u64::MAX - 1, u64::MAX]);
        assert_eq!(counts.len(), 427);

        for compression in [10.0, 100.0] {
            for &count in &counts {
                let mut digest = repeated_value(3.0, count, compression);
                assert_mass_invariant(&digest);
                if count < u64::MAX {
                    digest.add(7.0).unwrap();
                    assert_eq!(digest.count(), count + 1);
                    assert_mass_invariant(&digest);
                }
                if count < u64::MAX - 1 {
                    let mut single = TDigest::new(compression).unwrap();
                    single.add(-5.0).unwrap();
                    digest.merge(&single).unwrap();
                    assert_eq!(digest.count(), count + 2);
                    assert_mass_invariant(&digest);
                }
            }
        }
    }

    #[test]
    fn boundary_merges_conserve_mass_and_keep_extreme_queries_usable() {
        let pairs = [
            (0, 0),
            (0, 1),
            (1, 0),
            (1, 1),
            (2, 3),
            ((1_u64 << 53) - 1, 2),
            (1_u64 << 53, 1),
            (1_u64 << 53, 3),
            (1_u64 << 63, (1_u64 << 63) - 1),
            (u64::MAX - 1, 1),
            (u64::MAX, 0),
            (u64::MAX, 1),
            (1_u64 << 63, 1_u64 << 63),
        ];
        for compression in [10.0, 40.0, 100.0] {
            for (left_count, right_count) in pairs {
                let mut left = repeated_value(-f64::MAX, left_count, compression);
                let right = repeated_value(f64::MAX, right_count, compression);
                let left_before = left.clone();
                let right_before = right.clone();
                let total = u128::from(left_count) + u128::from(right_count);
                let result = left.merge(&right);
                if total > u128::from(u64::MAX) {
                    assert_eq!(result, Err(SketchError::ObservationCountOverflow));
                    assert_same_state(&left, &left_before);
                } else {
                    assert_eq!(result, Ok(()));
                    assert_eq!(u128::from(left.count()), total);
                    assert_mass_invariant(&left);
                    if !left.is_empty() {
                        assert_usable_quantiles(&left);
                    }
                }
                assert_same_state(&right, &right_before);
            }
        }
    }

    #[test]
    fn overflow_preserves_merged_buffered_and_source_state() {
        let mut left = repeated_value(0.0, u64::MAX - 2, 10.0);
        left.add(-1.0).unwrap();
        left.add(1.0).unwrap();
        assert!(!left.centroids.is_empty());
        assert_eq!(left.buffered.len(), 2);
        assert_eq!(left.next_sequence, 2);
        let mut right = TDigest::new(10.0).unwrap();
        right.add(-f64::MAX).unwrap();
        right.add(f64::MAX).unwrap();
        let left_before = left.clone();
        let right_before = right.clone();

        assert_eq!(
            left.merge(&right),
            Err(SketchError::ObservationCountOverflow)
        );
        assert_same_state(&left, &left_before);
        assert_same_state(&right, &right_before);
        assert_eq!(
            left.add(f64::MAX),
            Err(SketchError::ObservationCountOverflow)
        );
        assert_same_state(&left, &left_before);
        assert_mass_invariant(&left);
    }

    #[test]
    fn incompatible_compression_precedes_count_overflow_without_mutation() {
        let mut left = repeated_value(3.0, u64::MAX, 10.0);
        let mut right = TDigest::new(11.0).unwrap();
        right.add(-f64::MAX).unwrap();
        let left_before = left.clone();
        let right_before = right.clone();
        assert_eq!(
            left.merge(&right),
            Err(SketchError::IncompatibleSketches(
                "compression must match for merge"
            ))
        );
        assert_same_state(&left, &left_before);
        assert_same_state(&right, &right_before);
    }

    #[test]
    fn clear_at_count_limit_resets_all_state_and_allows_reuse() {
        let mut digest = repeated_value(3.0, u64::MAX, 10.0);
        digest.clear();
        assert_eq!(digest.count(), 0);
        assert_eq!(digest.next_sequence, 0);
        assert_mass_invariant(&digest);
        let empty = digest.clone();
        for value in [f64::NAN, f64::NEG_INFINITY, f64::INFINITY] {
            assert_eq!(digest.add(value), Ok(()));
            assert_same_state(&digest, &empty);
        }
        digest.add(-f64::MAX).unwrap();
        let mut other = TDigest::new(10.0).unwrap();
        other.add(f64::MAX).unwrap();
        digest.merge(&other).unwrap();
        assert_eq!(digest.count(), 2);
        assert_mass_invariant(&digest);
        assert_usable_quantiles(&digest);
    }

    #[test]
    fn additions_are_buffered_and_batch_compressed() {
        let mut digest = TDigest::new(10.0).unwrap();
        let buffer_limit = digest.buffer_limit();

        for value in (1..buffer_limit).rev() {
            digest.add(value as f64).unwrap();
        }

        assert!(digest.centroids.is_empty());
        assert_eq!(digest.buffered.len(), buffer_limit - 1);
        assert!(digest.quantile(0.5).unwrap().is_finite());

        digest.add(0.0).unwrap();

        assert!(digest.buffered.is_empty());
        assert!(!digest.centroids.is_empty());
        assert!(
            digest
                .centroids
                .windows(2)
                .all(|pair| pair[0].mean <= pair[1].mean)
        );
    }

    #[test]
    fn quantile_reads_merged_and_buffered_state_without_mutation() {
        let mut digest = TDigest::new(10.0).unwrap();
        let buffer_limit = digest.buffer_limit();
        for value in 0..buffer_limit + 37 {
            digest
                .add(((value * 47) % (buffer_limit + 37)) as f64)
                .unwrap();
        }

        assert!(!digest.centroids.is_empty());
        assert!(!digest.buffered.is_empty());
        let merged_len = digest.centroids.len();
        let buffered_len = digest.buffered.len();
        let next_sequence = digest.next_sequence;

        for q in [0.01, 0.5, 0.99] {
            assert!(digest.quantile(q).unwrap().is_finite());
        }

        assert_eq!(digest.centroids.len(), merged_len);
        assert_eq!(digest.buffered.len(), buffered_len);
        assert_eq!(digest.next_sequence, next_sequence);
        assert!(
            digest
                .ordered_centroids()
                .map(|centroid| centroid.mean)
                .zip(
                    digest
                        .ordered_centroids()
                        .skip(1)
                        .map(|centroid| centroid.mean)
                )
                .all(|(left, right)| left <= right)
        );
    }

    #[test]
    fn quantile_rejects_invalid_input() {
        let digest = TDigest::new(100.0).unwrap();
        assert!(digest.quantile(0.5).is_err());
        assert!(digest.quantile(-0.1).is_err());
        assert!(digest.quantile(1.1).is_err());
    }

    #[test]
    fn finite_lerp_handles_extreme_finite_endpoints() {
        for (left, right, fraction) in [
            (-f64::MAX, f64::MAX, 0.25),
            (-f64::MAX, f64::MAX, 0.5),
            (-f64::MAX, f64::MAX, 0.75),
            (f64::MAX / 2.0, f64::MAX, 0.5),
            (-f64::MAX, -f64::MAX / 2.0, 0.5),
        ] {
            let value = finite_lerp(left, right, fraction);
            assert!(value.is_finite(), "left={left} right={right} value={value}");
            assert!(
                left.min(right) <= value && value <= left.max(right),
                "left={left} right={right} value={value}"
            );
        }

        assert_eq!(finite_lerp(-f64::MAX, f64::MAX, 0.0), -f64::MAX);
        assert_eq!(finite_lerp(-f64::MAX, f64::MAX, 0.5), 0.0);
        assert_eq!(finite_lerp(-f64::MAX, f64::MAX, 1.0), f64::MAX);
    }

    #[test]
    fn weighted_average_normalizes_extreme_weights_before_interpolation() {
        assert_eq!(
            weighted_average(-f64::MAX, f64::MAX, f64::MAX, f64::MAX),
            0.0
        );

        let unequal = weighted_average(-f64::MAX, f64::MAX, f64::MAX, f64::MAX / 2.0);
        let expected = -f64::MAX / 3.0;
        assert!(unequal.is_finite());
        assert!((unequal - expected).abs() <= expected.abs() * 4.0 * f64::EPSILON);
    }

    #[test]
    fn extreme_centroid_and_endpoint_interpolation_stays_finite() {
        let between_centroids = TDigest {
            compression: 100.0,
            centroids: vec![
                Centroid {
                    mean: -f64::MAX,
                    weight: 4,
                },
                Centroid {
                    mean: f64::MAX,
                    weight: 4,
                },
            ],
            buffered: BTreeMap::new(),
            next_sequence: 0,
            count: 8,
            min: -f64::MAX,
            max: f64::MAX,
        };
        assert_eq!(between_centroids.quantile(0.5).unwrap(), 0.0);

        let left_endpoint = TDigest {
            compression: 100.0,
            centroids: vec![
                Centroid {
                    mean: f64::MAX / 2.0,
                    weight: 4,
                },
                Centroid {
                    mean: f64::MAX,
                    weight: 4,
                },
            ],
            buffered: BTreeMap::new(),
            next_sequence: 0,
            count: 8,
            min: -f64::MAX,
            max: f64::MAX,
        };
        assert!(left_endpoint.quantile(0.1875).unwrap().is_finite());

        let right_endpoint = TDigest {
            compression: 100.0,
            centroids: vec![
                Centroid {
                    mean: -f64::MAX,
                    weight: 4,
                },
                Centroid {
                    mean: -f64::MAX / 2.0,
                    weight: 4,
                },
            ],
            buffered: BTreeMap::new(),
            next_sequence: 0,
            count: 8,
            min: -f64::MAX,
            max: f64::MAX,
        };
        assert!(right_endpoint.quantile(0.8125).unwrap().is_finite());
    }

    #[test]
    fn extreme_finite_stream_produces_finite_monotonic_quantiles() {
        let mut digest = TDigest::new(20.0).unwrap();
        for _ in 0..1_000 {
            digest.add(-f64::MAX).unwrap();
            digest.add(f64::MAX).unwrap();
        }

        assert!(
            digest
                .ordered_centroids()
                .all(|centroid| centroid.mean.is_finite())
        );

        let mut previous = digest.quantile(0.0).unwrap();
        for step in 1..=1_000 {
            let current = digest.quantile(step as f64 / 1_000.0).unwrap();
            assert!(current.is_finite(), "step={step} current={current}");
            assert!(
                previous <= current,
                "step={step} previous={previous} current={current}"
            );
            previous = current;
        }
    }

    #[test]
    fn shifted_and_scaled_streams_have_comparable_normalized_quantiles() {
        const SAMPLE_COUNT: usize = 10_000;
        let scale = f64::MAX / (SAMPLE_COUNT as f64 * 2.0);
        let shift = 5.0e307;
        let shifted_step = 4.0e292;

        let mut baseline = TDigest::new(100.0).unwrap();
        let mut scaled = TDigest::new(100.0).unwrap();
        let mut shifted = TDigest::new(100.0).unwrap();
        for index in 0..SAMPLE_COUNT {
            let value = index as f64;
            baseline.add(value).unwrap();
            scaled.add(value * scale).unwrap();
            shifted.add(shift + value * shifted_step).unwrap();
        }

        for q in [0.01, 0.5, 0.99] {
            let expected = baseline.quantile(q).unwrap();
            let scaled_normalized = scaled.quantile(q).unwrap() / scale;
            let shifted_normalized = (shifted.quantile(q).unwrap() - shift) / shifted_step;

            assert!(scaled_normalized.is_finite());
            assert!(shifted_normalized.is_finite());
            assert!(
                (scaled_normalized - expected).abs() <= 2.0,
                "q={q} expected={expected} scaled={scaled_normalized}"
            );
            assert!(
                (shifted_normalized - expected).abs() <= 2.0,
                "q={q} expected={expected} shifted={shifted_normalized}"
            );
        }
    }

    #[test]
    fn buffered_ingestion_retains_accuracy_across_orderings_and_compressions() {
        const SAMPLE_COUNT: usize = 10_000;

        for compression in [20.0, 100.0, 300.0] {
            for ordering in ["ascending", "descending", "permuted"] {
                let mut digest = TDigest::new(compression).unwrap();
                for index in 0..SAMPLE_COUNT {
                    let value = match ordering {
                        "ascending" => index,
                        "descending" => SAMPLE_COUNT - 1 - index,
                        "permuted" => (index * 7_919) % SAMPLE_COUNT,
                        _ => unreachable!(),
                    };
                    digest.add(value as f64).unwrap();
                }

                for q in [0.01, 0.5, 0.99] {
                    let estimate = digest.quantile(q).unwrap();
                    let exact = (q * SAMPLE_COUNT as f64)
                        .floor()
                        .min((SAMPLE_COUNT - 1) as f64);
                    let normalized_rank_error = (estimate - exact).abs() / SAMPLE_COUNT as f64;
                    assert!(
                        normalized_rank_error <= 0.03,
                        "compression={compression} ordering={ordering} q={q} \
                         estimate={estimate} error={normalized_rank_error}"
                    );
                }
            }
        }
    }

    #[test]
    fn two_singletons_step_at_the_empirical_rank_boundary() {
        let mut digest = TDigest::new(100.0).unwrap();
        digest.add(0.0).unwrap();
        digest.add(10.0).unwrap();

        let below = f64::from_bits(0.5_f64.to_bits() - 1);
        let above = f64::from_bits(0.5_f64.to_bits() + 1);
        assert_eq!(digest.quantile(below).unwrap(), 0.0);
        assert_eq!(digest.quantile(0.5).unwrap(), 10.0);
        assert_eq!(digest.quantile(above).unwrap(), 10.0);
    }

    #[test]
    fn multi_sample_centroids_interpolate_at_midpoint_ranks_and_extrema() {
        let digest = TDigest {
            compression: 100.0,
            centroids: vec![
                Centroid {
                    mean: 0.0,
                    weight: 4,
                },
                Centroid {
                    mean: 10.0,
                    weight: 4,
                },
            ],
            buffered: BTreeMap::new(),
            next_sequence: 0,
            count: 8,
            min: -2.0,
            max: 12.0,
        };

        for (q, expected) in [
            (0.0, -2.0),
            (0.125, -2.0),
            (0.1875, -1.0),
            (0.25, 0.0),
            (0.5, 5.0),
            (0.75, 10.0),
            (0.8125, 11.0),
            (0.875, 12.0),
            (1.0, 12.0),
        ] {
            assert_close(digest.quantile(q).unwrap(), expected);
        }
    }

    #[test]
    fn endpoint_queries_do_not_depend_on_terminal_centroid_means() {
        let digest = TDigest {
            compression: 100.0,
            centroids: vec![Centroid {
                mean: 5.0,
                weight: 8,
            }],
            buffered: BTreeMap::new(),
            next_sequence: 0,
            count: 8,
            min: 0.0,
            max: 10.0,
        };

        assert_eq!(digest.quantile(0.0).unwrap(), 0.0);
        assert_eq!(digest.quantile(1.0).unwrap(), 10.0);
    }

    #[test]
    fn adjacent_singletons_are_not_interpolated() {
        let digest = TDigest {
            compression: 100.0,
            centroids: vec![
                Centroid {
                    mean: 0.0,
                    weight: 4,
                },
                Centroid {
                    mean: 10.0,
                    weight: 1,
                },
                Centroid {
                    mean: 20.0,
                    weight: 1,
                },
            ],
            buffered: BTreeMap::new(),
            next_sequence: 0,
            count: 6,
            min: -2.0,
            max: 20.0,
        };

        assert_eq!(digest.quantile(5.0 / 6.0 - 1e-12).unwrap(), 10.0);
        assert_eq!(digest.quantile(5.0 / 6.0).unwrap(), 20.0);
        assert_eq!(digest.quantile(5.0 / 6.0 + 1e-12).unwrap(), 20.0);
    }

    #[test]
    fn exact_extrema_survive_compression_and_merge() {
        let mut left = TDigest::new(20.0).unwrap();
        left.add(-123.5).unwrap();
        for value in 0_u64..5_000 {
            left.add(value as f64).unwrap();
        }

        let mut right = TDigest::new(20.0).unwrap();
        for value in 5_000_u64..10_000 {
            right.add(value as f64).unwrap();
        }
        right.add(12_345.5).unwrap();

        left.merge(&right).unwrap();

        assert_eq!(left.quantile(0.0).unwrap(), -123.5);
        assert_eq!(left.quantile(1.0).unwrap(), 12_345.5);
    }

    #[test]
    fn quantiles_are_monotonic_across_centroid_boundaries() {
        let mut digest = TDigest::new(40.0).unwrap();
        for value in 0_u64..20_000 {
            digest.add((value % 1_003) as f64).unwrap();
        }

        let mut previous = digest.quantile(0.0).unwrap();
        for step in 1..=10_000 {
            let current = digest.quantile(step as f64 / 10_000.0).unwrap();
            assert!(
                previous <= current,
                "step={step} previous={previous} current={current}"
            );
            previous = current;
        }
    }

    #[test]
    fn median_estimate_is_reasonable() {
        let mut digest = TDigest::new(120.0).unwrap();
        for value in 0_u64..10_000 {
            digest.add(value as f64).unwrap();
        }

        let p50 = digest.quantile(0.5).unwrap();
        assert!(p50 > 4_400.0 && p50 < 5_600.0, "p50={p50}");
    }

    #[test]
    fn high_quantile_tracks_tail() {
        let mut digest = TDigest::new(120.0).unwrap();
        for value in 0_u64..10_000 {
            digest.add(value as f64).unwrap();
        }

        let p95 = digest.quantile(0.95).unwrap();
        let p99 = digest.quantile(0.99).unwrap();
        assert!(p95 <= p99, "p95={p95} p99={p99}");
        assert!(p99 > 9_000.0);
    }

    #[test]
    fn merge_combines_streams() {
        let mut left = TDigest::new(80.0).unwrap();
        let mut right = TDigest::new(80.0).unwrap();

        for value in 0_u64..5_000 {
            left.add(value as f64).unwrap();
        }
        for value in 5_000_u64..10_000 {
            right.add(value as f64).unwrap();
        }

        left.merge(&right).unwrap();
        let p90 = left.quantile(0.9).unwrap();
        assert!(p90 > 8_000.0);
    }

    #[test]
    fn merge_rejects_mismatched_compression() {
        let mut left = TDigest::new(80.0).unwrap();
        let right = TDigest::new(81.0).unwrap();
        assert!(left.merge(&right).is_err());
    }

    #[test]
    fn clear_resets_state() {
        let mut digest = TDigest::new(50.0).unwrap();
        digest.add(1.0).unwrap();
        digest.add(2.0).unwrap();
        digest.clear();
        assert!(digest.is_empty());
        assert!(digest.quantile(0.5).is_err());

        digest.add(9.0).unwrap();
        assert_eq!(digest.quantile(0.0).unwrap(), 9.0);
        assert_eq!(digest.quantile(1.0).unwrap(), 9.0);
    }
}
