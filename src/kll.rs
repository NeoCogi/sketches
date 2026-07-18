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
//! KLL sketch for approximate quantile queries.
//!
//! This implementation follows the [original KLL paper]'s basic fully
//! mergeable hierarchy of varying-capacity compactors. For a hierarchy of
//! height `H`, level `h` has capacity proportional to `k * c^(H - h)`, where
//! `c = 2/3`. Consequently, capacity increases toward the high-weight top level
//! and all capacities are reconsidered whenever the hierarchy grows or sketches
//! are merged.
//!
//! It does not implement the paper's later sampler or GK-based refinements.
//! Those refinements improve asymptotic space or failure-probability dependence
//! but are separate from the basic fully mergeable construction used here.
//!
//! [Original KLL paper]: https://arxiv.org/pdf/1603.05346

use std::collections::hash_map::RandomState;
use std::hash::BuildHasher;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::{SketchError, splitmix64};

const CAPACITY_DECAY: f64 = 2.0 / 3.0;
const ERROR_BOUND_CONSTANT: f64 = CAPACITY_DECAY * CAPACITY_DECAY * (2.0 * CAPACITY_DECAY - 1.0);
const DEFAULT_FAILURE_PROBABILITY: f64 = 0.01;
const SEED_INCREMENT: u64 = 0x9E37_79B9_7F4A_7C15;

static PROCESS_SEED: OnceLock<u64> = OnceLock::new();
static SKETCH_SEQUENCE: AtomicU64 = AtomicU64::new(0);

fn fresh_rng_state() -> u64 {
    let process_seed =
        *PROCESS_SEED.get_or_init(|| RandomState::new().hash_one("sketches::kll::KllSketch"));
    let sequence = SKETCH_SEQUENCE.fetch_add(SEED_INCREMENT, Ordering::Relaxed);
    splitmix64(process_seed.wrapping_add(sequence))
}

fn required_k(rank_error: f64, failure_probability: f64) -> Option<usize> {
    let required = (rank_error_bound(1, failure_probability) / rank_error).ceil();

    if !required.is_finite() || required > usize::MAX as f64 {
        return None;
    }
    Some((required as usize).max(2))
}

fn rank_error_bound(k: usize, failure_probability: f64) -> f64 {
    ((2.0 / failure_probability).ln() / ERROR_BOUND_CONSTANT).sqrt() / k as f64
}

/// Approximate quantile sketch using KLL-style compaction.
///
/// # Example
/// ```rust
/// use sketches::kll::KllSketch;
///
/// let mut kll = KllSketch::new(200).unwrap();
/// for value in 0_u64..10_000 {
///     kll.add(value as f64);
/// }
///
/// let p50 = kll.quantile(0.50).unwrap();
/// assert!(p50 > 4_000.0 && p50 < 6_000.0);
/// ```
#[derive(Debug, Clone)]
pub struct KllSketch {
    k: usize,
    levels: Vec<Vec<f64>>,
    count: u64,
    rng_state: u64,
}

impl KllSketch {
    /// Creates a sketch with compaction parameter `k`.
    ///
    /// Higher `k` increases accuracy at the cost of memory. Each sketch receives
    /// a distinct process-randomized compaction stream; use [`Self::with_seed`]
    /// when deterministic reproduction is required.
    ///
    /// # Errors
    /// Returns [`SketchError::InvalidParameter`] when `k < 2`.
    pub fn new(k: usize) -> Result<Self, SketchError> {
        Self::with_seed(k, fresh_rng_state())
    }

    /// Creates a sketch with a deterministic compaction seed.
    ///
    /// This is useful for reproducible tests and data pipelines. Independently
    /// merged shards should use different seeds so their compaction errors are
    /// not correlated.
    ///
    /// # Errors
    /// Returns [`SketchError::InvalidParameter`] when `k < 2`.
    pub fn with_seed(k: usize, seed: u64) -> Result<Self, SketchError> {
        if k < 2 {
            return Err(SketchError::InvalidParameter(
                "k must be greater than or equal to 2",
            ));
        }

        Ok(Self {
            k,
            levels: vec![Vec::new()],
            count: 0,
            rng_state: splitmix64(seed),
        })
    }

    /// Creates a sketch for a target rank error with 99% single-query
    /// confidence.
    ///
    /// The sizing follows the single-quantile bound from the basic mergeable
    /// construction in the original KLL paper. For `c = 2/3`, it selects the
    /// smallest `k` satisfying
    /// `2 * exp(-(4 / 27) * rank_error^2 * k^2) <= 0.01`.
    /// This is the paper's bound for one fixed rank/quantile query, not a
    /// simultaneous guarantee over every possible query. Callers issuing many
    /// queries must account for their combined failure probability.
    ///
    /// # Errors
    /// Returns [`SketchError::InvalidParameter`] for invalid `rank_error`.
    pub fn with_error_rate(rank_error: f64) -> Result<Self, SketchError> {
        Self::with_error_rate_and_failure_probability(rank_error, DEFAULT_FAILURE_PROBABILITY)
    }

    /// Creates a sketch from a target rank error and failure probability.
    ///
    /// Sizing uses the paper's single-quantile bound for the basic fully
    /// mergeable varying-capacity construction. Both parameters must be finite
    /// and strictly between zero and one.
    ///
    /// # Errors
    /// Returns [`SketchError::InvalidParameter`] for invalid or unrepresentable
    /// parameters.
    pub fn with_error_rate_and_failure_probability(
        rank_error: f64,
        failure_probability: f64,
    ) -> Result<Self, SketchError> {
        if !rank_error.is_finite() || rank_error <= 0.0 || rank_error >= 1.0 {
            return Err(SketchError::InvalidParameter(
                "rank_error must be finite and strictly between 0 and 1",
            ));
        }
        if !failure_probability.is_finite()
            || failure_probability <= 0.0
            || failure_probability >= 1.0
        {
            return Err(SketchError::InvalidParameter(
                "failure_probability must be finite and strictly between 0 and 1",
            ));
        }

        let k = required_k(rank_error, failure_probability).ok_or(
            SketchError::InvalidParameter("rank_error requires an unrepresentable k"),
        )?;
        Self::new(k)
    }

    /// Returns the configured compaction parameter.
    pub fn k(&self) -> usize {
        self.k
    }

    /// Returns the number of observed values.
    pub fn count(&self) -> u64 {
        self.count
    }

    /// Returns `true` when no values have been added.
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// Adds one value to the sketch.
    ///
    /// Non-finite values are ignored.
    pub fn add(&mut self, value: f64) {
        if !value.is_finite() {
            return;
        }

        self.levels[0].push(value);
        self.count = self.count.saturating_add(1);
        self.compact_all_levels();
    }

    /// Returns the approximate quantile at `q` where `q` is in `[0, 1]`.
    ///
    /// # Errors
    /// Returns [`SketchError::InvalidParameter`] for invalid `q` or empty
    /// sketches.
    pub fn quantile(&self, q: f64) -> Result<f64, SketchError> {
        if !q.is_finite() || !(0.0..=1.0).contains(&q) {
            return Err(SketchError::InvalidParameter(
                "q must be finite and in [0, 1]",
            ));
        }
        if self.count == 0 {
            return Err(SketchError::InvalidParameter(
                "quantile is undefined for an empty sketch",
            ));
        }

        let mut weighted_values = Vec::new();
        for (level, values) in self.levels.iter().enumerate() {
            let weight = 1_u64.checked_shl(level as u32).unwrap_or(u64::MAX);
            for &value in values {
                weighted_values.push((value, weight));
            }
        }

        weighted_values.sort_unstable_by(|left, right| left.0.total_cmp(&right.0));

        let total_weight: u128 = weighted_values
            .iter()
            .map(|(_, weight)| *weight as u128)
            .sum();
        let target_rank = ((total_weight.saturating_sub(1)) as f64 * q).round() as u128;

        let mut cumulative = 0_u128;
        for (value, weight) in weighted_values {
            cumulative = cumulative.saturating_add(weight as u128);
            if cumulative > target_rank {
                return Ok(value);
            }
        }

        Err(SketchError::InvalidParameter(
            "unable to compute quantile from current state",
        ))
    }

    /// Merges another sketch into this one.
    ///
    /// Levels of equal weight are concatenated, then all capacities are
    /// recalculated from the resulting hierarchy height before overflowing
    /// levels are compacted. Independently produced sketches should use
    /// independent seeds, as [`Self::new`] does by default.
    ///
    /// # Errors
    /// Returns [`SketchError::IncompatibleSketches`] when `k` differs.
    pub fn merge(&mut self, other: &Self) -> Result<(), SketchError> {
        if self.k != other.k {
            return Err(SketchError::IncompatibleSketches("k must match for merge"));
        }

        if self.levels.len() < other.levels.len() {
            self.levels.resize_with(other.levels.len(), Vec::new);
        }
        for (level, values) in other.levels.iter().enumerate() {
            self.levels[level].extend(values.iter().copied());
        }
        self.count = self.count.saturating_add(other.count);
        self.compact_all_levels();
        Ok(())
    }

    /// Clears all retained state.
    pub fn clear(&mut self) {
        self.levels.clear();
        self.levels.push(Vec::new());
        self.count = 0;
    }

    fn level_capacity(&self, level: usize) -> usize {
        self.level_capacity_for_height(level, self.levels.len())
    }

    fn level_capacity_for_height(&self, level: usize, height: usize) -> usize {
        debug_assert!(level < height);
        let distance_from_top = height - level - 1;
        let exponent = distance_from_top.min(i32::MAX as usize) as i32;
        let capacity = self.k as f64 * CAPACITY_DECAY.powi(exponent);
        capacity.ceil().max(2.0) as usize
    }

    fn compact_all_levels(&mut self) {
        let mut level = 0;
        while level < self.levels.len() {
            let capacity = self.level_capacity(level);
            if self.levels[level].len() > capacity {
                let previous_height = self.levels.len();
                self.compact_level(level);

                if self.levels.len() > previous_height {
                    // A new top level lowers every existing lower-level
                    // capacity. Restart so they are checked against the new H.
                    level = 0;
                    continue;
                }
            }
            level += 1;
        }
    }

    fn compact_level(&mut self, level: usize) {
        if level + 1 == self.levels.len() {
            self.levels.push(Vec::new());
        }

        let mut values = std::mem::take(&mut self.levels[level]);
        values.sort_unstable_by(f64::total_cmp);

        // Keep one value at this level if the level length is odd.
        let carry = if values.len() % 2 == 1 {
            values.pop()
        } else {
            None
        };

        let offset = self.next_u64() as usize & 1;
        for index in (offset..values.len()).step_by(2) {
            self.levels[level + 1].push(values[index]);
        }

        if let Some(value) = carry {
            self.levels[level].push(value);
        }
    }

    fn next_u64(&mut self) -> u64 {
        self.rng_state = splitmix64(self.rng_state.wrapping_add(0x9E37_79B9_7F4A_7C15));
        self.rng_state
    }
}

#[cfg(test)]
mod tests {
    use super::{DEFAULT_FAILURE_PROBABILITY, KllSketch, rank_error_bound, required_k};
    use crate::splitmix64;

    const REGRESSION_SEED: u64 = 0xD1B5_4A32_C192_ED03;

    fn retained_weight(sketch: &KllSketch) -> u128 {
        sketch
            .levels
            .iter()
            .enumerate()
            .map(|(level, values)| {
                let weight = 1_u128.checked_shl(level as u32).unwrap();
                values.len() as u128 * weight
            })
            .sum()
    }

    fn normalized_rank_error(values: &[f64], estimate: f64, quantile: f64) -> f64 {
        let mut sorted = values.to_vec();
        sorted.sort_unstable_by(f64::total_cmp);

        let lower_rank = sorted.partition_point(|value| *value < estimate) as f64;
        let upper_rank = sorted
            .partition_point(|value| *value <= estimate)
            .saturating_sub(1) as f64;
        let target_rank = quantile * sorted.len().saturating_sub(1) as f64;
        let absolute_error = if target_rank < lower_rank {
            lower_rank - target_rank
        } else if target_rank > upper_rank {
            target_rank - upper_rank
        } else {
            0.0
        };

        absolute_error / sorted.len() as f64
    }

    fn deterministic_shuffle(values: &mut [f64], mut state: u64) {
        for upper in (1..values.len()).rev() {
            state = splitmix64(state);
            values.swap(upper, state as usize % (upper + 1));
        }
    }

    fn input_patterns(length: usize) -> Vec<(&'static str, Vec<f64>)> {
        let ascending: Vec<_> = (0..length).map(|value| value as f64).collect();

        let mut descending = ascending.clone();
        descending.reverse();

        let mut shuffled = ascending.clone();
        deterministic_shuffle(&mut shuffled, 0xA409_3822_299F_31D0);

        let repeated = (0..length).map(|value| (value % 17) as f64).collect();
        let skewed = (0..length)
            .map(|value| {
                if value < length * 9 / 10 {
                    (value % 5) as f64
                } else {
                    (10_000 + value) as f64
                }
            })
            .collect();

        vec![
            ("ascending", ascending),
            ("descending", descending),
            ("shuffled", shuffled),
            ("repeated", repeated),
            ("skewed", skewed),
        ]
    }

    fn assert_rank_error(
        sketch: &KllSketch,
        values: &[f64],
        quantile: f64,
        error_limit: f64,
        context: &str,
    ) {
        let estimate = sketch.quantile(quantile).unwrap();
        let error = normalized_rank_error(values, estimate, quantile);
        assert!(
            error <= error_limit,
            "{context}: q={quantile} estimate={estimate} error={error} limit={error_limit}"
        );
    }

    #[test]
    fn constructor_validates_k() {
        assert!(KllSketch::new(1).is_err());
        assert!(KllSketch::new(2).is_ok());
        assert!(KllSketch::with_seed(1, 7).is_err());
    }

    #[test]
    fn constructors_use_independent_or_reproducible_random_streams() {
        let first = KllSketch::new(50).unwrap();
        let second = KllSketch::new(50).unwrap();
        assert_ne!(first.rng_state, second.rng_state);

        let mut seeded_first = KllSketch::with_seed(50, 7).unwrap();
        let mut seeded_second = KllSketch::with_seed(50, 7).unwrap();
        for value in 0_u64..10_000 {
            seeded_first.add(value as f64);
            seeded_second.add(value as f64);
        }
        assert_eq!(seeded_first.levels, seeded_second.levels);
        assert_eq!(seeded_first.rng_state, seeded_second.rng_state);
    }

    #[test]
    fn error_rate_constructor_uses_documented_paper_bound() {
        let rank_error = 0.01;
        let failure_probability = 0.01;
        let expected_k = required_k(rank_error, failure_probability).unwrap();
        let configured =
            KllSketch::with_error_rate_and_failure_probability(rank_error, failure_probability)
                .unwrap();
        let default = KllSketch::with_error_rate(rank_error).unwrap();

        assert_eq!(configured.k(), expected_k);
        assert_eq!(default.k(), expected_k);
        assert!(rank_error_bound(expected_k, failure_probability) <= rank_error);
        assert!(rank_error_bound(expected_k - 1, failure_probability) > rank_error);

        assert!(KllSketch::with_error_rate(0.0).is_err());
        assert!(KllSketch::with_error_rate(f64::NAN).is_err());
        assert!(KllSketch::with_error_rate_and_failure_probability(0.01, 0.0).is_err());
        assert!(KllSketch::with_error_rate_and_failure_probability(0.01, f64::NAN).is_err());
    }

    #[test]
    fn quantile_rejects_empty_sketch() {
        let kll = KllSketch::new(64).unwrap();
        assert!(kll.quantile(0.5).is_err());
    }

    #[test]
    fn capacities_increase_toward_the_current_top_level() {
        let sketch = KllSketch::with_seed(50, 1).unwrap();
        let height = 12;
        let capacities: Vec<_> = (0..height)
            .map(|level| sketch.level_capacity_for_height(level, height))
            .collect();

        assert_eq!(capacities[0], 2);
        assert_eq!(capacities[height - 1], 50);
        assert!(capacities.windows(2).all(|pair| pair[0] <= pair[1]));

        for (level, old_capacity) in capacities.into_iter().enumerate() {
            assert!(sketch.level_capacity_for_height(level, height + 1) <= old_capacity);
        }
    }

    #[test]
    fn ordered_stream_regression_has_bounded_median_error() {
        let mut kll = KllSketch::with_seed(50, REGRESSION_SEED).unwrap();
        let values: Vec<_> = (0_u64..100_000).map(|value| value as f64).collect();
        for &value in &values {
            kll.add(value);
        }

        let median = kll.quantile(0.5).unwrap();
        let error = normalized_rank_error(&values, median, 0.5);
        assert!(error <= 0.05, "median={median} rank_error={error}");
    }

    #[test]
    fn rank_error_covers_orderings_k_values_and_seeds() {
        let patterns = input_patterns(10_000);

        for &k in &[50_usize, 100, 200] {
            let error_limit = rank_error_bound(k, DEFAULT_FAILURE_PROBABILITY);
            for &seed in &[7_u64, 11] {
                for (name, values) in &patterns {
                    let mut sketch = KllSketch::with_seed(k, seed).unwrap();
                    for &value in values {
                        sketch.add(value);
                    }

                    let context = format!("pattern={name} k={k} seed={seed}");
                    for &quantile in &[0.1, 0.5, 0.9] {
                        assert_rank_error(&sketch, values, quantile, error_limit, &context);
                    }
                }
            }
        }
    }

    #[test]
    fn retained_weight_matches_count_after_add_and_merge() {
        let mut combined = KllSketch::with_seed(80, 1_000).unwrap();

        for shard in 0_u64..8 {
            let mut part = KllSketch::with_seed(80, 2_000 + shard).unwrap();
            for value in 0_u64..(2_000 + shard * 137) {
                part.add((shard * 10_000 + value) as f64);
            }

            assert_eq!(retained_weight(&part), part.count() as u128);
            combined.merge(&part).unwrap();
            assert_eq!(retained_weight(&combined), combined.count() as u128);
            for level in 0..combined.levels.len() {
                assert!(combined.levels[level].len() <= combined.level_capacity(level));
            }
        }
    }

    #[test]
    fn direct_and_merged_ingestion_meet_the_same_error_contract() {
        let mut values: Vec<_> = (0_u64..50_000).map(|value| value as f64).collect();
        deterministic_shuffle(&mut values, 0x243F_6A88_85A3_08D3);

        for &k in &[50_usize, 100, 200] {
            let mut direct = KllSketch::with_seed(k, 100 + k as u64).unwrap();
            for &value in &values {
                direct.add(value);
            }

            let mut merged = KllSketch::with_seed(k, 10_000 + k as u64).unwrap();
            for (shard, chunk) in values.chunks(5_000).enumerate() {
                let mut part = KllSketch::with_seed(k, 20_000 + k as u64 + shard as u64).unwrap();
                for &value in chunk {
                    part.add(value);
                }
                merged.merge(&part).unwrap();
            }

            assert_eq!(direct.count(), values.len() as u64);
            assert_eq!(merged.count(), values.len() as u64);
            assert_eq!(retained_weight(&direct), values.len() as u128);
            assert_eq!(retained_weight(&merged), values.len() as u128);

            let error_limit = rank_error_bound(k, DEFAULT_FAILURE_PROBABILITY);
            for &quantile in &[0.1, 0.5, 0.9] {
                assert_rank_error(
                    &direct,
                    &values,
                    quantile,
                    error_limit,
                    &format!("direct k={k}"),
                );
                assert_rank_error(
                    &merged,
                    &values,
                    quantile,
                    error_limit,
                    &format!("merged k={k}"),
                );
            }
        }
    }

    #[test]
    fn quantiles_are_monotonic() {
        let mut kll = KllSketch::with_seed(128, 4).unwrap();
        for value in 0_u64..20_000 {
            kll.add(value as f64);
        }

        let p50 = kll.quantile(0.5).unwrap();
        let p90 = kll.quantile(0.9).unwrap();
        assert!(p50 <= p90, "p50={p50} p90={p90}");
    }

    #[test]
    fn merge_rejects_different_k() {
        let mut left = KllSketch::with_seed(100, 7).unwrap();
        let right = KllSketch::with_seed(101, 8).unwrap();
        assert!(left.merge(&right).is_err());
    }

    #[test]
    fn clear_resets_state() {
        let mut kll = KllSketch::with_seed(128, 9).unwrap();
        kll.add(1.0);
        kll.add(2.0);
        kll.clear();
        assert!(kll.is_empty());
        assert!(kll.quantile(0.5).is_err());
    }
}
