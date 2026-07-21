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
//! Quantiles use the crate's empirical inverse-CDF convention: for retained
//! weighted mass `N`, `q` selects zero-based rank
//! `min(floor(q * N), N - 1)`. This is also the exact-sample convention used by
//! [`crate::tdigest::TDigest`].
//!
//! It does not implement the paper's later sampler or GK-based refinements.
//! Those refinements improve asymptotic space or failure-probability dependence
//! but are separate from the basic fully mergeable construction used here.
//!
//! # Randomness and merging
//!
//! Each sketch owns its compaction random-number state. There is no global seed
//! allocator and sketches do not coordinate with one another. [`KllSketch::new`]
//! uses a fixed default seed, making standalone sketches reproducible.
//!
//! When independently populated sketches may later be merged, construct them
//! with different seeds using [`KllSketch::with_seed`]. The caller should draw
//! those seeds from a caller-owned random-number generator or derive them from
//! a master seed and stable shard identifiers. Seeds do not need to match for
//! merging; different seeds avoid correlated compaction choices across shards.
//!
//! [Original KLL paper]: https://arxiv.org/pdf/1603.05346

use crate::{SketchError, splitmix64};

const CAPACITY_DECAY: f64 = 2.0 / 3.0;
const ERROR_BOUND_CONSTANT: f64 = CAPACITY_DECAY * CAPACITY_DECAY * (2.0 * CAPACITY_DECAY - 1.0);
const DEFAULT_FAILURE_PROBABILITY: f64 = 0.01;
const DEFAULT_SEED: u64 = 0xD1B5_4A32_C192_ED03;

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
    /// Higher `k` increases accuracy at the cost of memory. This constructor
    /// uses a fixed default compaction seed, so separate sketches receiving the
    /// same input make the same compaction choices.
    ///
    /// This is convenient for standalone, reproducible sketches. If separately
    /// populated sketches may later be merged, use [`Self::with_seed`] with a
    /// different caller-generated seed for each sketch. KLL seeds do not need
    /// to match for merging.
    ///
    /// # Errors
    /// Returns [`SketchError::InvalidParameter`] when `k < 2`.
    pub fn new(k: usize) -> Result<Self, SketchError> {
        Self::with_seed(k, DEFAULT_SEED)
    }

    /// Creates a sketch with a deterministic compaction seed.
    ///
    /// The seed initializes only this sketch's owned compaction state; there is
    /// no global seed state. This is useful for reproducible tests and data
    /// pipelines. Independently populated sketches that may later be merged
    /// should receive different seeds generated by the caller so their
    /// compaction errors are not correlated. Seeds do not need to match for
    /// merge compatibility.
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
        Self::with_error_rate_and_seed(rank_error, DEFAULT_SEED)
    }

    /// Creates a deterministically seeded sketch for a target rank error with
    /// 99% single-query confidence.
    ///
    /// Use a different caller-generated `seed` for each independently populated
    /// sketch that may later be merged.
    ///
    /// # Errors
    /// Returns [`SketchError::InvalidParameter`] for invalid `rank_error`.
    pub fn with_error_rate_and_seed(rank_error: f64, seed: u64) -> Result<Self, SketchError> {
        Self::with_error_rate_and_failure_probability_and_seed(
            rank_error,
            DEFAULT_FAILURE_PROBABILITY,
            seed,
        )
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
        Self::with_error_rate_and_failure_probability_and_seed(
            rank_error,
            failure_probability,
            DEFAULT_SEED,
        )
    }

    /// Creates a deterministically seeded sketch from a target rank error and
    /// failure probability.
    ///
    /// Use a different caller-generated `seed` for each independently populated
    /// sketch that may later be merged. Seeds do not need to match for merging.
    ///
    /// # Errors
    /// Returns [`SketchError::InvalidParameter`] for invalid or unrepresentable
    /// parameters.
    pub fn with_error_rate_and_failure_probability_and_seed(
        rank_error: f64,
        failure_probability: f64,
        seed: u64,
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
        Self::with_seed(k, seed)
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
    ///
    /// # Panics
    /// Panics if the observation count is already `u64::MAX`. This limit is
    /// unreachable through practical single-value ingestion; fallible merges
    /// report [`SketchError::ObservationCountOverflow`] instead.
    pub fn add(&mut self, value: f64) {
        if !value.is_finite() {
            return;
        }

        let new_count = self
            .count
            .checked_add(1)
            .expect("KLL observation count exceeds u64::MAX");

        self.levels[0].push(value);
        self.count = new_count;
        self.compact_after_add();
    }

    /// Returns the approximate quantile at `q` where `q` is in `[0, 1]`.
    ///
    /// The selected zero-based rank is `min(floor(q * N), N - 1)`, where `N`
    /// is the retained weighted mass. For example, the median of `[0, 10]` is
    /// `10`. This is the crate-wide empirical inverse-CDF convention shared
    /// with [`crate::tdigest::TDigest`].
    ///
    /// # Errors
    /// Returns [`SketchError::InvalidParameter`] for invalid `q` or empty
    /// sketches.
    pub fn quantile(&self, q: f64) -> Result<f64, SketchError> {
        Self::validate_quantile(q)?;
        self.validate_non_empty()?;

        let weighted_values = self.sorted_weighted_values();
        let total_weight = self.total_weight(&weighted_values);
        let target_rank = Self::target_rank(q, total_weight);

        Self::value_at_rank(&weighted_values, target_rank).ok_or(SketchError::InvalidParameter(
            "unable to compute quantile from current state",
        ))
    }

    /// Returns approximate quantiles for every query in `queries`.
    ///
    /// Results preserve the input query order, including duplicate and
    /// unsorted queries. The retained weighted values are allocated and sorted
    /// once, then all target ranks are answered in a single cumulative scan.
    /// This is more efficient than calling [`Self::quantile`] repeatedly.
    ///
    /// An empty query slice returns an empty vector, including for an empty
    /// sketch.
    ///
    /// # Errors
    /// Returns [`SketchError::InvalidParameter`] when any query is non-finite or
    /// outside `[0, 1]`, or when a non-empty query slice is used with an empty
    /// sketch.
    pub fn quantiles(&self, queries: &[f64]) -> Result<Vec<f64>, SketchError> {
        for &query in queries {
            Self::validate_quantile(query)?;
        }
        if queries.is_empty() {
            return Ok(Vec::new());
        }
        self.validate_non_empty()?;

        let weighted_values = self.sorted_weighted_values();
        let total_weight = self.total_weight(&weighted_values);
        let mut targets = Vec::with_capacity(queries.len());
        for (index, &query) in queries.iter().enumerate() {
            targets.push((Self::target_rank(query, total_weight), index));
        }
        targets.sort_unstable_by_key(|&(rank, _)| rank);

        let mut results = vec![0.0; queries.len()];
        let mut next_target = 0;
        let mut cumulative = 0_u128;
        for &(value, weight) in &weighted_values {
            cumulative += weight as u128;
            while next_target < targets.len() && cumulative > targets[next_target].0 {
                results[targets[next_target].1] = value;
                next_target += 1;
            }
        }

        if next_target == targets.len() {
            Ok(results)
        } else {
            Err(SketchError::InvalidParameter(
                "unable to compute quantiles from current state",
            ))
        }
    }

    /// Merges another sketch into this one.
    ///
    /// Levels of equal weight are concatenated, then all capacities are
    /// recalculated from the resulting hierarchy height before overflowing
    /// levels are compacted.
    ///
    /// Seeds are not a compatibility parameter and do not need to match. In
    /// fact, independently populated sketches should have been constructed with
    /// different caller-generated seeds so their earlier compaction choices are
    /// not correlated. The merge itself uses this sketch's owned RNG state for
    /// any new compactions and does not access global state.
    ///
    /// # Errors
    /// Returns [`SketchError::IncompatibleSketches`] when `k` differs, or
    /// [`SketchError::ObservationCountOverflow`] when the combined observation
    /// count would exceed `u64::MAX`. Validation occurs before mutation, so an
    /// error leaves this sketch unchanged.
    pub fn merge(&mut self, other: &Self) -> Result<(), SketchError> {
        if self.k != other.k {
            return Err(SketchError::IncompatibleSketches("k must match for merge"));
        }

        let merged_count = self
            .count
            .checked_add(other.count)
            .ok_or(SketchError::ObservationCountOverflow)?;

        if self.levels.len() < other.levels.len() {
            self.levels.resize_with(other.levels.len(), Vec::new);
        }
        for (level, values) in other.levels.iter().enumerate() {
            self.levels[level].extend(values.iter().copied());
        }
        self.count = merged_count;
        self.compact_all_levels();
        Ok(())
    }

    /// Clears all retained state.
    pub fn clear(&mut self) {
        self.levels.clear();
        self.levels.push(Vec::new());
        self.count = 0;
    }

    fn validate_quantile(q: f64) -> Result<(), SketchError> {
        if !q.is_finite() || !(0.0..=1.0).contains(&q) {
            return Err(SketchError::InvalidParameter(
                "q must be finite and in [0, 1]",
            ));
        }
        Ok(())
    }

    fn validate_non_empty(&self) -> Result<(), SketchError> {
        if self.count == 0 {
            return Err(SketchError::InvalidParameter(
                "quantile is undefined for an empty sketch",
            ));
        }
        Ok(())
    }

    fn sorted_weighted_values(&self) -> Vec<(f64, u64)> {
        let retained = self.levels.iter().map(Vec::len).sum();
        let mut weighted_values = Vec::with_capacity(retained);

        for (level, values) in self.levels.iter().enumerate() {
            let weight = 1_u64
                .checked_shl(level as u32)
                .expect("KLL level exceeds the supported observation-count range");
            weighted_values.extend(values.iter().map(|&value| (value, weight)));
        }

        weighted_values.sort_unstable_by(|left, right| left.0.total_cmp(&right.0));
        weighted_values
    }

    fn total_weight(&self, weighted_values: &[(f64, u64)]) -> u128 {
        let total_weight = weighted_values
            .iter()
            .map(|(_, weight)| *weight as u128)
            .sum();
        debug_assert_eq!(
            total_weight, self.count as u128,
            "retained KLL weight must equal the observation count"
        );
        total_weight
    }

    fn target_rank(q: f64, total_weight: u128) -> u128 {
        ((total_weight as f64 * q).floor() as u128).min(total_weight.saturating_sub(1))
    }

    fn value_at_rank(weighted_values: &[(f64, u64)], target_rank: u128) -> Option<f64> {
        let mut cumulative = 0_u128;
        for &(value, weight) in weighted_values {
            cumulative += weight as u128;
            if cumulative > target_rank {
                return Some(value);
            }
        }
        None
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

    fn compact_after_add(&mut self) {
        let mut level = 0;
        loop {
            if self.levels[level].len() <= self.level_capacity(level) {
                // Only level zero and levels reached by promotion can have
                // changed. Once the promotion cascade stops, all higher levels
                // still satisfy their existing capacities.
                return;
            }

            let previous_height = self.levels.len();
            self.compact_level(level);

            if self.levels.len() > previous_height {
                // Growing the hierarchy lowers every existing lower-level
                // capacity. Reconsider the complete hierarchy under the new H.
                self.compact_all_levels();
                return;
            }

            // The compaction changed only the next level, so continue the
            // overflow cascade there.
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

        values.clear();
        if let Some(value) = carry {
            values.push(value);
        }

        // Reuse the level's allocation across ordinary compactions. If a
        // hierarchy growth has made this level much smaller, release excess
        // historical capacity so allocated space continues to follow the
        // varying-capacity hierarchy rather than retaining O(k) per level.
        let required_capacity = self.level_capacity(level).saturating_add(1);
        if values.capacity() > required_capacity.saturating_mul(2) {
            values.shrink_to(required_capacity);
        }
        self.levels[level] = values;
    }

    fn next_u64(&mut self) -> u64 {
        self.rng_state = splitmix64(self.rng_state.wrapping_add(0x9E37_79B9_7F4A_7C15));
        self.rng_state
    }
}

#[cfg(test)]
mod tests {
    use super::{DEFAULT_FAILURE_PROBABILITY, KllSketch, rank_error_bound, required_k};
    use crate::{SketchError, splitmix64};

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
        let target_rank = (quantile * sorted.len() as f64)
            .floor()
            .min(sorted.len().saturating_sub(1) as f64);
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

    fn add_with_full_scan(sketch: &mut KllSketch, value: f64) {
        if !value.is_finite() {
            return;
        }

        let new_count = sketch
            .count
            .checked_add(1)
            .expect("test stream must fit in the supported count");
        sketch.levels[0].push(value);
        sketch.count = new_count;
        sketch.compact_all_levels();
    }

    #[test]
    fn constructor_validates_k() {
        assert!(KllSketch::new(1).is_err());
        assert!(KllSketch::new(2).is_ok());
        assert!(KllSketch::with_seed(1, 7).is_err());
    }

    #[test]
    fn constructors_use_reproducible_default_or_explicit_random_streams() {
        let mut default_first = KllSketch::new(50).unwrap();
        let mut default_second = KllSketch::new(50).unwrap();
        for value in 0_u64..10_000 {
            default_first.add(value as f64);
            default_second.add(value as f64);
        }
        assert_eq!(default_first.levels, default_second.levels);
        assert_eq!(default_first.rng_state, default_second.rng_state);

        let mut seeded_first = KllSketch::with_seed(50, 7).unwrap();
        let mut seeded_second = KllSketch::with_seed(50, 7).unwrap();
        for value in 0_u64..10_000 {
            seeded_first.add(value as f64);
            seeded_second.add(value as f64);
        }
        assert_eq!(seeded_first.levels, seeded_second.levels);
        assert_eq!(seeded_first.rng_state, seeded_second.rng_state);

        let differently_seeded = KllSketch::with_seed(50, 8).unwrap();
        assert_ne!(seeded_first.rng_state, differently_seeded.rng_state);
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
        let seeded = KllSketch::with_error_rate_and_seed(rank_error, 7).unwrap();
        let fully_configured = KllSketch::with_error_rate_and_failure_probability_and_seed(
            rank_error,
            failure_probability,
            8,
        )
        .unwrap();

        assert_eq!(configured.k(), expected_k);
        assert_eq!(default.k(), expected_k);
        assert_eq!(seeded.k(), expected_k);
        assert_eq!(fully_configured.k(), expected_k);
        assert_ne!(seeded.rng_state, fully_configured.rng_state);
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
    fn affected_level_compaction_matches_full_scan_scheduling() {
        for &k in &[2_usize, 3, 8, 50, 200] {
            let seed = 0xA409_3822_299F_31D0 ^ k as u64;
            let mut affected_levels = KllSketch::with_seed(k, seed).unwrap();
            let mut full_scan = KllSketch::with_seed(k, seed).unwrap();

            for index in 0_u64..25_000 {
                let value = index.wrapping_mul(104_729) % 100_003;
                affected_levels.add(value as f64);
                add_with_full_scan(&mut full_scan, value as f64);

                assert_eq!(
                    affected_levels.count, full_scan.count,
                    "k={k} index={index}"
                );
                assert_eq!(
                    affected_levels.rng_state, full_scan.rng_state,
                    "k={k} index={index}"
                );
                assert_eq!(
                    affected_levels.levels, full_scan.levels,
                    "k={k} index={index}"
                );
            }
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
    fn batched_quantiles_match_scalar_queries_and_preserve_order() {
        let mut sketch = KllSketch::with_seed(128, 4).unwrap();
        for index in 0_u64..50_000 {
            let value = index.wrapping_mul(104_729) % 100_003;
            sketch.add(value as f64);
        }

        let queries = [1.0, 0.0, 0.5, 0.1, 0.5, 0.999, 0.75, 0.25];
        let expected: Vec<_> = queries
            .iter()
            .map(|&query| sketch.quantile(query).unwrap())
            .collect();

        assert_eq!(sketch.quantiles(&queries).unwrap(), expected);
    }

    #[test]
    fn batched_quantiles_validate_queries_and_empty_sketches() {
        let empty = KllSketch::with_seed(128, 4).unwrap();
        assert_eq!(empty.quantiles(&[]).unwrap(), Vec::<f64>::new());
        assert!(empty.quantiles(&[0.5]).is_err());

        let mut sketch = KllSketch::with_seed(128, 4).unwrap();
        sketch.add(1.0);
        assert!(sketch.quantiles(&[0.5, f64::NAN]).is_err());
        assert!(sketch.quantiles(&[-0.1]).is_err());
        assert!(sketch.quantiles(&[1.1]).is_err());
    }

    #[test]
    fn merge_rejects_different_k() {
        let mut left = KllSketch::with_seed(100, 7).unwrap();
        let right = KllSketch::with_seed(101, 8).unwrap();
        assert!(left.merge(&right).is_err());
    }

    #[test]
    fn merge_rejects_observation_count_overflow_without_mutation() {
        let mut sketch = KllSketch::with_seed(2, 7).unwrap();
        sketch.add(1.0);

        for _ in 0..63 {
            let copy = sketch.clone();
            sketch.merge(&copy).unwrap();
        }

        assert_eq!(sketch.count, 1_u64 << 63);
        let before = sketch.clone();
        let error = sketch.merge(&before).unwrap_err();

        assert_eq!(error, SketchError::ObservationCountOverflow);
        assert_eq!(sketch.count, before.count);
        assert_eq!(sketch.levels, before.levels);
        assert_eq!(sketch.rng_state, before.rng_state);
        assert!(sketch.levels.len() <= u64::BITS as usize);
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
