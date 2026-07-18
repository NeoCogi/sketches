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
//! Space-Saving sketch for approximate heavy hitters.
//!
//! Space-Saving keeps at most `capacity` counters and replaces the smallest
//! counter when a new item arrives and capacity is full.
//!
//! For a tracked item, the stored estimate is an upper bound and
//! `estimate - error` is a lower bound on its frequency, provided the exact
//! frequency is representable as a `u64`. Merging follows Algorithms 3 and 4
//! of Cafaro, Pulimeno, and Tempesta's [parallel Space-Saving construction]:
//! estimates and errors are combined symmetrically, using a full summary's
//! minimum counter as the bound for an item missing from that summary.
//!
//! [parallel Space-Saving construction]: https://arxiv.org/pdf/1401.0702

use std::collections::HashMap;
use std::hash::Hash;

use crate::SketchError;

#[derive(Debug, Clone, Copy)]
struct CounterEntry {
    count: u64,
    error: u64,
}

/// Approximate top-k tracker using the Space-Saving algorithm.
///
/// `SpaceSaving<T>` stores up to `capacity` candidate heavy hitters.
///
/// # Example
/// ```rust
/// use sketches::space_saving::SpaceSaving;
///
/// let mut hh = SpaceSaving::new(3).unwrap();
/// hh.add("apple".to_string(), 100);
/// hh.add("banana".to_string(), 80);
/// hh.add("carrot".to_string(), 10);
/// hh.add("durian".to_string(), 5);
///
/// let top = hh.top_k(2);
/// assert_eq!(top.len(), 2);
/// assert!(top.iter().any(|(item, _, _)| item == "apple"));
/// ```
#[derive(Debug, Clone)]
pub struct SpaceSaving<T>
where
    T: Eq + Hash + Clone,
{
    capacity: usize,
    counters: HashMap<T, CounterEntry>,
    total_count: u64,
}

impl<T> SpaceSaving<T>
where
    T: Eq + Hash + Clone,
{
    /// Creates a sketch with the given number of tracked counters.
    ///
    /// # Errors
    /// Returns [`SketchError::InvalidParameter`] when `capacity == 0`.
    pub fn new(capacity: usize) -> Result<Self, SketchError> {
        if capacity == 0 {
            return Err(SketchError::InvalidParameter(
                "capacity must be greater than zero",
            ));
        }

        Ok(Self {
            capacity,
            counters: HashMap::with_capacity(capacity),
            total_count: 0,
        })
    }

    /// Returns the maximum number of tracked counters.
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Returns the number of items currently tracked.
    pub fn tracked_items(&self) -> usize {
        self.counters.len()
    }

    /// Returns the total inserted weight, saturated at [`u64::MAX`].
    ///
    /// This value is tracked independently from the sum of retained counter
    /// estimates. A merge may discard counters, so that sum can be smaller
    /// than the combined input weight.
    pub fn total_count(&self) -> u64 {
        self.total_count
    }

    /// Returns `true` when no items have been inserted.
    pub fn is_empty(&self) -> bool {
        self.total_count == 0
    }

    /// Inserts one occurrence of `item`.
    pub fn insert(&mut self, item: T) {
        self.add(item, 1);
    }

    /// Inserts `count` occurrences of `item`.
    ///
    /// Counts and the total inserted weight saturate at [`u64::MAX`].
    pub fn add(&mut self, item: T, count: u64) {
        if count == 0 {
            return;
        }

        if let Some(entry) = self.counters.get_mut(&item) {
            entry.count = entry.count.saturating_add(count);
            self.total_count = self.total_count.saturating_add(count);
            return;
        }

        if self.counters.len() < self.capacity {
            self.counters.insert(item, CounterEntry { count, error: 0 });
            self.total_count = self.total_count.saturating_add(count);
            return;
        }

        let (min_item, min_entry) = self
            .counters
            .iter()
            .min_by_key(|(_, entry)| entry.count)
            .map(|(tracked_item, entry)| (tracked_item.clone(), *entry))
            .expect("non-empty map when capacity is full");

        self.counters.remove(&min_item);
        self.counters.insert(
            item,
            CounterEntry {
                count: min_entry.count.saturating_add(count),
                error: min_entry.count,
            },
        );

        self.total_count = self.total_count.saturating_add(count);
    }

    /// Returns the estimated count for `item` if it is currently tracked.
    pub fn estimate(&self, item: &T) -> Option<u64> {
        self.counters.get(item).map(|entry| entry.count)
    }

    /// Returns `(estimate, max_error)` for `item` if currently tracked.
    ///
    /// Before integer saturation, the exact frequency is in the inclusive
    /// interval `estimate - max_error..=estimate`.
    pub fn estimate_with_error(&self, item: &T) -> Option<(u64, u64)> {
        self.counters
            .get(item)
            .map(|entry| (entry.count, entry.error))
    }

    /// Returns the conservative lower bound for `item` if currently tracked.
    ///
    /// Before integer saturation, this is no greater than the exact frequency.
    pub fn lower_bound(&self, item: &T) -> Option<u64> {
        self.counters
            .get(item)
            .map(|entry| entry.count.saturating_sub(entry.error))
    }

    /// Returns up to `k` tracked items sorted by estimated count descending.
    ///
    /// Each tuple is `(item, estimate, max_error)`.
    pub fn top_k(&self, k: usize) -> Vec<(T, u64, u64)> {
        if k == 0 {
            return Vec::new();
        }

        let mut entries: Vec<_> = self
            .counters
            .iter()
            .map(|(item, entry)| (item.clone(), entry.count, entry.error))
            .collect();

        entries.sort_unstable_by(|left, right| right.1.cmp(&left.1));
        entries.truncate(k.min(entries.len()));
        entries
    }

    /// Clears tracked counters and total count.
    pub fn clear(&mut self) {
        self.counters.clear();
        self.total_count = 0;
    }

    /// Merges another sketch while preserving Space-Saving error bounds.
    ///
    /// Both sketches must have the same `capacity`.
    ///
    /// This implements the combine and prune operation from Algorithms 3 and
    /// 4 of the [parallel Space-Saving construction]. For an item tracked by
    /// both summaries, estimates and errors are added. For an item tracked by
    /// only one summary, the other summary's minimum estimate is added to both
    /// its estimate and error. That minimum is zero when the other summary is
    /// not full, because absence from an underfull summary is exact. The
    /// largest `capacity` combined estimates are retained.
    ///
    /// The operation uses `O(capacity)` temporary memory and expected
    /// `O(capacity)` time for hash-table operations and top-counter selection.
    /// The total inserted weight is combined separately and saturates at
    /// [`u64::MAX`]. The receiver remains unchanged if compatibility validation
    /// fails.
    ///
    /// # Errors
    /// Returns [`SketchError::IncompatibleSketches`] when capacities differ.
    ///
    /// [parallel Space-Saving construction]: https://arxiv.org/pdf/1401.0702
    pub fn merge(&mut self, other: &Self) -> Result<(), SketchError> {
        if self.capacity != other.capacity {
            return Err(SketchError::IncompatibleSketches(
                "capacity must match for merge",
            ));
        }

        let self_min = self.untracked_upper_bound();
        let other_min = other.untracked_upper_bound();
        let mut combined =
            Vec::with_capacity(self.counters.len().saturating_add(other.counters.len()));

        for (item, self_entry) in &self.counters {
            let entry = if let Some(other_entry) = other.counters.get(item) {
                CounterEntry {
                    count: self_entry.count.saturating_add(other_entry.count),
                    error: self_entry.error.saturating_add(other_entry.error),
                }
            } else {
                CounterEntry {
                    count: self_entry.count.saturating_add(other_min),
                    error: self_entry.error.saturating_add(other_min),
                }
            };
            combined.push((item.clone(), entry));
        }

        for (item, other_entry) in &other.counters {
            if !self.counters.contains_key(item) {
                combined.push((
                    item.clone(),
                    CounterEntry {
                        count: other_entry.count.saturating_add(self_min),
                        error: other_entry.error.saturating_add(self_min),
                    },
                ));
            }
        }

        if combined.len() > self.capacity {
            combined.select_nth_unstable_by(self.capacity, |left, right| {
                right.1.count.cmp(&left.1.count)
            });
            combined.truncate(self.capacity);
        }

        let mut counters = HashMap::with_capacity(self.capacity);
        counters.extend(combined);

        let total_count = self.total_count.saturating_add(other.total_count);
        self.counters = counters;
        self.total_count = total_count;
        Ok(())
    }

    fn untracked_upper_bound(&self) -> u64 {
        if self.counters.len() < self.capacity {
            return 0;
        }

        self.counters
            .values()
            .map(|entry| entry.count)
            .min()
            .expect("non-empty map when capacity is full")
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::SpaceSaving;

    fn assert_valid_bounds(sketch: &SpaceSaving<u64>, exact: &HashMap<u64, u64>) {
        let retained = sketch.top_k(sketch.capacity());
        for (item, estimate, error) in &retained {
            let exact_count = exact.get(item).copied().unwrap_or(0);
            let lower_bound = estimate.saturating_sub(*error);
            assert!(
                lower_bound <= exact_count,
                "item {item}: lower bound {lower_bound} exceeds exact count {exact_count}"
            );
            assert!(
                exact_count <= *estimate,
                "item {item}: exact count {exact_count} exceeds estimate {estimate}"
            );
        }

        if sketch.tracked_items() == sketch.capacity() {
            let minimum = retained
                .iter()
                .map(|(_, estimate, _)| *estimate)
                .min()
                .unwrap();
            for (item, exact_count) in exact {
                if sketch.estimate(item).is_none() {
                    assert!(
                        *exact_count <= minimum,
                        "untracked item {item}: exact count {exact_count} exceeds minimum {minimum}"
                    );
                }
            }
        }
    }

    #[test]
    fn constructor_validates_capacity() {
        assert!(SpaceSaving::<String>::new(0).is_err());
        assert!(SpaceSaving::<String>::new(4).is_ok());
    }

    #[test]
    fn heavy_hitters_are_retained() {
        let mut sketch = SpaceSaving::new(5).unwrap();
        sketch.add("apple".to_string(), 5_000);
        sketch.add("banana".to_string(), 3_000);
        sketch.add("carrot".to_string(), 1_000);

        for value in 0..200_u64 {
            sketch.insert(format!("noise-{value}"));
        }

        let top = sketch.top_k(3);
        let names: Vec<_> = top.iter().map(|(item, _, _)| item.as_str()).collect();
        assert!(names.contains(&"apple"));
        assert!(names.contains(&"banana"));
    }

    #[test]
    fn estimates_expose_error_bounds() {
        let mut sketch = SpaceSaving::new(2).unwrap();
        sketch.add("a".to_string(), 10);
        sketch.add("b".to_string(), 5);
        sketch.add("c".to_string(), 2);

        let estimate = sketch.estimate_with_error(&"c".to_string());
        if let Some((count, error)) = estimate {
            assert!(count >= error);
        }
    }

    #[test]
    fn merge_preserves_capacity_one_source_error() {
        let mut left = SpaceSaving::new(1).unwrap();
        let mut right = SpaceSaving::new(1).unwrap();

        right.insert(0_u64);
        right.insert(1_u64);
        assert_eq!(right.estimate_with_error(&1), Some((2, 1)));

        left.merge(&right).unwrap();

        assert_eq!(left.estimate_with_error(&1), Some((2, 1)));
        assert_eq!(left.lower_bound(&1), Some(1));
        assert_eq!(left.total_count(), 2);
    }

    #[test]
    fn merge_combines_overlapping_estimates_and_errors() {
        let mut left = SpaceSaving::new(2).unwrap();
        left.add(0_u64, 5);
        left.add(1, 2);
        left.add(2, 10);
        assert_eq!(left.estimate_with_error(&2), Some((12, 2)));

        let mut right = SpaceSaving::new(2).unwrap();
        right.add(3_u64, 4);
        right.add(4, 1);
        right.add(2, 10);
        assert_eq!(right.estimate_with_error(&2), Some((11, 1)));

        left.merge(&right).unwrap();

        assert_eq!(left.estimate_with_error(&2), Some((23, 3)));
        assert_eq!(left.lower_bound(&2), Some(20));
        assert_eq!(left.total_count(), 32);
    }

    #[test]
    fn merge_applies_full_summary_minima_before_pruning() {
        let mut left = SpaceSaving::new(3).unwrap();
        left.add(0_u64, 10);
        left.add(1, 5);
        left.add(2, 3);

        let mut right = SpaceSaving::new(3).unwrap();
        right.add(0_u64, 7);
        right.add(3, 4);
        right.add(4, 2);

        left.merge(&right).unwrap();

        assert_eq!(left.estimate_with_error(&0), Some((17, 0)));
        assert_eq!(left.estimate_with_error(&1), Some((7, 2)));
        assert_eq!(left.estimate_with_error(&3), Some((7, 3)));
        assert_eq!(left.estimate(&2), None);
        assert_eq!(left.estimate(&4), None);

        let exact = HashMap::from([(0, 17), (1, 5), (2, 3), (3, 4), (4, 2)]);
        assert_valid_bounds(&left, &exact);
    }

    #[test]
    fn merge_uses_zero_as_the_underfull_missing_item_bound() {
        let mut left = SpaceSaving::new(4).unwrap();
        left.add(0_u64, 10);
        left.add(1, 5);

        let mut right = SpaceSaving::new(4).unwrap();
        right.add(0_u64, 7);
        right.add(2, 4);

        left.merge(&right).unwrap();

        assert_eq!(left.estimate_with_error(&0), Some((17, 0)));
        assert_eq!(left.estimate_with_error(&1), Some((5, 0)));
        assert_eq!(left.estimate_with_error(&2), Some((4, 0)));
        assert_eq!(left.total_count(), 26);
    }

    #[test]
    fn merge_tracks_total_count_independently_from_retained_estimates() {
        let mut left = SpaceSaving::new(3).unwrap();
        left.add(0_u64, 10);
        left.add(1, 5);
        left.add(2, 3);

        let mut right = SpaceSaving::new(3).unwrap();
        right.add(3_u64, 7);
        right.add(4, 4);
        right.add(5, 2);

        left.merge(&right).unwrap();

        let retained_sum: u64 = left
            .top_k(left.capacity())
            .iter()
            .map(|(_, estimate, _)| estimate)
            .sum();
        assert_eq!(retained_sum, 29);
        assert_eq!(left.total_count(), 31);

        let exact = HashMap::from([(0, 10), (1, 5), (2, 3), (3, 7), (4, 4), (5, 2)]);
        assert_valid_bounds(&left, &exact);
    }

    #[test]
    fn direct_and_repeated_merged_ingestion_preserve_bounds() {
        const CAPACITY: usize = 32;
        const SHARD_COUNT: usize = 8;
        const OBSERVATIONS: usize = 50_000;

        let mut direct = SpaceSaving::new(CAPACITY).unwrap();
        let mut shards: Vec<_> = (0..SHARD_COUNT)
            .map(|_| SpaceSaving::new(CAPACITY).unwrap())
            .collect();
        let mut exact = HashMap::new();
        let mut random = 0x9e37_79b9_7f4a_7c15_u64;

        for index in 0..OBSERVATIONS {
            random = random
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            let item = match index % 20 {
                0..=5 => 0,
                6..=9 => 1,
                10..=12 => 2,
                _ => 10 + random % 1_000,
            };

            direct.insert(item);
            shards[index % SHARD_COUNT].insert(item);
            *exact.entry(item).or_insert(0) += 1;
        }

        let mut sequential_merge = SpaceSaving::new(CAPACITY).unwrap();
        for shard in &shards {
            sequential_merge.merge(shard).unwrap();
        }

        let mut reduction = shards;
        while reduction.len() > 1 {
            let mut next = Vec::with_capacity(reduction.len().div_ceil(2));
            let mut pairs = reduction.into_iter();
            while let Some(mut left) = pairs.next() {
                if let Some(right) = pairs.next() {
                    left.merge(&right).unwrap();
                }
                next.push(left);
            }
            reduction = next;
        }
        let tree_merge = reduction.pop().unwrap();

        for sketch in [&direct, &sequential_merge, &tree_merge] {
            assert_eq!(sketch.total_count(), OBSERVATIONS as u64);
            assert_valid_bounds(sketch, &exact);
            assert!(sketch.estimate(&0).is_some());
            assert!(sketch.estimate(&1).is_some());
            assert!(sketch.estimate(&2).is_some());
        }
    }

    #[test]
    fn merge_total_count_saturates() {
        let mut left = SpaceSaving::new(2).unwrap();
        left.add(0_u64, u64::MAX);
        let mut right = SpaceSaving::new(2).unwrap();
        right.insert(1_u64);

        left.merge(&right).unwrap();

        assert_eq!(left.total_count(), u64::MAX);
    }

    #[test]
    fn merge_rejects_mismatched_capacity() {
        let mut left: SpaceSaving<String> = SpaceSaving::new(4).unwrap();
        let right: SpaceSaving<String> = SpaceSaving::new(5).unwrap();
        left.add("preserved".to_string(), 12);

        assert!(left.merge(&right).is_err());
        assert_eq!(
            left.estimate_with_error(&"preserved".to_string()),
            Some((12, 0))
        );
        assert_eq!(left.total_count(), 12);
    }

    #[test]
    fn clear_resets_state() {
        let mut sketch = SpaceSaving::new(3).unwrap();
        sketch.add("x".to_string(), 10);
        assert!(!sketch.is_empty());
        sketch.clear();
        assert!(sketch.is_empty());
        assert_eq!(sketch.tracked_items(), 0);
    }
}
