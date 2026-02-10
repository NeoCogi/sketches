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

    /// Returns the total inserted weight.
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
    pub fn estimate_with_error(&self, item: &T) -> Option<(u64, u64)> {
        self.counters.get(item).map(|entry| (entry.count, entry.error))
    }

    /// Returns the conservative lower bound for `item` if currently tracked.
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

    /// Merges another sketch by replaying its tracked counts.
    ///
    /// Both sketches must have the same `capacity`.
    ///
    /// # Errors
    /// Returns [`SketchError::IncompatibleSketches`] when capacities differ.
    pub fn merge(&mut self, other: &Self) -> Result<(), SketchError> {
        if self.capacity != other.capacity {
            return Err(SketchError::IncompatibleSketches(
                "capacity must match for merge",
            ));
        }

        for (item, entry) in &other.counters {
            self.add(item.clone(), entry.count);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::SpaceSaving;

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
    fn merge_combines_observations() {
        let mut left = SpaceSaving::new(4).unwrap();
        let mut right = SpaceSaving::new(4).unwrap();

        left.add("alpha".to_string(), 50);
        right.add("alpha".to_string(), 30);
        right.add("beta".to_string(), 20);

        left.merge(&right).unwrap();
        let alpha_estimate = left.estimate(&"alpha".to_string()).unwrap();
        assert!(alpha_estimate >= 70);
    }

    #[test]
    fn merge_rejects_mismatched_capacity() {
        let mut left: SpaceSaving<String> = SpaceSaving::new(4).unwrap();
        let right: SpaceSaving<String> = SpaceSaving::new(5).unwrap();
        assert!(left.merge(&right).is_err());
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
