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
//! Space-Saving keeps at most `capacity` counters and replaces a minimum
//! counter when a previously untracked item arrives after the summary is full.
//! This implementation accepts unit-weight observations through
//! [`SpaceSaving::insert`], matching the update model in the [original
//! Space-Saving paper].
//!
//! # Stream-Summary representation
//!
//! The paper's Stream-Summary groups equal counters under count buckets. The
//! buckets form a doubly linked list ordered by count, and the counters in each
//! bucket form another doubly linked list. A hash table maps each tracked item
//! to its counter. Because a unit update moves a counter only from `count` to
//! `count + 1`, its destination is either the next bucket or a new bucket
//! inserted immediately after the current one.
//!
//! Rust cannot safely store ordinary references into vectors that may move, so
//! this implementation uses stable integer handles into private arenas. It has
//! the same link operations as the paper without self-referential structs or
//! unsafe code. Empty buckets are removed immediately and their arena slots are
//! reused.
//!
//! # Complexity
//!
//! Let `C` be the configured counter capacity, `m` the number of currently
//! tracked counters (`m <= C`), and `k` the requested result size.
//! The expected bounds assume expected `O(1)` hash-table operations and treat
//! hashing, equality, cloning, and dropping one item as `O(1)`.
//!
//! | Operation | Time | Additional space | Why |
//! | --- | ---: | ---: | --- |
//! | [`SpaceSaving::insert`] | expected amortized `O(1)` | `O(C)` worst case during growth | Constant link work; private arenas occasionally grow |
//! | [`SpaceSaving::estimate`] / [`SpaceSaving::estimate_with_error`] / [`SpaceSaving::lower_bound`] | expected `O(1)` | `O(1)` | One hash lookup |
//! | [`SpaceSaving::top_k`] | `O(min(k, m))` | `O(min(k, m))` | Traverses buckets from largest to smallest and clones only returned items |
//! | [`SpaceSaving::merge`] | expected `O(C)` | `O(C)` | Combines counters and reconstructs an independently reserved full-capacity owner |
//! | [`SpaceSaving::clear`] | `O(C)` | `O(1)` | Resets reserved lookup storage and drops tracked counter/bucket state |
//! | Other accessors | `O(1)` | `O(1)` | Read stored fields |
//!
//! Each insertion performs a constant number of link changes, but growing a
//! private arena can take `O(C)` time. A moving reallocation can temporarily
//! keep both old and new buffers live, using `O(C)` additional workspace.
//! Geometric growth and reuse of released bucket slots make complete insertion
//! expected amortized `O(1)` over the storage lifecycle. In particular, the first
//! insertion after merge reconstruction may grow an exactly reserved bucket
//! arena before releasing the counter's previous bucket.
//!
//! The retained representation itself uses `O(C)` space. Merge additionally
//! reserves a replacement lookup table and counter arena for `C` counters,
//! even when the inputs are empty or underfull. Existing inputs remain live
//! until reconstruction succeeds; the replacement is then committed. The
//! live-entry buffers and fixed-pass radix ordering also fit within `O(C)`.
//!
//! A nonempty `clear` also touches the lookup table's reserved control storage,
//! whose size is proportional to `C`, even when only one counter is tracked.
//! The current standard-library `HashMap` skips that reset when already empty;
//! this fast path does not make nonempty clearing proportional only to `m`.
//! Clearing retains allocated buffers and the configured capacity for reuse.
//!
//! For a tracked item, the stored estimate is an upper bound and
//! `estimate - error` is a lower bound on its frequency, provided the exact
//! frequency is representable as a `u64`. Merging follows Algorithms 3 and 4
//! of Cafaro, Pulimeno, and Tempesta's [parallel Space-Saving construction]:
//! estimates and errors are combined symmetrically, using a full summary's
//! minimum counter as the bound for an item missing from that summary.
//!
//! [original Space-Saving paper]: https://www.cs.ucsb.edu/sites/default/files/documents/2005-23.pdf
//! [parallel Space-Saving construction]: https://arxiv.org/pdf/1401.0702

use std::collections::HashMap;
use std::hash::Hash;
use std::sync::Arc;

use crate::SketchError;

type CounterHandle = usize;
type BucketHandle = usize;

#[derive(Debug, Clone, Copy)]
struct CounterEntry {
    count: u64,
    error: u64,
}

/// One tracked item. Handles remain valid even when either arena reallocates.
#[derive(Debug, Clone)]
struct CounterNode<T> {
    item: Arc<T>,
    count: u64,
    error: u64,
    bucket: BucketHandle,
    previous: Option<CounterHandle>,
    next: Option<CounterHandle>,
}

/// One distinct counter value in the ordered Stream-Summary bucket list.
#[derive(Debug, Clone)]
struct BucketNode {
    count: u64,
    head: Option<CounterHandle>,
    previous: Option<BucketHandle>,
    next: Option<BucketHandle>,
}

/// Approximate top-k tracker using Space-Saving and Stream-Summary.
///
/// `SpaceSaving<T>` stores up to `capacity` candidate heavy hitters and accepts
/// one stream observation per call to [`insert`](Self::insert). Weighted or
/// batched updates are intentionally not part of this API: Stream-Summary's
/// constant-time link update relies on every counter increasing by exactly one.
///
/// # Example
///
/// ```rust
/// use sketches::space_saving::SpaceSaving;
///
/// let mut hh = SpaceSaving::new(3).unwrap();
/// for item in ["apple", "apple", "banana", "apple", "carrot", "durian"] {
///     hh.insert(item);
/// }
///
/// let top = hh.top_k(2);
/// assert_eq!(top[0].0, "apple");
/// assert_eq!(top[0].1, 3);
/// ```
#[derive(Debug, Clone)]
pub struct SpaceSaving<T>
where
    T: Eq + Hash + Clone,
{
    capacity: usize,
    /// Shares each immutable item allocation with its counter node.
    lookup: HashMap<Arc<T>, CounterHandle>,
    /// Counter nodes are never removed; full-summary replacement reuses one.
    counters: Vec<CounterNode<T>>,
    /// Bucket slots may be removed and subsequently reused.
    buckets: Vec<Option<BucketNode>>,
    free_buckets: Vec<BucketHandle>,
    minimum_bucket: Option<BucketHandle>,
    maximum_bucket: Option<BucketHandle>,
    total_count: u64,
}

impl<T> SpaceSaving<T>
where
    T: Eq + Hash + Clone,
{
    /// Creates a sketch with the given number of tracked counters.
    ///
    /// Reserves the lookup table and counter arena before returning an owner.
    /// Count-bucket storage and item allocations grow during later insertions.
    ///
    /// # Errors
    /// Returns [`SketchError::InvalidParameter`] when `capacity == 0` or the
    /// initial lookup/counter storage cannot be represented or reserved.
    pub fn new(capacity: usize) -> Result<Self, SketchError> {
        if capacity == 0 {
            return Err(SketchError::InvalidParameter(
                "capacity must be greater than zero",
            ));
        }

        Self::empty_with_capacity(capacity)
    }

    /// Returns the maximum number of tracked counters.
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Returns the number of items currently tracked.
    pub fn tracked_items(&self) -> usize {
        self.lookup.len()
    }

    /// Returns the total number of inserted observations, saturated at
    /// [`u64::MAX`].
    ///
    /// This value is tracked independently from the sum of retained counter
    /// estimates. A merge may discard counters, so that sum can be smaller
    /// than the combined input length.
    pub fn total_count(&self) -> u64 {
        self.total_count
    }

    /// Returns `true` when no observations have been inserted.
    pub fn is_empty(&self) -> bool {
        self.total_count == 0
    }

    /// Inserts one occurrence of `item`.
    ///
    /// This is the unit-weight update from the original Space-Saving
    /// algorithm. Expected amortized time is `O(1)`: the item hash lookup and
    /// bucket/counter link changes take expected constant time, while private
    /// arena growth occasionally takes `O(capacity)` time and can require
    /// `O(capacity)` additional workspace during a moving reallocation.
    /// Counts and the total stream length saturate at [`u64::MAX`].
    pub fn insert(&mut self, item: T) {
        if let Some(&counter) = self.lookup.get(&item) {
            self.increment_counter(counter);
        } else if self.counters.len() < self.capacity {
            self.insert_new_counter(item);
        } else {
            self.replace_minimum(item);
        }

        self.total_count = self.total_count.saturating_add(1);
    }

    /// Returns the estimated count for `item` if it is currently tracked.
    pub fn estimate(&self, item: &T) -> Option<u64> {
        self.lookup
            .get(item)
            .map(|&counter| self.counters[counter].count)
    }

    /// Returns `(estimate, max_error)` for `item` if currently tracked.
    ///
    /// Before integer saturation, the exact frequency is in the inclusive
    /// interval `estimate - max_error..=estimate`.
    pub fn estimate_with_error(&self, item: &T) -> Option<(u64, u64)> {
        self.lookup.get(item).map(|&counter| {
            let node = &self.counters[counter];
            (node.count, node.error)
        })
    }

    /// Returns the conservative lower bound for `item` if currently tracked.
    ///
    /// Before integer saturation, this is no greater than the exact frequency.
    pub fn lower_bound(&self, item: &T) -> Option<u64> {
        self.lookup.get(item).map(|&counter| {
            let node = &self.counters[counter];
            node.count.saturating_sub(node.error)
        })
    }

    /// Returns up to `k` tracked items sorted by estimated count descending.
    ///
    /// Each tuple is `(item, estimate, max_error)`. Items with equal estimates
    /// may appear in any order. The query walks the Stream-Summary from its
    /// maximum bucket and clones only the returned items, taking
    /// `O(min(k, tracked_items))` time and output space.
    pub fn top_k(&self, k: usize) -> Vec<(T, u64, u64)> {
        let result_len = k.min(self.lookup.len());
        let mut result = Vec::with_capacity(result_len);
        if result_len == 0 {
            return result;
        }
        let mut bucket = self.maximum_bucket;

        while let Some(bucket_handle) = bucket {
            let bucket_node = self.bucket(bucket_handle);
            let mut counter = bucket_node.head;

            while let Some(counter_handle) = counter {
                let node = &self.counters[counter_handle];
                result.push((node.item.as_ref().clone(), node.count, node.error));
                if result.len() == result_len {
                    return result;
                }
                counter = node.next;
            }

            bucket = bucket_node.previous;
        }

        result
    }

    /// Clears tracked counters, Stream-Summary buckets, and total count.
    ///
    /// Retains the configured capacity and allocated buffers for reuse. Releases
    /// this summary's item handles; items shared with cloned or merged summaries
    /// remain alive until their last owner releases them.
    ///
    /// Takes `O(capacity)` time and `O(1)` additional space, assuming constant-time
    /// item destruction. A nonempty lookup resets its reserved control storage,
    /// so sparse summaries do not have an `O(tracked_items)` clear bound. The
    /// current standard-library `HashMap` returns immediately when already empty.
    pub fn clear(&mut self) {
        self.lookup.clear();
        self.counters.clear();
        self.buckets.clear();
        self.free_buckets.clear();
        self.minimum_bucket = None;
        self.maximum_bucket = None;
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
    /// `O(capacity)` time. Reconstructing the ordered Stream-Summary is linear:
    /// eight stable counting passes order the retained `u64` estimates without
    /// introducing an `O(capacity * log(capacity))` comparison sort. The new
    /// lookup table and counter arena reserve the full configured capacity,
    /// including for empty or underfull inputs. The total
    /// stream length is combined separately and saturates at [`u64::MAX`]. The
    /// receiver remains unchanged if compatibility validation or any internal
    /// storage reservation fails. The donor is always borrowed immutably.
    ///
    /// # Errors
    /// Returns [`SketchError::IncompatibleSketches`] when capacities differ.
    /// Returns [`SketchError::InvalidParameter`] when the temporary or rebuilt
    /// storage cannot be represented or reserved. Hashing/equality and item
    /// destructors retain their caller-defined behavior.
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
        let combined_len = self.lookup.len().checked_add(other.lookup.len()).ok_or(
            SketchError::InvalidParameter("combined entry count exceeds usize"),
        )?;
        let mut combined = Vec::new();
        combined.try_reserve_exact(combined_len).map_err(|_| {
            SketchError::InvalidParameter("combined entries cannot be represented or reserved")
        })?;

        for (item, &self_counter) in &self.lookup {
            let self_entry = self.counter_entry(self_counter);
            let entry = if let Some(&other_counter) = other.lookup.get(item) {
                let other_entry = other.counter_entry(other_counter);
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
            combined.push((Arc::clone(item), entry));
        }

        for (item, &other_counter) in &other.lookup {
            if !self.lookup.contains_key(item) {
                let other_entry = other.counter_entry(other_counter);
                combined.push((
                    Arc::clone(item),
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

        let total_count = self.total_count.saturating_add(other.total_count);
        // Commit only after every reservation and reconstruction succeeds;
        // all preceding mutations are confined to independent temporary state.
        let replacement = Self::from_entries(self.capacity, total_count, &combined)?;
        *self = replacement;
        Ok(())
    }

    /// Builds an empty owner with all lookup and counter capacity reserved.
    /// Callers validate positive capacity. Reservation errors discard only
    /// local storage; bucket arenas remain lazy for ordinary ingestion.
    fn empty_with_capacity(capacity: usize) -> Result<Self, SketchError> {
        let mut lookup = HashMap::new();
        lookup.try_reserve(capacity).map_err(|_| {
            SketchError::InvalidParameter("lookup storage cannot be represented or reserved")
        })?;
        let mut counters = Vec::new();
        counters.try_reserve_exact(capacity).map_err(|_| {
            SketchError::InvalidParameter("counter storage cannot be represented or reserved")
        })?;
        Ok(Self {
            capacity,
            lookup,
            counters,
            buckets: Vec::new(),
            free_buckets: Vec::new(),
            minimum_bucket: None,
            maximum_bucket: None,
            total_count: 0,
        })
    }

    fn insert_new_counter(&mut self, item: T) {
        let bucket = match self.minimum_bucket {
            None => self.allocate_bucket_after(None, 1),
            Some(minimum) if self.bucket(minimum).count == 1 => minimum,
            Some(_) => self.allocate_bucket_after(None, 1),
        };
        let item = Arc::new(item);
        let counter = self.counters.len();

        self.counters.push(CounterNode {
            item: Arc::clone(&item),
            count: 1,
            error: 0,
            bucket,
            previous: None,
            next: None,
        });
        self.attach_counter(counter, bucket);
        self.lookup.insert(item, counter);
    }

    fn replace_minimum(&mut self, item: T) {
        let minimum = self
            .minimum_bucket
            .expect("a full summary has a minimum bucket");
        let minimum_count = self.bucket(minimum).count;
        let counter = self
            .bucket(minimum)
            .head
            .expect("every active bucket contains a counter");
        let old_item = Arc::clone(&self.counters[counter].item);
        let removed = self.lookup.remove(old_item.as_ref());
        debug_assert_eq!(removed, Some(counter));

        let item = Arc::new(item);
        self.counters[counter].item = Arc::clone(&item);
        self.counters[counter].error = minimum_count;
        self.lookup.insert(item, counter);
        self.increment_counter(counter);
    }

    fn increment_counter(&mut self, counter: CounterHandle) {
        let old_bucket = self.counters[counter].bucket;
        let old_count = self.counters[counter].count;
        let new_count = old_count.saturating_add(1);

        // Saturation leaves the counter in the already-correct maximum-valued
        // bucket and avoids manufacturing another bucket with the same count.
        if new_count == old_count {
            return;
        }

        let next_bucket = self.bucket(old_bucket).next;
        let destination = match next_bucket {
            Some(next) if self.bucket(next).count == new_count => next,
            _ => self.allocate_bucket_after(Some(old_bucket), new_count),
        };

        self.detach_counter(counter);
        self.counters[counter].count = new_count;
        self.attach_counter(counter, destination);

        if self.bucket(old_bucket).head.is_none() {
            self.remove_bucket(old_bucket);
        }
    }

    fn attach_counter(&mut self, counter: CounterHandle, bucket: BucketHandle) {
        let old_head = self.bucket(bucket).head;
        {
            let node = &mut self.counters[counter];
            node.bucket = bucket;
            node.previous = None;
            node.next = old_head;
        }

        if let Some(head) = old_head {
            self.counters[head].previous = Some(counter);
        }
        self.bucket_mut(bucket).head = Some(counter);
    }

    fn detach_counter(&mut self, counter: CounterHandle) {
        let bucket = self.counters[counter].bucket;
        let previous = self.counters[counter].previous;
        let next = self.counters[counter].next;

        if let Some(previous) = previous {
            self.counters[previous].next = next;
        } else {
            self.bucket_mut(bucket).head = next;
        }
        if let Some(next) = next {
            self.counters[next].previous = previous;
        }

        self.counters[counter].previous = None;
        self.counters[counter].next = None;
    }

    /// Allocates a bucket immediately after `previous`, or at the front when
    /// `previous` is `None`. Callers know this exact position because unit
    /// increments cannot skip an integer-valued bucket.
    fn allocate_bucket_after(
        &mut self,
        previous: Option<BucketHandle>,
        count: u64,
    ) -> BucketHandle {
        let next = match previous {
            Some(previous) => self.bucket(previous).next,
            None => self.minimum_bucket,
        };

        debug_assert!(previous.is_none_or(|handle| self.bucket(handle).count < count));
        debug_assert!(next.is_none_or(|handle| count < self.bucket(handle).count));

        let bucket = if let Some(free) = self.free_buckets.pop() {
            self.buckets[free] = Some(BucketNode {
                count,
                head: None,
                previous,
                next,
            });
            free
        } else {
            let bucket = self.buckets.len();
            self.buckets.push(Some(BucketNode {
                count,
                head: None,
                previous,
                next,
            }));
            bucket
        };

        if let Some(previous) = previous {
            self.bucket_mut(previous).next = Some(bucket);
        } else {
            self.minimum_bucket = Some(bucket);
        }
        if let Some(next) = next {
            self.bucket_mut(next).previous = Some(bucket);
        } else {
            self.maximum_bucket = Some(bucket);
        }

        bucket
    }

    fn remove_bucket(&mut self, bucket: BucketHandle) {
        let removed = self.buckets[bucket]
            .take()
            .expect("active bucket handle points to a bucket");
        debug_assert!(removed.head.is_none());

        if let Some(previous) = removed.previous {
            self.bucket_mut(previous).next = removed.next;
        } else {
            self.minimum_bucket = removed.next;
        }
        if let Some(next) = removed.next {
            self.bucket_mut(next).previous = removed.previous;
        } else {
            self.maximum_bucket = removed.previous;
        }

        self.free_buckets.push(bucket);
    }

    fn untracked_upper_bound(&self) -> u64 {
        if self.lookup.len() < self.capacity {
            return 0;
        }

        self.minimum_bucket
            .map(|bucket| self.bucket(bucket).count)
            .expect("a full summary has a minimum bucket")
    }

    fn counter_entry(&self, counter: CounterHandle) -> CounterEntry {
        let node = &self.counters[counter];
        CounterEntry {
            count: node.count,
            error: node.error,
        }
    }

    fn bucket(&self, bucket: BucketHandle) -> &BucketNode {
        self.buckets[bucket]
            .as_ref()
            .expect("active bucket handle points to a bucket")
    }

    fn bucket_mut(&mut self, bucket: BucketHandle) -> &mut BucketNode {
        self.buckets[bucket]
            .as_mut()
            .expect("active bucket handle points to a bucket")
    }

    /// Reconstructs a separate owner from at most capacity retained entries.
    /// All entries have distinct keys. Existing immutable item allocations
    /// are shared through Arc; counter/bucket links belong only to the new owner.
    /// Every required storage reservation succeeds before linking begins.
    fn from_entries(
        capacity: usize,
        total_count: u64,
        entries: &[(Arc<T>, CounterEntry)],
    ) -> Result<Self, SketchError> {
        debug_assert!(entries.len() <= capacity);
        let mut summary = Self::empty_with_capacity(capacity)?;
        summary.total_count = total_count;
        let order = Self::radix_order(entries)?;
        // Reserve only the distinct count groups, rather than one bucket per
        // entry. Building the ordered list then requires no further allocation.
        let bucket_count = usize::from(!order.is_empty())
            + order
                .windows(2)
                .filter(|pair| entries[pair[0]].1.count != entries[pair[1]].1.count)
                .count();
        summary
            .buckets
            .try_reserve_exact(bucket_count)
            .map_err(|_| {
                SketchError::InvalidParameter("bucket storage cannot be represented or reserved")
            })?;
        let mut current_bucket = None;
        let mut current_count = None;

        for index in order {
            let (item, entry) = &entries[index];
            let bucket = if current_count == Some(entry.count) {
                current_bucket.expect("an equal count already has a bucket")
            } else {
                let bucket = summary.allocate_bucket_after(current_bucket, entry.count);
                current_bucket = Some(bucket);
                current_count = Some(entry.count);
                bucket
            };
            let counter = summary.counters.len();

            summary.counters.push(CounterNode {
                item: Arc::clone(item),
                count: entry.count,
                error: entry.error,
                bucket,
                previous: None,
                next: None,
            });
            summary.attach_counter(counter, bucket);
            summary.lookup.insert(Arc::clone(item), counter);
        }

        Ok(summary)
    }

    /// Returns entry indices ordered by their `u64` counts. Eight byte-wise
    /// stable counting passes keep Stream-Summary reconstruction linear in the
    /// number of retained counters.
    /// Reserves both index buffers fallibly before the counting passes.
    fn radix_order(entries: &[(Arc<T>, CounterEntry)]) -> Result<Vec<usize>, SketchError> {
        let mut order = Vec::new();
        order.try_reserve_exact(entries.len()).map_err(|_| {
            SketchError::InvalidParameter("radix order cannot be represented or reserved")
        })?;
        order.extend(0..entries.len());
        let mut scratch = Vec::new();
        scratch.try_reserve_exact(entries.len()).map_err(|_| {
            SketchError::InvalidParameter("radix scratch cannot be represented or reserved")
        })?;
        scratch.resize(entries.len(), 0);

        for shift in (0..u64::BITS).step_by(8) {
            let mut counts = [0_usize; 256];
            for &index in &order {
                let byte = ((entries[index].1.count >> shift) & 0xff) as usize;
                counts[byte] += 1;
            }

            let mut offsets = [0_usize; 256];
            let mut offset = 0;
            for (byte, count) in counts.into_iter().enumerate() {
                offsets[byte] = offset;
                offset += count;
            }

            for &index in &order {
                let byte = ((entries[index].1.count >> shift) & 0xff) as usize;
                scratch[offsets[byte]] = index;
                offsets[byte] += 1;
            }

            std::mem::swap(&mut order, &mut scratch);
        }

        Ok(order)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};
    use std::fmt::Debug;
    use std::hash::Hash;

    use super::{CounterEntry, SpaceSaving};
    use crate::SketchError;
    use std::sync::Arc;

    #[test]
    fn impossible_initial_and_rebuilt_layouts_return_errors() {
        let item = Arc::new(7_u64);
        let entries = [(Arc::clone(&item), CounterEntry { count: 3, error: 1 })];
        for capacity in [1usize << (usize::BITS - 1), usize::MAX] {
            assert!(matches!(
                SpaceSaving::<u64>::new(capacity),
                Err(SketchError::InvalidParameter(_))
            ));
            assert!(matches!(
                SpaceSaving::from_entries(capacity, 3, &entries),
                Err(SketchError::InvalidParameter(_))
            ));
            assert_eq!(Arc::strong_count(&item), 2);
        }
    }

    #[test]
    fn reconstruction_groups_every_u64_byte_and_preserves_shared_items() {
        let counts = [
            2,
            1,
            0xff,
            0x100,
            0xffff,
            0x1_0000,
            1_u64 << 24,
            1_u64 << 32,
            1_u64 << 40,
            1_u64 << 48,
            1_u64 << 56,
            u64::MAX,
        ];
        let entries: Vec<_> = counts
            .into_iter()
            .rev()
            .enumerate()
            .flat_map(|(index, count)| {
                // Two distinct keys at every count exercise shared buckets
                // while the unsorted counts cross all eight radix passes.
                (0..2).map(move |copy| {
                    (
                        Arc::new((index * 2 + copy) as u64),
                        CounterEntry {
                            count,
                            error: count / 2,
                        },
                    )
                })
            })
            .collect();
        let mut summary = SpaceSaving::from_entries(entries.len(), u64::MAX, &entries).unwrap();
        assert_stream_summary_invariants(&summary);
        assert_eq!(summary.buckets.len(), counts.len());
        assert_eq!(summary.total_count(), u64::MAX);
        for (item, entry) in &entries {
            assert_eq!(
                summary.estimate_with_error(item),
                Some((entry.count, entry.error))
            );
            assert_eq!(Arc::strong_count(item), 3);
        }
        summary.clear();
        assert_stream_summary_invariants(&summary);
        assert!(entries.iter().all(|(item, _)| Arc::strong_count(item) == 1));
        summary.insert(100);
        assert_eq!(summary.estimate(&100), Some(1));
        assert_stream_summary_invariants(&summary);
    }

    #[test]
    fn empty_reconstruction_retains_capacity_and_supports_ingestion() {
        let mut summary = SpaceSaving::<u64>::from_entries(3, 0, &[]).unwrap();
        assert_eq!(summary.capacity(), 3);
        assert!(summary.is_empty());
        assert_stream_summary_invariants(&summary);
        summary.insert(7);
        assert_eq!(summary.estimate(&7), Some(1));
        assert_stream_summary_invariants(&summary);
    }

    fn insert_repeated<T>(sketch: &mut SpaceSaving<T>, item: T, count: u64)
    where
        T: Eq + Hash + Clone,
    {
        for _ in 0..count {
            sketch.insert(item.clone());
        }
    }

    fn assert_stream_summary_invariants<T>(sketch: &SpaceSaving<T>)
    where
        T: Eq + Hash + Clone + Debug,
    {
        assert_eq!(sketch.lookup.len(), sketch.counters.len());
        assert!(sketch.lookup.len() <= sketch.capacity);
        assert!(sketch.buckets.len() <= sketch.capacity.saturating_add(1));

        let mut visited_buckets = HashSet::new();
        let mut visited_counters = HashSet::new();
        let mut previous_bucket = None;
        let mut previous_count = None;
        let mut bucket = sketch.minimum_bucket;

        while let Some(bucket_handle) = bucket {
            assert!(visited_buckets.insert(bucket_handle));
            let bucket_node = sketch.bucket(bucket_handle);
            assert_eq!(bucket_node.previous, previous_bucket);
            assert!(bucket_node.head.is_some());
            if let Some(previous_count) = previous_count {
                assert!(previous_count < bucket_node.count);
            }

            let mut previous_counter = None;
            let mut counter = bucket_node.head;
            while let Some(counter_handle) = counter {
                assert!(visited_counters.insert(counter_handle));
                let node = &sketch.counters[counter_handle];
                assert_eq!(node.bucket, bucket_handle);
                assert_eq!(node.count, bucket_node.count);
                assert_eq!(node.previous, previous_counter);
                assert_eq!(sketch.lookup.get(&node.item), Some(&counter_handle));
                previous_counter = Some(counter_handle);
                counter = node.next;
            }

            previous_bucket = Some(bucket_handle);
            previous_count = Some(bucket_node.count);
            bucket = bucket_node.next;
        }

        assert_eq!(previous_bucket, sketch.maximum_bucket);
        assert_eq!(visited_counters.len(), sketch.counters.len());
        assert_eq!(
            visited_buckets.len() + sketch.free_buckets.len(),
            sketch.buckets.len()
        );
        for &free in &sketch.free_buckets {
            assert!(sketch.buckets[free].is_none());
            assert!(!visited_buckets.contains(&free));
        }
        for counter in 0..sketch.counters.len() {
            assert!(visited_counters.contains(&counter));
        }

        if sketch.counters.is_empty() {
            assert_eq!(sketch.minimum_bucket, None);
            assert_eq!(sketch.maximum_bucket, None);
        }
    }

    fn assert_valid_bounds(sketch: &SpaceSaving<u64>, exact: &HashMap<u64, u64>) {
        assert_stream_summary_invariants(sketch);
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
    fn stream_summary_keeps_buckets_ordered_and_top_k_descending() {
        let mut sketch = SpaceSaving::new(4).unwrap();
        insert_repeated(&mut sketch, "one", 1);
        insert_repeated(&mut sketch, "two", 2);
        insert_repeated(&mut sketch, "three", 3);
        insert_repeated(&mut sketch, "four", 4);

        assert_stream_summary_invariants(&sketch);
        assert_eq!(
            sketch.top_k(3),
            vec![("four", 4, 0), ("three", 3, 0), ("two", 2, 0)]
        );
        assert!(sketch.top_k(0).is_empty());
    }

    #[test]
    fn high_cardinality_replacements_preserve_stream_summary_links() {
        let mut sketch = SpaceSaving::new(64).unwrap();
        let mut exact = HashMap::new();

        for index in 0..20_000_u64 {
            let item = if index % 7 == 0 { 0 } else { index + 1 };
            sketch.insert(item);
            *exact.entry(item).or_insert(0) += 1;
        }

        assert_valid_bounds(&sketch, &exact);
    }

    #[test]
    fn heavy_hitters_are_retained() {
        let mut sketch = SpaceSaving::new(5).unwrap();
        insert_repeated(&mut sketch, "apple".to_string(), 5_000);
        insert_repeated(&mut sketch, "banana".to_string(), 3_000);
        insert_repeated(&mut sketch, "carrot".to_string(), 1_000);

        for value in 0..200_u64 {
            sketch.insert(format!("noise-{value}"));
        }

        let top = sketch.top_k(3);
        let names: Vec<_> = top.iter().map(|(item, _, _)| item.as_str()).collect();
        assert!(names.contains(&"apple"));
        assert!(names.contains(&"banana"));
        assert_stream_summary_invariants(&sketch);
    }

    #[test]
    fn estimates_expose_error_bounds() {
        let mut sketch = SpaceSaving::new(2).unwrap();
        insert_repeated(&mut sketch, "a".to_string(), 10);
        insert_repeated(&mut sketch, "b".to_string(), 5);
        insert_repeated(&mut sketch, "c".to_string(), 2);

        let estimate = sketch.estimate_with_error(&"c".to_string());
        if let Some((count, error)) = estimate {
            assert!(count >= error);
        }
        assert_stream_summary_invariants(&sketch);
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
        assert_stream_summary_invariants(&left);
    }

    #[test]
    fn merge_combines_overlapping_estimates_and_errors() {
        let mut left = SpaceSaving::new(2).unwrap();
        insert_repeated(&mut left, 0_u64, 5);
        insert_repeated(&mut left, 1, 2);
        insert_repeated(&mut left, 2, 10);
        assert_eq!(left.estimate_with_error(&2), Some((12, 2)));

        let mut right = SpaceSaving::new(2).unwrap();
        insert_repeated(&mut right, 3_u64, 4);
        insert_repeated(&mut right, 4, 1);
        insert_repeated(&mut right, 2, 10);
        assert_eq!(right.estimate_with_error(&2), Some((11, 1)));

        left.merge(&right).unwrap();

        assert_eq!(left.estimate_with_error(&2), Some((23, 3)));
        assert_eq!(left.lower_bound(&2), Some(20));
        assert_eq!(left.total_count(), 32);
        assert_stream_summary_invariants(&left);
    }

    #[test]
    fn merge_applies_full_summary_minima_before_pruning() {
        let mut left = SpaceSaving::new(3).unwrap();
        insert_repeated(&mut left, 0_u64, 10);
        insert_repeated(&mut left, 1, 5);
        insert_repeated(&mut left, 2, 3);

        let mut right = SpaceSaving::new(3).unwrap();
        insert_repeated(&mut right, 0_u64, 7);
        insert_repeated(&mut right, 3, 4);
        insert_repeated(&mut right, 4, 2);

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
        insert_repeated(&mut left, 0_u64, 10);
        insert_repeated(&mut left, 1, 5);

        let mut right = SpaceSaving::new(4).unwrap();
        insert_repeated(&mut right, 0_u64, 7);
        insert_repeated(&mut right, 2, 4);

        left.merge(&right).unwrap();

        assert_eq!(left.estimate_with_error(&0), Some((17, 0)));
        assert_eq!(left.estimate_with_error(&1), Some((5, 0)));
        assert_eq!(left.estimate_with_error(&2), Some((4, 0)));
        assert_eq!(left.total_count(), 26);
        assert_stream_summary_invariants(&left);
    }

    #[test]
    fn merge_tracks_total_count_independently_from_retained_estimates() {
        let mut left = SpaceSaving::new(3).unwrap();
        insert_repeated(&mut left, 0_u64, 10);
        insert_repeated(&mut left, 1, 5);
        insert_repeated(&mut left, 2, 3);

        let mut right = SpaceSaving::new(3).unwrap();
        insert_repeated(&mut right, 3_u64, 7);
        insert_repeated(&mut right, 4, 4);
        insert_repeated(&mut right, 5, 2);

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
        left.insert(0_u64);
        for _ in 0..64 {
            let copy = left.clone();
            left.merge(&copy).unwrap();
        }
        let mut right = SpaceSaving::new(2).unwrap();
        right.insert(1_u64);

        left.merge(&right).unwrap();

        assert_eq!(left.total_count(), u64::MAX);
        assert_stream_summary_invariants(&left);
    }

    #[test]
    fn merge_rejects_mismatched_capacity_without_modification() {
        let mut left: SpaceSaving<String> = SpaceSaving::new(4).unwrap();
        let right: SpaceSaving<String> = SpaceSaving::new(5).unwrap();
        insert_repeated(&mut left, "preserved".to_string(), 12);

        assert!(left.merge(&right).is_err());
        assert_eq!(
            left.estimate_with_error(&"preserved".to_string()),
            Some((12, 0))
        );
        assert_eq!(left.total_count(), 12);
        assert_stream_summary_invariants(&left);
    }

    #[test]
    fn clear_resets_state_and_allows_reuse() {
        let mut sketch = SpaceSaving::new(3).unwrap();
        insert_repeated(&mut sketch, "x".to_string(), 10);
        assert!(!sketch.is_empty());
        sketch.clear();
        assert!(sketch.is_empty());
        assert_eq!(sketch.tracked_items(), 0);
        assert_stream_summary_invariants(&sketch);

        sketch.insert("reused".to_string());
        assert_eq!(sketch.estimate(&"reused".to_string()), Some(1));
        assert_stream_summary_invariants(&sketch);
    }
}
