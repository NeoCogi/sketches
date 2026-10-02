//! Public merge checks against independently counted observations.
//!
//! Approximate summaries need not retain identical tied keys across different
//! merge trees. The oracle instead checks exact mass, frequency intervals,
//! missing-item bounds, capacity, ordering, and exact underfull results.
//! Large counts are built using real public merges, never private state.

use std::collections::{BTreeMap, BTreeSet};
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use sketches::space_saving::SpaceSaving;

/// A real sketch and an exact histogram of the observations it represents.
#[derive(Clone)]
struct CheckedSummary {
    /// Public owner under test; its implementation is not used as the oracle.
    sketch: SpaceSaving<u64>,
    /// Exact counts exceed u64 only in the explicit saturation-policy test.
    exact: BTreeMap<u64, u128>,
}

impl CheckedSummary {
    /// Creates an empty stream at a positive, bounded counter capacity.
    fn new(capacity: usize) -> Self {
        Self {
            sketch: SpaceSaving::new(capacity).unwrap(),
            exact: BTreeMap::new(),
        }
    }

    /// Counts one actual observation independently of the sketch update.
    fn insert(&mut self, item: u64) {
        self.sketch.insert(item);
        *self.exact.entry(item).or_default() += 1;
    }

    /// Creates a stream owner without assuming any particular replacement ties.
    fn from_stream(capacity: usize, stream: &[u64]) -> Self {
        let mut result = Self::new(capacity);
        for &item in stream {
            result.insert(item);
        }
        result
    }

    /// Merges real owners, adds exact histograms, and checks donor immutability.
    fn merge(&mut self, donor: &Self) {
        let before = canonical_rows(&donor.sketch);
        let before_total = donor.sketch.total_count();
        self.sketch.merge(&donor.sketch).unwrap();
        for (&item, &count) in &donor.exact {
            *self.exact.entry(item).or_default() += count;
        }
        assert_eq!(canonical_rows(&donor.sketch), before);
        assert_eq!(donor.sketch.total_count(), before_total);
        self.check();
    }

    /// Checks public invariants without reconstructing the merge algorithm.
    fn check(&self) {
        let total: u128 = self.exact.values().sum();
        assert_eq!(
            self.sketch.total_count(),
            u64::try_from(total).unwrap_or(u64::MAX)
        );
        assert_eq!(self.sketch.is_empty(), total == 0);
        let rows = self.sketch.top_k(usize::MAX);
        assert_eq!(rows.len(), self.sketch.tracked_items());
        assert!(rows.len() <= self.sketch.capacity());
        assert!(rows.windows(2).all(|pair| pair[0].1 >= pair[1].1));
        let distinct: BTreeSet<_> = rows.iter().map(|row| row.0).collect();
        assert_eq!(distinct.len(), rows.len());
        for &(item, estimate, error) in &rows {
            assert!(estimate >= error);
            assert_eq!(self.sketch.estimate(&item), Some(estimate));
            assert_eq!(
                self.sketch.estimate_with_error(&item),
                Some((estimate, error))
            );
            assert_eq!(self.sketch.lower_bound(&item), Some(estimate - error));
            let exact = self.exact[&item];
            if total <= u128::from(u64::MAX) {
                assert!(
                    u128::from(estimate - error) <= exact && exact <= u128::from(estimate),
                    "item={item} exact={exact} estimate={estimate} error={error} capacity={}",
                    self.sketch.capacity()
                );
            }
        }
        if total <= u128::from(u64::MAX) {
            let minimum = rows.last().map(|row| row.1).unwrap_or(0);
            // Pruning may discard counter mass, but cannot create more mass
            // than the represented observations. Together with m <= C, this
            // bounds a full summary's minimum (and every stored error) by N/C.
            let retained_mass: u128 = rows.iter().map(|row| u128::from(row.1)).sum();
            assert!(retained_mass <= total);
            assert!(rows.iter().all(|row| row.2 <= minimum));
            if rows.len() == self.sketch.capacity() {
                assert!(u128::from(minimum) <= total / self.sketch.capacity() as u128);
            }
            for (&item, &count) in &self.exact {
                if self.sketch.estimate(&item).is_none() {
                    assert_eq!(rows.len(), self.sketch.capacity());
                    assert!(count <= u128::from(minimum));
                    assert_eq!(self.sketch.estimate_with_error(&item), None);
                    assert_eq!(self.sketch.lower_bound(&item), None);
                }
            }
            if self.exact.len() <= self.sketch.capacity() {
                assert_eq!(rows.len(), self.exact.len());
                for (&item, &count) in &self.exact {
                    assert_eq!(
                        self.sketch.estimate_with_error(&item),
                        Some((count as u64, 0))
                    );
                }
            }
        }
        for k in [
            0,
            1,
            self.sketch.capacity() / 2,
            self.sketch.capacity(),
            usize::MAX,
        ] {
            let selected = self.sketch.top_k(k);
            assert_eq!(selected.len(), k.min(rows.len()));
            assert!(selected.windows(2).all(|pair| pair[0].1 >= pair[1].1));
            let keys: BTreeSet<_> = selected.iter().map(|row| row.0).collect();
            assert_eq!(keys.len(), selected.len());
            for row in &selected {
                assert!(rows.contains(row));
            }
            if let Some(last) = selected.last() {
                assert!(
                    rows.iter()
                        .filter(|row| !keys.contains(&row.0))
                        .all(|row| row.1 <= last.1)
                );
            }
        }
        if !self.exact.contains_key(&u64::MAX) {
            assert_eq!(self.sketch.estimate(&u64::MAX), None);
        }
    }

    /// Starts a new stream while retaining the configured capacity.
    fn clear(&mut self) {
        self.sketch.clear();
        self.exact.clear();
        self.check();
    }
}

/// Normalizes unspecified bucket-tie order for immutable-state comparisons.
fn canonical_rows(sketch: &SpaceSaving<u64>) -> Vec<(u64, u64, u64)> {
    let mut rows = sketch.top_k(usize::MAX);
    rows.sort_unstable();
    rows
}

/// Enumerates every ternary stream of length zero through five (364 streams).
fn small_streams() -> Vec<Vec<u64>> {
    let mut streams = Vec::new();
    for length in 0..=5_u32 {
        for mut encoding in 0..3_u32.pow(length) {
            let mut stream = Vec::new();
            for _ in 0..length {
                stream.push(u64::from(encoding % 3));
                encoding /= 3;
            }
            streams.push(stream);
        }
    }
    streams
}

#[test]
fn exhaustive_small_stream_pairs_preserve_exact_counts_and_frequency_bounds() {
    let streams = small_streams();
    assert_eq!(streams.len(), 364);
    let mut merges = 0;
    for capacity in [1, 2, 3, 4, 8] {
        let sources: Vec<_> = streams
            .iter()
            .map(|stream| CheckedSummary::from_stream(capacity, stream))
            .collect();
        for left in &sources {
            for right in &sources {
                let mut result = left.clone();
                result.merge(right);
                assert_eq!(result.sketch.capacity(), capacity);
                merges += 1;
            }
        }
    }
    assert_eq!(merges, 662_480);
    println!("{merges} exhaustive ordered stream-pair merges passed");
}

/// Advances fixture-owned deterministic state, without statistical claims.
fn next_word(state: &mut u64) -> u64 {
    *state ^= *state << 13;
    *state ^= *state >> 7;
    *state ^= *state << 17;
    *state
}

/// Reduces real shards in a balanced tree, including odd unpaired leaves.
fn balanced_reduce(mut shards: Vec<CheckedSummary>) -> CheckedSummary {
    while shards.len() > 1 {
        let mut next = Vec::new();
        let mut pairs = shards.into_iter();
        while let Some(mut left) = pairs.next() {
            if let Some(right) = pairs.next() {
                left.merge(&right);
            }
            next.push(left);
        }
        shards = next;
    }
    shards.pop().unwrap()
}

#[test]
fn varied_shards_and_merge_trees_preserve_bounds_against_exact_streams() {
    let mut cases = 0;
    for capacity in [1, 2, 3, 7, 16, 31, 64, 256] {
        for shard_count in [1, 2, 3, 8, 17] {
            for distribution in 0..4 {
                let mut state = 0xA076_1D64_78BD_642F;
                let mut direct = CheckedSummary::new(capacity);
                let mut shards: Vec<_> = (0..shard_count)
                    .map(|_| CheckedSummary::new(capacity))
                    .collect();
                for index in 0..4096 {
                    let word = next_word(&mut state);
                    let item = match distribution {
                        0 => word % 2048,
                        1 => match index % 10 {
                            0..=4 => 0,
                            5..=7 => 1,
                            _ => 2 + word % 2048,
                        },
                        2 => word % 3,
                        _ => index as u64,
                    };
                    direct.insert(item);
                    shards[index % shard_count].insert(item);
                }
                direct.check();
                let mut forward = CheckedSummary::new(capacity);
                let mut reverse = CheckedSummary::new(capacity);
                for shard in &shards {
                    forward.merge(shard);
                }
                for shard in shards.iter().rev() {
                    reverse.merge(shard);
                }
                let tree = balanced_reduce(shards.clone());
                shards.reverse();
                let reversed_tree = balanced_reduce(shards);
                for result in [&forward, &reverse, &tree, &reversed_tree] {
                    assert_eq!(result.exact, direct.exact);
                    result.check();
                }
                cases += 1;
            }
        }
    }
    assert_eq!(cases, 160);
    println!(
        "{cases} distributions/shard/capacity cases passed in four merge orders; 655360 exact observations"
    );
}

#[test]
fn repeated_merge_insert_clone_and_clear_preserve_reusable_owners() {
    let mut operations = 0;
    let mut merges = 0;
    for capacity in [1, 2, 3, 7, 16, 31, 64, 256] {
        for seed in [1, 7, 0xA076_1D64_78BD_642F, 0xE703_7ED1_A0B4_28DB] {
            let mut state = seed;
            let mut receiver = CheckedSummary::new(capacity);
            for step in 0..2048 {
                match step % 17 {
                    0 => {
                        let mut donor = CheckedSummary::new(capacity);
                        for _ in 0..32 {
                            donor.insert(next_word(&mut state) % 128);
                        }
                        receiver.merge(&donor);
                        donor.clear();
                        donor.insert(500);
                        donor.check();
                        receiver.check();
                        merges += 1;
                    }
                    1 if step % 211 == 1 => receiver.clear(),
                    2 => {
                        let before = canonical_rows(&receiver.sketch);
                        let mut copy = receiver.clone();
                        copy.insert(501);
                        copy.check();
                        assert_eq!(canonical_rows(&receiver.sketch), before);
                    }
                    _ => receiver.insert(next_word(&mut state) % 128),
                }
                if step % 31 == 0 {
                    receiver.check();
                }
                operations += 1;
            }
            receiver.check();
            receiver.clear();
            receiver.insert(99);
            assert_eq!(receiver.sketch.estimate_with_error(&99), Some((1, 0)));
        }
    }
    println!("{operations} lifecycle operations and {merges} batch merges passed");
}

/// Encodes an actual repeated stream with public doubling merges and insertions.
/// This avoids astronomical observation loops while retaining an exact oracle.
fn repeated_stream(capacity: usize, item: u64, count: u64) -> CheckedSummary {
    let mut result = CheckedSummary::new(capacity);
    for bit in (0..u64::BITS - count.leading_zeros()).rev() {
        let copy = result.clone();
        result.merge(&copy);
        if count & (1_u64 << bit) != 0 {
            result.insert(item);
        }
        result.check();
    }
    assert_eq!(
        result.exact.get(&item).copied().unwrap_or(0),
        u128::from(count)
    );
    result
}

#[test]
fn public_large_counts_cross_every_radix_byte_without_losing_mass_or_order() {
    let counts = [
        1,
        2,
        3,
        0xff,
        0x100,
        0xffff,
        0x1_0000,
        1_u64 << 24,
        1_u64 << 32,
        1_u64 << 40,
        1_u64 << 48,
        1_u64 << 56,
        1_u64 << 63,
    ];
    let mut receiver = CheckedSummary::new(16);
    for (item, &count) in counts.iter().rev().enumerate() {
        let donor = repeated_stream(16, item as u64, count);
        receiver.merge(&donor);
    }
    receiver.check();
    assert!(receiver.sketch.total_count() < u64::MAX);
    assert_eq!(receiver.sketch.top_k(1)[0].1, 1_u64 << 63);
}

#[test]
fn saturation_policy_and_clear_remain_explicit_at_public_integer_boundaries() {
    for capacity in [1, 2, 8] {
        let mut result = repeated_stream(capacity, 7, 1_u64 << 63);
        let copy = result.clone();
        result.merge(&copy);
        assert_eq!(result.sketch.total_count(), u64::MAX);
        assert_eq!(result.sketch.estimate_with_error(&7), Some((u64::MAX, 0)));
        result.insert(8);
        result.check();
        assert_eq!(result.sketch.total_count(), u64::MAX);
        if capacity == 1 {
            assert_eq!(
                result.sketch.estimate_with_error(&8),
                Some((u64::MAX, u64::MAX))
            );
        } else {
            assert_eq!(result.sketch.estimate_with_error(&8), Some((1, 0)));
        }
        // Frequency intervals above the representable range are deliberately
        // not asserted. Saturation and successful reuse are the tested policy.
        result.clear();
        result.insert(9);
        result.check();
    }
}

/// Lawful colliding keys with an owned payload and observable deep copies.
/// Only the numeric identity participates in equality; every hash is constant.
struct CollidingKey {
    /// Logical identity independent of its owned payload allocation.
    id: u64,
    /// Owned data whose unnecessary cloning would be observable.
    payload: Vec<u8>,
    /// Shared callback counter; atomics let Clone report without borrowing.
    clones: Arc<AtomicUsize>,
}

impl PartialEq for CollidingKey {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}
impl Eq for CollidingKey {}
impl Hash for CollidingKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        0_u8.hash(state);
    }
}
impl Clone for CollidingKey {
    fn clone(&self) -> Self {
        self.clones.fetch_add(1, Ordering::Relaxed);
        Self {
            id: self.id,
            payload: self.payload.clone(),
            clones: Arc::clone(&self.clones),
        }
    }
}

#[test]
fn lawful_collisions_preserve_bounds_and_merge_shares_owned_items_without_deep_clones() {
    for capacity in [1, 2, 7, 64] {
        let clones = Arc::new(AtomicUsize::new(0));
        let key = |id| CollidingKey {
            id,
            payload: vec![id as u8; 512],
            clones: Arc::clone(&clones),
        };
        let mut left = SpaceSaving::new(capacity).unwrap();
        let mut right = SpaceSaving::new(capacity).unwrap();
        for id in 0..32 {
            left.insert(key(id));
        }
        for id in 16..48 {
            right.insert(key(id));
        }
        let before: Vec<_> = right
            .top_k(usize::MAX)
            .into_iter()
            .map(|(key, count, error)| (key.id, count, error))
            .collect();
        let before_clones = clones.load(Ordering::Relaxed);
        let copy = left.clone();
        assert_eq!(clones.load(Ordering::Relaxed), before_clones);
        left.merge(&right).unwrap();
        assert_eq!(clones.load(Ordering::Relaxed), before_clones);
        assert_eq!(left.total_count(), 64);
        let rows = left.top_k(usize::MAX);
        assert_eq!(clones.load(Ordering::Relaxed), before_clones + rows.len());
        let minimum = rows.last().unwrap().1;
        for id in 0..48 {
            let exact = if (16..32).contains(&id) { 2 } else { 1 };
            if let Some((estimate, error)) = left.estimate_with_error(&key(id)) {
                assert!(estimate - error <= exact && exact <= estimate);
            } else {
                assert!(exact <= minimum);
            }
        }
        left.clear();
        assert_eq!(
            right
                .top_k(usize::MAX)
                .into_iter()
                .map(|(key, count, error)| (key.id, count, error))
                .collect::<Vec<_>>(),
            before
        );
        assert_eq!(copy.total_count(), 32);
        for (item, _, _) in rows {
            assert_eq!(item.payload, vec![item.id as u8; 512]);
        }
        left.insert(key(99));
        assert_eq!(left.estimate_with_error(&key(99)), Some((1, 0)));
    }
}
