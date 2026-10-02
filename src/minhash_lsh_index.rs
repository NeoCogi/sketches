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
//! Classical MinHash banding LSH for approximate candidate search.
//!
//! A signature with `m = b * r` components is split into `b` consecutive bands
//! of `r` rows. Each band is hashed into its own table, and a query retrieves the
//! union of the buckets matching its bands. Under the ideal independent-MinHash
//! model, two sets with Jaccard similarity `s` become candidates with
//! probability `1 - (1 - s^r)^b`. Candidate selection is therefore
//! probabilistic: a true nearest neighbor can be absent when none of its bands
//! match the query.
//!
//! Each user ID is owned once in an internal record arena. Band tables contain
//! only machine-word handles, so the algorithm-required `O(items * bands)`
//! postings do not become deep copies of string or compound IDs. The index
//! retains one compact MinHash signature per record for removal and approximate
//! Jaccard reranking.
//!
//! [`MinHash`] uses the classical multiple-hash construction rather than
//! one-permutation hashing or densification. Building an `m`-component MinHash
//! from `d` input elements therefore costs `O(d * m)`; this index receives that
//! completed signature and hashes its `m` components once per insertion or
//! query. The table repetition follows [Gionis, Indyk, and Motwani][gionis], and
//! the MinHash banding analysis is presented in [Mining of Massive
//! Datasets][mmds].
//!
//! [gionis]: https://www.vldb.org/conf/1999/P49.pdf
//! [mmds]: https://infolab.stanford.edu/~ullman/mmds/book.pdf

use std::alloc::Layout;
use std::cmp::{Ordering, Reverse};
use std::collections::{BinaryHeap, HashMap, HashSet, hash_map::RandomState};
use std::hash::{BuildHasher, Hash};

use crate::minhash::MinHash;
use crate::{SketchError, seeded_hash64, splitmix64};

/// Stable internal reference to one arena record.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct EntryHandle(usize);

/// Candidate score retained by the bounded top-k heap.
///
/// Scores sort ascending so wrapping this type in [`Reverse`] makes the heap
/// root the worst candidate currently retained. Handles provide a total,
/// deterministic tie-break without cloning user IDs.
#[derive(Debug, Clone, Copy)]
struct ScoredHandle {
    handle: EntryHandle,
    similarity: f64,
}

impl PartialEq for ScoredHandle {
    fn eq(&self, other: &Self) -> bool {
        self.similarity.total_cmp(&other.similarity) == Ordering::Equal
            && self.handle == other.handle
    }
}

impl Eq for ScoredHandle {}

impl PartialOrd for ScoredHandle {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ScoredHandle {
    fn cmp(&self, other: &Self) -> Ordering {
        self.similarity
            .total_cmp(&other.similarity)
            .then_with(|| self.handle.0.cmp(&other.handle.0))
    }
}

/// Minimal MinHash state needed for removal and approximate reranking.
#[derive(Debug, Clone)]
struct StoredSignature {
    values: Box<[u64]>,
    observed_any: bool,
}

impl StoredSignature {
    fn from_minhash(signature: &MinHash) -> Self {
        Self {
            values: signature.signature().into(),
            observed_any: !signature.is_empty(),
        }
    }
}

/// Canonical per-ID state. `next_same_hash` resolves distinct IDs sharing a
/// randomized 64-bit lookup hash, including user types with colliding hashes.
/// The chain's hash belongs to `id_heads`; removal already computes it for lookup.
#[derive(Debug, Clone)]
struct Entry<Id> {
    id: Id,
    next_same_hash: Option<EntryHandle>,
    signature: StoredSignature,
}

/// Locality-Sensitive Hashing index built on MinHash signatures.
///
/// # Example
/// ```rust
/// use sketches::minhash_lsh_index::MinHashLshIndex;
/// use sketches::minhash::MinHash;
///
/// let num_hashes = 128;
/// let mut index = MinHashLshIndex::new(num_hashes, 32).unwrap();
///
/// let mut doc_a = MinHash::new(num_hashes).unwrap();
/// let mut doc_b = MinHash::new(num_hashes).unwrap();
/// let mut query = MinHash::new(num_hashes).unwrap();
///
/// for token in 0_u64..10_000 {
///     doc_a.add(&token);
/// }
/// for token in 20_000_u64..30_000 {
///     doc_b.add(&token);
/// }
/// for token in 1_000_u64..11_000 {
///     query.add(&token);
/// }
///
/// index.insert(1_u64, &doc_a).unwrap();
/// index.insert(2_u64, &doc_b).unwrap();
///
/// let candidates = index.query_candidates(&query).unwrap();
/// assert!(candidates.contains(&1));
/// ```
///
/// # Representation and complexity
///
/// For `n` items, `b` bands, and `m` MinHash components, the index stores
/// `O(nm)` signature words and `O(nb)` machine-word postings. Each `Id` is owned
/// once regardless of `b`. Excluding the cost of hashing a user ID, insertion
/// and removal take `O(m + b)` expected time; candidate lookup takes
/// `O(m + postings visited)` expected time before output IDs are cloned.
///
/// For `c` unique candidates and a requested result count `q`,
/// [`Self::query_top_k`] spends `O(cm)` time scoring retained signatures,
/// `O(c log q)` maintaining its bounded heap, and `O(q log q)` ordering the
/// result. Only the final `min(c, q)` IDs are cloned.
#[derive(Debug, Clone)]
pub struct MinHashLshIndex<Id>
where
    Id: Eq + Hash + Clone,
{
    num_hashes: usize,
    bands: usize,
    rows_per_band: usize,
    band_seeds: Vec<u64>,
    hash_family_seed: Option<u64>,
    tables: Vec<HashMap<u64, HashSet<EntryHandle>>>,
    entries: Vec<Option<Entry<Id>>>,
    free_entries: Vec<EntryHandle>,
    id_hash_builder: RandomState,
    id_heads: HashMap<u64, EntryHandle>,
    entry_count: usize,
}

impl<Id> MinHashLshIndex<Id>
where
    Id: Eq + Hash + Clone,
{
    /// Creates an LSH index from signature width and number of bands.
    ///
    /// `num_hashes` must be divisible by `bands`, and `bands` cannot exceed
    /// `num_hashes`.
    ///
    /// # Errors
    /// Returns [`SketchError::InvalidParameter`] for invalid dimensions,
    /// unrepresentable signature storage, or index configuration storage that
    /// cannot be reserved.
    pub fn new(num_hashes: usize, bands: usize) -> Result<Self, SketchError> {
        if num_hashes == 0 {
            return Err(SketchError::InvalidParameter(
                "num_hashes must be greater than zero",
            ));
        }
        if bands == 0 {
            return Err(SketchError::InvalidParameter(
                "bands must be greater than zero",
            ));
        }
        if bands > num_hashes {
            return Err(SketchError::InvalidParameter(
                "bands must not exceed num_hashes",
            ));
        }
        if !num_hashes.is_multiple_of(bands) {
            return Err(SketchError::InvalidParameter(
                "num_hashes must be divisible by bands",
            ));
        }

        // The index does not allocate an `m`-word query signature itself, but
        // accepting a width for which such a `[u64; m]` layout is impossible
        // would create an index that no compatible MinHash could represent.
        Layout::array::<u64>(num_hashes)
            .map_err(|_| SketchError::InvalidParameter("num_hashes is too large to represent"))?;

        let rows_per_band = num_hashes / bands;

        // Reserve both configuration vectors explicitly so capacity overflow
        // and allocator failure become constructor errors rather than panics.
        // `resize_with` calls `HashMap::new` once per band; empty maps do not
        // allocate bucket arrays until their first posting is inserted.
        let mut tables = Vec::new();
        tables
            .try_reserve_exact(bands)
            .map_err(|_| SketchError::InvalidParameter("bands are too large to allocate"))?;
        tables.resize_with(bands, HashMap::new);

        let mut band_seeds = Vec::new();
        band_seeds
            .try_reserve_exact(bands)
            .map_err(|_| SketchError::InvalidParameter("bands are too large to allocate"))?;
        band_seeds.extend(
            (0..bands).map(|band| splitmix64((band as u64).wrapping_add(0xA076_1D64_78BD_642F))),
        );

        Ok(Self {
            num_hashes,
            bands,
            rows_per_band,
            band_seeds,
            hash_family_seed: None,
            tables,
            entries: Vec::new(),
            free_entries: Vec::new(),
            id_hash_builder: RandomState::new(),
            id_heads: HashMap::new(),
            entry_count: 0,
        })
    }

    /// Returns the MinHash signature width configured for this index.
    pub fn num_hashes(&self) -> usize {
        self.num_hashes
    }

    /// Returns the configured number of bands.
    pub fn bands(&self) -> usize {
        self.bands
    }

    /// Returns the number of rows per band.
    pub fn rows_per_band(&self) -> usize {
        self.rows_per_band
    }

    /// Returns the modeled probability that two sets become LSH candidates at
    /// a specified Jaccard similarity.
    ///
    /// With `b` bands and `r` rows per band, the ideal independent-MinHash
    /// model gives `s^r` as the probability of matching one complete band at
    /// similarity `s`. The probability of matching at least one band is then
    /// `1 - (1 - s^r)^b`.
    ///
    /// Calculations use ordinary `f64` rounding. In the tiny tail, the band
    /// factor is applied between two half powers so an underflowing `s^r`
    /// does not erase a representable final probability. A final result below
    /// the floating-point range can still round to zero.
    ///
    /// This is a model of the candidate-selection curve, not a per-query
    /// guarantee. The crate uses deterministically derived practical hash
    /// functions rather than ideal random permutations, and the final 64-bit
    /// band hash has a negligible additional false-positive collision chance.
    ///
    /// # Errors
    /// Returns [`SketchError::InvalidParameter`] unless `similarity` is finite
    /// and in the inclusive range `[0, 1]`.
    pub fn candidate_probability(&self, similarity: f64) -> Result<f64, SketchError> {
        if !similarity.is_finite() || !(0.0..=1.0).contains(&similarity) {
            return Err(SketchError::InvalidParameter(
                "similarity must be finite and between zero and one",
            ));
        }

        if similarity == 0.0 || similarity == 1.0 {
            return Ok(similarity);
        }

        let bands = self.bands as f64;
        let rows = self.rows_per_band as f64;
        let one_band_match = similarity.powf(rows);
        if self.bands == 1 {
            return Ok(one_band_match);
        }

        if self.rows_per_band > 1 && one_band_match < f64::MIN_POSITIVE {
            // For x = s^r < 2^-1022, p = b*x + O((b*x)^2). The checked
            // signature layout bounds b by usize::MAX / 8, so b*x < 2^-961
            // even on a 64-bit target: the correction is far below f64
            // precision. Split the power before multiplying by b; a nonzero
            // final result has normal half powers and no underflowing product
            // until the final rounding. Unlike a log/exp fallback, this keeps
            // power evaluation at the transition on the same numerical scale.
            let half_band_match = similarity.powf(rows * 0.5);
            return Ok((bands * half_band_match) * half_band_match);
        }

        // Directly evaluating `1 - (1 - one_band_match).powf(b)` loses
        // precision when the result is close to zero. `ln_1p` accurately forms
        // log(1 - x), and `-exp_m1` accurately forms 1 - exp(x).
        let no_band_match_log = bands * (-one_band_match).ln_1p();
        Ok(-no_band_match_log.exp_m1())
    }

    /// Returns the modeled Jaccard similarity at which the requested candidate
    /// probability is reached.
    ///
    /// This is the inverse of [`Self::candidate_probability`]. In particular,
    /// passing `0.5` returns the modeled midpoint of the configured S-curve; the
    /// commonly quoted `(1 / bands)^(1 / rows_per_band)` threshold is only a
    /// rough approximation to that point.
    ///
    /// The inverse carries the band probability on a logarithmic scale until
    /// taking the root, including for positive subnormal inputs. Calculations
    /// use ordinary `f64` rounding; final quantization prevents exact round
    /// trips for every possible input.
    ///
    /// # Errors
    /// Returns [`SketchError::InvalidParameter`] unless `probability` is finite
    /// and in the inclusive range `[0, 1]`.
    pub fn similarity_for_candidate_probability(
        &self,
        probability: f64,
    ) -> Result<f64, SketchError> {
        if !probability.is_finite() || !(0.0..=1.0).contains(&probability) {
            return Err(SketchError::InvalidParameter(
                "probability must be finite and between zero and one",
            ));
        }

        if probability == 0.0 || probability == 1.0 {
            return Ok(probability);
        }

        let rows = self.rows_per_band as f64;
        if self.bands == 1 {
            return Ok(probability.powf(1.0 / rows));
        }

        let all_band_miss_magnitude = -(-probability).ln_1p();
        if self.rows_per_band == 1 {
            // There is no later root to amplify a tiny intermediate. Keep
            // ordinary rounding, including final subnormal quantization.
            return Ok(-(-all_band_miss_magnitude / self.bands as f64).exp_m1());
        }

        // t = -ln(1-p)/b can underflow before taking the r-th root. Compute
        // log(t) from its original operands, then use the same log-scale
        // calculation throughout the domain to avoid a discontinuous switch
        // between a tail approximation and the ordinary rounded power.
        let log_miss_magnitude = all_band_miss_magnitude.ln() - (self.bands as f64).ln();
        let log_band_match = log_one_minus_exp_negative_from_log(log_miss_magnitude);
        Ok((log_band_match / rows).exp())
    }

    /// Returns the number of indexed items.
    pub fn len(&self) -> usize {
        self.entry_count
    }

    /// Returns `true` when no items are indexed.
    pub fn is_empty(&self) -> bool {
        self.entry_count == 0
    }

    /// Returns `true` when an id is currently indexed.
    pub fn contains_id(&self, id: &Id) -> bool {
        self.find_handle(id).is_some()
    }

    /// Inserts (or replaces) one signature by id.
    ///
    /// The index takes ownership of `id` without cloning it. Each band receives
    /// only a numeric handle. If the id already exists, its retained signature
    /// and band postings are replaced in place.
    ///
    /// The borrowed MinHash signature is copied once into compact index-owned
    /// storage so the index remains independent of subsequent caller changes.
    ///
    /// # Errors
    /// Returns [`SketchError::IncompatibleSketches`] when `signature` does not
    /// match the index dimensions or the hash family established by previously
    /// inserted signatures.
    pub fn insert(&mut self, id: Id, signature: &MinHash) -> Result<(), SketchError> {
        self.ensure_compatible(signature)?;
        if self.hash_family_seed.is_none() {
            self.hash_family_seed = Some(signature.hash_family_seed());
        }

        let id_hash = self.hash_id(&id);
        if let Some(handle) = self.find_handle_with_hash(&id, id_hash) {
            self.remove_handle_from_bands(handle);
            self.entries[handle.0]
                .as_mut()
                .expect("live handle must reference an entry")
                .signature = StoredSignature::from_minhash(signature);
            self.add_handle_to_bands(handle);
            return Ok(());
        }

        let entry = Entry {
            id,
            next_same_hash: self.id_heads.get(&id_hash).copied(),
            signature: StoredSignature::from_minhash(signature),
        };
        let handle = self.allocate_entry(entry);
        self.id_heads.insert(id_hash, handle);
        self.add_handle_to_bands(handle);
        self.entry_count += 1;
        Ok(())
    }

    /// Removes one indexed id.
    ///
    /// Returns `true` if the id existed.
    pub fn remove(&mut self, id: &Id) -> bool {
        let id_hash = self.hash_id(id);
        let Some(handle) = self.find_handle_with_hash(id, id_hash) else {
            return false;
        };

        self.remove_handle_from_bands(handle);
        self.unlink_id_handle(handle, id_hash);
        self.entries[handle.0] = None;
        self.free_entries.push(handle);
        self.entry_count -= 1;
        true
    }

    /// Returns candidate ids sharing at least one band with the query.
    ///
    /// Band collisions are deduplicated as numeric handles. The underlying ID
    /// is cloned only once for each unique value returned by this owned-result
    /// API, regardless of how many bands selected it.
    ///
    /// Candidate selection is probabilistic: an item with no matching band is
    /// absent even if it would have a high MinHash similarity estimate. Use
    /// [`Self::candidate_probability`] to inspect the configured ideal-model
    /// selection curve.
    ///
    /// # Errors
    /// Returns [`SketchError::IncompatibleSketches`] when the query dimensions
    /// or hash family mismatch this index.
    pub fn query_candidates(&self, query: &MinHash) -> Result<Vec<Id>, SketchError> {
        let handles = self.candidate_handles(query)?;
        Ok(handles
            .into_iter()
            .filter_map(|handle| self.entries.get(handle.0)?.as_ref())
            .map(|entry| entry.id.clone())
            .collect())
    }

    /// Returns top `k` candidates reranked by MinHash Jaccard estimate.
    ///
    /// Output tuples are `(id, estimated_jaccard)`, sorted descending. Candidate
    /// handles are deduplicated before signatures are scored. A bounded min-heap
    /// retains only the best `k` handles, so IDs are cloned only for returned
    /// results.
    ///
    /// This method returns the best retained estimates among LSH candidates; it
    /// is not a global top-`k` scan. An indexed item that shares no band with the
    /// query is not scored and cannot appear in the result.
    ///
    /// # Errors
    /// Returns [`SketchError::IncompatibleSketches`] when the query dimensions
    /// or hash family mismatch this index.
    pub fn query_top_k(&self, query: &MinHash, k: usize) -> Result<Vec<(Id, f64)>, SketchError> {
        if k == 0 {
            self.ensure_compatible(query)?;
            return Ok(Vec::new());
        }

        let handles = self.candidate_handles(query)?;
        if handles.is_empty() {
            return Ok(Vec::new());
        }

        let result_count = k.min(handles.len());
        let mut best = BinaryHeap::with_capacity(result_count);
        let family_seed = self
            .hash_family_seed
            .unwrap_or_else(|| query.hash_family_seed());

        for handle in handles {
            let entry = self.entries[handle.0]
                .as_ref()
                .expect("candidate handle must reference a live entry");
            let similarity = query.estimate_jaccard_signature(
                &entry.signature.values,
                entry.signature.observed_any,
                family_seed,
            )?;

            let candidate = ScoredHandle { handle, similarity };

            if best.len() < result_count {
                best.push(Reverse(candidate));
                continue;
            }

            let worst_retained = best
                .peek()
                .expect("top-k heap must be non-empty after reaching its capacity")
                .0;
            if candidate > worst_retained {
                *best
                    .peek_mut()
                    .expect("top-k heap must be non-empty after reaching its capacity") =
                    Reverse(candidate);
            }
        }

        let mut selected: Vec<_> = best
            .into_iter()
            .map(|Reverse(candidate)| candidate)
            .collect();
        selected.sort_unstable_by(|left, right| right.cmp(left));

        Ok(selected
            .into_iter()
            .map(|candidate| {
                let entry = self.entries[candidate.handle.0]
                    .as_ref()
                    .expect("selected handle must reference a live entry");
                (entry.id.clone(), candidate.similarity)
            })
            .collect())
    }

    /// Clears all index state.
    pub fn clear(&mut self) {
        self.hash_family_seed = None;
        self.entries.clear();
        self.free_entries.clear();
        self.id_heads.clear();
        self.entry_count = 0;
        for table in &mut self.tables {
            table.clear();
        }
    }

    fn ensure_compatible(&self, signature: &MinHash) -> Result<(), SketchError> {
        if signature.num_hashes() != self.num_hashes {
            return Err(SketchError::IncompatibleSketches(
                "signature num_hashes must match index num_hashes",
            ));
        }
        if self
            .hash_family_seed
            .is_some_and(|seed| seed != signature.hash_family_seed())
        {
            return Err(SketchError::IncompatibleSketches(
                "signature hash family must match index hash family",
            ));
        }
        Ok(())
    }

    fn candidate_handles(&self, query: &MinHash) -> Result<HashSet<EntryHandle>, SketchError> {
        self.ensure_compatible(query)?;

        let mut candidates = HashSet::new();
        for band in 0..self.bands {
            let band_hash = self.band_hash(query.signature(), band);
            if let Some(bucket) = self.tables[band].get(&band_hash) {
                candidates.extend(bucket.iter().copied());
            }
        }
        Ok(candidates)
    }

    fn add_handle_to_bands(&mut self, handle: EntryHandle) {
        for band in 0..self.bands {
            let band_hash = self.band_hash_for_handle(handle, band);
            self.tables[band]
                .entry(band_hash)
                .or_default()
                .insert(handle);
        }
    }

    fn remove_handle_from_bands(&mut self, handle: EntryHandle) {
        for band in 0..self.bands {
            let band_hash = self.band_hash_for_handle(handle, band);
            let should_remove_bucket =
                self.tables[band].get_mut(&band_hash).is_some_and(|bucket| {
                    bucket.remove(&handle);
                    bucket.is_empty()
                });
            if should_remove_bucket {
                self.tables[band].remove(&band_hash);
            }
        }
    }

    fn band_hash_for_handle(&self, handle: EntryHandle, band: usize) -> u64 {
        let signature = &self.entries[handle.0]
            .as_ref()
            .expect("live handle must reference an entry")
            .signature
            .values;
        self.band_hash(signature, band)
    }

    fn allocate_entry(&mut self, entry: Entry<Id>) -> EntryHandle {
        if let Some(handle) = self.free_entries.pop() {
            debug_assert!(self.entries[handle.0].is_none());
            self.entries[handle.0] = Some(entry);
            handle
        } else {
            let handle = EntryHandle(self.entries.len());
            self.entries.push(Some(entry));
            handle
        }
    }

    fn hash_id(&self, id: &Id) -> u64 {
        self.id_hash_builder.hash_one(id)
    }

    fn find_handle(&self, id: &Id) -> Option<EntryHandle> {
        self.find_handle_with_hash(id, self.hash_id(id))
    }

    fn find_handle_with_hash(&self, id: &Id, id_hash: u64) -> Option<EntryHandle> {
        let mut current = self.id_heads.get(&id_hash).copied();
        while let Some(handle) = current {
            let entry = self.entries.get(handle.0)?.as_ref()?;
            if &entry.id == id {
                return Some(handle);
            }
            current = entry.next_same_hash;
        }
        None
    }

    /// Unlinks a live handle from the equality-checked ID lookup chain.
    ///
    /// `id_hash` must be the hash used to find `handle` in `remove`. Forwarding
    /// that value avoids storing it per entry or hashing the user ID again.
    /// The entry stays live until its successor link has been read and repaired.
    fn unlink_id_handle(&mut self, handle: EntryHandle, id_hash: u64) {
        let entry = self.entries[handle.0]
            .as_ref()
            .expect("live handle must reference an entry");
        let target_next = entry.next_same_hash;
        let head = self.id_heads[&id_hash];

        if head == handle {
            if let Some(next) = target_next {
                self.id_heads.insert(id_hash, next);
            } else {
                self.id_heads.remove(&id_hash);
            }
            return;
        }

        let mut current = head;
        loop {
            let next = self.entries[current.0]
                .as_ref()
                .expect("ID chain handle must reference an entry")
                .next_same_hash
                .expect("target handle must be linked from its ID hash head");
            if next == handle {
                self.entries[current.0]
                    .as_mut()
                    .expect("ID chain handle must reference an entry")
                    .next_same_hash = target_next;
                return;
            }
            current = next;
        }
    }

    fn band_hash(&self, signature: &[u64], band: usize) -> u64 {
        let start = band * self.rows_per_band;
        let end = start + self.rows_per_band;
        seeded_hash64(&signature[start..end], self.band_seeds[band])
    }
}

/// Computes `ln(1 - exp(-t))` from `log_t = ln(t)` for positive finite `t`.
///
/// The caller owns input validation. For normal `t`, `ln(-expm1(-t))`
/// avoids subtracting nearly equal numbers and keeps the result nonpositive.
/// For subnormal `t`, use the original `log_t`: the difference between
/// `ln(1 - exp(-t))` and `ln(t)` is `-t/2 + O(t^2)`, far below the precision
/// of this logarithm. This also preserves the scale if exponentiating
/// `log_t` underflows completely. Switching only in the extreme tail avoids
/// cancellation between opposing logarithms in an ordinary-range correction.
///
/// This is a local application of stable logarithmic probability arithmetic;
/// related `log1p`/`expm1` formulations are analyzed in Mächler's note:
/// <https://cran.r-project.org/web/packages/Rmpfr/vignettes/log1mexp-note.pdf>.
/// It introduces no cached state and does not promise correctly rounded results
/// for every input or every platform's transcendental implementation.
fn log_one_minus_exp_negative_from_log(log_t: f64) -> f64 {
    let t = log_t.exp();
    if t < f64::MIN_POSITIVE {
        log_t
    } else {
        (-(-t).exp_m1()).ln()
    }
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::hash::{Hash, Hasher};
    use std::rc::Rc;

    use super::MinHashLshIndex;
    use crate::minhash::MinHash;

    #[derive(Debug)]
    struct CloneCountedId {
        value: u64,
        clones: Rc<Cell<usize>>,
    }

    impl Clone for CloneCountedId {
        fn clone(&self) -> Self {
            self.clones.set(self.clones.get() + 1);
            Self {
                value: self.value,
                clones: Rc::clone(&self.clones),
            }
        }
    }

    impl PartialEq for CloneCountedId {
        fn eq(&self, other: &Self) -> bool {
            self.value == other.value
        }
    }

    impl Eq for CloneCountedId {}

    impl Hash for CloneCountedId {
        fn hash<H: Hasher>(&self, state: &mut H) {
            self.value.hash(state);
        }
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct CollidingId(u64);

    impl Hash for CollidingId {
        fn hash<H: Hasher>(&self, state: &mut H) {
            0_u8.hash(state);
        }
    }

    fn signature_for_range(start: u64, end: u64, num_hashes: usize) -> MinHash {
        let mut signature = MinHash::new(num_hashes).unwrap();
        for value in start..end {
            signature.add(&value);
        }
        signature
    }

    #[test]
    fn constructor_validates_parameters() {
        assert!(MinHashLshIndex::<u64>::new(0, 8).is_err());
        assert!(MinHashLshIndex::<u64>::new(64, 0).is_err());
        assert!(MinHashLshIndex::<u64>::new(8, 16).is_err());
        assert!(MinHashLshIndex::<u64>::new(63, 8).is_err());
        assert!(MinHashLshIndex::<u64>::new(usize::MAX, 1).is_err());
        assert!(MinHashLshIndex::<u64>::new(usize::MAX, usize::MAX).is_err());
        assert!(MinHashLshIndex::<u64>::new(64, 8).is_ok());
    }

    #[test]
    fn shape_accessors_report_configuration() {
        let index = MinHashLshIndex::<u64>::new(96, 12).unwrap();
        assert_eq!(index.num_hashes(), 96);
        assert_eq!(index.bands(), 12);
        assert_eq!(index.rows_per_band(), 8);
    }

    #[test]
    fn candidate_probability_matches_the_classical_banding_formula() {
        let index = MinHashLshIndex::<u64>::new(128, 32).unwrap();
        let similarity = 0.5_f64;
        let expected = 1.0 - (1.0 - similarity.powi(4)).powi(32);
        let actual = index.candidate_probability(similarity).unwrap();

        assert!((actual - expected).abs() < 1e-15);
        assert_eq!(index.candidate_probability(0.0).unwrap(), 0.0);
        assert_eq!(index.candidate_probability(1.0).unwrap(), 1.0);
    }

    #[test]
    fn candidate_probability_inverse_roundtrips_probabilities() {
        let index = MinHashLshIndex::<u64>::new(128, 32).unwrap();

        // Round-trip probabilities rather than high similarities: near the top
        // of this steep S-curve, many distinguishable similarities necessarily
        // round to the same `f64` probability close to one.
        for probability in [0.0_f64, 0.001, 0.1, 0.5, 0.9, 0.99, 1.0] {
            let similarity = index
                .similarity_for_candidate_probability(probability)
                .unwrap();
            let recovered = index.candidate_probability(similarity).unwrap();

            assert!(
                (recovered - probability).abs() < 1e-12,
                "similarity={similarity} probability={probability} recovered={recovered}"
            );
        }
    }

    #[test]
    fn candidate_probability_helpers_validate_inputs() {
        let index = MinHashLshIndex::<u64>::new(128, 32).unwrap();

        for invalid in [-f64::EPSILON, 1.0 + f64::EPSILON, f64::NAN, f64::INFINITY] {
            assert!(index.candidate_probability(invalid).is_err());
            assert!(index.similarity_for_candidate_probability(invalid).is_err());
        }
    }

    #[test]
    fn candidate_probability_preserves_final_subnormal_band_amplification() {
        let index = MinHashLshIndex::<u64>::new(128, 32).unwrap();
        // Independently rounded from the exact binary input using 900-digit
        // Decimal arithmetic: s^4 underflows, but 32*s^4 rounds to six units
        // of the smallest subnormal. The full model has the same rounded value.
        assert_eq!(index.candidate_probability(1e-81).unwrap().to_bits(), 6);
        assert_eq!(index.candidate_probability(1e-100).unwrap(), 0.0);
    }

    #[test]
    fn inverse_probability_preserves_subnormal_inputs_before_taking_the_root() {
        let minimum = f64::from_bits(1);
        for (width, bands, expected) in [
            (4, 2, 1.5717277847026288e-162),
            (8, 4, 1.1113793747425387e-162),
            (128, 32, 6.268428400928395e-82),
        ] {
            let index = MinHashLshIndex::<u64>::new(width, bands).unwrap();
            let actual = index.similarity_for_candidate_probability(minimum).unwrap();
            // References are independent of the implementation. Relative
            // error makes a zero result fail regardless of the tiny magnitude.
            assert!(actual.is_normal());
            assert!((actual / expected - 1.0).abs() < 1e-13);
        }
    }

    #[test]
    fn probability_single_band_and_single_row_boundaries_remain_exact() {
        let minimum = f64::from_bits(1);
        for (width, bands) in [(1, 1), (4, 1), (2, 2), (128, 32)] {
            let index = MinHashLshIndex::<u64>::new(width, bands).unwrap();
            for endpoint in [0.0, -0.0, 1.0] {
                assert_eq!(index.candidate_probability(endpoint).unwrap(), endpoint);
                assert_eq!(
                    index
                        .similarity_for_candidate_probability(endpoint)
                        .unwrap(),
                    endpoint
                );
            }
        }
        let identity = MinHashLshIndex::<u64>::new(1, 1).unwrap();
        for input in [minimum, f64::MIN_POSITIVE, 0.25, 0.5, 1.0] {
            assert_eq!(identity.candidate_probability(input).unwrap(), input);
            assert_eq!(
                identity
                    .similarity_for_candidate_probability(input)
                    .unwrap(),
                input
            );
        }
        let single_row = MinHashLshIndex::<u64>::new(32, 32).unwrap();
        assert_eq!(
            single_row.candidate_probability(minimum).unwrap().to_bits(),
            32
        );
        // Here there is no amplifying root: the final inverse itself is below
        // the representable range and legitimately rounds to zero.
        assert_eq!(
            single_row
                .similarity_for_candidate_probability(minimum)
                .unwrap(),
            0.0
        );
    }

    #[test]
    fn probability_models_are_monotone_across_the_normal_tail_boundary() {
        for bands in [2, 3, 7, 32, 63] {
            for rows in [2, 3, 4, 5, 16, 64, 128, 1024] {
                let index = MinHashLshIndex::<u64>::new(bands * rows, bands).unwrap();
                let similarity_center = f64::MIN_POSITIVE.powf(1.0 / rows as f64);
                let probability_center = -(-(bands as f64) * f64::MIN_POSITIVE).exp_m1();
                for inverse in [false, true] {
                    let center = if inverse {
                        probability_center
                    } else {
                        similarity_center
                    };
                    let mut previous = 0.0;
                    for bits in center.to_bits() - 1024..=center.to_bits() + 1024 {
                        let input = f64::from_bits(bits);
                        let actual = if inverse {
                            index.similarity_for_candidate_probability(input).unwrap()
                        } else {
                            index.candidate_probability(input).unwrap()
                        };
                        assert!(actual.is_finite() && (0.0..=1.0).contains(&actual));
                        assert!(
                            actual >= previous,
                            "bands={bands} rows={rows} inverse={inverse} input={input:e} previous={previous:e} actual={actual:e}"
                        );
                        previous = actual;
                    }
                }
            }
        }
    }

    #[test]
    fn insert_rejects_incompatible_signature() {
        let mut index = MinHashLshIndex::<u64>::new(64, 8).unwrap();
        let signature = signature_for_range(0, 1_000, 32);
        assert!(index.insert(1, &signature).is_err());
    }

    #[test]
    fn queries_reject_incompatible_signature() {
        let index = MinHashLshIndex::<u64>::new(64, 8).unwrap();
        let query = signature_for_range(0, 1_000, 32);
        assert!(index.query_candidates(&query).is_err());
        assert!(index.query_top_k(&query, 5).is_err());
    }

    #[test]
    fn insert_and_contains_id_work() {
        let mut index = MinHashLshIndex::<u64>::new(64, 8).unwrap();
        let signature = signature_for_range(0, 1_000, 64);
        index.insert(10, &signature).unwrap();
        assert!(index.contains_id(&10));
        assert_eq!(index.len(), 1);
        assert!(!index.is_empty());
    }

    #[test]
    fn insertion_does_not_clone_id_and_query_clones_each_candidate_once() {
        let clones = Rc::new(Cell::new(0));
        let id = CloneCountedId {
            value: 7,
            clones: Rc::clone(&clones),
        };
        let signature = signature_for_range(0, 1_000, 64);
        let mut index = MinHashLshIndex::new(64, 8).unwrap();

        index.insert(id, &signature).unwrap();
        assert_eq!(clones.get(), 0, "insertion must move the canonical ID");

        let candidates = index.query_candidates(&signature).unwrap();
        assert_eq!(candidates.len(), 1);
        assert_eq!(clones.get(), 1, "deduplicate handles before cloning IDs");
    }

    #[test]
    fn query_top_k_clones_only_returned_ids() {
        let clones = Rc::new(Cell::new(0));
        let signature = signature_for_range(0, 1_000, 64);
        let mut index = MinHashLshIndex::new(64, 8).unwrap();

        for value in 0..100 {
            index
                .insert(
                    CloneCountedId {
                        value,
                        clones: Rc::clone(&clones),
                    },
                    &signature,
                )
                .unwrap();
        }
        assert_eq!(clones.get(), 0, "insertion must move every canonical ID");

        let top = index.query_top_k(&signature, 3).unwrap();
        assert_eq!(top.len(), 3);
        assert_eq!(
            clones.get(),
            3,
            "top-k lookup must clone only the IDs it returns"
        );
    }

    #[test]
    fn cloning_index_clones_each_canonical_id_once() {
        let clones = Rc::new(Cell::new(0));
        let id = CloneCountedId {
            value: 7,
            clones: Rc::clone(&clones),
        };
        let signature = signature_for_range(0, 1_000, 64);
        let mut index = MinHashLshIndex::new(64, 8).unwrap();
        index.insert(id, &signature).unwrap();

        let cloned = index.clone();
        assert_eq!(cloned.len(), 1);
        assert_eq!(clones.get(), 1);

        let lookup = CloneCountedId {
            value: 7,
            clones: Rc::clone(&clones),
        };
        assert!(cloned.contains_id(&lookup));
        assert_eq!(clones.get(), 1, "lookup must use the cloned hash state");
    }

    #[test]
    fn randomized_id_hash_collisions_are_resolved_by_equality() {
        let signature_a = signature_for_range(0, 1_000, 64);
        let signature_b = signature_for_range(10_000, 11_000, 64);
        let mut index = MinHashLshIndex::new(64, 8).unwrap();

        index.insert(CollidingId(1), &signature_a).unwrap();
        index.insert(CollidingId(2), &signature_b).unwrap();
        assert!(index.contains_id(&CollidingId(1)));
        assert!(index.contains_id(&CollidingId(2)));

        assert!(index.remove(&CollidingId(1)));
        assert!(!index.contains_id(&CollidingId(1)));
        assert!(index.contains_id(&CollidingId(2)));

        let candidates = index.query_candidates(&signature_b).unwrap();
        assert!(candidates.contains(&CollidingId(2)));
        assert!(!candidates.contains(&CollidingId(1)));
    }

    #[test]
    fn collision_chain_removal_works_in_every_order() {
        let signature = signature_for_range(0, 100, 16);
        let mut permutations = 0;
        // Enumerate all 24 permutations, exercising head, middle, tail, and
        // final-chain removal without relying on a randomized hash collision.
        for encoded in 0..256_u64 {
            let order = [0, 1, 2, 3].map(|position| (encoded >> (position * 2)) & 3);
            if (0..4).any(|position| order[..position].contains(&order[position])) {
                continue;
            }
            permutations += 1;
            let mut index = MinHashLshIndex::new(16, 4).unwrap();
            let mut remaining = vec![0, 1, 2, 3];
            for value in &remaining {
                index.insert(CollidingId(*value), &signature).unwrap();
            }

            for removed in order {
                assert!(index.remove(&CollidingId(removed)));
                assert!(!index.remove(&CollidingId(removed)));
                remaining.retain(|value| *value != removed);
                for value in 0..4 {
                    assert_eq!(
                        index.contains_id(&CollidingId(value)),
                        remaining.contains(&value)
                    );
                }
                let mut candidates: Vec<_> = index
                    .query_candidates(&signature)
                    .unwrap()
                    .into_iter()
                    .map(|id| id.0)
                    .collect();
                candidates.sort_unstable();
                assert_eq!(candidates, remaining);
                let ranked = index.query_top_k(&signature, 4).unwrap();
                assert_eq!(ranked.len(), remaining.len());
                assert!(
                    ranked
                        .iter()
                        .all(|(id, score)| { remaining.contains(&id.0) && *score == 1.0 })
                );
            }
            assert!(index.id_heads.is_empty());
            assert!(index.tables.iter().all(|table| table.is_empty()));
        }
        assert_eq!(permutations, 24);
    }

    #[test]
    fn removal_hashes_the_lookup_id_once_and_never_rehashes_the_stored_id() {
        #[derive(Debug, Clone)]
        struct HashCountedId {
            value: u64,
            calls: Rc<Cell<usize>>,
        }

        impl PartialEq for HashCountedId {
            fn eq(&self, other: &Self) -> bool {
                self.value == other.value
            }
        }

        impl Eq for HashCountedId {}

        impl Hash for HashCountedId {
            fn hash<H: Hasher>(&self, state: &mut H) {
                self.calls.set(self.calls.get() + 1);
                0_u8.hash(state);
            }
        }

        let stored_calls = Rc::new(Cell::new(0));
        let lookup_calls = Rc::new(Cell::new(0));
        let signature = signature_for_range(0, 100, 16);
        let mut index = MinHashLshIndex::new(16, 4).unwrap();
        for value in 0..4 {
            index
                .insert(
                    HashCountedId {
                        value,
                        calls: Rc::clone(&stored_calls),
                    },
                    &signature,
                )
                .unwrap();
        }
        assert_eq!(stored_calls.get(), 4);

        // Middle, head, tail, missing, and final live entry in one hash chain.
        for (value, existed) in [(2, true), (3, true), (0, true), (9, false), (1, true)] {
            lookup_calls.set(0);
            assert_eq!(
                index.remove(&HashCountedId {
                    value,
                    calls: Rc::clone(&lookup_calls),
                }),
                existed
            );
            assert_eq!(lookup_calls.get(), 1);
            assert_eq!(stored_calls.get(), 4);
        }
    }

    #[test]
    fn remove_existing_and_missing_ids() {
        let mut index = MinHashLshIndex::<u64>::new(64, 8).unwrap();
        let signature = signature_for_range(0, 1_000, 64);
        index.insert(10, &signature).unwrap();

        assert!(index.remove(&10));
        assert!(!index.remove(&10));
        assert!(index.is_empty());
    }

    #[test]
    fn removal_clears_postings_before_handle_reuse() {
        let first = signature_for_range(0, 1_000, 64);
        let second = signature_for_range(10_000, 11_000, 64);
        let mut index = MinHashLshIndex::new(64, 8).unwrap();

        index.insert(1_u64, &first).unwrap();
        let handle = index.find_handle(&1).unwrap();
        assert!(index.remove(&1));
        assert!(
            index
                .tables
                .iter()
                .all(|table| table.values().all(|bucket| !bucket.contains(&handle)))
        );

        index.insert(2_u64, &second).unwrap();
        assert_eq!(index.find_handle(&2), Some(handle));
        assert!(!index.query_candidates(&first).unwrap().contains(&1));
        assert!(index.query_candidates(&second).unwrap().contains(&2));
    }

    #[test]
    fn insert_replaces_existing_id_signature() {
        let mut index = MinHashLshIndex::<u64>::new(128, 32).unwrap();

        let first = signature_for_range(0, 10_000, 128);
        let second = signature_for_range(20_000, 30_000, 128);
        index.insert(7, &first).unwrap();
        index.insert(7, &second).unwrap();

        assert_eq!(index.len(), 1);
        let top = index.query_top_k(&second, 1).unwrap();
        assert_eq!(top.len(), 1);
        assert_eq!(top[0].0, 7);
        assert!(top[0].1 > 0.9);
    }

    #[test]
    fn query_candidates_finds_high_overlap_item() {
        let mut index = MinHashLshIndex::<u64>::new(128, 32).unwrap();

        let doc_a = signature_for_range(0, 10_000, 128);
        let doc_b = signature_for_range(30_000, 40_000, 128);
        let query = signature_for_range(1_000, 11_000, 128);

        index.insert(1, &doc_a).unwrap();
        index.insert(2, &doc_b).unwrap();

        let candidates = index.query_candidates(&query).unwrap();
        assert!(candidates.contains(&1));
    }

    #[test]
    fn query_top_k_returns_descending_scores() {
        let mut index = MinHashLshIndex::<u64>::new(128, 32).unwrap();

        let very_close = signature_for_range(0, 10_000, 128);
        let medium = signature_for_range(5_000, 15_000, 128);
        let far = signature_for_range(25_000, 35_000, 128);
        let query = signature_for_range(500, 10_500, 128);

        index.insert(1, &very_close).unwrap();
        index.insert(2, &medium).unwrap();
        index.insert(3, &far).unwrap();

        let top = index.query_top_k(&query, 3).unwrap();
        assert!(!top.is_empty());
        for pair in top.windows(2) {
            assert!(pair[0].1 >= pair[1].1);
        }
        assert_eq!(top[0].0, 1);
    }

    #[test]
    fn query_top_k_heap_keeps_the_best_candidates() {
        let query = signature_for_range(0, 1_000, 64);
        let signatures = vec![
            (1_u64, signature_for_range(0, 1_000, 64)),
            (2, signature_for_range(0, 1_100, 64)),
            (3, signature_for_range(0, 1_200, 64)),
            (4, signature_for_range(0, 1_300, 64)),
            (5, signature_for_range(0, 1_400, 64)),
        ];
        // One row per band makes every signature in this nested family a
        // candidate while leaving the retained MinHash scores distinct.
        let mut index = MinHashLshIndex::new(64, 64).unwrap();
        for (id, signature) in &signatures {
            index.insert(*id, signature).unwrap();
        }

        let candidates = index.query_candidates(&query).unwrap();
        assert_eq!(candidates.len(), signatures.len());

        let mut expected: Vec<_> = signatures
            .iter()
            .filter(|(id, _)| candidates.contains(id))
            .map(|(id, signature)| (*id, signature.estimate_jaccard(&query).unwrap()))
            .collect();
        expected.sort_unstable_by(|left, right| {
            right
                .1
                .total_cmp(&left.1)
                .then_with(|| right.0.cmp(&left.0))
        });
        expected.truncate(2);

        assert_eq!(index.query_top_k(&query, 2).unwrap(), expected);
    }

    #[test]
    fn query_top_k_respects_k_and_zero_k() {
        let mut index = MinHashLshIndex::<u64>::new(64, 8).unwrap();
        let signature = signature_for_range(0, 10_000, 64);
        index.insert(1, &signature).unwrap();
        index.insert(2, &signature).unwrap();

        assert!(index.query_top_k(&signature, 0).unwrap().is_empty());
        assert!(index.query_top_k(&signature, 1).unwrap().len() <= 1);
    }

    #[test]
    fn identical_signature_is_always_a_candidate() {
        let mut index = MinHashLshIndex::<u64>::new(64, 8).unwrap();
        let signature = signature_for_range(0, 5_000, 64);
        index.insert(42, &signature).unwrap();

        let candidates = index.query_candidates(&signature).unwrap();
        assert!(candidates.contains(&42));
    }

    #[test]
    fn clear_resets_index_state() {
        let mut index = MinHashLshIndex::<u64>::new(64, 8).unwrap();
        let signature = signature_for_range(0, 2_000, 64);

        index.insert(1, &signature).unwrap();
        index.insert(2, &signature).unwrap();
        assert_eq!(index.len(), 2);

        index.clear();
        assert!(index.is_empty());
        assert_eq!(index.len(), 0);
        assert!(index.entries.is_empty());
        assert!(index.free_entries.is_empty());
        assert!(index.id_heads.is_empty());
        assert!(index.hash_family_seed.is_none());
        assert!(index.query_candidates(&signature).unwrap().is_empty());
    }
}
