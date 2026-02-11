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
//! MinHash banding LSH index for approximate nearest-neighbor candidate search.
//!
//! The index splits a MinHash signature into bands and hashes each band into a
//! table bucket. A query retrieves candidates that collide in at least one band.

use std::collections::{HashMap, HashSet};
use std::hash::Hash;

use crate::{SketchError, seeded_hash64, splitmix64};
use crate::minhash::MinHash;

/// Locality-Sensitive Hashing index built on MinHash signatures.
///
/// # Example
/// ```rust
/// use sketches::lsh_minhash::MinHashLshIndex;
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
#[derive(Debug, Clone)]
pub struct MinHashLshIndex<Id>
where
    Id: Eq + Hash + Clone,
{
    num_hashes: usize,
    bands: usize,
    rows_per_band: usize,
    band_seeds: Vec<u64>,
    tables: Vec<HashMap<u64, HashSet<Id>>>,
    signatures: HashMap<Id, MinHash>,
}

impl<Id> MinHashLshIndex<Id>
where
    Id: Eq + Hash + Clone,
{
    /// Creates an LSH index from signature width and number of bands.
    ///
    /// `num_hashes` must be divisible by `bands`.
    ///
    /// # Errors
    /// Returns [`SketchError::InvalidParameter`] for invalid dimensions.
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
        if num_hashes % bands != 0 {
            return Err(SketchError::InvalidParameter(
                "num_hashes must be divisible by bands",
            ));
        }

        let rows_per_band = num_hashes / bands;
        if rows_per_band == 0 {
            return Err(SketchError::InvalidParameter(
                "rows_per_band must be greater than zero",
            ));
        }

        let band_seeds = (0..bands)
            .map(|band| splitmix64((band as u64).wrapping_add(0xA076_1D64_78BD_642F)))
            .collect();

        Ok(Self {
            num_hashes,
            bands,
            rows_per_band,
            band_seeds,
            tables: vec![HashMap::new(); bands],
            signatures: HashMap::new(),
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

    /// Returns the number of indexed items.
    pub fn len(&self) -> usize {
        self.signatures.len()
    }

    /// Returns `true` when no items are indexed.
    pub fn is_empty(&self) -> bool {
        self.signatures.is_empty()
    }

    /// Returns `true` when an id is currently indexed.
    pub fn contains_id(&self, id: &Id) -> bool {
        self.signatures.contains_key(id)
    }

    /// Inserts (or replaces) one signature by id.
    ///
    /// If the id already exists, its old signature is removed and replaced.
    ///
    /// # Errors
    /// Returns [`SketchError::IncompatibleSketches`] when `signature` does not
    /// match index dimensions.
    pub fn insert(&mut self, id: Id, signature: &MinHash) -> Result<(), SketchError> {
        self.ensure_compatible(signature)?;

        self.remove(&id);

        for band in 0..self.bands {
            let band_hash = self.band_hash(signature.signature(), band);
            self.tables[band].entry(band_hash).or_default().insert(id.clone());
        }

        self.signatures.insert(id, signature.clone());
        Ok(())
    }

    /// Removes one indexed id.
    ///
    /// Returns `true` if the id existed.
    pub fn remove(&mut self, id: &Id) -> bool {
        let Some(signature) = self.signatures.remove(id) else {
            return false;
        };

        for band in 0..self.bands {
            let band_hash = self.band_hash(signature.signature(), band);

            let mut should_remove_bucket = false;
            if let Some(bucket) = self.tables[band].get_mut(&band_hash) {
                bucket.remove(id);
                should_remove_bucket = bucket.is_empty();
            }

            if should_remove_bucket {
                self.tables[band].remove(&band_hash);
            }
        }

        true
    }

    /// Returns candidate ids sharing at least one band with the query.
    ///
    /// # Errors
    /// Returns [`SketchError::IncompatibleSketches`] when query dimensions
    /// mismatch this index.
    pub fn query_candidates(&self, query: &MinHash) -> Result<Vec<Id>, SketchError> {
        self.ensure_compatible(query)?;

        let mut candidates = HashSet::new();
        for band in 0..self.bands {
            let band_hash = self.band_hash(query.signature(), band);
            if let Some(bucket) = self.tables[band].get(&band_hash) {
                candidates.extend(bucket.iter().cloned());
            }
        }

        Ok(candidates.into_iter().collect())
    }

    /// Returns top `k` candidates reranked by MinHash Jaccard estimate.
    ///
    /// Output tuples are `(id, estimated_jaccard)`, sorted descending.
    ///
    /// # Errors
    /// Returns [`SketchError::IncompatibleSketches`] when query dimensions
    /// mismatch this index.
    pub fn query_top_k(&self, query: &MinHash, k: usize) -> Result<Vec<(Id, f64)>, SketchError> {
        self.ensure_compatible(query)?;
        if k == 0 {
            return Ok(Vec::new());
        }

        let mut scored = Vec::new();
        for id in self.query_candidates(query)? {
            if let Some(signature) = self.signatures.get(&id) {
                let similarity = signature.estimate_jaccard(query)?;
                scored.push((id, similarity));
            }
        }

        scored.sort_unstable_by(|left, right| right.1.total_cmp(&left.1));
        scored.truncate(k.min(scored.len()));
        Ok(scored)
    }

    /// Clears all index state.
    pub fn clear(&mut self) {
        self.signatures.clear();
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
        Ok(())
    }

    fn band_hash(&self, signature: &[u64], band: usize) -> u64 {
        let start = band * self.rows_per_band;
        let end = start + self.rows_per_band;
        seeded_hash64(&signature[start..end], self.band_seeds[band])
    }
}

#[cfg(test)]
mod tests {
    use super::MinHashLshIndex;
    use crate::minhash::MinHash;

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
        assert!(MinHashLshIndex::<u64>::new(63, 8).is_err());
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
    fn remove_existing_and_missing_ids() {
        let mut index = MinHashLshIndex::<u64>::new(64, 8).unwrap();
        let signature = signature_for_range(0, 1_000, 64);
        index.insert(10, &signature).unwrap();

        assert!(index.remove(&10));
        assert!(!index.remove(&10));
        assert!(index.is_empty());
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
        assert!(index.query_candidates(&signature).unwrap().is_empty());
    }
}
