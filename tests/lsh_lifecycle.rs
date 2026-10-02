//! Public LSH lifecycle checks with lawful constant-hash IDs and owned signatures.
//! Candidate references compare contiguous signature bands in a fixed corpus;
//! they do not repeat the index's hash, arena, free-list, or collision-chain code.
//! Ranking checks allow arbitrary ID choices among tied scores.

use std::cell::Cell;
use std::collections::{BTreeMap, BTreeSet};
use std::hash::{Hash, Hasher};
use std::rc::Rc;

use sketches::minhash::MinHash;
use sketches::minhash_lsh_index::MinHashLshIndex;

/// Equality and hashing use only the logical key. The origin tag distinguishes
/// the first canonical owner from later equal IDs supplied for replacement.
#[derive(Debug)]
struct OwnedId {
    key: u64,
    origin: u64,
    clones: Rc<Cell<usize>>,
}

impl PartialEq for OwnedId {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
    }
}

impl Eq for OwnedId {}

impl Hash for OwnedId {
    fn hash<H: Hasher>(&self, state: &mut H) {
        0_u8.hash(state);
    }
}

impl Clone for OwnedId {
    fn clone(&self) -> Self {
        self.clones.set(self.clones.get() + 1);
        Self {
            key: self.key,
            origin: self.origin,
            clones: Rc::clone(&self.clones),
        }
    }
}

/// Independent logical contents, with no LSH handles or posting state.
#[derive(Clone)]
struct Record {
    origin: u64,
    signature: MinHash,
}

/// Empty and overlapping nonempty sets, built through ordinary MinHash updates.
fn corpus(width: usize) -> Vec<MinHash> {
    let mut result = vec![MinHash::new(width).unwrap()];
    for start in 0..12_u64 {
        let mut signature = MinHash::new(width).unwrap();
        for item in start * 7..start * 7 + 60 {
            signature.add(&item);
        }
        result.push(signature);
    }
    result
}

/// Checks count, canonical ownership, candidate deduplication, exact retained
/// MinHash scores, top-k selection, and clones confined to returned IDs.
fn check_index(
    index: &MinHashLshIndex<OwnedId>,
    reference: &BTreeMap<u64, Record>,
    clones: &Rc<Cell<usize>>,
    signatures: &[MinHash],
) {
    assert_eq!(index.len(), reference.len());
    assert_eq!(index.is_empty(), reference.is_empty());
    for key in (0..18).chain([u64::MAX]) {
        assert_eq!(
            index.contains_id(&OwnedId {
                key,
                origin: u64::MAX,
                clones: Rc::clone(clones),
            }),
            reference.contains_key(&key)
        );
    }
    for query in [0, 1, 6, 12].map(|position| &signatures[position]) {
        // Equal bands necessarily hash alike. This bounded deterministic corpus
        // has no additional collisions between unequal bands; this fixture does
        // not promise collision-free band hashing for arbitrary caller inputs.
        let expected: Vec<_> = reference
            .iter()
            .filter(|(_, record)| {
                query
                    .signature()
                    .chunks_exact(index.rows_per_band())
                    .zip(
                        record
                            .signature
                            .signature()
                            .chunks_exact(index.rows_per_band()),
                    )
                    .any(|(left, right)| left == right)
            })
            .map(|(&key, _)| key)
            .collect();
        let before = clones.get();
        let candidates = index.query_candidates(query).unwrap();
        assert_eq!(clones.get() - before, candidates.len());
        for id in &candidates {
            assert_eq!(id.origin, reference[&id.key].origin);
        }
        let mut keys: Vec<_> = candidates.iter().map(|id| id.key).collect();
        keys.sort_unstable();
        assert_eq!(keys, expected);

        for k in [0, 1, 3, usize::MAX] {
            let before = clones.get();
            let ranked = index.query_top_k(query, k).unwrap();
            assert_eq!(clones.get() - before, ranked.len());
            assert_eq!(ranked.len(), expected.len().min(k));
            let selected: BTreeSet<_> = ranked.iter().map(|(id, _)| id.key).collect();
            assert_eq!(selected.len(), ranked.len());
            assert!(ranked.windows(2).all(|pair| pair[0].1 >= pair[1].1));
            for (id, score) in &ranked {
                assert!(expected.contains(&id.key));
                assert_eq!(id.origin, reference[&id.key].origin);
                assert_eq!(
                    *score,
                    query
                        .estimate_jaccard(&reference[&id.key].signature)
                        .unwrap()
                );
            }
            if let Some((_, minimum)) = ranked.last() {
                for key in expected.iter().filter(|key| !selected.contains(key)) {
                    assert!(query.estimate_jaccard(&reference[key].signature).unwrap() <= *minimum);
                }
            }
        }
    }
}

#[test]
fn colliding_id_lifecycles_match_independent_public_oracles() {
    for (width, bands) in [(16, 1), (16, 4), (18, 6), (16, 16)] {
        let signatures = corpus(width);
        let clones = Rc::new(Cell::new(0));
        let mut index = MinHashLshIndex::new(width, bands).unwrap();
        let mut reference = BTreeMap::<u64, Record>::new();
        for operation in 0..1024_u64 {
            let key = (operation * 11 + operation / 17) % 17;
            let id = OwnedId {
                key,
                origin: operation,
                clones: Rc::clone(&clones),
            };
            match operation % 31 {
                0..=19 => {
                    let mut signature =
                        signatures[(operation / 3) as usize % signatures.len()].clone();
                    let before = clones.get();
                    index.insert(id, &signature).unwrap();
                    assert_eq!(
                        clones.get(),
                        before,
                        "insertion moves rather than clones IDs"
                    );
                    reference
                        .entry(key)
                        .and_modify(|record| record.signature = signature.clone())
                        .or_insert_with(|| Record {
                            origin: operation,
                            signature: signature.clone(),
                        });
                    // The index must retain an independent copy, including the
                    // empty/nonempty flag, after the caller mutates its input.
                    signature.clear();
                    signature.add(&u64::MAX);
                }
                20..=25 => {
                    let before = clones.get();
                    assert_eq!(index.remove(&id), reference.remove(&key).is_some());
                    assert_eq!(clones.get(), before);
                }
                26 => {
                    let before = clones.get();
                    let mut copied = index.clone();
                    assert_eq!(clones.get() - before, reference.len());
                    let mut copied_reference = reference.clone();
                    assert_eq!(copied.remove(&id), copied_reference.remove(&key).is_some());
                    copied
                        .insert(
                            OwnedId {
                                key: 17,
                                origin: operation,
                                clones: Rc::clone(&clones),
                            },
                            &signatures[6],
                        )
                        .unwrap();
                    copied_reference.insert(
                        17,
                        Record {
                            origin: operation,
                            signature: signatures[6].clone(),
                        },
                    );
                    check_index(&copied, &copied_reference, &clones, &signatures);
                }
                27 => {
                    let incompatible = MinHash::new(width + 1).unwrap();
                    let before = clones.get();
                    assert!(index.insert(id, &incompatible).is_err());
                    assert_eq!(clones.get(), before);
                }
                28 => {
                    assert!(!index.remove(&OwnedId {
                        key: u64::MAX,
                        origin: 0,
                        clones: Rc::clone(&clones),
                    }));
                }
                29 => {
                    for &key in reference.keys() {
                        assert!(index.remove(&OwnedId {
                            key,
                            origin: 0,
                            clones: Rc::clone(&clones),
                        }));
                    }
                    reference.clear();
                }
                _ => {
                    index.clear();
                    reference.clear();
                }
            }
            check_index(&index, &reference, &clones, &signatures);
        }
    }
}
