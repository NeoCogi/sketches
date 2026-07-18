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
//! Probabilistic data structures for scalable approximate analytics.
//!
//! The crate currently exposes:
//! - [`minmax_sketch::MinMaxSketch`] for approximate frequency estimation.
//! - [`hyperloglog::HyperLogLog`] for approximate cardinality estimation.
//! - [`jacard`] for approximate set overlap/Jaccard helpers on HyperLogLog.
//! - [`bloom_filter::BloomFilter`] for approximate set membership checks.
//! - [`count_sketch::CountSketch`] for signed approximate frequency estimation.
//! - [`space_saving::SpaceSaving`] for approximate heavy hitters.
//! - [`kll::KllSketch`] for approximate quantiles.
//! - [`tdigest::TDigest`] for tail-friendly quantiles.
//! - [`cuckoo_filter::CuckooFilter`] for membership with deletions.
//! - [`minhash::MinHash`] for approximate Jaccard estimation.
//! - [`lsh_minhash::MinHashLshIndex`] for approximate nearest-neighbor lookup.
//! - [`reservoir_sampling::ReservoirSampling`] for uniform stream sampling.

use core::fmt;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

pub mod bloom_filter;
pub mod count_sketch;
pub mod cuckoo_filter;
pub mod hyperloglog;
pub mod jacard;
pub mod kll;
pub mod lsh_minhash;
pub mod minhash;
pub mod minmax_sketch;
pub mod reservoir_sampling;
pub mod space_saving;
pub mod tdigest;

/// Errors returned by sketch constructors and merge operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SketchError {
    /// Returned when a constructor receives an invalid argument.
    InvalidParameter(&'static str),
    /// Returned when combining two sketches that are not shape-compatible.
    IncompatibleSketches(&'static str),
}

impl fmt::Display for SketchError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidParameter(message) => write!(f, "invalid parameter: {message}"),
            Self::IncompatibleSketches(message) => write!(f, "incompatible sketches: {message}"),
        }
    }
}

impl std::error::Error for SketchError {}

/// Computes a deterministic 64-bit hash using an item and a fixed seed.
pub(crate) fn seeded_hash64<T: Hash + ?Sized>(item: &T, seed: u64) -> u64 {
    let mut hasher = DefaultHasher::new();
    seed.hash(&mut hasher);
    item.hash(&mut hasher);
    hasher.finish()
}

/// SplitMix64 mixer used for deriving independent row/hash seeds.
pub(crate) fn splitmix64(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
    x = (x ^ (x >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    x ^ (x >> 31)
}

#[cfg(test)]
mod quantile_contract_tests {
    use crate::kll::KllSketch;
    use crate::tdigest::TDigest;

    type QuantileCase<'a> = (&'a [f64], &'a [(f64, f64)]);

    #[test]
    fn kll_and_tdigest_share_the_exact_small_sample_convention() {
        let cases: &[QuantileCase<'_>] = &[
            (&[7.0], &[(0.0, 7.0), (0.5, 7.0), (1.0, 7.0)]),
            (
                &[0.0, 10.0],
                &[
                    (0.0, 0.0),
                    (0.5 - f64::EPSILON, 0.0),
                    (0.5, 10.0),
                    (0.5 + f64::EPSILON, 10.0),
                    (1.0, 10.0),
                ],
            ),
            (
                &[0.0, 10.0, 20.0],
                &[
                    (0.0, 0.0),
                    (1.0 / 3.0, 10.0),
                    (0.5, 10.0),
                    (2.0 / 3.0, 20.0),
                    (1.0, 20.0),
                ],
            ),
            (
                &[0.0, 10.0, 20.0, 30.0],
                &[
                    (0.0, 0.0),
                    (0.25, 10.0),
                    (0.5, 20.0),
                    (0.75, 30.0),
                    (1.0, 30.0),
                ],
            ),
            (
                &[0.0, 0.0, 10.0, 10.0],
                &[(0.0, 0.0), (0.25, 0.0), (0.5, 10.0), (1.0, 10.0)],
            ),
        ];

        for &(values, queries) in cases {
            let mut kll = KllSketch::with_seed(200, 7).unwrap();
            let mut tdigest = TDigest::new(100.0).unwrap();
            for &value in values {
                kll.add(value);
                tdigest.add(value);
            }

            for &(q, expected) in queries {
                assert_eq!(
                    kll.quantile(q).unwrap(),
                    expected,
                    "KLL values={values:?} q={q}"
                );
                assert_eq!(
                    tdigest.quantile(q).unwrap(),
                    expected,
                    "t-digest values={values:?} q={q}"
                );
            }
        }
    }
}
