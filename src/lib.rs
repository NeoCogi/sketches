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
//! - [`count_min_sketch::CountMinSketch`] for approximate non-negative frequency
//!   estimation.
//! - [`minmax_sketch::MinMaxSketch`] for approximate ordered-value lookup.
//! - [`hyperloglog::HyperLogLog`] for approximate cardinality estimation.
//! - [`ultraloglog::UltraLogLog`] for more space-efficient approximate
//!   cardinality estimation.
//! - [`jaccard`] for approximate set overlap/Jaccard helpers on cardinality and
//!   similarity sketches.
//! - [`bloom_filter::BloomFilter`] for approximate set membership checks.
//! - [`count_sketch::CountSketch`] for signed approximate frequency estimation.
//! - [`space_saving::SpaceSaving`] for approximate heavy hitters in
//!   unit-weight streams.
//! - [`kll::KllSketch`] for approximate quantiles.
//! - [`tdigest::TDigest`] for tail-friendly quantiles.
//! - [`cuckoo_filter::CuckooFilter`] for membership with deletions.
//! - [`minhash::MinHash`] for approximate Jaccard estimation.
//! - [`minhash_lsh_index::MinHashLshIndex`] for approximate nearest-neighbor lookup.
//! - [`reservoir_sampling::ReservoirSampling`] for uniform stream sampling.
//! - [`vector_welford::VectorWelford`] for streaming vector moments without sketch approximation.
//! - [`rv_coefficient::RvCoefficient`] for streaming RV coefficient matrix correlation.

use core::fmt;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

pub mod bloom_filter;
pub mod count_min_sketch;
pub mod count_sketch;
pub mod cuckoo_filter;
pub mod hyperloglog;
pub mod jaccard;
pub mod kll;
pub mod minhash;
pub mod minhash_lsh_index;
pub mod minmax_sketch;
pub mod reservoir_sampling;
pub mod rv_coefficient;
pub mod space_saving;
pub mod tdigest;
pub mod ultraloglog;
pub mod vector_welford;

/// Errors returned by sketch construction, update, query, and merge operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SketchError {
    /// Returned for invalid arguments or unsupported dimensions/storage at
    /// operations that document fallible sizing and reservation.
    InvalidParameter(&'static str),
    /// Returned when combining two sketches that are not shape-compatible.
    IncompatibleSketches(&'static str),
    /// Returned when combining sketches would exceed the supported observation
    /// count.
    ObservationCountOverflow,
    /// Returned when a query exceeds its supported observation-count limit.
    ///
    /// The sketch's exact count can remain valid for ingestion and merging.
    /// KLL quantile queries use this error above `2^52` observations.
    ObservationLimitExceeded {
        /// Maximum observation count accepted by the requested query, inclusive.
        limit: u64,
    },
    /// Returned when a Count Sketch update would exceed its exact signed
    /// counter range.
    CounterOverflow,
    /// Returned when a set-relation query requires a nonfinite cardinality.
    ///
    /// Saturated cardinality sketches are valid, but their infinite estimates
    /// cannot supply the finite scale needed for inclusion-exclusion.
    EstimateUnavailable,
}

impl fmt::Display for SketchError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidParameter(message) => write!(f, "invalid parameter: {message}"),
            Self::IncompatibleSketches(message) => write!(f, "incompatible sketches: {message}"),
            Self::ObservationCountOverflow => {
                write!(f, "observation count exceeds u64::MAX")
            }
            Self::ObservationLimitExceeded { limit } => {
                write!(
                    f,
                    "observation count exceeds supported query limit of {limit}"
                )
            }
            Self::CounterOverflow => {
                write!(f, "Count Sketch counter update exceeds the exact i64 range")
            }
            Self::EstimateUnavailable => {
                write!(f, "set-relation estimate requires finite cardinalities")
            }
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

/// Odd state increment used by the SplitMix64 mixer and constructor stream.
const SPLITMIX_INCREMENT: u64 = 0x9E37_79B9_7F4A_7C15;

/// SplitMix64 mixer used for deterministic row/hash seed derivation.
///
/// Adds one increment before mixing. Callers choose their own input schedule;
/// this function neither owns nor advances any shared state.
pub(crate) fn splitmix64(mut x: u64) -> u64 {
    x = x.wrapping_add(SPLITMIX_INCREMENT);
    x = (x ^ (x >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    x ^ (x >> 31)
}

/// Constructor-local coefficient stream for Count-Min, Count Sketch and MinMax.
///
/// Owns one caller-selected, domain-separated seed and performs no allocation,
/// callbacks or fallible work. Consumers retain their distinct row algorithms,
/// domains and coefficient draw order. MinHash's index-based schedule and the
/// streaming RNGs in KLL/Reservoir deliberately use their own schedules.
pub(crate) struct SeedStream {
    /// Input to the next mixer call, advanced with wrapping u64 arithmetic.
    state: u64,
}

impl SeedStream {
    /// Starts at the supplied domain-separated seed without consuming a word.
    pub(crate) fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    /// Mixes the current input, then advances it by one SplitMix increment.
    /// The first word is `splitmix64(seed)`; this preserves the original three
    /// constructors' sequence, including wrapping at the u64 boundary.
    pub(crate) fn next_u64(&mut self) -> u64 {
        let value = splitmix64(self.state);
        self.state = self.state.wrapping_add(SPLITMIX_INCREMENT);
        value
    }

    /// Concatenates two successive words: first in the high 64 bits, then low.
    /// Frequency sketches use this wider domain for multiply-shift coefficients.
    pub(crate) fn next_u128(&mut self) -> u128 {
        (u128::from(self.next_u64()) << 64) | u128::from(self.next_u64())
    }
}

#[cfg(test)]
mod seed_stream_tests {
    use super::SeedStream;

    #[test]
    fn literal_word_sequences_preserve_initial_phase_and_wrapping() {
        // Independently evaluated with unsigned Python integer arithmetic,
        // masking every multiply/add to 64 bits. The u64::MAX case wraps on
        // the first advance; no production helper computes these expectations.
        for (seed, words) in [
            (
                0,
                [
                    0xE220_A839_7B1D_CDAF,
                    0x6E78_9E6A_A1B9_65F4,
                    0x06C4_5D18_8009_454F,
                    0xF88B_B8A8_724C_81EC,
                    0x1B39_896A_51A8_749B,
                    0x53CB_9F0C_747E_A2EA,
                ],
            ),
            (
                1,
                [
                    0x910A_2DEC_8902_5CC1,
                    0xBEEB_8DA1_658E_EC67,
                    0xF893_A2EE_FB32_555E,
                    0x71C1_8690_EE42_C90B,
                    0x71BB_54D8_D101_B5B9,
                    0xC34D_0BFF_9015_0280,
                ],
            ),
            (
                u64::MAX,
                [
                    0xE4D9_7177_1B65_2C20,
                    0xE99F_F867_DBF6_82C9,
                    0x382F_F84C_B272_81E9,
                    0x6D1D_B36C_CBA9_82D2,
                    0xB4A0_472E_5780_69AE,
                    0xD31D_ADBD_A438_BB33,
                ],
            ),
        ] {
            let mut stream = SeedStream::new(seed);
            for word in words {
                assert_eq!(stream.next_u64(), word, "seed={seed}");
            }
        }
    }

    #[test]
    fn wide_draws_preserve_word_order_consumption_and_independent_ownership() {
        let mut stream = SeedStream::new(0);
        let mut untouched = SeedStream::new(0);
        assert_eq!(
            stream.next_u128(),
            0xE220_A839_7B1D_CDAF_6E78_9E6A_A1B9_65F4
        );
        assert_eq!(stream.next_u64(), 0x06C4_5D18_8009_454F);
        assert_eq!(
            stream.next_u128(),
            0xF88B_B8A8_724C_81EC_1B39_896A_51A8_749B
        );
        assert_eq!(stream.next_u64(), 0x53CB_9F0C_747E_A2EA);
        // Advancing another owner never changes the newly constructed stream.
        assert_eq!(untouched.next_u64(), 0xE220_A839_7B1D_CDAF);
        assert_eq!(
            untouched.next_u128(),
            0x6E78_9E6A_A1B9_65F4_06C4_5D18_8009_454F
        );
    }
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
                tdigest.add(value).unwrap();
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
