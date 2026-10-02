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
//! Reservoir sampling for uniform samples from streaming data.
//!
//! Algorithm R keeps every item until the reservoir is full. At observation
//! `n`, it draws a uniform index in `0..n` and replaces that slot if the index
//! is below the sample capacity. Under the independent uniform random-word
//! model, each stream position has inclusion probability `min(1, capacity/n)`.
//!
//! Bounded indices use rejection sampling so converting a random `u64` does
//! not introduce modulo bias. Each sampler owns its deterministic random state;
//! the statistical statement uses the conventional pseudorandom model and
//! independently chosen seeds. It is not a cryptographic randomness guarantee.

use crate::{SketchError, splitmix64};

/// Fixed-size uniform reservoir sample over a stream.
///
/// # Example
/// ```rust
/// use sketches::reservoir_sampling::ReservoirSampling;
///
/// let mut reservoir = ReservoirSampling::new(100, 42).unwrap();
/// for value in 0_u64..10_000 {
///     reservoir.add(value);
/// }
///
/// assert_eq!(reservoir.len(), 100);
/// assert_eq!(reservoir.seen(), 10_000);
/// ```
#[derive(Debug, Clone)]
pub struct ReservoirSampling<T> {
    /// Maximum number of owned items retained from the stream; always positive.
    capacity: usize,
    /// Retained items, with length `min(capacity, seen)` for representable counts.
    samples: Vec<T>,
    /// Exact observation count; additions panic before mutation on overflow.
    seen: u64,
    /// This sampler's owned random-word state, advanced only for full reservoirs.
    rng_state: u64,
}

impl<T> ReservoirSampling<T> {
    /// Creates a reservoir with the given sample size and deterministic seed.
    ///
    /// Samplers constructed with the same capacity and seed make the same
    /// choices for the same input stream. Use independently generated seeds
    /// when independent samples are required. Rejection sampling can consume
    /// several random words for one replacement decision.
    ///
    /// # Errors
    /// Returns [`SketchError::InvalidParameter`] when `capacity == 0`.
    pub fn new(capacity: usize, seed: u64) -> Result<Self, SketchError> {
        if capacity == 0 {
            return Err(SketchError::InvalidParameter(
                "capacity must be greater than zero",
            ));
        }

        Ok(Self {
            capacity,
            samples: Vec::with_capacity(capacity),
            seen: 0,
            rng_state: seed,
        })
    }

    /// Returns the configured sample capacity.
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Returns the current number of sampled items.
    pub fn len(&self) -> usize {
        self.samples.len()
    }

    /// Returns `true` when no item has been seen yet.
    pub fn is_empty(&self) -> bool {
        self.seen == 0
    }

    /// Returns the total number of items seen from the stream.
    pub fn seen(&self) -> u64 {
        self.seen
    }

    /// Returns the sampled items.
    pub fn samples(&self) -> &[T] {
        &self.samples
    }

    /// Adds one item from the stream.
    ///
    /// Once full, a uniform index in `0..seen` decides whether to replace a
    /// retained item. Rejected random words advance only the RNG; they do not
    /// count as additional observations. Under independent uniform words, a
    /// bounded draw requires fewer than two words on average.
    ///
    /// # Panics
    /// Panics if the observation count is already `u64::MAX`.
    pub fn add(&mut self, item: T) {
        let new_seen = self
            .seen
            .checked_add(1)
            .expect("reservoir observation count exceeds u64::MAX");
        self.seen = new_seen;

        if self.samples.len() < self.capacity {
            self.samples.push(item);
            return;
        }

        let replacement_index = self.next_below(self.seen);
        if replacement_index < self.capacity as u64 {
            self.samples[replacement_index as usize] = item;
        }
    }

    /// Adds all items from an iterator.
    ///
    /// # Panics
    /// Panics if adding the iterator's items would make the observation count
    /// exceed `u64::MAX`.
    pub fn extend<I>(&mut self, items: I)
    where
        I: IntoIterator<Item = T>,
    {
        for item in items {
            self.add(item);
        }
    }

    /// Removes all sampled items and resets stream counters.
    ///
    /// The random stream continues from its current state. Construct a new
    /// sampler with the original seed to replay a stream from the beginning.
    pub fn clear(&mut self) {
        self.samples.clear();
        self.seen = 0;
    }

    /// Consumes the sampler and returns the sample buffer.
    pub fn into_samples(self) -> Vec<T> {
        self.samples
    }

    /// Draws an unbiased index in `0..bound` under the uniform-word model.
    ///
    /// The caller supplies a positive bound (the checked observation count).
    /// If `2^64 = q * bound + r`, discarding words below `r` leaves exactly `q`
    /// accepted preimages per index. Wrapping negation computes `r` without
    /// representing `2^64`, including for bounds one and `u64::MAX`.
    fn next_below(&mut self, bound: u64) -> u64 {
        debug_assert!(bound > 0);
        let threshold = bound.wrapping_neg() % bound;
        loop {
            let word = self.next_u64();
            if word >= threshold {
                return word % bound;
            }
        }
    }

    /// Advances the existing instance-owned pseudorandom stream by one word.
    fn next_u64(&mut self) -> u64 {
        self.rng_state = splitmix64(self.rng_state.wrapping_add(0x9E37_79B9_7F4A_7C15));
        self.rng_state
    }
}

#[cfg(test)]
mod tests {
    use super::ReservoirSampling;
    use crate::splitmix64;

    /// Inverts the bijective word generator to force its first output in tests.
    ///
    /// Undo the xor shifts and the two odd multipliers, then both additions
    /// (one in next_u64, one in splitmix64). No production RNG hook is needed.
    fn seed_for_first_word(word: u64) -> u64 {
        let mut value = word ^ (word >> 31) ^ (word >> 62);
        value = value.wrapping_mul(0x3196_42B2_D24D_8EC3);
        value = value ^ (value >> 27) ^ (value >> 54);
        value = value.wrapping_mul(0x96DE_1B17_3F11_9089);
        value = value ^ (value >> 30) ^ (value >> 60);
        value
            .wrapping_sub(0x9E37_79B9_7F4A_7C15)
            .wrapping_sub(0x9E37_79B9_7F4A_7C15)
    }

    #[test]
    fn bounded_draw_rejects_exactly_the_u128_remainder_prefix() {
        let word_count = 1_u128 << 64;
        let mut bounds: Vec<u64> = (1..=255).collect();
        bounds.push(u64::MAX);
        bounds.push(u64::MAX - 1);
        for bit in 1..64 {
            let power = 1_u64 << bit;
            bounds.extend([power - 1, power, power + 1]);
        }
        bounds.sort_unstable();
        bounds.dedup();

        for bound in bounds {
            // Exact wider arithmetic is independent of wrapping_neg in the
            // implementation. All accepted residues have this preimage count.
            let rejected_count = (word_count % u128::from(bound)) as u64;
            let accepted_count = word_count - u128::from(rejected_count);
            assert_eq!(accepted_count % u128::from(bound), 0);
            assert!(accepted_count > word_count / 2);
            let mut first_words = vec![0, rejected_count, rejected_count + 1, u64::MAX];
            if let Some(last_rejected) = rejected_count.checked_sub(1) {
                first_words.push(last_rejected);
            }
            first_words.sort_unstable();
            first_words.dedup();

            for first_word in first_words {
                let seed = seed_for_first_word(first_word);
                let mut state = seed;
                let mut expected = None;
                // Read the existing word stream and apply the exact u128
                // acceptance interval, bounded here only to keep tests finite.
                for draw in 1..=64 {
                    state = splitmix64(state.wrapping_add(0x9E37_79B9_7F4A_7C15));
                    if draw == 1 {
                        assert_eq!(state, first_word);
                    }
                    if u128::from(state) >= word_count % u128::from(bound) {
                        expected = Some(state % bound);
                        break;
                    }
                }
                let expected = expected.expect("fixture stream must reach an accepted word");
                let mut reservoir = ReservoirSampling::<u64>::new(1, seed).unwrap();
                assert_eq!(
                    reservoir.next_below(bound),
                    expected,
                    "bound={bound} first={first_word}"
                );
                assert_eq!(
                    reservoir.rng_state, state,
                    "bound={bound} first={first_word}"
                );
            }
        }
    }

    #[test]
    fn rejection_mapping_is_uniform_for_every_eight_bit_bound() {
        // Exhaustive finite analogue: independently divide 256 by each bound,
        // discard the incomplete prefix and count every accepted preimage.
        // Real 64-bit implementation boundaries are checked separately above.
        for bound in 1_u16..=255 {
            let quotient = 256 / bound;
            let remainder = 256 - quotient * bound;
            let mut counts = vec![0_u16; usize::from(bound)];
            for word in remainder..256 {
                counts[usize::from(word % bound)] += 1;
            }
            assert!(
                counts.iter().all(|&count| count == quotient),
                "bound={bound}"
            );
            if bound == 3 {
                assert_eq!(counts, [85, 85, 85]);
            }
        }
    }

    #[test]
    fn bounded_draw_can_reject_multiple_words_and_accept_index_zero() {
        // This bound rejects the first three known words; rejection must loop
        // without treating any of them as an observation or replacement.
        let bound = (1_u64 << 63) + 1;
        let mut reservoir = ReservoirSampling::<u64>::new(1, 0xC391_0C8D_016B_07D6).unwrap();
        assert_eq!(reservoir.next_below(bound), 0x0576_B56C_3603_C2B8);
        assert_eq!(reservoir.rng_state, 0x8576_B56C_3603_C2B9);
        assert_eq!(reservoir.seen(), 0);
        assert!(reservoir.samples().is_empty());

        // At MAX, word MAX is accepted and legitimately maps to index zero.
        let seed = seed_for_first_word(u64::MAX);
        let mut reservoir = ReservoirSampling::<u64>::new(1, seed).unwrap();
        assert_eq!(reservoir.next_below(u64::MAX), 0);
        assert_eq!(reservoir.rng_state, u64::MAX);
    }

    #[test]
    fn constructor_validates_capacity() {
        assert!(ReservoirSampling::<u64>::new(0, 7).is_err());
        assert!(ReservoirSampling::<u64>::new(10, 7).is_ok());
    }

    #[test]
    fn sample_size_never_exceeds_capacity() {
        let mut reservoir = ReservoirSampling::new(64, 7).unwrap();
        for value in 0_u64..10_000 {
            reservoir.add(value);
        }
        assert_eq!(reservoir.len(), 64);
        assert_eq!(reservoir.seen(), 10_000);
    }

    #[test]
    fn short_stream_keeps_all_values() {
        let mut reservoir = ReservoirSampling::new(10, 7).unwrap();
        reservoir.extend([1_u64, 2, 3, 4]);
        assert_eq!(reservoir.len(), 4);
        assert_eq!(reservoir.samples(), &[1, 2, 3, 4]);
    }

    #[test]
    fn deterministic_for_same_input_stream() {
        let mut left = ReservoirSampling::new(50, 7).unwrap();
        let mut right = ReservoirSampling::new(50, 7).unwrap();

        for value in 0_u64..5_000 {
            left.add(value);
            right.add(value);
        }

        assert_eq!(left.samples(), right.samples());
    }

    #[test]
    fn different_seeds_select_different_samples() {
        let mut left = ReservoirSampling::new(50, 7).unwrap();
        let mut right = ReservoirSampling::new(50, 8).unwrap();

        for value in 0_u64..5_000 {
            left.add(value);
            right.add(value);
        }

        assert_ne!(left.samples(), right.samples());
    }

    #[test]
    fn add_rejects_zero_before_selecting_at_the_count_limit() {
        // This seed makes the first random word zero. The count is a lawful
        // synthetic boundary state, without ingesting u64::MAX observations.
        let mut reservoir = ReservoirSampling::new(1, 0xC391_0C8D_016B_07D6).unwrap();
        reservoir.add(7_u64);
        reservoir.seen = u64::MAX - 1;
        reservoir.add(19);

        // At bound MAX, zero is the sole rejected word. The next accepted word
        // chooses an index outside capacity one, so the old item stays sampled.
        assert_eq!(reservoir.samples(), &[7]);
        assert_eq!(reservoir.seen(), u64::MAX);
        assert_eq!(reservoir.rng_state, 0x6E78_9E6A_A1B9_65F4);
    }

    #[test]
    #[should_panic(expected = "reservoir observation count exceeds u64::MAX")]
    fn add_panics_when_observation_count_overflows() {
        let mut reservoir = ReservoirSampling::new(1, 7).unwrap();
        reservoir.seen = u64::MAX;
        reservoir.add(1_u64);
    }

    #[test]
    fn clear_resets_state() {
        let mut reservoir = ReservoirSampling::new(8, 7).unwrap();
        reservoir.extend(0_u64..100);
        reservoir.clear();
        assert_eq!(reservoir.len(), 0);
        assert_eq!(reservoir.seen(), 0);
        assert!(reservoir.is_empty());
    }
}
