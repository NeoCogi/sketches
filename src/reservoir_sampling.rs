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
    capacity: usize,
    samples: Vec<T>,
    seen: u64,
    rng_state: u64,
}

impl<T> ReservoirSampling<T> {
    /// Creates a reservoir with the given sample size and deterministic seed.
    ///
    /// Samplers constructed with the same capacity and seed make the same
    /// choices for the same input stream. Use independently generated seeds
    /// when independent samples are required.
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

        let replacement_index = self.next_u64() % self.seen;
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
    pub fn clear(&mut self) {
        self.samples.clear();
        self.seen = 0;
    }

    /// Consumes the sampler and returns the sample buffer.
    pub fn into_samples(self) -> Vec<T> {
        self.samples
    }

    fn next_u64(&mut self) -> u64 {
        self.rng_state = splitmix64(self.rng_state.wrapping_add(0x9E37_79B9_7F4A_7C15));
        self.rng_state
    }
}

#[cfg(test)]
mod tests {
    use super::ReservoirSampling;

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
