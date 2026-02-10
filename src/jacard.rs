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
//! Jaccard similarity trait shared by sketch implementations.

use crate::SketchError;

/// Common API for sketches that can estimate Jaccard similarity.
///
/// The returned value is expected to be in `[0, 1]`:
/// - `0.0` means disjoint sets,
/// - `1.0` means identical sets.
///
/// # Example
/// ```rust
/// use sketches::jacard::JacardIndex;
/// use sketches::minhash::MinHash;
///
/// fn compare<S: JacardIndex>(left: &S, right: &S) -> f64 {
///     left.jaccard_index(right).unwrap()
/// }
///
/// let mut left = MinHash::new(128).unwrap();
/// let mut right = MinHash::new(128).unwrap();
/// for value in 0_u64..5_000 {
///     left.add(&value);
/// }
/// for value in 2_500_u64..7_500 {
///     right.add(&value);
/// }
///
/// let similarity = compare(&left, &right);
/// assert!(similarity > 0.20 && similarity < 0.60);
/// ```
pub trait JacardIndex {
    /// Returns the estimated Jaccard index `|A ∩ B| / |A ∪ B|`.
    ///
    /// # Errors
    /// Implementations return [`SketchError::IncompatibleSketches`] when two
    /// sketches are not compatible for comparison.
    fn jaccard_index(&self, other: &Self) -> Result<f64, SketchError>;
}

#[cfg(test)]
mod tests {
    use crate::{hyperloglog::HyperLogLog, jacard::JacardIndex, minhash::MinHash};

    #[test]
    fn trait_api_works_for_hyperloglog() {
        let mut left = HyperLogLog::new(12).unwrap();
        let mut right = HyperLogLog::new(12).unwrap();
        for value in 0_u64..5_000 {
            left.add(&value);
        }
        for value in 2_500_u64..7_500 {
            right.add(&value);
        }

        let similarity = JacardIndex::jaccard_index(&left, &right).unwrap();
        assert!(similarity > 0.20 && similarity < 0.60);
    }

    #[test]
    fn trait_api_works_for_minhash() {
        let mut left = MinHash::new(128).unwrap();
        let mut right = MinHash::new(128).unwrap();
        for value in 0_u64..5_000 {
            left.add(&value);
        }
        for value in 2_500_u64..7_500 {
            right.add(&value);
        }

        let similarity = JacardIndex::jaccard_index(&left, &right).unwrap();
        assert!(similarity > 0.20 && similarity < 0.60);
    }
}
