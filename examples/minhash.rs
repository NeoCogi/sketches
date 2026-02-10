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
use sketches::minhash::MinHash;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Use a 256-component MinHash signature.
    let mut left = MinHash::new(256)?;
    let mut right = MinHash::new(256)?;

    // Build two overlapping sets:
    // left  = [0, 10_000)
    // right = [5_000, 15_000)
    for value in 0_u64..10_000 {
        left.add(&value);
    }
    for value in 5_000_u64..15_000 {
        right.add(&value);
    }

    let jaccard = left.estimate_jaccard(&right)?;
    println!("Estimated Jaccard similarity: {:.4}", jaccard);

    Ok(())
}
