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
use sketches::hyperloglog::HyperLogLog;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Use the same precision for both sketches so merge-based operations are valid.
    let mut left = HyperLogLog::new(14)?;
    let mut right = HyperLogLog::new(14)?;

    // Build two sets with a controlled overlap:
    // left  = [0, 10_000)
    // right = [5_000, 15_000)
    // exact intersection = 5_000, exact union = 15_000.
    for value in 0_u64..10_000 {
        left.add(&value);
    }
    for value in 5_000_u64..15_000 {
        right.add(&value);
    }

    // Estimate set relations derived from the two sketches.
    let union = left.union_estimate(&right)?;
    let intersection = left.intersection_estimate(&right)?;
    let jaccard = left.jaccard_index(&right)?;

    // Print both approximate and exact values to make error intuitive.
    let exact_union = 15_000.0;
    let exact_intersection = 5_000.0;
    let exact_jaccard = exact_intersection / exact_union;

    println!("union estimate:        {:.2} (exact {:.2})", union, exact_union);
    println!(
        "intersection estimate: {:.2} (exact {:.2})",
        intersection, exact_intersection
    );
    println!(
        "jaccard estimate:      {:.4} (exact {:.4})",
        jaccard, exact_jaccard
    );

    Ok(())
}
