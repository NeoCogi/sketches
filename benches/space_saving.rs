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

use std::hint::black_box;
use std::time::{Duration, Instant};

use sketches::space_saving::SpaceSaving;

const INSERTIONS: usize = 500_000;
const QUERIES: usize = 50_000;

fn throughput(operations: usize, elapsed: Duration) -> f64 {
    operations as f64 / elapsed.as_secs_f64()
}

fn main() {
    println!("Space-Saving Stream-Summary benchmark");
    println!("capacity\tinsert ops/s\ttop-10 queries/s\ttracked");

    for capacity in [64, 1_024, 16_384] {
        let mut summary = SpaceSaving::new(capacity).unwrap();
        let started = Instant::now();
        for item in 0..INSERTIONS as u64 {
            // Every item after the initial fill forces a minimum replacement.
            summary.insert(black_box(item));
        }
        let insertion_elapsed = started.elapsed();

        let started = Instant::now();
        for _ in 0..QUERIES {
            black_box(summary.top_k(black_box(10)));
        }
        let query_elapsed = started.elapsed();

        println!(
            "{capacity}\t\t{:.0}\t\t{:.0}\t\t{}",
            throughput(INSERTIONS, insertion_elapsed),
            throughput(QUERIES, query_elapsed),
            summary.tracked_items(),
        );
    }
}
