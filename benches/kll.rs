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

use sketches::kll::KllSketch;

const INSERTIONS: usize = 1_000_000;
const QUERIES: usize = 20_000;

fn input_value(index: usize) -> f64 {
    ((index.wrapping_mul(104_729)) % 1_000_003) as f64
}

fn throughput(operations: usize, elapsed: Duration) -> f64 {
    operations as f64 / elapsed.as_secs_f64()
}

fn main() {
    println!("KLL affected-level compaction benchmark");
    println!("k\tinsert ops/s\tquery ops/s\tobservations");

    for k in [50, 200, 600] {
        let mut sketch = KllSketch::with_seed(k, 7).unwrap();

        let started = Instant::now();
        for index in 0..INSERTIONS {
            sketch.add(black_box(input_value(index)));
        }
        let insertion_elapsed = started.elapsed();

        let started = Instant::now();
        for index in 0..QUERIES {
            let quantile = (index % 10_001) as f64 / 10_000.0;
            black_box(sketch.quantile(black_box(quantile)).unwrap());
        }
        let query_elapsed = started.elapsed();

        println!(
            "{k}\t{:.0}\t\t{:.0}\t\t{}",
            throughput(INSERTIONS, insertion_elapsed),
            throughput(QUERIES, query_elapsed),
            sketch.count(),
        );
    }
}
