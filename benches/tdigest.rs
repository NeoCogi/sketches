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

use sketches::tdigest::TDigest;

const INGESTION_SAMPLES: usize = 200_000;
const QUERY_SAMPLES: usize = 20_000;

fn input_value(index: usize) -> f64 {
    ((index.wrapping_mul(104_729)) % 1_000_003) as f64
}

fn throughput(operations: usize, elapsed: Duration) -> f64 {
    operations as f64 / elapsed.as_secs_f64()
}

fn main() {
    println!("t-digest progressive-merge benchmark");
    println!("compression\tingest ops/s\tquery ops/s\tcentroids");

    for compression in [20.0, 100.0, 500.0] {
        let started = Instant::now();
        let mut digest = TDigest::new(compression).unwrap();
        for index in 0..INGESTION_SAMPLES {
            digest.add(black_box(input_value(index)));
        }
        let ingestion_elapsed = started.elapsed();

        let started = Instant::now();
        for index in 0..QUERY_SAMPLES {
            let q = (index % 10_001) as f64 / 10_000.0;
            black_box(digest.quantile(black_box(q)).unwrap());
        }
        let query_elapsed = started.elapsed();

        println!(
            "{compression:.0}\t\t{:.0}\t\t{:.0}\t\t{}",
            throughput(INGESTION_SAMPLES, ingestion_elapsed),
            throughput(QUERY_SAMPLES, query_elapsed),
            digest.centroid_count(),
        );
    }
}
