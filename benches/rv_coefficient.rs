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

use sketches::rv_coefficient::RvCoefficient;

const INGESTION_SAMPLES: usize = 200_000;
const QUERY_SAMPLES: usize = 100_000;

fn throughput(operations: usize, elapsed: Duration) -> f64 {
    operations as f64 / elapsed.as_secs_f64()
}

fn input_vector(dim: usize, index: usize) -> Vec<f64> {
    (0..dim)
        .map(|i| (((index + i).wrapping_mul(104_729)) % 1_000_003) as f64)
        .collect()
}

fn main() {
    println!("RV coefficient streaming benchmark");
    println!("(p, q)\t\tingest ops/s\tquery ops/s\trv_result");

    for (p, q) in [(2, 2), (4, 4), (8, 8)] {
        let x_inputs: Vec<Vec<f64>> = (0..1024).map(|i| input_vector(p, i)).collect();
        let y_inputs: Vec<Vec<f64>> = (0..1024).map(|i| input_vector(q, i + 100)).collect();

        let mut rv = RvCoefficient::new(p, q).unwrap();

        let started = Instant::now();
        for index in 0..INGESTION_SAMPLES {
            let x = &x_inputs[index % x_inputs.len()];
            let y = &y_inputs[index % y_inputs.len()];
            rv.add(black_box(x), black_box(y)).unwrap();
        }
        let ingestion_elapsed = started.elapsed();

        let started = Instant::now();
        let mut last_res = 0.0;
        for _ in 0..QUERY_SAMPLES {
            last_res = black_box(rv.rv_coefficient().unwrap());
        }
        let query_elapsed = started.elapsed();

        println!(
            "({}, {})\t\t{:.0}\t\t{:.0}\t\t{:.6}",
            p,
            q,
            throughput(INGESTION_SAMPLES, ingestion_elapsed),
            throughput(QUERY_SAMPLES, query_elapsed),
            last_res,
        );
    }
}
