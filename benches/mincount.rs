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

use sketches::mincount_sketch::MinCountSketch;

const OPERATIONS: usize = 1_000_000;
const WIDTH: usize = 8_192;
const DEPTH: usize = 5;
const SEED: u64 = 0x510E_527F_ADE6_82D1;

fn throughput(operations: usize, elapsed: Duration) -> f64 {
    operations as f64 / elapsed.as_secs_f64()
}

fn item_id(index: usize) -> u64 {
    let mut value = index as u64 + 0x9E37_79B9_7F4A_7C15;
    value = (value ^ (value >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    value ^ (value >> 31)
}

fn main() {
    let items: Vec<u64> = (0..65_536).map(item_id).collect();
    println!("MinCount conservative-update benchmark");
    println!("operation\tops/s");

    let mut generic = MinCountSketch::with_dimensions(WIDTH, DEPTH, SEED).unwrap();
    let started = Instant::now();
    for operation in 0..OPERATIONS {
        generic.increment(black_box(&items[operation % items.len()]));
    }
    println!(
        "generic increment\t{:.0}",
        throughput(OPERATIONS, started.elapsed())
    );
    black_box(&generic);

    let mut direct = MinCountSketch::with_dimensions(WIDTH, DEPTH, SEED).unwrap();
    let started = Instant::now();
    for operation in 0..OPERATIONS {
        direct.increment_u64(black_box(items[operation % items.len()]));
    }
    println!(
        "u64 increment\t{:.0}",
        throughput(OPERATIONS, started.elapsed())
    );

    let started = Instant::now();
    let mut checksum = 0;
    for operation in 0..OPERATIONS {
        checksum ^= direct.estimate_u64(black_box(items[operation % items.len()]));
    }
    println!(
        "u64 estimate\t{:.0}",
        throughput(OPERATIONS, started.elapsed())
    );
    black_box(checksum);
}
