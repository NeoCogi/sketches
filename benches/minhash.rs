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

//! Public-API MinHash sizing and component-work benchmark.
//!
//! Run with `cargo bench --locked --offline --bench minhash`. TSV output retains
//! individual samples and actual selected widths for before/after comparisons.
//! Constructor samples include allocation, initialization and destruction.
//! Other operations construct/populate their owned inputs outside the timed
//! region. The boundary target intentionally permits the pre-fix width of 18
//! so the identical harness can measure both implementations.

use std::hint::black_box;
use std::time::{Duration, Instant};

use sketches::minhash::MinHash;

/// Measured samples per workload, after one discarded full-size warmup.
const SAMPLES: usize = 7;
/// Consecutive integer keys shared by update workloads, prepared before timing.
const ITEM_COUNT: u64 = 65_536;
/// Exact target immediately below the reported worst-case error for width 18.
const BOUNDARY_TARGET: f64 = f64::from_bits(0x3FBE_2B7D_DDFE_FA66);

/// Selects the public constructor without introducing a second sizing formula.
#[derive(Clone, Copy)]
enum Constructor {
    /// Explicit width; used as an unchanged control across revisions.
    Width(usize),
    /// Error-based sizing, including the rounding-boundary fixture.
    Target(f64),
}

impl Constructor {
    /// Builds a fresh owned sketch; all configured inputs are valid and small.
    fn build(self) -> MinHash {
        match self {
            Self::Width(width) => MinHash::new(black_box(width)).unwrap(),
            Self::Target(target) => MinHash::with_error_rate(black_box(target)).unwrap(),
        }
    }
}

/// Operations timed independently so construction and width changes are visible.
#[derive(Clone, Copy)]
enum Operation {
    /// Includes dropping each completed sketch and its two owned allocations.
    Construct,
    /// Updates a freshly prepared sketch with `u64` keys.
    Add,
    /// Compares two populated compatible signatures, without allocation.
    Jaccard,
    /// Repeatedly merges the same compatible donor into owned destination state.
    Merge,
}

/// Measures one workload with allocation/population outside nonconstructor timing.
///
/// Black boxes retain the complete public operation and prevent constant folding
/// of constructor parameters or loop inputs. Merge timing includes its ordinary
/// idempotent steady state; it does not include cloning or reset allocation.
fn sample(
    operation: Operation,
    constructor: Constructor,
    items: &[u64],
    iterations: usize,
) -> Duration {
    if let Operation::Construct = operation {
        let start = Instant::now();
        for _ in 0..iterations {
            black_box(constructor.build());
        }
        return start.elapsed();
    }

    let mut left = constructor.build();
    let mut right = constructor.build();
    for item in &items[..256] {
        left.add(item);
    }
    for item in &items[128..384] {
        right.add(item);
    }

    let start = Instant::now();
    match operation {
        Operation::Construct => unreachable!("constructor samples return before setup"),
        Operation::Add => {
            for item in items.iter().cycle().take(iterations) {
                left.add(black_box(item));
            }
        }
        Operation::Jaccard => {
            for _ in 0..iterations {
                black_box(left.estimate_jaccard(black_box(&right)).unwrap());
            }
        }
        Operation::Merge => {
            for _ in 0..iterations {
                black_box(&mut left).merge(black_box(&right)).unwrap();
            }
        }
    }
    let elapsed = start.elapsed();
    black_box((left, right));
    elapsed
}

/// Emits raw nanoseconds per operation after warming the identical workload.
fn report(
    label: &str,
    operation: Operation,
    constructor: Constructor,
    items: &[u64],
    iterations: usize,
) {
    let width = constructor.build().num_hashes();
    black_box(sample(operation, constructor, items, iterations));
    for index in 0..SAMPLES {
        let elapsed = sample(operation, constructor, items, iterations);
        let ns_per_operation = elapsed.as_secs_f64() * 1e9 / iterations as f64;
        println!("{label}\t{width}\t{iterations}\t{index}\t{ns_per_operation:.6}");
    }
}

/// Runs sizing cases and matched explicit-width controls on deterministic keys.
fn main() {
    let items: Vec<_> = (0..ITEM_COUNT).collect();
    println!("workload\twidth\titerations\tsample\tns_per_op");
    for (label, constructor, iterations) in [
        ("construct/new18", Constructor::Width(18), 100_000),
        ("construct/error0.5", Constructor::Target(0.5), 100_000),
        ("construct/error0.1", Constructor::Target(0.1), 100_000),
        ("construct/error0.01", Constructor::Target(0.01), 5_000),
        (
            "construct/boundary18",
            Constructor::Target(BOUNDARY_TARGET),
            100_000,
        ),
    ] {
        report(label, Operation::Construct, constructor, &items, iterations);
    }

    for (label, operation, iterations) in [
        ("add", Operation::Add, 30_000),
        ("jaccard", Operation::Jaccard, 300_000),
        ("merge", Operation::Merge, 300_000),
    ] {
        for (configuration, constructor) in [
            ("new18", Constructor::Width(18)),
            ("error0.05", Constructor::Target(0.05)),
            ("boundary18", Constructor::Target(BOUNDARY_TARGET)),
        ] {
            report(
                &format!("{label}/{configuration}"),
                operation,
                constructor,
                &items,
                iterations,
            );
        }
    }
}
