# sketches
[![crates.io](https://img.shields.io/crates/v/sketches?logo=rust&label=crates.io)](https://crates.io/crates/sketches)

Probabilistic data structures for scalable approximate analytics in Rust.

This crate gives you memory-efficient sketches for:
- frequency estimation,
- distinct counting,
- set membership,
- set similarity (Jaccard),
- heavy hitter detection,
- quantiles,
- and stream sampling.

All sketches are designed for streaming workloads where exact data structures
(`HashMap`, full sorted buffers, exact sets) are too expensive in memory or
throughput.

### Note
This crate was designed by humans, but coded with AI.

## Add To A Project

This repository is currently consumed as a local crate:

```toml
[dependencies]
sketches = { path = "../sketches" }
```

## What Is Included

| Sketch | Module | Use it when | Notes |
| --- | --- | --- | --- |
| Bloom Filter | `bloom_filter` | You need very fast membership checks and can tolerate false positives | No deletions |
| Cuckoo Filter | `cuckoo_filter` | You need membership checks and deletions | Can fail inserts at high load |
| HyperLogLog | `hyperloglog` | You need approximate distinct counts (`COUNT(DISTINCT ...)`) | Mergeable, tiny memory footprint |
| MinMax Sketch | `minmax_sketch` | You need approximate non-negative frequency counts | Conservative updates reduce overestimation |
| Count Sketch | `count_sketch` | You need approximate signed frequency updates | Good for turnstile streams (+/- updates) |
| Space-Saving | `space_saving` | You need top-k / heavy hitters from a stream | Tracks only bounded number of candidates |
| KLL Sketch | `kll` | You need general quantiles (median, p90, p99) | Good default quantile sketch |
| t-digest | `tdigest` | You care most about tail quantiles (p95/p99/p999) | Typically stronger tail behavior |
| MinHash | `minhash` | You need Jaccard similarity between sets | Best default for similarity tasks |
| MinHash LSH | `lsh_minhash` | You need fast near-duplicate/candidate lookup before reranking | Uses banding over MinHash signatures |
| Reservoir Sampling | `reservoir_sampling` | You need a uniform sample from an unbounded stream | Fixed-size unbiased sample |
| Jaccard trait/helpers | `jacard` | You want a shared Jaccard API across sketches | Provides `JacardIndex` trait |

## Which Sketch Should I Use?

If your primary goal is:

- Distinct counting: use `HyperLogLog`.
- Jaccard similarity: use `MinHash` first.
- Candidate retrieval for similarity search: use `MinHashLshIndex`, then rerank with MinHash Jaccard.
- Jaccard from existing cardinality pipelines: use `HyperLogLog` + `jacard` helpers.
- Membership without delete: use `BloomFilter`.
- Membership with delete: use `CuckooFilter`.
- Approximate frequency (non-negative): use `MinMaxSketch`.
- Approximate frequency (signed +/- updates): use `CountSketch`.
- Heavy hitters / top-k: use `SpaceSaving`.
- General quantiles: use `KllSketch`.
- Tail-sensitive quantiles: use `TDigest`.
- Keep a representative stream sample: use `ReservoirSampling`.

## Quick Examples

Approximate distinct counting:

```rust
use sketches::hyperloglog::HyperLogLog;

let mut hll = HyperLogLog::new(12)?;
for i in 0_u64..100_000 {
    hll.add(&i);
}
println!("distinct ~ {}", hll.count());
# Ok::<(), Box<dyn std::error::Error>>(())
```

Approximate Jaccard similarity (recommended via MinHash):

```rust
use sketches::jacard::JacardIndex;
use sketches::minhash::MinHash;

let mut left = MinHash::new(256)?;
let mut right = MinHash::new(256)?;

for v in 0_u64..10_000 {
    left.add(&v);
}
for v in 5_000_u64..15_000 {
    right.add(&v);
}

println!("jaccard ~ {:.4}", left.jaccard_index(&right)?);
# Ok::<(), Box<dyn std::error::Error>>(())
```

## Run Examples

```bash
cargo run --example bloom_filter
cargo run --example cuckoo_filter
cargo run --example hyperloglog
cargo run --example jacard
cargo run --example minhash
cargo run --example lsh_minhash
cargo run --example minmax_sketch
cargo run --example count_sketch
cargo run --example space_saving
cargo run --example kll
cargo run --example tdigest
cargo run --example reservoir_sampling
```

## Validate

```bash
cargo test
cargo check --examples
```
