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
| Cuckoo Filter | `cuckoo_filter` | You need membership checks and deletions | Delete only items known to have been inserted; inserts can fail at high load |
| HyperLogLog | `hyperloglog` | You need approximate distinct counts (`COUNT(DISTINCT ...)`) | Mergeable; target standard errors below `0.00203125` are unsupported |
| MinMax Sketch | `minmax_sketch` | You need approximate non-negative frequency counts | Conservative updates reduce overestimation |
| Count Sketch | `count_sketch` | You need approximate signed frequency updates | Good for turnstile streams (+/- updates) |
| Space-Saving | `space_saving` | You need top-k / heavy hitters from a unit-weight stream | Stream-Summary keeps updates expected `O(1)` and `top_k(k)` proportional to `k` |
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
- Jaccard from existing cardinality pipelines: `HyperLogLog` + `jacard` helpers
  are available, but read the low-overlap limitations below before using them.
- Membership without delete: use `BloomFilter`.
- Membership with delete: use `CuckooFilter`; delete only items known to have been inserted successfully.
- Approximate frequency (non-negative): use `MinMaxSketch`.
- Approximate frequency (signed +/- updates): use `CountSketch`.
- Heavy hitters / top-k: use `SpaceSaving`.
- General quantiles: use `KllSketch`.
- Tail-sensitive quantiles: use `TDigest`.
- Keep a representative stream sample: use `ReservoirSampling`.

## Cuckoo Filter Parameters

Automatic cuckoo filters use four-entry buckets, fingerprints from 6 through
16 bits, and a table sized to at most 96% target occupancy. The six-bit minimum
follows the original paper's empirical finding that shorter fingerprints can
prevent partial-key cuckoo hashing from reaching high occupancy in large
tables. `CuckooFilter::with_parameters` rejects widths outside `6..=16`.

The paper and reference implementation use a maximum of 500 relocation kicks,
which is also the default used by this crate's automatic constructor. A larger
limit can be selected explicitly with `CuckooFilter::with_parameters` when an
application prefers more relocation work in exchange for fewer early failures
near capacity.

`expected_items` is a sizing target, not a guarantee that every insertion up to
that count succeeds. At dense target loads, the randomized 500-kick insertion
may fail earlier.

## Cuckoo Filter Deletion Contract

Call `CuckooFilter::delete` only for an item instance that the caller knows was
previously inserted successfully and has not already been deleted. A positive
`contains` result is insufficient because it may be a false positive. Deleting
such a non-member can remove a different real item's colliding fingerprint and
introduce a false negative. Applications that must delete arbitrary keys safely
need exact membership tracking outside the filter.

## Space-Saving Update Contract

`SpaceSaving` accepts one observation per `insert(item)` call. It intentionally
does not expose a weighted or batched update: the original Stream-Summary data
structure obtains expected constant-time updates because every counter moves
only from `count` to `count + 1`. Equal counters share a bucket, count buckets
stay linked in sorted order, and `top_k(k)` walks down from the largest bucket
without sorting every retained counter.

For example:

```rust
use sketches::space_saving::SpaceSaving;

let mut heavy_hitters = SpaceSaving::new(3)?;
for item in ["apple", "apple", "banana", "apple", "carrot", "durian"] {
    heavy_hitters.insert(item);
}

assert_eq!(heavy_hitters.top_k(1)[0].0, "apple");
# Ok::<(), Box<dyn std::error::Error>>(())
```

## HyperLogLog Error Contract

`HyperLogLog::with_error_rate(target)` treats `target` as a nominal relative
standard error and selects the smallest precision from 4 through 18 for which
`1.04 / sqrt(2^precision) <= target`. It returns an error when the target is
below `0.00203125`, the nominal standard error at precision 18, instead of
silently returning a less accurate sketch. This is a statistical standard
error, not a deterministic maximum error for every estimate. The achieved
nominal value is available through `expected_relative_error()`.

Cardinality is calculated using the maximum-likelihood estimator presented as
the second single-sketch estimator in [Ertl's paper](https://arxiv.org/pdf/1702.01284).
In the paper this is **Algorithm 8**. Its literal Algorithm 2 is the
register-wise merge operation, not a cardinality estimator. The implementation
builds the register multiplicity vector and follows Algorithm 8's lower-bound
initialization and stable secant iteration; it does not combine that estimator
with the original HyperLogLog `2.5m` transition or large-range correction.

## HyperLogLog Intersection and Jaccard Limitations

**HyperLogLog only supports union natively.** Merging takes the register-wise
maximum, producing a valid sketch for `A ∪ B`. This crate keeps
`intersection_estimate()` and `jaccard_index()` for workflows where only HLL
state is available, but those helpers use conventional inclusion-exclusion:

```text
|A ∩ B| ≈ estimate(A) + estimate(B) - estimate(A ∪ B)
```

This subtraction can amplify cardinality-estimation noise dramatically. As
[Ertl explains](https://arxiv.org/pdf/1702.01284), inclusion-exclusion can be
quite inaccurate, especially for small Jaccard indices. When the intersection
is small relative to the input sets, the error in the three much larger
cardinality estimates can equal or exceed the intersection itself.

The implementation clamps an intersection to `[0, min(|A|, |B|)]` and Jaccard
to `[0, 1]`, but clamping only prevents mathematically impossible outputs. It
does **not** correct the statistical error. In particular:

- a returned intersection or Jaccard of zero does not prove disjointness;
- a positive estimate does not prove that the exact intersection is nonzero;
- `expected_relative_error()` applies to single-sketch cardinality, not to the
  derived intersection or Jaccard estimate;
- accuracy degrades as the true intersection/Jaccard becomes small relative to
  the input sets.

Use `MinHash` when Jaccard similarity is the primary workload. If data must
remain in HLL form and better set-operation estimates are required, use the
joint maximum-likelihood approach from Ertl's paper rather than interpreting
these inclusion-exclusion helpers as precise low-overlap estimators.

## Quantile Convention

`KllSketch` and `TDigest` use the same empirical inverse-CDF convention. For
`N` exact samples and `q` in `[0, 1]`, the selected zero-based rank is:

```text
min(floor(q * N), N - 1)
```

Consequently, the median of `[0, 10]` is `10`. KLL returns a retained sample at
the selected approximate rank, so after compaction its endpoint queries are the
smallest and largest retained values rather than guaranteed exact stream
extrema. t-digest follows the same rank rule for singleton centroids, may
interpolate between multi-sample centroid midpoint ranks, and separately
retains the exact observed minimum and maximum for `q = 0` and `q = 1`.
Its centroid means and interpolated quantiles remain finite across the complete
finite `f64` input range, including mixtures of `-f64::MAX` and `f64::MAX`.
Additions are accumulated in an ordered buffer of roughly `10 * compression`
entries and batch-merged with the compressed centroids. Quantile queries merge
those two ordered views while reading, so they neither clone nor sort the
centroid state.

## Quick Examples

Approximate distinct counting:

```rust
use sketches::hyperloglog::HyperLogLog;

let mut hll = HyperLogLog::with_error_rate(0.01)?;
for i in 0_u64..100_000 {
    hll.add(&i);
}
println!("distinct ~ {}", hll.count());
println!("nominal relative standard error = {}", hll.expected_relative_error());
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
