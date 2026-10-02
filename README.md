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
- streaming vector means, variances, and covariances,
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
| UltraLogLog | `ultraloglog` | You want a more space-efficient mergeable distinct counter | One-byte registers; fast FGRA and accuracy-first MLE estimators |
| Count-Min Sketch | `count_min_sketch` | You need approximate non-negative frequency counts | Count-Min with conservative updates; estimates are one-sided upper bounds |
| MinMax Sketch | `minmax_sketch` | You need to compress a fixed key-to-ordered-value mapping | Insert-min/query-max; estimates for inserted keys are one-sided lower bounds |
| Count Sketch | `count_sketch` | You need approximate signed frequency updates | Good for turnstile streams (+/- updates) |
| Space-Saving | `space_saving` | You need top-k / heavy hitters from a unit-weight stream | Stream-Summary keeps updates expected `O(1)` and `top_k(k)` proportional to `k` |
| KLL Sketch | `kll` | You need general quantiles (median, p90, p99) | Good default quantile sketch |
| t-digest | `tdigest` | You care most about tail quantiles (p95/p99/p999) | Typically stronger tail behavior |
| MinHash | `minhash` | You need Jaccard similarity between sets | Best default for similarity tasks |
| MinHash LSH | `lsh_minhash` | You need fast near-duplicate/candidate lookup before reranking | Uses banding over MinHash signatures |
| Reservoir Sampling | `reservoir_sampling` | You need a uniform sample from an unbounded stream | Fixed-size unbiased sample |
| Vector Welford's Algorithm | `vector_welford` | You need streaming means, variances, and covariances of numeric vectors | Online moments using `f64`, without sketch approximation; `O(d²)` space; mergeable |
| Jaccard trait/helpers | `jacard` | You want a shared Jaccard API across sketches | Provides `JacardIndex` trait |

## Which Sketch Should I Use?

If your primary goal is:

- Distinct counting with established HLL compatibility: use `HyperLogLog`.
- New mergeable distinct-count pipelines: use `UltraLogLog` for better
  precision at the same state size.
- Jaccard similarity: use `MinHash` first.
- Candidate retrieval for similarity search: use `MinHashLshIndex`, then rerank with MinHash Jaccard.
- Jaccard from existing cardinality pipelines: `HyperLogLog` or `UltraLogLog`
  plus the `jacard` trait are available, but read the low-overlap limitations
  below before using them.
- Membership without delete: use `BloomFilter`.
- Membership with delete: use `CuckooFilter`; delete only items known to have been inserted successfully.
- Approximate frequency (non-negative): use `CountMinSketch`.
- Approximate frequency (signed +/- updates): use `CountSketch`.
- Compact ordered values such as quantile-bucket indices: use `MinMaxSketch`.
- Heavy hitters / top-k: use `SpaceSaving`.
- General quantiles: use `KllSketch`.
- Tail-sensitive quantiles: use `TDigest`.
- Keep a representative stream sample: use `ReservoirSampling`.
- Track vector means, variances, and covariances: use `VectorWelford`.

## Vector Welford's Algorithm

`VectorWelford` uses a multivariate extension of Welford's scalar variance
recurrence to summarize fixed-dimension vectors in one pass. It returns
population moments (divided by `n`) or sample moments (divided by `n - 1`),
including the full covariance matrix. Empty streams have no mean or moments;
sample moments need at least two observations. Independently accumulated batches
of the same dimension can be merged. Each observation must have the configured
number of finite coordinates.

```rust
use sketches::vector_welford::VectorWelford;

let mut stats = VectorWelford::new(2)?;
for vector in [[1.0, 2.0], [2.0, 4.0], [3.0, 6.0]] {
    stats.add(&vector)?;
}
assert_eq!(stats.mean(), Some(&[2.0, 4.0][..]));
assert_eq!(stats.sample_variance(), Some(vec![1.0, 4.0]));
assert_eq!(stats.sample_covariance().unwrap()[0][1], 2.0);
# Ok::<(), sketches::SketchError>(())
```

The state uses `O(d²)` memory and each update costs `O(d²)` for `d` coordinates.
Covariance updates use `delta = observation - old_mean` on both sides of the
symmetric correction `(old_count / new_count) * delta * deltaᵀ`. Means are
updated separately, so the correction does not depend on a rounded new-mean
residual. This is the singleton case of the pairwise covariance update in
[Pébay, SAND2008-6212, equations (3.1) and (3.12)](https://digital.library.unt.edu/ark:/67531/metadc837537/m2/1/high_res_d/1028931.pdf#page=13).

Results follow ordinary `f64` arithmetic. Variances remain nonnegative while
the calculations remain finite; constant coordinates have zero variance, and
underflow can round a positive variance to zero. Floating-point rounding does
not guarantee a positive semidefinite full covariance matrix or identical
results across arbitrary batch partitions. Extreme finite coordinates may
overflow intermediate calculations, eventually producing infinity or NaN.
The accumulator maintains moments without sketch approximation, but does not
promise exact real arithmetic.

## Count-Min Sketch Parameters and Seeds

`CountMinSketch` is a Count-Min frequency sketch with conservative updates. It
supports non-negative updates and returns a one-sided upper estimate. The seed
selects the fingerprint and row-hash families; choose it independently of the
stream and reuse it only for sketches that may later be merged:

```rust
use sketches::count_min_sketch::CountMinSketch;

let seed = 0x510E_527F_ADE6_82D1;
let mut counts = CountMinSketch::new(0.01, 0.01, seed)?;
counts.add(&"GET /api/users", 10);
assert!(counts.estimate(&"GET /api/users") >= 10);
# Ok::<(), Box<dyn std::error::Error>>(())
```

Generic keys are fingerprinted once per operation. Applications that already
have stable, distinct `u64` identifiers can use `add_u64` and `estimate_u64` to
skip fingerprinting.

## MinMax Sketch Value Compression

`MinMaxSketch` implements the value-compression sketch from
[SketchML](https://doi.org/10.1145/3183713.3196894). It stores the minimum
ordered value mapped to each selected cell and returns the maximum selected
cell on lookup. Consequently, an estimate for an inserted key cannot exceed
the smallest value inserted for that key. Collisions can only lower it.

The value type defaults to `u8`, which fits the paper's quantile-bucket use
case, but any compact `Copy + Default + Ord` type can be used:

```rust
use sketches::minmax_sketch::MinMaxSketch;

let seed = 0x3C6E_F372_FE94_F82B;
// Width one deliberately forces a collision to expose the ordering rule.
let mut buckets = MinMaxSketch::<u8>::new(1, 4, seed)?;
buckets.insert(&"large", 17);
buckets.insert(&"small", 4);

// Each cell keeps min(17, 4), and lookup takes the maximum row candidate.
assert_eq!(buckets.estimate(&"large"), Some(4));
# Ok::<(), Box<dyn std::error::Error>>(())
```

Production widths should be much larger than one. The example forces a
collision only to make `Ord` observable: a collision retains the smaller value.
Custom value types can define what “smaller” means through their `Ord`
implementation.

The width and depth are explicit capacity/accuracy tradeoffs; the paper uses
multiple rows and sizes each row as a fraction of the number of mapped keys.
Generic keys are fingerprinted once per operation, while `insert_u64` and
`estimate_u64` accept stable identifiers directly. Compatible sketches merge
by taking cell-wise minima.

An empty selected cell makes `estimate` return `None`, which proves that a key
was not inserted. An unknown key whose cells were all occupied by other keys
can return a false-positive `Some` value, so MinMax is not a membership filter.
Reinserting a key can lower its value but cannot raise it; rebuild the sketch
when values need arbitrary replacement. Codes should increase away from the
desired conservative value. As in SketchML, signed gradients should use
separate positive and negative bucket mappings so underestimation cannot flip a
sign or increase a negative value's magnitude.

## Count Sketch Parameters and Seeds

`CountSketch::new(epsilon, delta, seed)` sizes a signed point-query sketch so
that, for one fixed non-adaptive query, the estimated frequency differs from
the true frequency by at most `epsilon * ||f without the queried item||_2` with
failure probability at most `delta` under the documented independent-hashing
model.

The constructor uses a power-of-two width of at least `8 / epsilon^2` and an
odd median depth derived from a Chernoff majority bound. Sizing uses
`-ln(delta)` and checks that bound in the log domain, accepting positive
subnormal probabilities when the dimensions and allocation fit. For example,
`epsilon = 0.9` and `delta = f64::from_bits(1)` select width 16 and depth 1803.
Sizing uses ordinary `f64` rounding. Updates return a
`Result`: Count Sketch must remain linear, so an update is rejected rather than
clamped if a counter or its sign correction would exceed the exact `i64`
range. Updates and merges preflight every affected counter, so an error leaves
the sketch unchanged.

The seed selects the fingerprint and all row hash functions. There is no global
random generator or lock:

```rust
use sketches::count_sketch::CountSketch;

// Fixed seeds are appropriate for reproducible examples and tests. In
// production, draw this independently of the stream being summarized.
let family_seed = 0xA409_3822_299F_31D0;
let mut sketch = CountSketch::new(0.05, 0.01, family_seed)?;

sketch.add(&"GET /v1/users", 120)?;
sketch.add(&"GET /v1/users", -20)?;
assert_eq!(sketch.estimate(&"GET /v1/users"), 100);
# Ok::<(), Box<dyn std::error::Error>>(())
```

Separately populated shards must use the same seed and dimensions before they
can be merged. Unrelated sketches should normally receive independently
generated seeds so they do not repeat the same unlucky collision pattern. A
seed is reproducibility/configuration data, not a secret cryptographic key.

```rust
use sketches::count_sketch::CountSketch;

// The coordinating application generates this once and gives the same value
// to every shard contributing to this aggregate.
let shared_seed = 0x1319_8A2E_0370_7344;
let mut first = CountSketch::new(0.05, 0.01, shared_seed)?;
let mut second = CountSketch::new(0.05, 0.01, shared_seed)?;
first.add(&"alpha", 40)?;
second.add(&"alpha", 2)?;
first.merge(&second)?;
assert_eq!(first.estimate(&"alpha"), 42);
# Ok::<(), Box<dyn std::error::Error>>(())
```

Applications with stable 64-bit item identifiers can use `add_u64` and
`estimate_u64` to bypass generic `Hash` fingerprinting:

```rust
use sketches::count_sketch::CountSketch;

let mut sketch = CountSketch::new(0.05, 0.01, 0x243F_6A88_85A3_08D3)?;
sketch.add_u64(42, 7)?;
assert_eq!(sketch.estimate_u64(42), 7);
# Ok::<(), Box<dyn std::error::Error>>(())
```

## Constructor Sizing and Reservations

Bloom, Cuckoo, Reservoir and Space-Saving constructors return
`SketchError::InvalidParameter` when their requested backing storage cannot be
represented or reserved. Storage is reserved before an initialized sketch is
returned. Reservoir uses the element type's actual container layout; zero-sized
elements support capacities up to `usize::MAX` without a backing allocation.

Bloom's `optimal_bit_len` and `optimal_num_hashes` are nonallocating
recommendations calculated with ordinary `f64` arithmetic. They return errors
when their rounded formulas exceed `usize` and `u32`, respectively, rather than
clipping the dimensions. A representable recommendation does not establish that
its backing storage fits available memory; construction performs that separate
reservation. The target false-positive rate retains its existing statistical
sizing meaning.

Space-Saving also reserves every temporary and rebuilt buffer in `merge`
fallibly. Reservation errors leave both summaries unchanged; the replacement
is committed only after reconstruction succeeds. Its constructor reserves the
lookup table and counter arena, while later insertions grow count buckets and
allocate items. Other operations, including cloning and materialized queries,
retain their existing allocation behavior.

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

## UltraLogLog Estimators and Merge Contract

`UltraLogLog` is implemented separately from `HyperLogLog`; their register
states are not interchangeable. It follows [Ertl's UltraLogLog paper](https://arxiv.org/abs/2308.16862)
and uses one byte per register. The default optimal-FGRA estimator has
asymptotic relative standard error `0.78224 / sqrt(m)`. The explicit
`estimate_mle()` path uses the bias-reduced maximum-likelihood estimator with
asymptotic relative standard error `0.76086 / sqrt(m)`. These correspond to
about 24% and 28% lower state size, respectively, than six-bit HLL at equal
asymptotic error.

UltraLogLog can combine sketches with different precisions. Calling
`low_precision.merge(&high_precision)` exactly reduces the source during the
merge. The reverse direction returns an incompatibility error so that a
receiver is never silently downsized. Use `left.merged(&right)` when the result
should automatically use the smaller precision. As with every hash-based
distinct counter, raw values passed to `add_hash()` must be uniformly
distributed high-quality 64-bit hashes.

`state()` borrows the serialized bytes and `into_state()` transfers ownership.
`from_state()` consumes an owned byte vector, infers precision from its length,
and validates every register before constructing the sketch. The length must
be `2^p` for `p` in `[3, 26]`. For `minimum = 4*p - 4`, the legal bytes are zero,
`minimum`, `minimum + 4`, `minimum + 6`, and all bytes at least `minimum + 8`.
Other bytes claim observations or predecessor flags below the smallest possible
rank and return `SketchError::InvalidParameter`; valid bytes are preserved.
For example, precision three accepts `0`, `8`, `12`, `14`, and `16..=255`,
and rejects `9`, `10`, `11`, `13`, and `15`:

```rust
use sketches::ultraloglog::UltraLogLog;

let restored = UltraLogLog::from_state(vec![14; 8])?;
assert_eq!(restored.state(), &[14; 8]);
assert!(UltraLogLog::from_state(vec![9; 8]).is_err());
# Ok::<(), sketches::SketchError>(())
```

Import validation takes `O(register_count)` time and accepts legal saturated
bytes. A fully saturated sketch can still return an infinite cardinality
estimate.

UltraLogLog also implements `JacardIndex` and provides
`intersection_estimate()` and `jaccard_index()`. These use the default FGRA
cardinality estimator and inclusion-exclusion; they are not a specialized joint
UltraLogLog estimator. Inputs with different precisions are first evaluated at
their smaller common precision. The lower cardinality variance of UltraLogLog
helps but does not fix the subtraction instability described below: small
intersections can still be dominated by error from the much larger input and
union estimates.

ULL and HLL intersection and Jaccard methods return `Result`. They require
finite cardinalities for both inputs and their union at the comparison
precision; otherwise they return `SketchError::EstimateUnavailable`. This
includes saturated self-comparisons and empty/saturated comparisons. Use
`intersection_estimate(&other)?` to propagate comparison errors. Cardinality and
union estimates can still return infinity for legal saturated states. Finite
empty-set conventions and feasibility clamping are unchanged.

```rust
use sketches::{SketchError, ultraloglog::UltraLogLog};

let saturated = UltraLogLog::from_state(vec![255; 8])?;
assert!(saturated.estimate().is_infinite());
assert_eq!(
    saturated.intersection_estimate(&saturated),
    Err(SketchError::EstimateUnavailable),
);
# Ok::<(), SketchError>(())
```

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

## MinHash Signature Sizing

`MinHash::new(k)` selects an explicit positive signature width. Under the ideal
independent-component model, the Jaccard estimator has standard error
`sqrt(J * (1 - J) / k)`, maximized at `J = 0.5` with `0.5 / sqrt(k)`.

`MinHash::with_error_rate(target)` is a convenience constructor for a finite
positive target. It starts from `ceil((0.5 / target)^2)` and checks the reported
`worst_case_standard_error()`, increasing the width if necessary. The returned
sketch satisfies `worst_case_standard_error() <= target` without a tolerance.
Calculations use ordinary `f64` rounding, and the width is conservative rather
than guaranteed mathematically minimal. Targets at least `0.5` select one
component; unrepresentable or unallocatable widths return `InvalidParameter`.

The check runs during construction. Each extra component retains an additional
seed and signature word (16 bytes of payload) and adds component work to updates,
comparisons and merges. Run `cargo bench --locked --offline --bench minhash` for
constructor and operation timings, including the width-18 rounding boundary.

## MinHash LSH Candidate Model

`MinHashLshIndex` uses classical MinHash banding. If a signature is divided
into `b` bands of `r` rows and two sets have Jaccard similarity `s`, the ideal
independent-MinHash model gives the candidate probability:

```text
1 - (1 - s^r)^b
```

The index exposes this curve through `candidate_probability` and its inverse
through `similarity_for_candidate_probability`. For example, 128 components
split into 32 bands of 4 rows select a pair with similarity `0.5` with modeled
probability about `0.873`.

Banding is a probabilistic candidate filter. `query_top_k` ranks only items that
match the query in at least one band; it does not scan every indexed signature
and therefore does not guarantee the global top `k`. MinHash signatures use the
classical multiple-hash construction in this crate. One-permutation hashing and
densification are not implemented.

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

KLL scalar queries and nonempty batches require at most `2^52` observations;
larger counts return `SketchError::ObservationLimitExceeded { limit: 1 << 52 }`
before allocating or sorting query state. The boundary is inclusive and applies
to endpoint queries too. Query values are validated first; an empty batch always
returns an empty vector. KLL ingestion and merging still support exact `u64`
counts. This conservative query limit keeps count conversion exact and preserves
ordinary `f64` product rounding, including the established small-sample ranks.

t-digest's centroid means and interpolated quantiles remain finite across the
complete finite `f64` input range, including mixtures of `-f64::MAX` and `f64::MAX`.
Additions are accumulated in an ordered buffer of roughly `10 * compression`
entries and batch-merged with the compressed centroids. Quantile queries merge
those two ordered views while reading, so they neither clone nor sort the
centroid state.

`TDigest` tracks observation counts and centroid weights as exact `u64`
integers. Its `add(value)` and `merge(&other)` methods return `Result` and reject
counts beyond `u64::MAX` with `SketchError::ObservationCountOverflow` before
changing any state. Non-finite additions are ignored and return `Ok(())`,
including at the count limit. Centroid means, compression decisions and query
ranks still use rounded `f64` arithmetic; exact counts do not promise distinct
adjacent ranks above `2^53`.

```rust
use sketches::tdigest::TDigest;

let mut digest = TDigest::new(100.0)?;
digest.add(10.0)?;
digest.add(20.0)?;
assert_eq!(digest.count(), 2);
# Ok::<(), sketches::SketchError>(())
```

KLL queries build a sorted weighted view of the retained samples. When several
quantiles are needed from the same sketch, use `KllSketch::quantiles(&queries)`
to allocate and sort that view once and answer every target rank in one scan.
Results are returned in the same order as the input queries.

`KllSketch::with_error_rate_and_failure_probability(rank_error, p)` selects
`k` from the basic construction's bound for one fixed quantile query:
`2 * exp(-(4/27) * rank_error^2 * k^2) <= p`. Both parameters must be finite
and strictly between zero and one. Sizing evaluates `ln(2/p)` as
`ln(2) - ln(p)` and checks the rounded candidate in the log domain, so positive
subnormal probabilities are supported when `k` fits in `usize`. With
`rank_error = 0.1`, `p = 1e-308` selects `k = 693` and
`p = f64::from_bits(1)` selects `k = 710`. Sizing uses ordinary `f64` rounding
and retains the existing single-query statistical contract and seed policy.

### KLL randomness and merging

Each `KllSketch` owns its compaction random-number state. The crate does not use
a global seed or coordinate separate sketches. `KllSketch::new(k)` uses a fixed
default seed and is deterministic, which is convenient for a standalone sketch.

When sketches are populated independently and may later be merged, the caller
should generate a different seed for each sketch and use `with_seed`:

```rust
use sketches::kll::KllSketch;

// In an application these come from a caller-owned RNG or are reproducibly
// derived from a master seed and stable shard identifiers.
let mut first = KllSketch::with_seed(200, 0xA11C_E001)?;
let mut second = KllSketch::with_seed(200, 0xA11C_E002)?;

first.add(10.0);
second.add(20.0);
first.merge(&second)?;
# Ok::<(), sketches::SketchError>(())
```

Seeds are not merge-compatibility identifiers and do not need to match.
Different seeds prevent independently built shards from making correlated
compaction choices. A shared RNG, if desired, is used only by the caller to
produce initial seeds; sketches never share an RNG while processing values.

## Reservoir Sampling Contract

`ReservoirSampling::new(capacity, seed)` implements Algorithm R with an owned
sample buffer and random stream. Under the independent uniform random-word
model, each stream position has inclusion probability
`min(1, capacity / observations_seen)`. Repeated values are separate stream
positions. This is the usual pseudorandom sampling model; choose seeds
independently when independent samples are needed.

Once the reservoir is full, replacement uses rejection sampling to draw a
uniform integer in `0..observations_seen`. It discards the incomplete prefix of
the `u64` range before reducing modulo the bound, removing the bias of direct
modulo reduction. This takes fewer than two random words on average under the
model, with `O(capacity)` retained storage. The same seed and input reproduce
the same sample, though rejection can change samples produced by the older
modulo-only implementation.

The observation count is exact up to `u64::MAX`; a further `add` panics before
changing the counter, sample or RNG. `extend` commits each item in order, so
an overflow preserves its completed prefix. `clear` drops retained items and
resets the count while continuing the existing random stream. Construct a new
sampler with the original seed to replay that stream from the beginning.

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
cargo run --example count_min_sketch
cargo run --example minmax_sketch
cargo run --example count_sketch
cargo run --example space_saving
cargo run --example kll
cargo run --example tdigest
cargo run --example reservoir_sampling
cargo run --example vector_welford
```

## Validate

```bash
cargo test
cargo check --examples
```
