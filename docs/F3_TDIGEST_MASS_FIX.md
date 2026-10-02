# F3: enforce exact, usable t-digest mass

Status: **fixed** on `fix/tdigest-observation-count`, based on the completed F1
and F2 work at `baecff1`. The implementation is in commit
`80e0bdf89cc5ac028ccbc696a88fb817929764c3`. Verified on 2026-10-01
(America/Toronto). This record supersedes the unresolved F3 status and count
policy in the repository reviews of `ec3e68e`; other findings retain their scope.

## Defect and evidence

[TDigest](../src/tdigest.rs) previously stored both total and centroid weights
as `f64`. Its private ingestion function checked that each incoming weight was
finite, then added it to the total without checking the aggregate. `merge`
preflighted compression compatibility only. Valid public cloning and merging
could therefore produce infinite total mass while returning `Ok(())`.

The capped review fixture began with `[-1, 1]` at compression ten. After 1,023
doublings, p25 was approximately `-0.555202662744207` with 2,199 centroids.
The next doubling made total mass infinite and p25 became the maximum, `1.0`.
Subsequent merges retained 4,996 and then 9,992 centroids. Infinite mass broke
rank selection and compression normalization. These observations demonstrate
invalid mass and failed compression, not NaN quantiles or an invalid finite
sample-value interpolation formula.

Fresh regressions also exposed a failure at a much smaller count: the old code
reported `9_223_372_036_854_767_616` after 62 doublings of the two observations,
which is **8,192 below the exact `2^63` count**. The next clone merge returned
`Ok(())` despite exceeding `u64::MAX`. Separate baseline logs preserve both
failures. Regressions now reject that merge and retain the exact count.

## Settled contract and ownership

- The count owner and every compressed/buffered centroid weight are exact
  `u64` integers. Positive weights sum to `count()`, and empty state has count
  zero. A successful operation supports counts through `u64::MAX` inclusive.
- `add(value)` now returns `Result<(), SketchError>`. Finite additions beyond
  the limit return `ObservationCountOverflow`. Non-finite additions retain
  their ignored-input policy and return `Ok(())`, even at the limit.
- `merge` checks compression compatibility first, then preflights the complete
  integer count addition. Either error leaves the destination unchanged.
  Neither overflow path changes count, extrema, centroid contents, buffer
  contents or sequence state. The borrowed source is also unchanged.
- The private centroid ingestion step operates only after the public owner has
  established count capacity. During a merge it increments the count as each
  incoming centroid is ingested, so progressive compression uses the mass of
  the data present at that point. Incoming weights sum to the source count;
  every partial sum is bounded by the preflighted final count.
- Compression combines integer weights and tracks completed mass in `u64`.
  These disjoint sums are bounded by the authoritative count. Weights are
  converted to `f64` only for means, scale decisions and query interpolation.
  The largest supported count converts to a finite value near `2^64`, so
  aggregate mass cannot become infinite.

Exact integer ownership fits the unit-observation API and its existing `u64`
count result. It removes the floating aggregate rather than retaining a second
mass owner. No public weighted-update API, mass rescaling, estimator fallback,
post-query clamp, new cache or ownership layer is introduced. The example,
benchmark, shared KLL/t-digest convention test and public example now handle
the fallible `add` result. The README documents the revised contract.

## Preserved behavior and limits

Finite sample values still use the existing sign-aware convex-combination
helpers. Exact observed extrema, singleton steps and the shared empirical
inverse-CDF convention remain. Pending observations stay in the ordered buffer;
queries borrow and merge its ordered view with the compressed array without
cloning, sorting or flushing. Count access and capacity preflight remain O(1);
ingestion, queries and progressive compression keep their existing costs.

Centroid means, compression decisions and query ranks still use rounded `f64`
arithmetic. Exact counts do **not** imply exact real-number interpolation,
distinguishable adjacent ranks above `2^53`, or a stronger statistical quantile
error bound. This fix addresses mass validity and count conservation. Allocation
failure policy and unrelated sketch count contracts are outside its scope.

## Regression coverage and fresh verification

All new large-count fixtures use public unit additions, cloning and binary
merging, rather than injecting oversized private weights.

- **854 count/compression cases** cover all counts 0–255, both sides of every
  remaining binary boundary through `2^63`, and the final four `u64` counts at
  compression ten and one hundred. Subsequent unit additions and singleton
  merges retain low count bits, including above `2^53`. An independent `u128`
  sum over actual compressed and buffered storage checks mass conservation.
- **39 merge cases** cover empty/unit inputs, odd counts near `2^53`, exact
  `u64::MAX` totals and overflowing pairs at three compression settings.
  Opposite finite extremes exercise mean/interpolation arithmetic. Each of the
  30 successful nonempty cases checks 1,001 quantiles for finiteness, ordering,
  range and exact endpoints, without imposing exact large integer ranks.
- The original clone-merge trigger checks every successful doubling's mass,
  usable interior quantiles and rejection at the supported count boundary.
- Overflow tests compare every owned field, float bit patterns, pending keys
  and query results, including a destination with both compressed and buffered
  data. Source preservation and compression-error precedence are explicit.
- Finite additions at the limit fail; non-finite additions at the limit and
  after clearing remain successful no-ops. Clearing resets the count, extrema,
  buffer, compressed array and sequence, and permits reuse.
- Existing finite-extreme, small-sample, accuracy, ordering, progressive-buffer
  and read-only-query regressions remain in the focused suite.

Fresh checks:

- Focused t-digest suite: **28 tests passed** in debug and release.
- `cargo test --locked`: **223 unit tests and 19 doctests passed**.
- `cargo check --locked --all-targets`: passed.
- `cargo run --locked --example tdigest`: passed with p95 `19000.50` and p99
  `19800.50` milliseconds.
- `cargo bench --locked --bench tdigest`: completed its 200,000 additions and
  20,000 queries per compression setting (20, 100, 500). This validates the
  migrated workload; it is not a before/after performance comparison.
- `cargo doc --locked --no-deps`: passed with the existing MinHash
  public-to-private documentation link warning at `src/minhash.rs:32`.
- Changed Rust formatting and `git diff --check`: passed. Repository-wide
  formatting still fails only at the existing `examples/jacard.rs` print
  statement, hunk beginning at line 48. Both unrelated files are unchanged.

Cargo commands use `/tmp/sketches-f3-fix-20261001/target`; logs are retained in
its parent directory. The target is cleaned before and immediately after each
commit. Both local review reports have F3 resolution notes and remain untracked.
Existing skill files remain outside the implementation commits.
