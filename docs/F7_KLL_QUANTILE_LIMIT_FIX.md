# F7: an explicit observation limit for KLL quantile queries

Status: **fixed** on `fix/kll-quantile-observation-limit`, based on completed
F1–F6 work at `20b81ae`. The implementation is in commit `a47a3d2`.
Verified on 2026-10-01 (America/Toronto). This record supersedes the unresolved
F7 precision-policy decision in both reviews of `ec3e68e`; other findings
retain their scope.

## Reproduced behavior and settled decision

[KLL](../src/kll.rs) stores observation counts and retained weights exactly,
but its target-rank helper converted total mass to `f64` before multiplying
by the query. Exact metadata therefore did not imply exact rank conversion.

A bounded public clone/merge fixture represents `2^52+2` zero observations
and `2^52+1` ten observations, for a total of **9,007,199,254,740,995**. Its
retained zero/ten masses match those exact counts. At `q=0.5`:

```text
exact floor(N/2):      4,503,599,627,370,497 -> zero
rounded f64 count:    9,007,199,254,740,996
rounded product rank: 4,503,599,627,370,498 -> ten
previous query:       Ok(10.0)
current query:        Err(ObservationLimitExceeded { limit: 4503599627370496 })
```

This establishes a one-rank discrepancy at enormous mass, not a failure of
KLL's statistical error bound. Public merges create the fixture with bounded
retained state; the tests do not ingest quadrillions of individual values or
fabricate inconsistent internal counts.

History confirms that `01c60ce` established exact mass consistency and
transactional `u64` overflow handling. `06bb52b` extracted the shared rank
helper while preserving scalar semantics and empty-batch success. The
current fix keeps both contracts.

The user's selected policy is **query-only rejection above `2^52`**, with the
boundary included. Integer counts convert exactly through `2^53`; `2^52`
is a deliberately conservative supported query domain, not the first count
that loses integer precision. Ordinary floating-point multiplication remains
allowed within the domain.

A full exact-binary-rational rewrite would change the established three-value
case at `q=1.0/3.0`: the represented query multiplied exactly by three is
`1-2^-54`, whose floor is zero, while the existing rounded product is one.
The fix preserves this small-sample convention. It does not promise exact real
arithmetic, eliminate compaction error or guarantee exact stream extrema.

## Shared implementation and error semantics

Both public query methods call one private count validator after validating
their query values and before allocating or sorting a retained weighted view:

```rust
const MAX_QUANTILE_COUNT: u64 = 1_u64 << 52;

fn validate_queryable(&self) -> Result<(), SketchError> {
    if self.count == 0 {
        return Err(SketchError::InvalidParameter(
            "quantile is undefined for an empty sketch",
        ));
    }
    if self.count > MAX_QUANTILE_COUNT {
        return Err(SketchError::ObservationLimitExceeded {
            limit: MAX_QUANTILE_COUNT,
        });
    }
    Ok(())
}
```

- `quantile(q)` accepts finite `q` in `[0,1]` on nonempty sketches with at
  most **4,503,599,627,370,496** observations. The cap applies to endpoints.
- `quantiles(queries)` validates every query first. Any invalid query returns
  the existing `InvalidParameter` error, including on empty or oversized
  sketches; no partial results are returned. A nonempty valid batch uses the
  same inclusive cap as the scalar method.
- `quantiles(&[])` always returns `Ok(Vec::new())`, including on empty and
  oversized sketches, because no rank calculation is requested.
- `SketchError::ObservationLimitExceeded { limit: u64 }` exposes the inclusive
  limit and displays it in the error message. It is distinct from
  `ObservationCountOverflow`, which still describes exceeding `u64::MAX`.
- Queries preserve retained samples, exact count, configuration and owned RNG
  state. The count check takes O(1) time; batch query validation still takes
  O(number of queries). Supported queries keep their existing sorting/scan
  complexity. There is no additional stored limit, cache or ownership layer.

## Ingestion and lifecycle

`add` and `merge` continue to accept valid counts above the query cap. They
retain exact observation ownership and existing `u64` overflow semantics:
finite `add` at `u64::MAX` panics before mutation, and overflowing merges
return `ObservationCountOverflow` before mutation. Nonfinite additions remain
ignored, even at the count limit. Merge compatibility and seed rules are
unchanged.

`clear` resets the count and retained samples while preserving the RNG stream.
The resulting empty sketch has its normal empty-query behavior, and subsequent
additions become queryable again. No fallible ingestion signature or secondary
count owner is needed for this selected policy.

README, module documentation, the public error and query methods describe the
limit and rounding contract. A public query doctest demonstrates exactly the
cap, one more addition and the typed rejection. Existing examples and benchmarks
already propagate query errors and require no signature migration.

## Regression coverage and fresh verification

Seven focused boundary tests cover the selected policy and its adjacent
contracts. A public merge helper constructs large boundary counts and independently
checks exact retained mass and legal level capacities.

- An inclusive-boundary regression failed on the preceding implementation at
  `2^52+1` and passes after the shared check.
- **84 real sketch configurations** span 14 counts, two `k` values and three
  seeds. Counts include zero, small samples, immediately below/at/above the cap,
  neighbors of `2^53`, the original fixture, `2^63` and both top `u64` values.
  Eleven ordered/duplicate query entries cover endpoints, signed zero, the
  smallest positive query, thirds and adjacent median/endpoint values.
- Invalid-query cases cover NaNs, infinities and both out-of-range boundaries,
  in scalar calls and four batch layouts across five count regimes. Empty
  batches and unchanged owned state are verified in every regime.
- The original zero/ten fixture independently checks exact versus rounded
  median ranks and now rejects scalar and batch requests, including endpoints.
- Merge/add transitions across the cap, continued ingestion above it,
  nonfinite additions, donor immutability and clear/reuse retain existing
  behavior. Real `u64::MAX` state distinguishes query rejection from unchanged
  transactional merge overflow and the pre-mutation add panic.
- A separate Python integer/fraction check confirms the fixture arithmetic,
  exact conversion within the accepted count boundary and the reason to
  preserve the three-value rounded-product convention.

Fresh checks:

- `cargo test --locked --offline`: **256 unit tests and 21 doctests passed**,
  including all 30 KLL tests and the shared small-sample quantile contract.
- Focused query-limit suite: **7 tests passed in debug**.
- `cargo test --locked --offline --release kll::tests`: **30 tests passed**.
- All-target/all-feature Cargo check and Clippy with `-D warnings`: passed.
- The KLL example and public query-limit doctest: passed.
- `cargo doc --locked --offline --no-deps`: passed with the existing MinHash
  private documentation-link warning at `src/minhash.rs:32`.
- Changed Rust formatting and `git diff --check`: passed. Repository formatting
  still fails only at the existing `examples/jacard.rs` print statement, hunk
  starting at line 48; that unrelated file is unchanged.
- Independent boundary arithmetic and Markdown fences/local file links: passed.

Evidence is retained in `/tmp/sketches-f7-fix-20261001/`, outside Cargo targets.
Cargo artifacts are cleaned before and immediately after each commit. Both local
review reports have updated F7 status and remain untracked; existing user skill
files stay outside implementation commits. F8/F9 and optional simplifications
are not part of this fix.
