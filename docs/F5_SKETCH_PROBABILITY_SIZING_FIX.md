# F5: stable sizing for tiny Count Sketch and KLL probabilities

Status: **fixed** on `fix/sketch-probability-sizing`, based on completed F1–F4
work at `4824e0a`. Count Sketch is repaired in commit `9d210f4`; the KLL repair
and this record form the following commit. Verified on 2026-10-01
(America/Toronto). This record supersedes unresolved F5 in both reviews of
`ec3e68e`; other findings retain their scope.

## Defect and evidence

Both constructors validated finite probabilities in `(0,1)`, then formed a
reciprocal before taking its logarithm. `1/delta` in Count Sketch and `2/p` in
KLL overflow for sufficiently small positive probabilities even though their
logarithms and the final dimensions fit. Infinity was reported as an
unrepresentable dimension, rejecting modest valid configurations.

Fresh regression tests failed against each prior sizing implementation. The
public constructors previously rejected these requests while explicit sizing
with the listed dimensions succeeded:

| Request | Former error | Repaired automatic size |
| --- | --- | --- |
| Count, `epsilon=0.9`, `delta=f64::from_bits(1)` | Unrepresentable depth | Width 16, depth 1803 |
| KLL, `rank_error=0.1`, `p=1e-308` | Unrepresentable `k` | `k=693` |
| KLL, `rank_error=0.1`, `p=f64::from_bits(1)` | Unrepresentable `k` | `k=710` |

The smallest positive probability has finite `ln(p)` of approximately
`-744.4400719213812`, while both reciprocals are infinite. Count needs 28,848
`i64` counters, about 225 KiB excluding row metadata. KLL's constructors create
an empty hierarchy rather than eagerly allocating all future retained samples.

History confirms two local defects: `6d4345d` retained Count's reciprocal
while introducing its current majority formula and checked allocation;
`4355759` introduced KLL's `2/p` sizing. `cf1fe4f` added independent normal-range
answers and statistical trials, but missed this arithmetic boundary.
`7395117` already used `-ln(delta)` in MinCount. Its different statistical proof
and conservative updates are not copied into Count or KLL.

## Settled numerical contract

Finite probabilities strictly between zero and one remain valid, including
positive subnormals, subject to representable dimensions and existing resource
limits. Sizing must not reject them solely because a reciprocal is unavailable.
Use logarithms directly when inverting exponential probability bounds, and use
log-domain comparisons when correcting rounded integer candidates.

[Count Sketch](../src/count_sketch.rs) retains its power-of-two width of at
least `8/epsilon^2` and odd majority depth:

```rust
let log_delta = delta.ln();
let minimum_depth = -2.0 * log_delta / DEPTH_DENOMINATOR;
// Keep the dimension checks, ceiling and checked odd adjustment.
// Correct insufficient candidates with checked increments of two:
// -(depth as f64) * DEPTH_DENOMINATOR / 2.0 <= log_delta
```

[KLL](../src/kll.rs) retains the basic fully mergeable single-query bound with
`C = c^2 * (2c-1)`, where `c=2/3` and mathematically `C=4/27`:

```rust
let log_threshold = std::f64::consts::LN_2 - failure_probability.ln();
let error_bound = (log_threshold / ERROR_BOUND_CONSTANT).sqrt() / k as f64;
// After checked sizing and ceiling, require:
let scaled_error = rank_error * k as f64;
let sufficient =
    ERROR_BOUND_CONSTANT * scaled_error * scaled_error >= log_threshold;
```

KLL increments an insufficient candidate with `checked_add`, returning the
existing unrepresentable-`k` error if that fails. Multiplying `rank_error * k`
before squaring avoids separately underflowing the rank-error square or
overflowing an integer `k^2`. Neither algorithm exponentiates the tiny bound
when deciding whether a rounded candidate is sufficient.

These calculations use ordinary `f64` logarithms, constants and rounding.
They do not promise exact real minimum dimensions or stronger statistical,
independence or cryptographic guarantees. They preserve the existing
single-fixed-query contracts; simultaneous-query budgeting remains the
caller's responsibility. The fix does not empirically validate an astronomically
small failure rate through finite sampling.

## Preserved errors, ownership and behavior

Invalid probabilities and invalid error targets still return the existing
`SketchError::InvalidParameter` messages. Tiny error targets that require
unrepresentable widths or `k` still fail. Count's checked table multiplication
and fallible allocation remain unchanged. Explicit dimension constructors and
all public signatures are unchanged; no consumer migration is needed.

Count retains caller-owned hash-family seeds, seed/dimension compatibility,
signed counters, transactional updates/merges and clear/reuse. KLL retains
instance-owned compaction randomness, fixed default seeds, caller-generated
seeds for independent shards, compaction, compatible merging, exact retained
mass and scalar/batch quantile behavior. No global probability service, RNG
allocator, cache or additional ownership layer is introduced.

## Regression coverage and fresh verification

- **28 Count and 29 KLL literal known answers** cover the smallest positive
  probability and its neighbors, both reciprocal-overflow boundaries, the
  normal/subnormal transition, ordinary probabilities and the value immediately
  below one. Additional pairs bracket several odd-depth and integer-`k`
  thresholds, including subnormal probabilities.
  A further KLL case requires `k=20` where the inverse quotient can round to
  19, directly exercising the log-domain correction after integer rounding.
- An independent Python Decimal evaluation at **100-digit precision** uses
  the exact binary inputs, `ln(16/7)` and `4/27`. It confirms all 57 integer
  answers and verifies both their sufficient bound and the failure of the
  preceding legal size. Tests store literal answers rather than using the
  production helper or reciprocal arithmetic as their expected calculation.
- Count publicly allocates every representable binary probability `2^-e` for
  `e=1..1074`: **1,074 constructions**, with finite odd monotonic depth.
  KLL covers the same full exponent range at rank errors 0.01, 0.1 and 0.9:
  **3,222 constructions**, with monotonic `k`, empty state and independent
  endpoint sizes. These are exhaustive over binary powers, not all `f64` values.
- Both KLL probability wrappers preserve explicit/default RNG initialization
  for every literal case. Tiny-probability stream tests compare automatic and
  explicit construction through ingestion, merging, queries and clear/reuse;
  KLL exercises real compaction and exact retained mass.
- Invalid values, both signed zeros, infinities, NaN and genuinely too-large
  dimensions retain their errors. Existing signed-update, allocation,
  compaction and many-seed statistical tests remain.

Fresh checks:

- Debug and release focused suites: **24 Count/MinCount tests** and
  **23 KLL tests** passed. The Count substring filter includes the 11 unchanged
  MinCount tests as adjacent numerical-contract coverage.
- `cargo test --locked --offline`: **240 unit tests and 20 doctests passed**.
- `cargo check --locked --offline --all-targets`: passed.
- `cargo clippy --locked --offline --all-targets --all-features -- -D warnings`:
  passed.
- The Count Sketch and KLL examples executed successfully.
- `cargo doc --locked --offline --no-deps`: passed with the existing MinHash
  public-to-private documentation link warning at `src/minhash.rs:32`.
- Changed Rust formatting and `git diff --check`: passed. Repository-wide
  formatting still fails only at the existing `examples/jacard.rs` print
  statement, hunk beginning at line 48; the unrelated example is unchanged.

Logs and the independent fixture checker are retained outside Cargo targets in
`/tmp/sketches-f5-fix-20261001/`. Cargo targets are cleaned before and immediately
after each commit. Both local review reports have F5 resolution notes and remain
untracked; existing user skill files are excluded from implementation commits.
