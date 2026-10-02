# F4: report unavailable cardinality relations at saturation

Status: **fixed** on `fix/cardinality-relation-saturation`, based on the
completed F1–F3 work at `78d56ec`. The implementation is in commit
`39749c2cce7029b25e125275b78c51d079e335cf`. Verified on 2026-10-01
(America/Toronto). This record supersedes the unresolved F4 policy in the
repository reviews of `ec3e68e`; other findings retain their scope.

## Defect and evidence

Legal saturated UltraLogLog registers can produce infinite cardinality
estimates. Previously the [shared inclusion-exclusion helper](../src/jacard.rs)
computed `infinity + infinity - infinity`, obtained NaN, then used `max(0.0)`.
Rust's maximum operation returned zero when its other operand was NaN.
Intersection became zero and Jaccard was zero divided by infinity. Both queries
therefore reported a successful finite result from unavailable cardinality
scale. HyperLogLog called the same helper at its supported saturation boundary.

The public fixture `UltraLogLog::from_state(vec![255; 8])` compared with itself
returned intersection `0.0` and Jaccard `Ok(0.0)`. It also occurs with saturation
constructed by 24 public raw-hash updates at precision three. Those deliberately
chosen hashes exercise a legal state boundary, rather than ordinary uniform-hash
accuracy. A fresh regression failed against the original implementation because
self-Jaccard did not return an error; its baseline log is retained.

## Settled contract

- Inclusion-exclusion requires finite cardinalities for both operands and their
  union at the comparison precision. Any nonfinite required estimate returns
  the concrete `SketchError::EstimateUnavailable` error.
- The shared helper validates availability before subtraction, clamping and
  the empty-union convention. Positive/negative infinity and NaN are rejected.
- The rule is uniform: saturated self-comparisons and empty/saturated
  comparisons return the error. No register-equality identity exception is
  introduced. Equal summaries cannot establish equal underlying sets.
- ULL `intersection_estimate` now returns `Result<f64, SketchError>`. ULL and
  HLL intersection/Jaccard methods propagate the shared error, including through
  the `JacardIndex` trait. The error has a concrete display message and is
  distinct from invalid parameters, incompatible shapes and count overflow.
- ULL continues comparing at the smaller input precision. Required cardinalities
  are evaluated after alignment: reduction can make a previously finite
  higher-precision sketch saturated. Even at equal precision, two individually
  finite operands can have an infinite union estimate.
- HLL still requires matching precisions. Its existing incompatibility error
  precedes availability checking, including when an operand is saturated.

```rust
use sketches::{SketchError, ultraloglog::UltraLogLog};

let saturated = UltraLogLog::from_state(vec![255; 8])?;
assert!(saturated.estimate().is_infinite());
assert_eq!(
    saturated.intersection_estimate(&saturated),
    Err(SketchError::EstimateUnavailable),
);
assert_eq!(
    saturated.jaccard_index(&saturated),
    Err(SketchError::EstimateUnavailable),
);
# Ok::<(), SketchError>(())
```

## Preserved ownership, behavior and limits

Inputs remain borrowed and unchanged on either success or error. Precision
reduction and union construction retain their existing owned temporary sketches;
this fix adds no cache, fallback estimator, finite cardinality cap or ownership
layer. The availability check adds O(1) work to the existing O(register_count)
relation queries. Optional allocation reductions from review S1 remain separate.

Saturation remains valid at import, update and merge boundaries. Cardinality and
union estimates may still be infinite, and the existing integer count conversion
still saturates at `u64::MAX`. Finite feasibility clamping and empty-set conventions
remain: two empty sketches have intersection zero and Jaccard one; one empty
operand with a finite nonempty union has intersection and Jaccard zero.

Finite floating arithmetic and ordinary rounding are unchanged. This repair
does not recover overlap information from saturated summaries or improve the
documented low-overlap statistical weakness of inclusion-exclusion. MinHash's
direct similarity calculation and compatibility rules are unchanged.

## Regression coverage and fresh verification

- **512 shared-helper combinations** cover finite zero/unit/extreme values,
  both infinities, quiet NaNs with different signs/payloads and a signaling NaN.
  Fixture indices classify values independently of the production guard. All
  **485 combinations containing a nonfinite required value** return the exact
  error, including zero-union cases; finite outputs retain their bounds.
- **576 directed ULL comparisons** cover empty, ordinary, complementary partial
  saturation, imported saturation and public-update saturation at precisions
  3, 4, 5 and 8. Inherent intersection/Jaccard and trait results are checked
  after common-precision alignment, with exact operand-state preservation.
- Dedicated ULL cases prove rejection when only the union is infinite and when
  precision reduction changes a finite higher-precision estimate to infinity.
  Both directions are covered; the unchanged higher-precision sketch retains
  its finite self-comparison. Clearing saturation restores finite queries and
  permits reuse.
- **225 directed HLL comparisons** cover five states at precisions 4, 5 and 6.
  The 150 different-precision cases retain incompatibility-error precedence.
  Compatible cases check availability, finite bounds and source preservation;
  a separate case exercises finite operands with an infinite union.
- HLL saturation fixtures use legal internal maximum-rank registers, including
  rank 61 at precision four. These are tests of valid internal storage, not a
  claim that the public item-hashing API cheaply constructs such rare ranks.
- Existing cardinality saturation, valid ULL import, finite identity/empty,
  ordinary overlap, mixed-precision and MinHash trait regressions remain.
  A new ULL intersection doctest demonstrates the unavailable error, and the
  generic trait example now propagates comparison failures.

Fresh checks:

- Focused suites pass in debug and release: **7 shared relation tests,
  26 ULL tests and 20 HLL tests**.
- `cargo test --locked`: **233 unit tests and 20 doctests passed**.
- `cargo check --locked --all-targets`: passed.
- Both affected examples executed successfully after the core change:
  `cargo run --locked --example jacard` and `--example hyperloglog`.
- `cargo doc --locked --no-deps`: passed with the existing MinHash
  public-to-private documentation link warning at `src/minhash.rs:32`.
- Changed Rust formatting and `git diff --check`: passed. Repository-wide
  formatting still fails only at the existing `examples/jacard.rs` print
  statement, hunk beginning at line 48. Both unrelated files are unchanged.

Cargo uses `/tmp/sketches-f4-fix-20261001/target`; logs are retained in its parent
directory. The target is cleaned before and immediately after each commit.
Both local review reports have F4 resolution notes and remain untracked.
Existing user skill files remain outside the implementation commits.
