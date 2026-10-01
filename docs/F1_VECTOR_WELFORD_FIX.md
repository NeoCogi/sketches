# F1: symmetric covariance updates

Status: **fixed** on `fix/vector-welford-symmetric-covariance`, from reviewed
revision `ec3e68e17132e2b4d1c847e81ba571fb21dd0c0c`. The implementation is in
commit `5681c901a3e6955a5344255dda92e5041579e2f0`. Verified on 2026-10-01
(America/Toronto). This record supersedes the unresolved F1 status in the
repository reviews of that revision; it does not change other findings.

## Defect and chosen correction

The previous `add` updated the mean and then multiplied an old-mean deviation
for one coordinate by a new-mean residual for the other. Mean rounding could
therefore change the result when coordinates were permuted. Mirroring the
computed triangle made storage symmetric without repairing that dependence.

For `[0, 1]` and `[1, 1 + e]`, with `e = f64::EPSILON`, direct ingestion returned
sample covariance `[[0.5, e], [e, e²]]`, while singleton merging returned the
represented-input reference `[[0.5, e/2], [e/2, e²/2]]`. The direct result had
derived correlation greater than one. Permuting the coordinates changed it.

[VectorWelford](../src/vector_welford.rs) now computes all deviations from the
old mean, updates the means separately, and adds
`(old_count / new_count) * delta * deltaᵀ` to the centered moment matrix. This
is the singleton specialization of the pairwise covariance update in
[Pébay, SAND2008-6212, equations (3.1) and (3.12)](https://digital.library.unt.edu/ark:/67531/metadc837537/m2/1/high_res_d/1028931.pdf#page=13).
It is a multivariate extension of the scalar Welford recurrence, rather than
an assertion that the scalar algorithm maintains a covariance matrix.

Both `add` and `merge` use one private weighted-product helper. Multiplication
order depends on operand magnitudes, preserving coordinate permutation for
finite corrections. A weight at most one scales the larger operand first;
a larger weight scales the smaller operand first. This avoids selected raw
product overflow and underflow without an additional representation or cache.

## Preserved contract and limitations

- State remains owned and mutable through `&mut self`. Updates, merges and
  storage remain O(d²); observations are not retained.
- Finite diagonal corrections are nonnegative. Starting from zero and merging
  nonnegative diagonals preserves nonnegative variances when the calculations
  remain finite. Constant coordinates have zero variance; underflow can also
  round a positive variance to zero.
- Ordinary `f64` rounding remains allowed. Nonnegative diagonals do not promise
  positive semidefiniteness of the full rounded matrix, an accuracy bound for
  every stream, or identical results across arbitrary batch partitions.
- Extreme finite inputs can overflow deviations, products, means or moment
  sums and eventually produce infinity or NaN. Such intermediate failures do
  not introduce a new rejection policy.
- Dimension checks, finite-input validation and count-overflow rejection still
  precede mutation. A rejected addition or merge preserves the complete state.
- Population/sample normalization, empty and singleton behavior, cloning and
  clearing retain their existing API and semantics.

## Regression coverage and verification

The epsilon-pair regression and the large singleton-merge correction regression
both fail against the original source and pass with the correction. Additional
tests cover:

- 864 dyadic two-point configurations across six coordinate permutations,
  translations, signs and coordinate scales. Sample and population moments
  match exact represented-input references and singleton merging.
- Six-point datasets at three scale combinations, checked against an independent
  pairwise-difference covariance reference. Every batch split, including empty
  batches and both receiver orders, uses a scale-relative tolerance.
- Finite nonnegative variances, constant coordinates, squared-deviation
  underflow, and weighted-product overflow/underflow boundaries in both operand
  orders and all sign combinations.
- Complete mean, count and moment preservation after invalid inputs,
  incompatible dimensions and observation-count overflow.

Fresh checks:

- `cargo test --locked`: **210 unit tests and 18 doctests passed**.
- `cargo check --locked --all-targets`: passed.
- `cargo run --locked --example vector_welford`: passed.
- `cargo doc --locked --no-deps`: passed, with the pre-existing MinHash warning
  described below.
- `rustfmt --check --edition 2024 src/vector_welford.rs src/lib.rs`: passed.
- `git diff --check`: passed.
- `cargo fmt --all -- --check`: the sole failure remains the pre-existing
  print-statement formatting in `examples/jacard.rs`, hunk beginning at line 48.
  That unrelated file is unchanged.
- `RUSTDOCFLAGS='-D warnings' cargo doc --locked --no-deps`, with and without
  `--document-private-items`, fails on the pre-existing public link to private
  `crate::seeded_hash64` in `src/minhash.rs:32`. That unrelated module is unchanged.

Cargo commands use `/tmp/sketches-f1-fix-20261001/target`; evidence logs are in
its parent directory. The target is cleaned before and immediately after each
implementation commit. The existing review reports are updated locally with F1
resolution notes and remain untracked, preserving the original worktree scope.
