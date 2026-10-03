# RV coefficient correctness and workspace fixes

Implemented on `fix/rv-coefficient-numerics-workspace` from reviewed revision
`6c0c78a4950462c5f68e49000da370cdc1ad5110`, verified on 2026-10-02
(America/Toronto). This resolves F1 and F2 of the RV review and implements the
requested preallocated deviation workspace.

## Agreed behavior

- Remove the incorrect pre-release `adjusted_rv_coefficient()` API and every
  example/test call. The accumulator provides standard RV. The old feature
  diagonal exclusion was unrelated to the claimed observation-Gram RV₂
  correction; second moments alone cannot determine the needed fourth-order
  row summaries. Removal is implemented in `712a6af`.
- Evaluate standard RV using scaled sums of squares. Avoid raw squared moments
  and norm products, so representable coefficients remain available when those
  intermediates would overflow or underflow. The query repair is in `a6169a8`.
- Construct two owned scratch vectors once, with lengths p and q. Reuse these
  vectors in every non-first addition and nonempty merge. Every merge branch,
  addition, clearing, and coefficient query performs zero heap allocations.
  Cloning and covariance exports allocate their own storage.
- Keep dimension, finite-input, and checked-count validation before any
  statistical or scratch mutation. Rejected inputs and failed merges preserve
  the entire receiver, and merges leave the donor unchanged.

Public contracts, private invariants, and ordering are documented in
[rv_coefficient.rs](../src/rv_coefficient.rs), [README](../README.md), and the
[unreleased changelog](../CHANGELOG.md). The example now reports standard RV only.

## Numerical representation

For each matrix, the query retains a temporary `(scale, sum_squares)` pair such
that its mathematical squared Frobenius norm is `scale² * sum_squares`. The scale
is the largest absolute entry, and the sum uses normalized squared ratios.
Neither the raw moment squares nor an overflowing Frobenius norm is formed.
Nonfinite entries are rejected and zero variance remains undefined.

The cross-block scale is divided by the smaller and then larger square-root
within-block scales. For exact covariance moments, this intermediate is bounded
by the larger root scale. The normalized sums are bounded by addressable matrix
lengths. Forming the square root of RV before the final square lets individually
tiny cross entries contribute to a representable subnormal coefficient. The
finite square-root ratio is clamped to [0,1] before squaring.

This retains O(p² + q² + pq) query time, O(1) workspace and zero query allocation.
It adds normalized arithmetic compared with the old raw sum-of-squares query.
Ordinary f64 accumulation still permits mean/moment overflow, tiny updates
rounding away, and differences across arbitrary batch partitions. Rounded joint
covariance is not guaranteed positive semidefinite. Queries return `None` for
insufficient count, zero variance, or nonfinite matrix/query arithmetic; very
small final coefficients can round to zero. Checked u64 counts do not imply exact
floating-point representations of those counts.

## Scratch ownership and lifecycle

`delta_x` and `delta_y` are private owned workspace, with no public statistical
meaning. After validation, `store_deviations` overwrites every scratch entry
against the original means. Mean updates then consume those deviations, and all
three centered-sum corrections consume the same unchanged deviations. This
preserves the earlier symmetric weighted-product arithmetic and avoids rounded
updated means changing covariance corrections.

An empty receiver copies the donor's statistical slices into its already owned
buffers; it does not copy or borrow donor workspace. Clones own independent
scratch. `clear()` zeros both statistics and scratch while retaining all buffer
lengths/capacities. No locks, interior mutability, shared workspace, or lazy
allocation are introduced.

The active retained float count is `p² + q² + pq + 2p + 2q`, an increase of p+q
floats for scratch. Construction has seven fallible buffer reservations rather
than five. Both new reservation failures use the existing InvalidParameter error
contract, and all earlier allocations are released on failure. No unconditional
add/merge allocation-failure path remains after successful construction.

## Regression evidence

- The finite-scale identity regression failed against the old query and passes
  for scales 1, 1e40, 1e-50, 1e100 and 1e-100 with finite nonzero moments.
- An independent pairwise-difference batch oracle verifies nonperfect RV for
  asymmetric (3,2) dimensions across 81 independent dyadic X/Y scale combinations,
  every batch split, empty batches, both receiver orders, and a block reflection.
- Valid finite synthetic moments exercise f64::MAX and minimum-subnormal matrix
  entries, opposing block scales, and aggregate subnormal RV output. Every
  nonfinite moment block returns `None`.
- The allocator regression failed before scratch reuse, observing two heap
  requests on a non-first update. It now observes zero requests for first and
  repeated additions, every empty/populated merge branch, queries, clone reuse,
  clearing, clear/reuse, and invalid-input/incompatible-merge returns. Dimensions
  include (1,1), (2,3), (4,4) and (8,2).
- Constructor failure injection covers all seven reservation positions, with a
  success after seven permitted allocations. Existing rejection/count-overflow
  tests now compare complete receiver snapshots, including scratch; donor
  preservation is also checked. All three moment matrices are compared against
  the independent batch oracle after direct and merged ingestion.

## Fresh validation

Logs, before-fix failures, and commit messages are retained outside build targets
at `/tmp/rv-coefficient-fix-20261002`.

| Command | Outcome |
| --- | --- |
| `cargo test --offline --locked` | 314 unit tests, 30 integration tests, and 23 doctests passed |
| `cargo check --offline --locked --all-targets` | Passed |
| `cargo clippy --offline --locked --all-targets -- -D warnings` | Passed |
| `cargo fmt --all --check` | Passed |
| `RUSTDOCFLAGS='-D warnings' cargo doc --offline --locked --no-deps` | Passed |
| `cargo run --offline --locked --example rv_coefficient` | Passed, 4 observations and RV 1 |
| `cargo bench --offline --locked --bench rv_coefficient` | Passed for dimensions 2, 4 and 8 |
| `git diff --check` | Passed |

Cargo builds run sequentially with
`CARGO_TARGET_DIR=/tmp/rv-coefficient-fix-20261002/target`. Both that target and the
repository target are cleaned immediately before and after every implementation
commit. The original untracked review is preserved with resolution notes; it is
excluded from the implementation commits.

## Local benchmark observation

The existing release benchmark uses 200,000 ingestions of prebuilt vectors and
100,000 repeated coefficient queries. One run at the original revision and one
run after these changes produced:

| (p,q) | Original ingest ops/s | Updated ingest ops/s | Original query ops/s | Updated query ops/s |
| --- | ---: | ---: | ---: | ---: |
| (2,2) | 16,808,196 | 27,539,712 | 132,845,172 | 47,243,127 |
| (4,4) | 13,108,850 | 14,699,460 | 76,545,472 | 17,033,491 |
| (8,8) | 5,109,419 | 5,433,513 | 20,929,268 | 4,721,505 |

These are short, single local runs, not controlled throughput guarantees.
Ingestion improved in this observation, particularly for small vectors; the
numerically safer query costs more arithmetic. The allocation-count guarantee is
established by tests independently of benchmark timing. No performance claim is
made for larger dimensions, alternate hardware, or arbitrary workloads.
