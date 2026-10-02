# F8: conservative MinHash sizing against the reported error

Status: **fixed** on `fix/minhash-rounded-error-sizing`, based on completed
F1–F7 work at `eeafccc`. Implementation and the complete before/after benchmark
are in commit `58eb5bf` (2026-10-01). Final verification: 2026-10-02,
America/Toronto. This record supersedes unresolved F8 in the second review of
`ec3e68e`; F9 and optional simplifications retain their scope.

## Reproduced boundary and selected contract

[MinHash](../src/minhash.rs) previously selected
`ceil((0.5 / target)^2)` without verifying the result. At target bits
`0x3fbe2b7dddfefa66`, rounded inversion produces `17.999999999999996` and
selects 18 components. The reported error then exceeds the target:

```text
target:                     0.11785113019775792
previous width:             18
previous reported error:    0.11785113019775793
corrected width:            19
corrected reported error:   0.11470786693528087
```

A fresh public-constructor regression failed on the previous implementation
with those values. Independent rational arithmetic confirms that 18 fails
`4*k*target^2 >= 1` and 19 passes for this particular target. The difference
is tiny statistically, but the constructor contradicted its reported bound.

`39964f3` established the correct independent-component model, explicit owned
seed/signature storage and fallible allocation. Those changes remain. Its
minimal-width test permitted `16 * EPSILON` tolerance, which hid the boundary
contradiction while public prose promised strict mathematical minimality.

The user selected ordinary-floating-point convenience sizing after discussing
common explicit-width APIs and runtime cost. The settled postcondition is:

```rust
sketch.worst_case_standard_error() <= target
```

This compares the reported `f64` model value without a tolerance. Widths are
conservative; exact ideal-model inequality and exact mathematical minimality
for every represented target are not promised. The old minimality prose and
tolerant test are superseded. An exact mantissa/exponent comparison is outside
this selected repair.

## Shared arithmetic and validation

One private function evaluates `0.5 / sqrt(k)` for both public reporting and
candidate validation. After rejecting invalid or unrepresentable dimensions,
sizing applies the postcondition before allocating:

```rust
let mut num_hashes = (required as usize).max(1);
while max_standard_error(num_hashes) > target {
    num_hashes = num_hashes.checked_add(1).ok_or(
        SketchError::InvalidParameter("requested standard error requires too many hashes"),
    )?;
}
```

Finite positive targets remain valid. Targets at least `0.5`, including
`f64::MAX`, select one component. Nonfinite/nonpositive inputs and unavailable
dimensions retain their existing errors. Constructors retain fallible storage
reservation and its existing `InvalidParameter` allocation error.

The check affects only construction. Owned seeds and signatures, insertion,
Jaccard estimation, merging, family compatibility and clear/reuse keep their
existing implementation. `MinHash::new(k)` remains the explicit-width API.
A corrected width of 19 has 16 additional bytes of seed/signature payload and
one extra component of work compared with width 18. Historical 18-component
sketches retain the existing width-mismatch rejection against corrected ones.

Sizing can conservatively select a width above one already satisfying the
rounded accessor: target `0x3fd6a09e667f3bcc` equals the reported width-2 error,
but rounded inversion selects three. This is permitted by the selected rule.
For very large representable dimensions, adjacent integers can share a float
value and several checked increments may be required. Correction count is not
promised to be zero or one. Construction costs O(k) initialization plus the
correction steps; each check uses ordinary floating-point operations and stores
no additional state.

README and the constructor/accessor/helper docs describe the selected contract.
A public doctest demonstrates the original boundary and strict reported
compliance. Existing MinHash and LSH consumers already propagate errors.

## Before/after benchmark

The permanent [benchmark harness](../benches/minhash.rs) uses public constructors
and operations, prints actual selected widths and retains individual TSV samples.
Run it with:

```sh
cargo bench --locked --offline --bench minhash
```

For the comparison, the identical harness and benchmark manifest stanza were
compiled against unmodified source at `eeafccc` and the repair in `58eb5bf`:

```sh
cargo bench --locked --offline --bench minhash --no-run
```

The two release executables ran serially on pinned CPU 2, alternating
before/after and after/before over **16 paired rounds**. Each workload had
one discarded full-size warmup and seven measured samples per invocation:
**112 measured samples per revision/workload**. The table reports the median
of the 16 per-run medians, in nanoseconds per operation.

Environment: Intel Core Ultra 7 155H, Linux x86_64, rustc 1.98.1 / LLVM 22.1.8,
default release profile and no custom Rust flags. The existing `powersave`
governor remained; frequency and turbo were not controlled. Construction
includes allocation, initialization and destruction. Other setup/population
occurs outside the clock, keys are `u64`, and merge measures repeated
idempotent merging of compatible populated signatures.

| Workload | Width before → after | Before ns/op | After ns/op | Change |
| --- | ---: | ---: | ---: | ---: |
| `construct/new18` | 18 | 41.380 | 41.454 | +0.18% |
| `construct/error0.5` | 1 | 30.521 | 32.504 | +6.50% |
| `construct/error0.1` | 25 | 53.695 | 55.932 | +4.17% |
| `construct/error0.01` | 2500 | 3015.510 | 3002.956 | -0.42% |
| `construct/boundary18` | 18 → 19 | 47.395 | 54.333 | +14.64% |
| `add/new18` | 18 | 189.851 | 190.466 | +0.32% |
| `add/error0.05` | 100 | 1017.950 | 1037.993 | +1.97% |
| `add/boundary18` | 18 → 19 | 191.454 | 203.154 | +6.11% |
| `jaccard/new18` | 18 | 6.697 | 6.483 | -3.20% |
| `jaccard/error0.05` | 100 | 22.984 | 22.822 | -0.71% |
| `jaccard/boundary18` | 18 → 19 | 6.803 | 6.927 | +1.83% |
| `merge/new18` | 18 | 9.779 | 9.765 | -0.14% |
| `merge/error0.05` | 100 | 58.270 | 60.884 | +4.49% |
| `merge/boundary18` | 18 → 19 | 9.709 | 10.503 | +8.18% |

For unchanged widths 1 and 25, measured construction differences were about
**+2.0 ns** and **+2.2 ns**. The corrected 18→19 case measured **+6.9 ns** in
construction and **+11.7 ns** per insertion, including the extra component.
The explicit-width-18 controls measured +0.18% construction and +0.32% insertion.
Small changes in comparison/merge and large-width construction overlap timing
variation and should be interpreted cautiously.

These are shared-host wall-clock observations. For example, width-25 constructor
per-run medians ranged 52.3–80.9 ns before and 53.6–98.3 ns after; insertion
controls ranged 179.3–298.0 ns before and 180.3–344.1 ns after. An eight-pair
CPU-0 pilot showed substantially larger timing variation; both evidence sets
are retained. The quoted CPU-2 measurements are observational, not a universal
latency guarantee or proof of a statistically resolved small difference.

The implementation commit message includes the complete table, baseline,
hardware/toolchain, workload definitions, sample counts and limitations.
The benchmark has no third-party measurement dependency and can be rerun on
another host. Production behavior is unchanged after the benchmark; the second
commit adds documentation and regression coverage.

## Boundary, ownership and consumer verification

- The original exact-bit constructor regression now selects 19 and checks the
  reported error strictly. Existing normal targets retain known conservative
  widths, with the former tolerance removed.
- **1,536 public constructor cases** cover the lower neighbor, reported edge
  and upper neighbor for every width 1–512. A lower-neighbor target must select
  more components than that edge width; seed/signature shape and empty state
  remain consistent.
- **All 2,098 binary exponents**, from the minimum subnormal to `2^1023`, are
  checked through the pure sizing owner without constructing enormous buffers.
  Integer exponent arithmetic supplies expected dimensions and availability.
  An independent Python arbitrary-precision checker validates these oracles.
- Invalid values, signed zeros, subnormals, infinities, NaNs and high finite
  targets retain their selected validation behavior. Conservative oversizing
  is explicitly tested instead of imposing an exact-minimum assertion.
- A valid 64-bit dimension fixture at target `0x3e1e2b7dddfefa66` crosses a
  floating plateau; one correction is insufficient. The current helper takes
  25 increments before reported compliance, independently reproduced in Python.
  No large signature is allocated.
- Corrected-width sketches match independently owned explicit-width-19 seeds
  and signatures. Tests cover insertion, duplicate inputs, cloning, donor
  immutability, merge equivalence, incompatible-width rejection without mutation,
  clear/reuse and retained seed ownership. Existing hash-family, similarity,
  LSH and shared-trait tests continue to pass.

Fresh checks:

- `cargo test --locked --offline`: **262 unit tests and 22 doctests passed**.
- Focused MinHash/LSH/trait suite: **46 tests passed in debug and release**.
- All-target/all-feature Cargo check and Clippy with `-D warnings`: passed.
- MinHash and MinHash LSH examples, including the new sizing doctest: passed.
- `cargo doc --locked --offline --no-deps`: passed with the existing MinHash
  private-link warning at `src/minhash.rs:32`.
- Changed Rust formatting and `git diff --check`: passed. Full formatting still
  reports only the existing `examples/jacard.rs` print statement, hunk at line 48.
- Independent arithmetic evidence, benchmark aggregation consistency and Markdown
  fences/local links: passed.

Evidence, raw TSV samples, source/binary checksums, conditions and the paired-run
script are retained in `/tmp/sketches-f8-fix-20261001/`, outside Cargo targets.
Cargo targets are cleaned before and immediately after each commit; saved
before/after benchmark executables were removed before the implementation
commit. Local review reports and user skill files remain untracked and outside
implementation commits. Only the second review contains F8 and its status was
updated; F9 remains unresolved.
