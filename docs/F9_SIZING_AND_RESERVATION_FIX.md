# F9: derived sizing and fallible storage reservations

Implemented on 2026-10-02 on `fix/sketch-sizing-and-reservation-errors`,
starting from `925e4388043f2d7f7b827f93384d3d935bb7f77d`. Commit `7511e2d`
repairs Bloom, Cuckoo and Reservoir; the following coherent commit repairs
Space-Saving construction/reconstruction and records final verification.

## Reproduction and selected contract

The reviewed code reproduced these outcomes on the 64-bit host:

```rust
BloomFilter::optimal_bit_len(usize::MAX, 0.01)
// Ok(18446744073709551615), while the rounded formula is about 1.76813e20.
ReservoirSampling::<u64>::new(usize::MAX, 7)
// Panics: capacity overflow.
CuckooFilter::with_parameters(1usize << 62, 6, 1)
// Panics: capacity overflow.
SpaceSaving::<u64>::new(usize::MAX)
// Panics: Hash table capacity overflow.
```

The three panic cases were caught under the default unwind profile and fail
at deterministic capacity boundaries. No enormous valid allocation was made,
and no oversized Bloom filter was constructed. Cuckoo's 6-bit packed buckets
require three bytes each; `3 * 2^62 + 7` fits `usize` but exceeds `isize::MAX`.
The seven-byte suffix is part of the vector's required layout.

Bloom's clipping contradicts its documented formula. The other constructors
originally documented scalar validation without guaranteeing fallible storage
creation. The selected repair explicitly extends these four constructors:

- Reject unrepresentable rounded Bloom recommendations before integer casts.
- Return `SketchError::InvalidParameter` when owned constructor storage cannot
  be represented or reserved, following existing MinHash/Count conventions.
- Propagate the same failure policy through Space-Saving's reconstruction and
  merge temporary storage. A returned error preserves both live summaries.
- Preserve ordinary floating-point sizing, concrete ownership, existing data
  layouts, sketch algorithms and their established complexity bounds.

This is a local reservation contract. Allocator failures returned by standard
`try_reserve` APIs become sketch errors. It does not establish universal
recoverable OOM, change custom allocators' behavior, or make every other sketch
operation fallible. Item hashing/equality/destructors retain caller-defined
behavior. Space-Saving insertion still allocates items and grows bucket arenas;
cloning and materialized queries retain their prior allocation behavior.

## Numeric and storage boundaries

`optimal_bit_len` validates its positive count and finite rate in `(0, 1)`,
evaluates the existing formula and ceiling, then checks the exclusive
`2^usize::BITS` bound. Using a power of two handles the case where converting
`usize::MAX` to `f64` rounds upward to `2^64`. A rounded value at that bound is
rejected. The result retains the minimum of one bit and ordinary `f64`
rounding; exact real arithmetic and minimality are not promised.

The adjacent `optimal_num_hashes` conversion is also repaired: after its
existing nearest-integer rounding, a value exceeding the exactly represented
`u32::MAX` is rejected before casting. Its minimum of one probe remains.
Both helpers are pure recommendations. A representable dimension does not
establish whether its backing allocation fits available memory.

Bloom reserves its word vector and only then zeroes it. Cuckoo retains checked
multiplication and padding addition, reserves the complete padded byte vector,
and only then zeroes it. Reservoir reserves its sample buffer using `Vec<T>`'s
actual layout. Zero-sized items retain supported capacities, including
`usize::MAX`, without backing allocation. Ordinary insertion, clearing,
reservoir seeds/rejection sampling and Cuckoo relocation rollback remain.

Space-Saving reserves its lookup table and counter arena before returning an
empty owner. Merge checks compatibility before allocation, checks its combined
entry count, and reserves the combined vector fallibly. Reconstruction reserves
the new lookup/counter owner, both radix index buffers, and exactly the number
of distinct count buckets. Sharing the existing immutable item allocations
through `Arc` does not allocate new items. Linking then runs within the reserved
capacities. The receiver is assigned only after reconstruction succeeds.

The extra pass counting distinct count groups is linear. Merge remains
expected `O(capacity)` time and `O(capacity)` temporary space; unit insertion
remains expected `O(1)`. Reservations stay with their concrete storage owners;
production ownership is unchanged. Runtime timings were not measured.

## Regression coverage

Thirteen new unit tests cover:

- Bloom's original oversized helper and automatic constructor; all 1,074
  positive binary rates below one at five counts (5,370 pairs), using a wider
  integer range oracle; both sides of the `usize` and `u32` rounding boundaries;
  invalid/nonfinite inputs, minimum dimensions and a subnormal rate.
- Explicit bitmap lengths 1, 63, 64, 65, 127, 128 and 129, including zeroed
  storage, membership, clearing and reuse.
- Impossible nonzero-sized reservoir layouts, maximum zero-sized capacity,
  exact count and reuse without RNG advancement during initial filling.
- All public Cuckoo fingerprint widths 6 through 16 at impossible power-of-two
  layouts; automatic oversized sizing; suffix arithmetic overflow and a suffix
  crossing `isize::MAX`. Existing exhaustive packing and rollback tests pass.
- Impossible Space-Saving initial/rebuilt layouts; reconstruction across all
  eight count bytes with equal-count groups, shared-item reference lifetimes,
  clearing and reuse; empty reconstruction and subsequent ingestion.

Five public integration tests install a thread-local failing allocator. It
delegates pointer/layout ownership to `System`, rejects only the scoped test
thread's selected requests, and restores ordinary allocation before assertions.
The callback counter needs thread-local interior mutability because allocator
callbacks cannot borrow the caller's state. Production ownership is unchanged.

They verify five filter/reservoir construction paths at allocator refusal,
zero-sized Reservoir without backing allocation, both Space-Saving constructor
reservations, compatibility-before-allocation, and every merge reservation
failure on valid empty, underfull and full summaries. Empty merging exposes two
reservations; four nonempty fixtures expose six each, for 26 merge failure
points. Public snapshots and internal debug state remain unchanged after each
error, the donor remains unchanged, and a subsequent normal merge succeeds.
No real huge allocations or malformed live summaries are needed.

## Fresh verification

On rustc 1.98.1 and `x86_64-unknown-linux-gnu`:

- `cargo test --locked --offline`: **275 unit, 5 integration and 22 doc tests**
  pass after the complete repair.
- Focused debug and release suites: Bloom **15**, Reservoir **18**, Cuckoo
  **19**, Space-Saving **17**; all **69** affected unit tests pass in release.
- All five reservation-failure integration tests pass in debug and release.
- `cargo check --locked --offline --all-targets --all-features` and Clippy
  with the same scope and `-D warnings` pass, including examples and benches.
- `cargo check --locked --offline --target wasm32-unknown-unknown --lib --tests`
  passes compilation of the 32-bit paths. Tests were not executed on Wasm.
  This target exposes an existing unused MinHash test import at line 341;
  the 64-bit-only test using it predates this repair.
- Bloom, Cuckoo, Reservoir and Space-Saving examples run successfully.
- `cargo doc --locked --offline --no-deps` succeeds with the existing MinHash
  private intra-doc-link warning at line 32.
- Changed Rust files pass rustfmt and `git diff --check` passes. Repository-wide
  formatting still reports only the pre-existing `examples/jacard.rs` print
  formatting hunk at line 48.

The first coherent commit independently passed 272 unit, 2 integration and
22 doc tests, all-target Clippy and its three affected examples. Logs remain at
`/tmp/sketches-f9-fix-20261002`, outside Cargo targets. The implementation skill
requires `cargo clean` before and immediately after each commit, covering debug,
release, documentation and 32-bit target artifacts. Existing untracked skills
and review reports remain outside the commits. F9's review status is updated
without including the full report in this implementation.
