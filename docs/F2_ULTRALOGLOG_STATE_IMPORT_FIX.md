# F2: validate reachable UltraLogLog state

Status: **fixed** on `fix/ultraloglog-state-validation`, based on the completed
F1 branch at `eb06464`. The import correction is in commit
`26963dea65b5523689b711d52b3756e06b34be7a`. Verified on 2026-10-01
(America/Toronto). This record supersedes the unresolved F2 status in the
repository reviews of `ec3e68e`; other unresolved findings retain their scope.

## Defect and import invariant

[UltraLogLog::from_state](../src/ultraloglog.rs) previously checked only a
precision-dependent minimum nonzero register value. The encoding also has
forbidden predecessor flags near that minimum. At precision three, it admitted
bytes `9`, `10`, `11`, `13` and `15`, which no raw-hash update can produce.

Observations set bits at or above `precision - 1`. A byte records its highest
observation and two predecessor flags. The smallest rank cannot have either
flag; the next rank permits only its immediate predecessor. For
`minimum = 4 * precision - 4`, reachable bytes are exactly:

```text
0                         canonical empty state
minimum                   smallest rank, no predecessor flags
minimum + 4, minimum + 6  next rank, only the permitted predecessor flag
minimum + 8 through 255   both predecessor flags are permitted
```

Malformed bytes violated estimator assumptions. FGRA could ignore them at its
lower boundary and return infinity. For MLE, eight registers containing seven
zeros and one byte `9` generated a wrapped scaled sum of zero. The valid-state
shortcut then selected infinity if the malformed byte was first, and zero if
it was last, despite identical histograms. The documented error contract was
therefore not established at the public import boundary.

Import now uses one private, precision-aware reachability predicate before
constructing the sketch. Invalid lengths and unsupported precisions are
checked before examining register bytes. Malformed state returns
`SketchError::InvalidParameter`; it is not normalized or passed to a permissive
estimator fallback. The existing valid-state estimator shortcuts are retained.

## Ownership, cost and preserved behavior

- `from_state` still consumes an owned `Vec<u8>` and transfers it into the
  sketch on success without changing the bytes. Public `state()` remains an
  immutable borrowed slice and `into_state()` transfers ownership.
- Import remains O(register_count), with no extra register representation,
  allocation, ownership layer or estimator cache.
- The precision-independent byte encoding and supported precision `[3, 26]`
  are unchanged. Legal empty, small, ordinary and saturated states are accepted.
- Fully saturated states retain infinite cardinality estimates. F2 does not
  change the separate F4 saturation behavior of intersection/Jaccard helpers.
- Hash updates, merging, downsizing, clearing, estimator selection and Hash4j
  known answers retain their contracts. Raw hashes still need good uniformity
  for statistical accuracy; import validation does not assess hash quality.

## Regression coverage and fresh verification

The forbidden-flag regression fails against the original importer at precision
three, byte `9`, register zero, and passes with the correction. Coverage includes:

- All **6,144 precision/byte combinations** against an independent observation-bit
  reachability model, using fixed-size scratch storage rather than allocating
  large high-precision sketches. The oracle uses neither the production
  validity predicate nor its pack/unpack functions.
- Raw-hash construction of every reachable register at precisions 3–6, followed
  by **1,024 public import cases** covering every byte. Accepted bytes are
  preserved; empty and saturated estimator boundaries are checked explicitly.
- Every forbidden boundary flag at precisions 3–6, both first and last register
  positions, and entirely malformed states.
- Exact FGRA and MLE equality after cyclic register permutations and reversal
  for legal boundary, mixed, empty and saturated states.
- Restoration of existing Hash4j known-answer vectors, updates after restoration,
  same/mixed-precision merging, downsizing, clearing and failed-merge preservation.
- Invalid lengths and too-small precisions, verifying that their established
  errors precede malformed-byte errors. A public restoration doctest also
  demonstrates a legal boundary state and rejection of byte `9`.

Fresh checks:

- Focused UltraLogLog suite: **22 tests passed**.
- `cargo test --locked`: **216 unit tests and 19 doctests passed**.
- `cargo check --locked --all-targets`: passed.
- `cargo doc --locked --no-deps`: passed with the existing public-to-private
  MinHash documentation link warning at `src/minhash.rs:32`.
- `rustfmt --check --edition 2024 src/ultraloglog.rs` and `git diff --check`: passed.
- `cargo fmt --all -- --check`: the sole failure remains the existing
  `examples/jacard.rs` print-statement formatting, hunk beginning at line 48.
  Both unrelated modules are unchanged by F2.

Cargo commands use `/tmp/sketches-f2-fix-20261001/target`, with logs in its parent
directory. The target is cleaned before and immediately after each commit.
Existing local review reports have F2 resolution notes and remain untracked;
user skill files are preserved outside the implementation commits.
