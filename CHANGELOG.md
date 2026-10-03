# Changelog

## 0.2.0 — Unreleased

This release includes incompatible API changes from 0.1.2.

### Migration from 0.1.2

| Previous API | 0.2 API or behavior |
| --- | --- |
| `minmax_sketch::MinMaxSketch` for frequency counts | `count_min_sketch::CountMinSketch`; constructors now take an explicit seed. The new `MinMaxSketch` stores ordered values with insert-min/query-max semantics. |
| `jacard` module and `JacardIndex` trait | `jaccard` module and `JaccardIndex` trait |
| `lsh_minhash` module | `minhash_lsh_index` module; `MinHashLshIndex` keeps its type name |
| `MinHash::expected_error()` | `worst_case_standard_error()`, or `standard_error_at(jaccard)?` for a specified similarity |
| `ReservoirSampling::new(capacity)` | `ReservoirSampling::new(capacity, seed)` |
| Count Sketch constructors without a seed; infallible updates | Pass a seed to `new` / `with_dimensions`, and propagate update errors with `?`. Signed counter overflow is checked before mutation. |
| `CountSketch::total_update_magnitude()` / `is_empty()` | Track stream magnitude and emptiness in the caller; these telemetry accessors were removed. |
| Weighted `SpaceSaving::add(item, count)` | Unit observations through `insert(item)`; merge combines independently accumulated summaries. |
| Infallible `TDigest::add(value)` | `add(value)?`; exact observation counts reject overflow, including during merge. |
| HLL intersection/Jaccard queries | Handle `SketchError::EstimateUnavailable` in addition to precision errors when an input or union cardinality is nonfinite. |
| KLL queries at arbitrarily large valid counts | Quantile queries return `ObservationLimitExceeded` above `2^52` observations; ingestion and merging retain their separately documented count limits. |

`SketchError` has additional variants. Update exhaustive matches accordingly.
Seeded streams, estimator values and internal sketch layouts have changed;
applications must not assume compatibility with states produced by 0.1.2.

### Added

- UltraLogLog distinct counting with FGRA and MLE estimators, validated state
  import/export, and mixed-precision merging and set queries.
- Vector Welford streaming means, variances and full covariance matrices, with
  pairwise merging and symmetric covariance updates.
- SketchML MinMax ordered-value compression and direct `u64` key operations for
  Count-Min, Count Sketch and MinMax.
- Seeded KLL constructors and batched quantile queries.
- MinHash LSH candidate-probability, inverse-model and sizing helpers.
- RV coefficient streaming multivariate matrix correlation between distinct
  vector-valued streams, with Welford-style cross-product updates, zero-allocation
  Frobenius queries and pairwise merging.

### Correctness and resource changes

- Standard RV queries use scaled Frobenius sums and a square-root ratio to avoid
  unnecessary overflow/underflow for finite retained moments; numerical state
  overflow remains subject to ordinary `f64` accumulation limits.
- Remove the pre-release `RvCoefficient::adjusted_rv_coefficient` method: feature
  covariance diagonal exclusion does not compute modified RV₂, whose observation
  Gram diagonal corrections need additional retained moment information.
- HLL uses Ertl's maximum-likelihood estimator; HLL and ULL document the
  statistical limitations and availability of inclusion-exclusion relations.
- Scalar HLL/ULL union, intersection and Jaccard queries use fixed stack
  histograms without allocating temporary register vectors. Owned ULL merge
  and downsize operations still return independent sketches.
- Space-Saving uses a linked Stream-Summary for unit observations, preserves
  merge error bounds, and documents insertion amortization, capacity-sized
  merge storage and clearing costs.
- Reservoir replacement uses rejection sampling to remove modulo bias.
- Probability and error sizing preserve tiny representable inputs; MinHash
  checks the final reported model bound, and LSH helpers avoid losing positive
  answers through overflowing or underflowing intermediate expressions.
- Bloom sizing rejects unrepresentable dimensions. Filter, reservoir,
  Space-Saving and MinMax reservations report errors at their documented
  fallible boundaries.
- Cuckoo insertion preserves state on failure, packs fingerprints and avoids
  repeated item hashing. Deletion retains its inserted-member precondition.
- t-digest uses progressive merging and exact aggregate observation counts;
  MinHash families are shared, and LSH uses compact entry handles and bounded
  top-k selection.

The publication archive includes source, examples, benchmarks, tests and their
reference data, the README, changelog, release guide and license. Repository
skills and audit documents are excluded.
