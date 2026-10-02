# F6: unbiased bounded replacement draws for reservoir sampling

Status: **fixed** on `fix/reservoir-unbiased-selection`, based on completed
F1–F5 work at `7074cab`. The implementation is in commit `32ad1af`.
Verified on 2026-10-01 (America/Toronto). This record supersedes unresolved F6
in both reviews of `ec3e68e`; other findings retain their scope.

## Defect and reproduced boundary

[Reservoir sampling](../src/reservoir_sampling.rs) implemented Algorithm R's
replacement draw with `next_u64() % seen`. The documented contract is a uniform
sample of stream positions, which requires an unbiased index in `0..seen` under
the conventional independent uniform random-word model.

Write `2^64 = q * seen + r`. Direct modulo gives indices below `r` one extra
possible random word. For `seen=3`, index zero has 6,148,914,691,236,517,206
preimages, and indices one and two each have 6,148,914,691,236,517,205. The
ordinary-size discrepancy is tiny, but it contradicts exact mapping uniformity.

At bound `u64::MAX`, zero and `u64::MAX` both map to index zero. Its replacement
probability is almost twice the ideal probability, with a still-tiny absolute
effect. A fresh seeded regression demonstrates the incorrect decision through
`add`: seed `0xC3910C8D016B07D6` produces first word zero. At a synthetic last
legal observation with capacity one, the previous implementation replaces the
sample immediately; the repair rejects zero and keeps the old sample because
the next accepted word selects an index outside the reservoir.

The baseline test failed with sample `[19]` instead of `[7]`. This fixture uses
lawful internal count/storage at the boundary; it does not claim to have ingested
`2^64` observations through the public API.

History confirms that `ff3d04c` introduced modulo selection and `fb0e1f8`
preserved it while settling explicit seeds and checked observation overflow.
The seed/count repair is retained; changing the bounded map requires no new RNG.

## Settled contract and implementation

Use one private rejection draw with the existing instance-owned word generator:

```rust
fn next_below(&mut self, bound: u64) -> u64 {
    debug_assert!(bound > 0);
    let threshold = bound.wrapping_neg() % bound;
    loop {
        let word = self.next_u64();
        if word >= threshold {
            return word % bound;
        }
    }
}
```

Wrapping negation represents `2^64-bound`, so the threshold equals `2^64 %
bound` without overflowing. Rejecting `[0, threshold)` leaves exactly
`q * bound` possible words, with `q` accepted preimages per index. Bounds one
and powers of two reject nothing. Bound `u64::MAX` rejects only zero and accepts
`u64::MAX` as the legitimate index zero.

The rejected prefix is always smaller than half the word range. Under independent
uniform words, a bounded draw uses fewer than two words on average, and the update
retains expected O(1) work with O(capacity) stored items. The loop has no fixed
draw cap; a forced modulo fallback would reintroduce bias.

The guarantee is conditional on the conventional pseudorandom model. This repair
removes bounded-conversion bias and preserves the existing word generator; it
does not establish cryptographic randomness or independently prove the generator's
joint statistical properties. Finite inclusion trials cannot resolve a discrepancy
of order `2^-64`; exact mapping evidence is authoritative for this defect.

## Ownership, lifecycle and compatibility

- The sampler retains its owned `Vec<T>`, exact `u64` count and instance-owned
  random state. Public signatures and explicit caller-provided seeds are unchanged.
  No shared RNG, generic RNG interface, cache or extra ownership layer is added.
- Rejected words advance only random state. They do not increment observations,
  discard the incoming item prematurely or replace a retained item.
- Filling the initial reservoir consumes no random words. Once full, the accepted
  index decides whether the new item replaces a slot or is discarded.
- Same capacity, seed and input remain reproducible. Rejection can change samples
  produced by the older implementation because it consumes additional words.
  Cloning copies the sample and random stream under the existing `T: Clone` bound.
- `add` at the count limit panics before changing count, sample or random state.
  `extend` processes items in order and preserves its completed prefix on overflow.
- `clear` drops retained items and resets the count, keeping capacity and continuing
  the RNG stream. Reconstructing with the original seed replays from the beginning.
  `into_samples` moves the buffer out; ordinary use requires no `T: Clone` bound.
- Constructor allocation policy and the separate oversized-layout finding F9
  remain unchanged. This repair does not add reservoir merging or persistence.

## Regression coverage and fresh verification

- **1,979 controlled first-word cases over 424 real `u64` bounds** cover every
  bound 1–255, every representable power-of-two boundary and its neighbors, and
  `u64::MAX-1`/`u64::MAX`. Inputs include zero, the last rejected word, the first
  accepted word, its neighbor and the maximum word. Exact `u128` remainder
  arithmetic supplies the acceptance interval independently of wrapping negation.
  Tests assert both the returned index and the final RNG state.
- Tests invert the existing word generator only to select seeds that force a
  chosen first word. The inverse is validated by reading that word in each fixture;
  no production injection hook is introduced.
- A known stream at bound `2^63+1` rejects three consecutive words before accepting
  the fourth. The count and sample remain untouched during the bounded draw.
  A separate case confirms legitimate index-zero acceptance at the maximum bound.
- An **exhaustive eight-bit analogue** counts every accepted word for all 255
  positive bounds. At bound three, counts become `[85,85,85]`, replacing the
  biased direct-modulo counts `[86,85,85]`. The reduced model complements the real
  64-bit boundary tests rather than substituting for them.
- A separate Python arbitrary-precision checker confirms reduced preimage counts
  and integer interval counts at the same real bounds. Its proof and logs are
  retained outside Cargo targets.
- Seeded `add`, fill, clear/reuse, cloning, single-item overflow and iterator-prefix
  overflow tests preserve count, sample and random-stream semantics. Non-Clone
  owned items are dropped exactly once across discard, replacement, clear and
  consuming the sample buffer.
- **32,768 deterministic seed/capacity trials** check ordinary inclusion over a
  17-position stream at capacities 1, 3, 8 and 17. Samples contain unique positions,
  exact counts and bounded size; inclusion counts stay within a six-standard-
  deviation sanity margin. These trials preserve ordinary Algorithm R behavior;
  they are not a proof of bias removal or RNG independence.

Fresh checks:

- Focused reservoir suite: **16 tests passed in debug and release**.
- `cargo test --locked --offline`: **249 unit tests and 20 doctests passed**.
- The reservoir doctest and example passed after the core repair.
- `cargo check --locked --offline --all-targets`: passed.
- `cargo clippy --locked --offline --all-targets --all-features -- -D warnings`:
  passed.
- `cargo doc --locked --offline --no-deps`: passed with the existing MinHash
  private documentation-link warning at `src/minhash.rs:32`.
- Changed Rust formatting and `git diff --check`: passed. Repository-wide
  formatting still fails only at the existing `examples/jacard.rs` print
  statement, hunk beginning at line 48; the unrelated example is unchanged.
- Independent integer preimage checks and Markdown fences/local file links:
  passed.

Logs and the independent checker are retained in
`/tmp/sketches-f6-fix-20261001/`, outside Cargo targets. Cargo artifacts are
cleaned before and immediately after each commit. Both local review reports
have F6 resolution notes and remain untracked; existing user skill files stay
outside implementation commits.
