---
name: implement-fix
description: Implement an agreed fix or architectural change in this NeoCogi repository with documented contracts, typed ownership, focused regression tests, and multiple descriptive commits. Use for implementation requests, including following up a review; do not turn explanation-only requests into code changes.
---

# Implement a fix

Carry the user's agreed change through implementation, consumers, tests, documentation,
and commits. Preserve the current request's scope and settled decisions. Backward
compatibility is not required for this pre-release repository; remove obsolete paths
when replacing a contract rather than retaining unnecessary compatibility machinery.

## Establish the contract

- Read relevant code, repository instructions, tests and documentation. When a review
  or prior fix is involved, inspect its revision/history before assuming it still applies.
- Identify the root cause and define the intended behavior, ownership, lifetime,
  validation and error semantics before implementing. Separate missing features from
  deliberate constraints and reproduced failures from source-derived concerns.
- Ask material unresolved questions early. If the session already establishes the
  answer, proceed; do not ask for renewed approval of authorized local work.

## Branch and commit workflow

- Create a new descriptive branch from the intended base. Preserve unrelated user
  changes and do not silently discard or include them.
- Implement substantial work in multiple coherent commits. Each commit should leave
  its affected boundaries understandable and validated, not merely split files arbitrarily.
- Give every commit a descriptive title and substantial body covering the problem,
  root cause, chosen contract, resulting behavior, tradeoffs and actual verification.
- Run `cargo clean` before **every** commit and again immediately after it. Finish
  the relevant tests before the pre-commit clean. Keep logs outside Cargo targets
  when needed as evidence; account for separate target directories used by tests.
- Run Cargo builds/tests sequentially when they share target directories. Avoid
  retaining duplicate build artifacts unnecessarily; disk usage matters here.

## Engineering requirements

- Do not introduce `std::any::Any`, `core::any::Any`, downcast registries or untyped
  payload substitutes. Use concrete types, generics, enums and explicit COM contracts.
- Prefer owned state and `&mut self`. Reduce `Cell`/`RefCell` where ownership permits;
  retain interior mutability only for a concrete required borrow/reentrancy boundary,
  and document that requirement. Do not substitute locks or raw pointers to hide it.
- Separate concerns: syntax and resolution, wire representation and application
  values, inspection and dispatch, persistence and live object lifetime, as relevant.
  Keep one authoritative representation of each contract and derive consumers from it.
- Avoid leaky abstractions: keep representation details behind their responsible
  boundary; expose ownership, errors and supported operations explicitly.
- Represent state machines with enums and exhaustive `match`/pattern matching.
  Document legal transitions, terminal states and failure/commit boundaries.
- Thoroughly document public and private structs, fields, enum variants, traits,
  functions and generated definitions introduced or materially changed by the work.
  Explain invariants, responsibilities, ownership and limitations. Comment nontrivial
  bodies where ordering, validation, resource accounting or lifetime behavior matters;
  avoid comments that merely repeat an obvious statement. Explain unsafe obligations
  at each affected unsafe boundary.
- Add very well defined test cases that test exhaustively the fix/feature.

## Verify and finish

- Add meaningful regressions for the defect and adjacent contract boundaries. For
  compiler/COM work, compile generated code and exercise real ABI calls where relevant,
  rather than relying only on emitted-text assertions. Test malformed inputs through
  valid raw storage and distinguish those checks from arbitrary pointer safety.
- Run focused suites, affected consumer checks and required repository checks. Report
  exactly what ran and its outcome; do not substitute historical passing logs for a
  fresh test result or claim a complete fix from a narrower test.
- Update the corresponding normative docs, examples, tools and review statuses when
  affected. State which constraints remain, what was superseded, and why the design
  was chosen so later reviews do not reopen settled issues using stale evidence.
- Report the branch, commits, behavior changed, validation and material remaining
  limitations. Verify the final working tree and complete the post-commit clean.
