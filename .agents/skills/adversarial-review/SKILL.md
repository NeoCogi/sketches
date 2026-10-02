---
name: adversarial-review
description: Audit requested NeoCogi subsystems or the whole repository with an independent agent, tracing code, test, documentation, and architectural findings through Git history. Identify recurring fixes, ambiguous contracts, and opportunities to remove or consolidate unnecessary complexity. Use for adversarial reviews and architectural audits; review requests do not authorize implementing fixes.
---

# Adversarial repository review

Produce a critical, evidence-based review of the requested scope, including old
code and decisions. Explain the problem, why it exists, and what would prevent
it from returning. A recent patch list or an earlier review is a lead, not the
boundary of a whole-repository review or proof that an issue still exists.

## Establish scope and settled contracts

- Record the repository root, branch, HEAD, and relevant uncommitted changes.
  State whether the review covers the working tree or a requested revision.
  For a whole-repository request, inventory the major packages, tools, examples,
  tests, documentation, and their boundaries. For selected parts, trace their
  callers and consumers as needed without silently broadening the assignment.
- Read applicable repository instructions and current contract documentation.
  Preserve the user's settled decisions; distinguish normative requirements,
  implementation behavior, historical recommendations, and reviewer preferences.
  Do not ask the user to decide a requirement already established in the session.
- NeoCogi is pre-release: backward compatibility is not a reason to preserve an
  obsolete path. Favor concrete types, explicit ownership and `&mut self`,
  separation of responsibilities, and exhaustive state handling. Do not propose
  `Any`, downcast registries, or untyped payloads as simplifications. Question
  interior mutability by explaining the actual borrow/reentrancy requirement.
- Review code, tests, and documentation together. A missing feature is a gap only
  against an established promise or intended scope. A deliberate exclusion or
  measured tradeoff is not automatically a defect.

## Require an independent reviewer

The orchestrating agent must spawn at least one new reviewer agent for each
invocation. This delegation requirement belongs to the orchestrator: a spawned
reviewer investigates its assigned scope without recursively spawning reviewers.
Give it a concrete scope that can run alongside local verification.
Use a fresh context (`fork_turns="none"` where supported), with the agreed
constraints and raw sources rather than the parent's suspected findings or
preferred fix. Do not restrict it to the latest changes unless the user does.

Give the reviewer:

- The repository path, revision/working-tree scope, requested subsystems, and
  any exclusions or requested depth/count of findings.
- This skill's absolute path and the applicable settled requirements, with their
  source. Instruct it to follow the investigation and finding requirements below.
- A read-only source assignment and a separate temporary location for its draft,
  exact reproduction commands, and logs. Tell it which build directory it may
  use and coordinate builds so shared Cargo targets are not used concurrently.
- The explicit task: independently challenge behavior and architecture; inspect
  history for **every proposed finding**; explain competing requirements behind
  repeated changes; recommend repairs and deletions without implementing them.

While it reviews, map the boundaries and independently verify candidate findings
against current source, callers, tests, and history. Additional agents are useful
only for bounded independent scopes; do not multiply identical whole-repo audits.
Save the exact task prompt sent to each reviewer for the report appendix. If
spawning is unavailable or fails, report that independence could not be obtained;
continue useful local analysis without describing it as an independent review.

Review work does not authorize source fixes, branch switches, commits, dependency
updates, or alteration of existing tests to make a reproduction pass. Use temporary
fixtures for experiments. Preserve user changes. Coordinate any builds with the
parent; keep evidence outside Cargo targets and clean review-created artifacts
when finished. Do not delete another running task's artifacts.

## Investigate behavior and architectural cost

Follow representative operations across real boundaries: construction, mutation,
event delivery, persistence, replacement, failure, and destruction as applicable.
Check ordering, partial failure, ownership, validation, error propagation, resource
bounds, and consistency between producers and consumers. Include build/generation
paths and independent plugins when the scope crosses those boundaries.

For tests, look for absent adversarial cases, assertions that only repeat the
implementation, mocks that bypass the failing boundary, feature combinations that
hide missing support, ignored coverage, and tests that establish only part of the
claimed contract. A test name or a historical passing log is not current proof.
For documentation, compare the promises and examples to callable APIs and actual
behavior; identify stale descriptions that could cause the old design to return.

For complexity, identify parallel representations, shadow state, redundant
forwarding layers, repeated validation policies, unnecessary allocation/lifetime
machinery, and abstractions whose callers must understand their internals.
Evaluate removal first, then consolidation under one authoritative owner, before
adding another layer. Consider fixes at multiple levels when responsibility is
split across an IDL/compiler, ABI, runtime, loader, tool, and application.

**Similar code is not sufficient evidence for merging responsibilities.** Compare
meaning, timing, ownership, errors, and lifecycle. For example, a synchronous
setter and an event receiver may both accept a typed value while having distinct
contracts. A shared helper may be appropriate without merging those interfaces.
Likewise, a smaller object does not establish a smaller binary, and fewer source
lines do not establish lower runtime cost. Separate measurements, calculations,
and hypotheses; describe the workload and assumptions behind estimates.

## Trace every finding through history

Use path history, `git blame`, `git log -S`/`-G`, and `git show` as appropriate.
Follow renames and the actual commits behind reverts or replacements; do not stop
at the latest touching commit or infer causality from its title alone.

For each finding, establish:

1. The relevant introduction or earliest available behavior and its intended
   problem/constraint, using the commit diff, message, tests and contemporary docs.
2. Later changes, attempted fixes and tradeoffs, including whether each addressed
   a symptom, relocated responsibility, or changed the contract.
3. What remains true at the reviewed revision, with a concrete current trigger.
   Keep a fixed historical defect out of the current findings unless it recurs or
   a distinct remaining failure is established.

Label rationale as **documented**, **inferred**, or **unknown**. If history is
shallow, missing, or insufficient, say what was inspected and what cannot be
established; do not invent the original author's motivation.

Repeated fix/revert/reintroduction cycles require a contract investigation.
Show the sequence and the competing obligations with examples. Identify every
unresolved requirement revealed by that sequence, including interactions between
them. Also distinguish already-settled requirements that were not enforced from
questions the user still needs to answer; a regression is not permission to
reopen a settled decision. Consider incomplete consumer migrations, inadequate
regressions, stale docs, and changing external constraints as contributing causes.

For each unresolved requirement, explain the alternatives, the behavior each
permits or forbids, ownership/timing/error consequences, and a recommended choice
with its tradeoffs. Avoid opaque questions such as “what identifies a connection?”:
name the concrete objects, operation, queued work, and observable outcomes.
State the resulting rule in language that can become documentation and a test.

## Write decision-ready findings

Write a detailed Markdown report at the requested path, or by default
`docs/ADVERSARIAL_REVIEW_<date>_<scope>.md`. Use a distinct filename when that path
already contains a historical review, unless the user requested an update.
Include relative source links with current line references and commit IDs so the
report works in VS Code preview. Give nontrivial architecture/lifecycle problems
a Mermaid diagram **and a plain-text explanation**; include concrete code or
input/output examples for every finding. Do not rely on a diagram to explain it.

Open with the reviewed revision/scope, a coverage map, settled constraints, and
an evidence-ranked findings table. Distinguish severity, confidence, and evidence:
**reproduced**, **source-confirmed**, **contract gap**, or **simplification**.
Separate unverified hypotheses from established findings. Do not inflate the
number of findings by splitting one root cause or counting deliberate exclusions.
If the requested count cannot be supported, report fewer and state the limitation.

Each finding needs:

- **Current problem and impact:** exact trigger, expected versus actual behavior,
  affected caller/user, source locations, and evidence status. State whether the
  issue is a code defect, test gap, documentation contradiction, or a combination.
- **Concrete example:** enough setup, values, and steps to understand the failure
  without guessing what a “route,” “owner,” “buffer,” or “component” means here.
- **History and rationale:** a short commit timeline, the problem each change
  intended to solve, its remaining limitation, and confidence in that explanation.
- **Root cause:** the missing invariant or misplaced responsibility, including
  why existing tests/docs did not prevent the problem or overstated the fix.
- **Contract decisions:** unresolved choices with illustrated alternatives and a
  recommendation, or an explicit statement that the requirement is already clear.
- **Repair and simplification:** the owning layer(s), what to remove/merge/move,
  guarantees to preserve, migration of internal consumers, and why the proposal
  resolves the underlying issue. Explain when a local patch is sufficient or why
  it would merely recreate the cycle. Include costs and legitimate boundaries.
- **Acceptance:** observable regressions and documentation changes that establish
  the contract. Exercise generated code and real COM calls when those are the
  relevant boundary; do not claim arbitrary foreign-pointer safety from malformed
  values stored in otherwise valid buffers.

End with a consolidated decision table linking each unsettled requirement to its
findings, examples, recommended rule, and acceptance criteria. Order proposed
implementation by dependencies and distinguish work ready to implement from work
blocked on a real decision. Add the exact reviewer task prompt(s), commands actually
run and outcomes, inspected versus uninspected areas, and reproduction limitations.

## Reconcile and deliver

Validate the independent agent's claims; do not forward its draft unchanged.
Check cited current lines and historical diffs, reproduce the most consequential
claims where practical, and challenge fixes against the original motivation and
settled constraints. Resolve disagreements with evidence; retain uncertainty when
it cannot be resolved. Recheck the reviewed revision and relevant working-tree
diff before finalizing, and disclose any changes that limit the conclusions.

Report confirmed findings, outstanding decisions, architectural deletion or
consolidation opportunities, and remaining coverage limits with a link to the
full document. Do not claim exhaustive coverage because the requested scope was
the whole repo. Do not implement recommendations or mark an issue fixed merely
because the report recommends a solution. Implementation is a separate request.
