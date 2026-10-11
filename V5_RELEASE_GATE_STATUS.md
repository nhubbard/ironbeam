# Ironbeam 5.0.0 release status

**Release decision: hold.** Last reviewed 2026-10-11 on
`codex/ironbeam-v5-runner-core`.

The root source uses v5 exclusively, but the root package is still versioned
`4.1.1` and the v5 crates are `0.1.0`. Publication is staged locally, as
requested. The target proof is local Prism; hosted Dataflow is a later
extension.

## At a glance

| Area | Status | Current proof |
| --- | --- | --- |
| Workspace cutover | **Mostly complete** | The root's local v4 source and `legacy-v4` feature are gone. All 26 local v5 packages passed package verification. |
| Bounded Direct behavior | **In progress** | The v4 reference corpus and v5 Direct helper tests pass. Public API and edge-case parity still need a complete audit. |
| Portable behavior | **In progress** | The 15-test live local Prism suite passes, including two-source Flatten → GroupByKey and a Serde record using the new Postcard coder. More shared success and failure cases are needed. |
| Streaming and recovery | **In progress** | Direct cursor checkpoints and portable production-source fixtures work. Worker restart and runner-wide output commit proofs remain. |
| Quality and release | **In progress** | Coverage floors and the local package gate pass. The final clean matrix, benchmarks, formatting, and version change remain. |

These are progress indicators, not release approvals. Each open item below must
be resolved before the version changes to 5.0.0.

## Release checklist

### Behavior and API

- [ ] Audit every public root API and promised v4 behavior against the
  [helper parity tracker](V5_HELPER_PARITY_TRACKER.md) and
  [migration guide](V4_TO_V5_MIGRATION.md).
- [ ] Run shared Direct and Prism success and failure graphs for every
  advertised portable operation, including remaining multi-input and output
  cases.
- [ ] Verify that both runners agree on values, grouping, event timestamps,
  windows, typed failures, and committed output for those graphs.

### Runtime and durability

- [ ] Prove worker recovery after an accepted unbounded batch and the
  remaining split-response and graph shapes.
- [ ] Finish the runner-wide output commit contract, abandoned-attempt and
  restart proofs, and v4 checkpoint-policy replacement decisions.
- [ ] Resolve the remaining legacy `ironbeam-cloud` package ownership and
  obsolete compatibility behavior.

### Release quality

- [ ] Review uncovered contract and failure paths; rerun clean coverage and
  the complete test, lint, documentation, and package gates.
- [ ] Set acceptable performance regression limits and rerun paired v4/v5
  latency and memory benchmarks.
- [x] Format both the root and separate Dataflow workspaces; rerun their test
  suites and the 15-test live Prism proof.
- [ ] Set the intended release versions after the remaining gates pass.

The release remains local. Publishing the packages is a separate decision.

## Contract decisions already made

For operations advertised by both Direct and portable execution, the same
graph and input must yield the same observable results: values, grouping,
event times, window intervals, typed failure category, and committed output.
Bundle size, scheduling, and retry timing may differ. Unsupported operations
must fail validation before a source or sink opens.

Portable ordered folds require an explicit order key. With an explicit order
or tie key, order-sensitive results are part of runner parity. The v4
encounter-order methods remain Direct compatibility behavior because Beam
shuffle does not preserve encounter order. See the
[API similarity contract](V5_REPLACEMENT_ROADMAP.md#api-similarity-contract).

## Latest verified checks

| Check | Result |
| --- | --- |
| Root workspace tests and doctests | Passed after the GroupByKey checkpoint-version correction. |
| Local release script | Passed dependency checks, workspace tests, feature matrix, documentation, and verification of 26 packages on 2026-10-11 using Mr Boxington. No crate was published. |
| Local Prism suite | All 15 live tests passed, including two-source Flatten → GroupByKey parity and a Postcard-coded production row. |
| Separate Dataflow workspace | Tests and doctests passed with the new coder; warning-free lint passed for the changed proof package. |
| Direct coverage | 2,860/3,132 source locations (91.32%) and 560/618 branches (90.61%), above the 91%/90% CI floors. |
| Postcard coder coverage | 13/13 source locations passed the new isolated gate; LLVM reports no instrumented branches. |
| Workspace formatting | `cargo fmt --all --check` passes in both workspaces; both workspace test suites, root all-feature examples, and all 15 live Prism tests passed afterward. |

These results describe the last complete gate run; in-flight changes require
a fresh run before they count as release evidence.

## Supporting detail

- [Evidence archive](V5_RELEASE_GATE_EVIDENCE.md): implementation history,
  fixture results, measurements, and known limits.
- [Replacement roadmap](V5_REPLACEMENT_ROADMAP.md): API and runtime scope.
- [Workspace architecture plan](WORKSPACE_ARCHITECTURE_PLAN.md) and
  [v5 execution plan](IRONBEAM_V5_EXECUTION_PLAN.md): design and exit criteria.
