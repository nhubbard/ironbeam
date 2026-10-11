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
| Portable behavior | **In progress** | All 18 live local Prism tests pass. Bounded Direct/Prism and mixed bounded/unbounded Prism joins and co-groups verify values and time metadata. More shared cases are needed. |
| Streaming and recovery | **In progress** | Direct cursor checkpoints and a two-batch production-source join/co-group Prism job work. Worker restart and runner-wide output commit proofs remain. |
| Quality and release | **In progress** | Coverage floors, package verification, and formatting pass. The final clean matrix, benchmarks, and version change remain. |

These are progress indicators, not release approvals. Each open item below must
be resolved before the version changes to 5.0.0.

## Remaining release gates

| Gate | What must be demonstrated | State |
| --- | --- | --- |
| API and v4 behavior | Audit the public root API and promised behavior against the [helper parity tracker](V5_HELPER_PARITY_TRACKER.md) and [migration guide](V4_TO_V5_MIGRATION.md). | Open |
| Portable operation coverage | Run shared Direct and Prism success and failure graphs for every advertised operation, including remaining multi-input and output cases. | Open |
| Runner agreement | Compare values, grouping, event times, windows, typed failures, and committed output for those graphs. | Open |
| Unbounded recovery | Prove worker recovery after an accepted batch, split responses, and remaining graph shapes. | Open |
| Output durability | Finish the runner-wide commit contract, abandoned-attempt and restart proofs, and v4 checkpoint-policy replacement decisions. | Open |
| Cloud package cleanup | Resolve legacy `ironbeam-cloud` ownership and obsolete compatibility behavior. | Open |
| Coverage and verification | Review uncovered contract paths, then rerun clean coverage, tests, lint, docs, and package checks. | Open |
| Performance | Set acceptable regression limits and rerun paired v4/v5 latency and memory benchmarks. | Open |
| Formatting | Format both workspaces and rerun their tests and the live Prism proof. | Passed |
| Versioning | Set intended release versions after the other gates pass. | Open |

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
| Root workspace tests and doctests | Passed after the CoGroupByKey event-time correction. |
| Local release script | Passed dependency checks, workspace tests, feature matrix, documentation, and verification of 26 packages on 2026-10-11 using Mr Boxington. No crate was published. |
| Local Prism suite | All 18 live tests passed. The production-reader join/co-group job resumed from a residual cursor and verified five exact committed rows across two windows. |
| Separate Dataflow workspace | Tests and doctests passed with the new coder; warning-free lint passed for the changed proof package. |
| Clean shared coverage corpus | Core: 3,055/3,178 source locations (96.13%), 468/528 branches (88.64%). Direct: 3,913/4,187 (93.46%), 837/928 (90.19%). Transforms: 1,163/1,189 (97.81%), 108/116 (93.10%). All pass their existing floors. |
| Postcard coder coverage | 13/13 source locations passed the new isolated gate; LLVM reports no instrumented branches. |
| Workspace formatting | `cargo fmt --all --check` passes in both workspaces; both workspace test suites and root all-feature examples passed afterward. |

These results describe the last complete gate run; in-flight changes require
a fresh run before they count as release evidence.

## Supporting detail

- [Evidence archive](V5_RELEASE_GATE_EVIDENCE.md): implementation history,
  fixture results, measurements, and known limits.
- [Replacement roadmap](V5_REPLACEMENT_ROADMAP.md): API and runtime scope.
- [Workspace architecture plan](WORKSPACE_ARCHITECTURE_PLAN.md) and
  [v5 execution plan](IRONBEAM_V5_EXECUTION_PLAN.md): design and exit criteria.
