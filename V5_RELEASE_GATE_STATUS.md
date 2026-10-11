# Ironbeam 5.0.0 release gates

**Decision: hold release.** Updated 2026-10-11 on
`codex/ironbeam-v5-runner-core`. The root source is v5-only, but its package
version is still `4.1.1`; the v5 crates are `0.1.0`. Publication remains
staged locally, as requested.

This page records the current decision. The detailed test and implementation
history is in [V5_RELEASE_GATE_EVIDENCE.md](V5_RELEASE_GATE_EVIDENCE.md).
The [replacement roadmap](V5_REPLACEMENT_ROADMAP.md) and
[migration guide](V4_TO_V5_MIGRATION.md) explain the intended API.

## Gate dashboard

| Gate | State | Evidence now | Required to close |
| --- | --- | --- | --- |
| V5-only root and modular packages | **Mostly done** | The root's 101 local v4 source files and `legacy-v4` feature are gone. Default root dependencies are core, Direct, transforms, shared I/O, and JSONL. The 25 local v5 packages verify without publication. | Decide the remaining legacy `ironbeam-cloud` package's ownership, remove any obsolete v4 behavior, and set the intended release versions after the other gates pass. |
| Bounded API and helper parity | **Partly done** | The v4 reference corpus runs against published 4.1.1 in `ironbeam-compat`; bounded Direct helper parity is tracked in [V5_HELPER_PARITY_TRACKER.md](V5_HELPER_PARITY_TRACKER.md). | Audit every public root path and behavior promised for 5.0.0, then finish the migration examples. The helper tracker does not cover streaming, portable execution, or durability. |
| Direct and portable behavior | **Open** | Shared fixtures compare elementwise pipelines, two-source Flatten → GroupByKey, grouped event time, explicit-order folds, and tie-key selection on Direct and local Prism. The broader Prism corpus exercises joins, CoGroupByKey, side inputs, windows, and many I/O formats. | Run the same success and failure graphs through both runners for every advertised operation, including remaining multi-input and failure cases. |
| Unbounded execution | **Open** | Direct runs one registered unbounded source through a linear route to idempotent sinks with cursor checkpointing. Local Prism runs both a TestStream fixture and a production polling source through the v5 worker. | Prove worker recovery after an accepted batch, split-response roots, and the remaining portable and Direct graph shapes. |
| Recovery and output commits | **Open** | Direct restores several coded bounded aggregate and linear group routes. Portable sinks stage and validate fragments before local final publication for JSONL, CSV, XML, Avro, Parquet, Arrow, Protobuf, TFRecord, MessagePack, and CBOR. | Finish the runner-wide commit contract, abandoned-attempt and restart proofs, and the remaining v4 checkpoint-policy replacement decisions. |
| Coverage and package quality | **Passing floors; review open** | Fresh measured Direct coverage is 2,860/3,132 locations (91.32%) and 560/618 branches (90.61%), above its 91%/90% CI floors. The full local package gate passed for all 25 crates. | Review uncovered contract and failure paths after the remaining features land; rerun clean coverage, strict lint, docs, package checks, and the full test matrix. |
| Performance and formatting | **Open** | Bounded results and checksums match the v4 corpus. Several hot paths improved, but the wider v4/v5 latency and memory comparisons remain mixed. | Define acceptable regression limits, rerun paired release benchmarks, and complete a workspace formatting pass. |
| Hosted cloud execution | **Deferred by scope** | Provider-neutral object storage and hosted clients exist; local Prism uses a shared filesystem. | Treat remote staging, hosted provider tests, and a real Dataflow cloud job as a later extension. The requested 5.0 proof uses local Prism. |

## Runner parity rule

For an operation advertised by both Direct and portable execution, the same
graph and input should produce the same values, grouping, event timestamps,
window intervals, typed failure category, and committed output. An explicit
order or tie key also makes order-sensitive results part of that contract.
Bundle size, scheduling, and retry timing may differ without changing these
observations. Unsupported operations must fail graph validation before a
source or sink opens.

Portable ordered folds require an explicit order key, per the agreed design.
The v4 encounter-order methods remain Direct compatibility behavior because
Beam shuffle does not preserve encounter order. See the
[roadmap contract](V5_REPLACEMENT_ROADMAP.md#api-similarity-contract)
and the detailed parity evidence for each fixture.

## Latest verified checks

- Root all-feature tests and example checks passed after the v4 source cutover.
- The complete root workspace test and doctest suite passed after the
  GroupByKey checkpoint-version correction.
- The local release script passed its dependency checks, workspace tests,
  feature matrix, docs, and verification of all 25 packages on 2026-10-11.
  It used Mr Boxington and published no crates.
- The complete local Prism suite now passes all 15 tests, including the new
  two-source Flatten → GroupByKey parity fixture. The Dataflow workspace
  test and doctest suite and strict lint for the changed packages also pass
  after the keyed coder propagation fix (`ironbeam-dataflow` `46ceb2c`).

## Next closure sequence

1. Extend the shared Direct/Prism corpus to remaining advertised operations
   and failures, especially additional multi-input and output cases.
2. Close the worker restart and runner-wide output-commit proofs.
3. Finish the public API, checkpoint-policy, cloud-package, and performance
   audits; run clean coverage and formatting checks.
4. Set release versions and repeat the complete local gate. Keep publication
   staged until explicitly authorized.

The full design and exit criteria remain in
[WORKSPACE_ARCHITECTURE_PLAN.md](WORKSPACE_ARCHITECTURE_PLAN.md) and
[IRONBEAM_V5_EXECUTION_PLAN.md](IRONBEAM_V5_EXECUTION_PLAN.md).
