# Phase 3: Direct runner migration

The v5 `ironbeam-direct` runner now lowers a validated core graph to its own private physical plan. For a selected output, the plan walks back to one bounded source, records the ordered `map`/`filter` operation keys, and executes those stateless operations as one fused stage. Source batches are bounded by `JobConfig::buffer_budget_elements`; Rayon works on each batch in a job-owned pool and result order follows the input for this elementwise path. `explain` returns a stable summary without opening the source. No v4 `Node`, local subplan, optimizer object, or `VecOps` value enters `ironbeam-core`.

The requested `run_collect` terminal returns a `Vec<T>` after execution. Only one source batch and its results are materialized at a time before the terminal store; the core does not require a whole-collection vector edge. The terminal store keeps encoded output under an approximate byte budget and can spill ordered segments to a job-specific temporary directory. It restores the requested vector at completion and removes spill files when the job store drops. The byte budget applies to retained output, while `buffer_budget_elements` limits source batch size. It is not a strict process RSS cap. Durable output requires a working `Element` coder; built-in integer, bool, and UTF-8 string coders are provided, while user types must implement their own.

Local counters are owned by each job: source elements read in this attempt, output elements (including recovered output), filtered elements, completed batches, peak batch size, spill files/bytes, checkpoint saves, and recovered elements. `JobHandle::processed()` follows output elements. Cancellation is checked at batch boundaries and before each transform; a panicking user transform becomes a typed execution error.

Checkpoints contain framed encoded output, a checksum, source offset, output count, and independent checkpoint schema version. A manifest is published after its data file is durable. Recovery verifies graph fingerprint, source configuration digest, selected source/output keys, output coder ID/version, and the caller-supplied `CheckpointConfig::binary_build_id` before opening the source. A build ID is required because the graph cannot prove compiled closure equivalence. Recovery rejects incompatible or corrupt data with typed errors. Checkpoints commit after whole batches; side effects in a failed batch may run again, so this is at-least-once transform execution. Use a dedicated checkpoint directory for one active writer. The `from_vec` source digest binds the actual encoded input values; custom sources and coders need their own portable source identity before durable recovery can be enabled.

## Behavior matrix

| Case | v4 reference | v5 Direct result |
| --- | --- | --- |
| Empty, singleton, and larger `from_vec → map → filter → collect` | Sequential values and order; parallel result multiset | Same values in sequential and parallel runs; v5 preserves input order for this path |
| Type-changing `String → i32` map and filter | Sequential values | Same values with forced spill and checkpoint recovery |
| Rejected elements | Filter removes values | Same values; per-job filtered count records removals |
| User map panic | v4 failure behavior characterized separately | Typed execution error; no unwind escapes the Direct job |
| Invalid budgets, unsupported nodes, or missing durable coder | No v5 equivalent | Typed rejection before source execution |
| Grouping, joins, multi-input fan-in, format I/O, windows, deferred sinks, unbounded sources | v4 or later-phase behavior | Explicit unsupported capability; Phase 4 or later owns these migrations |

The differential corpus is in `crates/ironbeam-compat/tests/differential.rs` and runs against published Ironbeam 4.1.1; focused plan, ordering, metrics, resource, spill, and recovery tests are in `crates/ironbeam-direct/tests/phase3.rs` and private module tests. The unbounded-source rejection fixture remains in `crates/ironbeam-direct/tests/vertical.rs`.

## Coverage and remaining risk

Package-scoped line/branch coverage after the exit-gate repair is **664/675 (98.37%) / 92/102 (90.20%)** for `ironbeam-core` and **806/920 (87.61%) / 89/106 (83.96%)** for `ironbeam-direct`. `coverage-baselines.json` ratchets these measured counts; both packages now exceed their Phase 2 line and branch percentages. The tests cover normal and forced spill, cleanup, frame corruption, checksum and manifest corruption, source and binary mismatch, partial-batch retry, and sequential/parallel differential output. Remaining uncovered lines are mainly file sync/write failures, impossible size overflows, and private lowering guards after public validation. Fault injection for those paths and a strict whole-process memory cap remain future hardening work.
