# Ironbeam 5 Phase 0 baseline

Measured October 10, 2026 on `codex/ironbeam-v5-runner-core` at `02f49f0`; its `src/` and Cargo manifest are unchanged from `main` `d64c26a`. The branch was clean before Phase 0 edits. The checkout's `target` link points at an inaccessible managed cache, so commands used the rustup Cargo binary and a temporary `CARGO_TARGET_DIR`.

## Behavior and test tiers

| Check | Result | Scope |
| --- | --- | --- |
| `cargo test --lib --tests` with default features | 1,434 v4 tests passed before corpus edits; 1,437 after adding three corpus tests; no failures | Portable default suite |
| `cargo llvm-cov --workspace --all-features --branch --summary-only` | Stopped after 875 passes and 37 failures in `tests/lib.rs` | MongoDB and SQL tests require Docker; this host cannot connect to it |
| Portable all-feature coverage | 9,382 / 11,229 lines (83.55%); 785 / 1,046 branches (75.05%) | Same all-feature build, with `--skip io::mongodb --skip io::sql_containers` |

The service tier comprises `tests/io/mongodb.rs` and `tests/io/sql_containers.rs` included by `tests/lib.rs` under optional features. No cloud or paid Dataflow job was run. The default suite passed without those features. The all-feature Docker failures are environment failures, not v5 regressions.

## Coverage

The full per-file report is in `V5_PHASE0_COVERAGE.md`. Representative module lines/branches: `collection.rs` 97.83% / 87.50%; `pipeline.rs` 100% / 100%; `planner.rs` 93.31% / 86.41%; `runner.rs` 61.82% / 56.82%; `helpers/joins.rs` 93.64% / 100%; `io/jsonl.rs` 87.28% / 75.00%; `io/mongodb.rs` 0% / 0% because its Docker tests were skipped. New core, validation, and lowering code should get branch-focused tests; a global percentage alone could hide an uncovered runner error path. The historical `lcov.info` in the execution plan is not used as a gate.

## Representative behavior and costs

`tests/v5_behavior_corpus.rs` fixes sequential elementwise order, empty/singleton behavior, parallel result multisets, keyed grouping and join multiplicity, and immediate bounded JSONL round trip. It intentionally does not promise global parallel ordering or distributed retry semantics. Current v4 local operations do not declare source replay or sink commit guarantees.

`examples/v4_benchmark_corpus.rs` prints stable workload names, row counts, elapsed nanoseconds, output counts, and checksums. On this host, a **debug** run with 10,000 rows gave map/filter 1.46 ms (5,000 rows; checksum 74,985,000), group-by-key 3.09 ms (100 groups; total 10,000 values), inner join 28.69 ms (10,000 rows; checksum 100,000,000), and JSONL write/read round trip 6.11 ms (10,000 rows; checksum 49,995,000). Earlier runs of the first three cases were faster, so these are characterization samples, not release thresholds. `examples/v5_benchmark_corpus.rs` now runs the same four workloads through the v5 facade for direct comparison. `/usr/bin/time -l` could not report peak RSS in the sandbox (`sysctl kern.clockrate: Operation not permitted`), so memory measurement remains an explicit gap.

## Runner-specific assumptions and Dataflow dependency check

- `src/helpers/common.rs` `collect*` executes a terminal through the direct `Runner`; `src/runner.rs` uses `ThreadPoolBuilder::build_global()` on parallel paths, so requested thread counts are process-wide best effort.
- `src/node.rs` embeds type-erased `Partition`, `VecOps`, and local partition closures in graph nodes. `src/planner.rs` optimizes a requested terminal chain and uses source `VecOps::len()` hints. These belong to a direct physical plan, not the v5 logical graph.
- `src/helpers/joins.rs` snapshots each input as a linear subplan, creates a dummy source anchor, and expects internal downcasts. `flatten`, `tee`, and `wait_on` also need explicit multi-input/output and dependency semantics in the v5 graph.
- `from_custom_source` exposes a type-erased `VecOps` payload; format readers and `read_*_streaming` remain bounded, often shard-backed. Immediate `write_*` methods collect before writing. The current `Timestamped<T>` and `TimestampMs = u64` do not encode streaming progress.
- The separate Dataflow workspace depends on `ironbeam = "4"` with `coders`, and patches it to `../ironbeam`. Full offline Cargo metadata fails because that path does not contain the package in this local layout (`rustflow` is the package checkout). `--no-deps` metadata succeeds, confirming manifest structure only. No file was changed in the separate repository.
- Its graph translator accepts one source-rooted linear chain; its fingerprint hashes node IDs, kinds, operation counts, and edges but no closure, coder, or source configuration. The v5 graph and runner sketch specifies stricter cross-process verification.

## Phase 1 entry check

The Phase 0 API inventory and contract sketch are in `V5_PHASE0_API_INVENTORY.md` and `V5_LOGICAL_GRAPH_CONTRACT.md`. Numeric coverage gates and package-specific CI should use the portable measured report rather than the historical LCOV file. The first workspace shell can preserve the v4 facade; package splits and the public runner trait wait for the vertical slice.
