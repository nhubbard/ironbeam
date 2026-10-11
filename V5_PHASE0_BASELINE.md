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

An aligned **release** build on 2026-10-10 timed each workload from input
construction through the completed terminal at 100,000 rows. One v4/v5 sample
in milliseconds was: map/filter 0.54/27.02, GroupByKey 1.89/18.64, inner join
19.66/52.93, and JSONL round trip 6.78/24.97. Output counts and checksums
matched in every case. These are single local runs, not statistically stable
benchmarks, but the gap is large enough to keep throughput and allocation
profiling open as a replacement gate. The v5 source computes a coder-backed
content digest for stable identity, while v4's vector source does not; that
is one concrete cost. A separate temporary 100,000-element `u32` source
construction probe measured 4.13 ms for v5 and 0.06 ms for v4 on one run.
This explains only part of the map/filter gap; the runner path still needs
profiling. Both examples now include input construction in the timed region.

The v5 Direct output store now obtains exact encoded lengths without
allocating for built-in scalar, string, vector, option, and tuple values.
Five repeated 100,000-row release runs after that change had v4/v5 median
times (ms) of map/filter 0.456/26.016, GroupByKey 1.862/18.142, inner join
22.775/38.610, and JSONL round trip 6.976/22.061. Join latency improved
from the earlier 52.93 ms sample, but a substantial gap remains in every
case. A trial that removed a per-row result vector only in single-worker
execution had no measurable gain and was reverted. The dedicated job worker
pool must remain in use even at parallelism one, as its existing test verifies.

The vector-source digest now reuses one wire buffer for built-in scalar and
string coders while retaining the same coder ID, version, length prefixes,
and SHA-256 result. A direct 100,000-element `u32` source probe fell from
4.13 ms before the change to 0.96–1.34 ms over five runs afterward. The
five-run v5 medians (ms) then measured map/filter 24.572, GroupByKey 18.138,
inner join 38.262, and JSONL 20.872. Source construction improved, but most
of the map/filter gap remains in Direct execution and requires profiling.

Direct now dispatches built-in zero-or-one-result transforms without a
per-row output vector; flat-map keeps its multi-result path. Five repeated
100,000-row release runs after that change measured v5 medians (ms) of
map/filter 14.058, GroupByKey 20.418, inner join 40.020, and JSONL 20.054.
The map/filter median improved substantially from 24.572 ms while the other
cases did not. Against the measured v4 map/filter median of 0.456 ms, the
remaining gap is still about 31× and remains a release blocker.

Processing the default single-worker route directly inside its job-owned
thread pool avoids another intermediate result batch. Five more release runs
measured medians (ms) of map/filter 13.332, GroupByKey 20.640, inner join
40.000, and JSONL 19.102. Map/filter improved modestly and remains about
29× the v4 median. The dedicated-pool and full workspace tests pass.

Direct now resolves single-result transform closures once per route instead
of looking up each node in the registry for every element. Five release runs
at 100,000 rows measured v5 medians (ms) of map/filter 5.565, GroupByKey
12.377, inner join 32.654, and JSONL 15.992. Counts and checksums matched
the earlier corpus. The map/filter median is still about 12× the previously
measured v4 median of 0.456 ms, so the performance gate remains open.
The benchmark can now print source, graph-construction, and runner timing
for the map/filter case with `IRONBEAM_BENCH_PHASES=1`. In four warmed
100,000-row release runs, source construction took 0.51–0.58 ms, graph
construction 0.004–0.006 ms, and Direct execution 4.94–5.54 ms. The
remaining gap is concentrated in row execution, not graph construction.

## Runner-specific assumptions and Dataflow dependency check

- `src/helpers/common.rs` `collect*` executes a terminal through the direct `Runner`; `src/runner.rs` uses `ThreadPoolBuilder::build_global()` on parallel paths, so requested thread counts are process-wide best effort.
- `src/node.rs` embeds type-erased `Partition`, `VecOps`, and local partition closures in graph nodes. `src/planner.rs` optimizes a requested terminal chain and uses source `VecOps::len()` hints. These belong to a direct physical plan, not the v5 logical graph.
- `src/helpers/joins.rs` snapshots each input as a linear subplan, creates a dummy source anchor, and expects internal downcasts. `flatten`, `tee`, and `wait_on` also need explicit multi-input/output and dependency semantics in the v5 graph.
- `from_custom_source` exposes a type-erased `VecOps` payload; format readers and `read_*_streaming` remain bounded, often shard-backed. Immediate `write_*` methods collect before writing. The current `Timestamped<T>` and `TimestampMs = u64` do not encode streaming progress.
- The separate Dataflow workspace depends on `ironbeam = "4"` with `coders`, and patches it to `../ironbeam`. Full offline Cargo metadata fails because that path does not contain the package in this local layout (`rustflow` is the package checkout). `--no-deps` metadata succeeds, confirming manifest structure only. No file was changed in the separate repository.
- Its graph translator accepts one source-rooted linear chain; its fingerprint hashes node IDs, kinds, operation counts, and edges but no closure, coder, or source configuration. The v5 graph and runner sketch specifies stricter cross-process verification.

## Phase 1 entry check

The Phase 0 API inventory and contract sketch are in `V5_PHASE0_API_INVENTORY.md` and `V5_LOGICAL_GRAPH_CONTRACT.md`. Numeric coverage gates and package-specific CI should use the portable measured report rather than the historical LCOV file. The first workspace shell can preserve the v4 facade; package splits and the public runner trait wait for the vertical slice.
