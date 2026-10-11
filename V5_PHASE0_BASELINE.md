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

`examples/v5_hotpath_probe.rs` separates source-only, map-only, filter-only,
and map/filter execution and accepts `IRONBEAM_BENCH_BATCH` for a buffer-size
probe. At the default 4,096-element batch size, source-only execution
accounted for roughly 2.6–3.0 ms in warmed 100,000-row runs before changing
batch allocation. Direct now reserves source batch capacity from the iterator's
lower size hint, retaining the configured limit and existing error handling.
Five warmed source-only runs then took 2.14–2.36 ms. The five warmed aligned
corpus runs measured medians (ms) of map/filter 4.786, GroupByKey 11.286,
inner join 32.388, and JSONL 15.093. The map/filter gap against the earlier
v4 median of 0.456 ms remains roughly 10.5×.

The paired corpus now includes `source_copy`: construct 100,000 integers and
collect them without a transform. Four warmed release runs in the same build
had v4 source-copy times of 0.054–0.064 ms and v5 times of 2.48–2.62 ms.
The matching map/filter times were 0.115–0.131 ms for v4 and 4.03–4.13 ms
for v5. Counts and checksums matched at both 1,000 and 100,000 rows. These
local samples show that source construction and collection account for much
of the gap before map/filter dispatch. The v5 vector source computes a
reproducible content digest and clones elements for repeatable reads, whereas
the v4 source has different ownership behavior; the test compares user-visible
workloads, not identical internal contracts. Batch-level output counter
updates also reduced the earlier five-run v5 map/filter median from 5.07 to
4.79 ms before `source_copy` was inserted; those measurements used a different
warmup sequence and should not be mixed with the paired times above.

Direct now opens a typed iterator when a source feeds an output without
transforms. This avoids creating and immediately downcasting one boxed
envelope per element. The ordinary envelope factory remains registered for
transform routes and portable workers, and both factories open the same
repeatable source. Four warmed 100,000-row `source_copy` runs after this
change measured 0.922–0.928 ms, down from the earlier 2.48–2.62 ms range.
The corresponding map/filter route still uses envelopes; its timings varied
between runs and do not establish a transform-path improvement. The Direct
tests cover typed custom source values, batch metrics, source-only checkpoint
recovery, and factory/iterator panic conversion.

On 2026-10-10, after the typed same-type Direct path landed, five warmed
100,000-row release runs of each corpus executable produced these medians.
The v4 executable used `legacy-v4,io-jsonl`; v5 used its default feature set.
Each time includes source construction through the terminal, and output counts
and checksums matched. These are local characterization samples, not stable
release thresholds:

| Workload | v4 ms | v5 ms | v5/v4 |
| --- | ---: | ---: | ---: |
| Source copy | 0.059 | 1.191 | 20.1× |
| Map/filter | 0.112 | 1.227 | 11.0× |
| GroupByKey | 0.682 | 12.384 | 18.1× |
| Inner join | 11.960 | 33.240 | 2.8× |
| JSONL round trip | 4.093 | 11.429 | 2.8× |

The earlier 0.63 ms v5 map/filter figure measured the execution segment
alone. The paired table measures the whole user-visible operation. A trial
that batched vector-source SHA-256 updates kept the versioned digest identical
but raised the warmed source-copy median to about 1.32 ms and map/filter to
about 1.34 ms, so it was reverted. The performance replacement gate remains
open, especially for vector-source identity and grouping.

The built-in two-element tuple coder now appends scalar wire bytes directly
into a reusable buffer. Its length prefixes, source digest, and decode format
are unchanged. In a five-run warmed v5 sample, the GroupByKey median fell
from 12.384 to 4.529 ms and inner join from 33.240 to 19.233 ms, with matching
counts and checksums. A phased sample put GroupByKey source construction near
1.3 ms and execution near 3.1 ms, down from about 9.1 and 3.0 ms before the
coder change. Higher-arity tuples, vectors, and options now append into a
reusable wire buffer as well. Tests compare their bytes with the earlier
length-prefixed encoding and verify that a custom element encodes only once.
Their latency has not yet been measured in the paired corpus. The performance
gate remains open.

On the later `ced416a` testing-coverage revision, six alternating release runs
per version at 100,000 rows used the first pair as warmup and compared the
five remaining medians. Each workload's output row count and checksum matched
between v4 and v5. These local timings still include source construction
through terminal completion and are characterization samples:

| Workload | v4 ms | v5 ms | v5/v4 |
| --- | ---: | ---: | ---: |
| Source copy | 0.066 | 1.152 | 17.4× |
| Map/filter | 0.126 | 1.191 | 9.5× |
| GroupByKey | 0.621 | 4.391 | 7.1× |
| Inner join | 11.235 | 17.671 | 1.6× |
| JSONL round trip | 4.085 | 11.710 | 2.9× |

In the warmed v5 runs, map/filter spent roughly 0.5–0.6 ms constructing the
versioned source and 0.6–0.8 ms in Direct execution. GroupByKey spent roughly
1.2–1.4 ms constructing its source and 3.1–3.4 ms executing. Inner join spent
roughly 2.4–2.7 ms constructing both sources and 14.9–15.5 ms executing.
The source-content digest is a real v5 contract cost, but it does not explain
the grouping and join execution gaps. Peak memory and build/test-time paired
measurements remain open.

Direct now updates materialized output counters once after a successful
terminal instead of performing two atomic writes for each emitted row. The
grouping and join metric tests still report the same counts. A separate
five-run warmed v5 sample measured the inner join at 17.167 ms versus
17.671 ms in the prior paired sample; GroupByKey measured 4.385 versus
4.391 ms. This small difference may be run noise and does not close the
performance gate.

On 2026-10-11, six alternating release runs of the 100,000-row source-copy
case used the first pair as warmup. The remaining medians were 0.304 ms for
v4 and 3.072 ms for v5, a 10.1× gap on that host load; row counts and checksums
matched. These absolute times differ from the earlier paired sample, so the
ratio should be read within this run. An experimental 16 KiB batch of the
source digest's SHA-256 updates preserved the digest byte contract but did not
improve repeated v5 timing, so the change was reverted. The source-copy
performance gate remains open.
One temporary release probe of that case split Direct's run into about
0.049 ms for snapshot, 0.001 ms for validation, 0.134 ms for planning and
pool setup together, and 1.160 ms for execution. Source construction before
the run accounted for most of the remaining roughly 1.66 ms. That probe was
removed after measurement; pool startup alone cannot explain the gap.

Direct's typed source-only route now appends a batch to the output store after
one exact budget check, falling back to per-element writes when spill is on.
Tests cover the exact memory boundary, spill files, and coder-panic attribution.
Two warmed six-run v5 source-copy samples had medians of 2.54 and 2.53 ms,
compared with the preceding 3.07 ms paired-run v5 median. These are adjacent
local characterization runs, not a controlled release threshold; the paired
v4 gap remains open.

The v5 benchmark now emits source-copy phase timings under
`IRONBEAM_BENCH_PHASES=1`, as it already does for the other in-memory cases.
One 2026-10-11 release run at 100,000 rows measured 1.403 ms in source
construction (including the stable content digest), 0.017 ms attaching the
terminal, and 1.187 ms in Direct execution; the full case was 2.723 ms.
These phases identify both construction and execution as material costs.
They are a diagnostic sample, not a new v4/v5 paired result.

After the bounded pair-output change, a new six-run alternating v4/v5 release
sample at 100,000 rows discarded each version's first run. Five-run medians
were 0.061/0.936 ms for source copy (15.5×), 0.099/1.158 ms for map/filter
(11.7×), 0.612/4.135 ms for GroupByKey (6.8×), 11.163/16.154 ms for inner
join (1.4×), and 3.990/10.665 ms for JSONL round trip (2.7×). Output counts
and checksums matched in every case. A separate six-run v5 source-copy phase
sample measured medians of 0.539 ms for source construction, 0.002 ms for
terminal attachment, and 0.410 ms for Direct execution. Host-load variation
remains material; these figures characterize the gap rather than set a
release threshold.
Six alternating runs of the full 100,000-row corpus under macOS
`/usr/bin/time -l` gave post-warmup median peak RSS of 25.8 MiB for v4 and 54.4 MiB for v5.
All five workloads' counts and checksums matched. This is whole-process peak
memory, not a per-operation allocation profile; the memory gate remains open.

## Runner-specific assumptions and Dataflow dependency check

- `src/helpers/common.rs` `collect*` executes a terminal through the direct `Runner`; `src/runner.rs` uses `ThreadPoolBuilder::build_global()` on parallel paths, so requested thread counts are process-wide best effort.
- `src/node.rs` embeds type-erased `Partition`, `VecOps`, and local partition closures in graph nodes. `src/planner.rs` optimizes a requested terminal chain and uses source `VecOps::len()` hints. These belong to a direct physical plan, not the v5 logical graph.
- `src/helpers/joins.rs` snapshots each input as a linear subplan, creates a dummy source anchor, and expects internal downcasts. `flatten`, `tee`, and `wait_on` also need explicit multi-input/output and dependency semantics in the v5 graph.
- `from_custom_source` exposes a type-erased `VecOps` payload; format readers and `read_*_streaming` remain bounded, often shard-backed. Immediate `write_*` methods collect before writing. The current `Timestamped<T>` and `TimestampMs = u64` do not encode streaming progress.
- The separate Dataflow workspace depends on `ironbeam = "4"` with `coders`, and patches it to `../ironbeam`. Full offline Cargo metadata fails because that path does not contain the package in this local layout (`rustflow` is the package checkout). `--no-deps` metadata succeeds, confirming manifest structure only. No file was changed in the separate repository.
- Its graph translator accepts one source-rooted linear chain; its fingerprint hashes node IDs, kinds, operation counts, and edges but no closure, coder, or source configuration. The v5 graph and runner sketch specifies stricter cross-process verification.

## Phase 1 entry check

The Phase 0 API inventory and contract sketch are in `V5_PHASE0_API_INVENTORY.md` and `V5_LOGICAL_GRAPH_CONTRACT.md`. Numeric coverage gates and package-specific CI should use the portable measured report rather than the historical LCOV file. The first workspace shell can preserve the v4 facade; package splits and the public runner trait wait for the vertical slice.
