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

The original per-file report is preserved in Git history at `866c517`.
Representative module lines/branches: `collection.rs` 97.83% / 87.50%;
`pipeline.rs` 100% / 100%; `planner.rs` 93.31% / 86.41%; `runner.rs`
61.82% / 56.82%; `helpers/joins.rs` 93.64% / 100%; `io/jsonl.rs` 87.28% /
75.00%; `io/mongodb.rs` 0% / 0% because its Docker tests were skipped. New
core, validation, and lowering code should get branch-focused tests; a
global percentage alone could hide an uncovered runner error path. The
historical `lcov.info` in the execution plan is not used as a gate.

## Representative behavior and costs

`crates/ironbeam-compat/tests/v4_baseline.rs` fixes sequential elementwise order, empty/singleton behavior, parallel result multisets, keyed grouping and join multiplicity, and immediate bounded JSONL round trip against published Ironbeam 4.1.1. It intentionally does not promise global parallel ordering or distributed retry semantics. Current v4 local operations do not declare source replay or sink commit guarantees.

`crates/ironbeam-compat/examples/v4_benchmark_corpus.rs` prints stable workload names, row counts, elapsed nanoseconds, output counts, and checksums. On this host, a **debug** run with 10,000 rows gave map/filter 1.46 ms (5,000 rows; checksum 74,985,000), group-by-key 3.09 ms (100 groups; total 10,000 values), inner join 28.69 ms (10,000 rows; checksum 100,000,000), and JSONL write/read round trip 6.11 ms (10,000 rows; checksum 49,995,000). Earlier runs of the first three cases were faster, so these are characterization samples, not release thresholds. `examples/v5_benchmark_corpus.rs` now runs the same four workloads through the v5 facade for direct comparison. `/usr/bin/time -l` could not report peak RSS in the sandbox (`sysctl kern.clockrate: Operation not permitted`), so memory measurement remains an explicit gap.

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

An adjacent alternating twelve-run 100,000-row v5 sample then compared the
same release executable before and after skipping pool creation for a plain
single vector-source copy. The first run of each executable was warmup.
Source-copy medians moved from 0.770 to 0.602 ms with matching counts and
checksums. Map/filter medians were 1.145 and 1.163 ms; user transforms still
run inside their own job pool. This change leaves source identity and repeatable
vector reads intact. The result remains substantially slower than v4, and the
performance gate stays open.

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

The benchmark examples now accept an optional second argument naming one case,
so macOS can measure each workload in a separate process. Four paired release
runs per case at 100,000 rows, discarding each binary's first run, measured:

| Case | v4 time | v5 time | v4 peak RSS | v5 peak RSS |
| --- | ---: | ---: | ---: | ---: |
| source copy | 0.099 ms | 0.880 ms | 3.33 MiB | 3.81 MiB |
| map/filter | 0.131 ms | 1.358 ms | 3.73 MiB | 3.67 MiB |
| GroupByKey | 0.641 ms | 4.207 ms | 5.41 MiB | 14.16 MiB |
| inner join | 11.018 ms | 16.533 ms | 21.44 MiB | 50.48 MiB |
| JSONL round trip | 3.882 ms | 10.740 ms | 4.36 MiB | 4.91 MiB |

Every pair matched output rows and checksums. These are whole-process peaks
for isolated cases, so they include runtime setup; they do not count retained
or temporary allocations within each operation. Grouping and joins are the
clearest memory targets. An exact source-vector reservation trial reduced
GroupByKey peak RSS from 14.19 to 13.08 MiB in an adjacent eight-run
comparison, but inner join moved from 48.94 to 50.48 MiB and its median
latency from 17.345 to 19.156 ms. That trial was reverted.

Direct now streams bounded linear GroupByKey input batches into its keyed
accumulator instead of retaining every input envelope before grouping. It
preserves the previous source budget check and group ordering; non-linear
inputs retain the materialized fallback. An adjacent eight-run alternating
release comparison at 100,000 rows measured GroupByKey at 14.19 MiB peak RSS
and 4.506 ms before, versus 4.72 MiB and 4.739 ms after. Output counts and
checksums matched. The roughly 5% latency difference needs more sampling;
the peak-memory reduction is much larger and consistent with avoiding the
whole-source envelope vector. Join output uses a separate path and its memory
gate remains open.

The benchmark corpus now also isolates a duplicate-key inner join: 100,000
left rows over 100 keys joined to one right row per key. The two versions
produce the same 100,000 output rows and checksum 5,005,000,000. The v5 join
index now stores a single left position inline and allocates a positions
vector only when a key repeats. In ten alternating release runs per case,
discarding the first run of each binary, the unique-key join moved from
17.332 ms and 50.53 MiB peak RSS to 15.356 ms and 47.42 MiB. The
duplicate-key join moved from 7.625 to 7.690 ms with peak RSS unchanged at
31.61 MiB. Counts and checksums matched in every run. This is a local sample;
the remaining v4/v5 join memory gap is still large.

Direct source-only batch collection now uses a fixed wire-length hint for
built-in scalars and fixed-size tuples, preserving the exact output budget
without asking each value for its encoded length. The existing exact-budget,
spill, and coder-panic tests pass. An adjacent six-run alternating release
sample measured source copy at 0.061 ms for v4 and 0.897 ms for v5 (14.8×),
with matching output and checksums. The prior v5 median was 0.936 ms, but the
small difference is within the range of local run variation and is not yet a
demonstrated performance improvement.

The `from_vec` source now retains a bounded slice reader alongside its typed
iterator. Source-only Direct jobs use that reader to clone each configured
batch without the per-value `Result` iterator layer; other source kinds and
checkpointed jobs keep their existing path. Adjacent six-run alternating
release samples measured source copy at 0.062 ms for v4 and 0.768 ms for v5
(12.4×), with matching counts and checksum. The previous v5 median was
0.897 ms. This improvement is promising but remains a local sample; source
construction and remaining Direct overhead still dominate v4.
After adding typed range errors to that reader, another alternating six-run
sample measured 0.058 ms for v4 and 0.780 ms for v5 (13.4×). Counts and
checksums still matched across all five workloads. The two adjacent v5
source-copy medians agree within 0.012 ms; the v4 denominator moved enough
to change the ratio.

A follow-up 100,000-row source-copy trial batched the bytes sent to SHA-256
while retaining the same digest format. Six alternating runs before the trial
measured post-warmup medians of 0.064 ms for v4 and 0.770 ms for v5, with
0.533 ms of the v5 case in source construction. Eight alternating runs after
the trial measured 0.060 ms for v4 and 0.909 ms for v5, with 0.662 ms in
source construction. The added byte copying outweighed fewer hash calls, so
the implementation was reverted. This is a local comparison, not a release
threshold; source identity and output checksums were unchanged.

Another eight-run alternating v4/v5 release sample on 2026-10-11 discarded
the first run of each executable. The 100,000-row medians were 0.069/0.924 ms
for source copy (13.5×), 0.100/1.322 ms for map/filter (13.2×),
0.629/4.802 ms for GroupByKey (7.6×), 11.646/18.182 ms for inner join
(1.6×), and 4.322/11.300 ms for JSONL round trip (2.6×). The examples
produced matching output counts and checksums. One v5 source-copy phase sample
within that run spent 0.664 ms constructing the source, 0.003 ms attaching
the terminal, and 0.214 ms executing Direct. Those phases indicate source
identity construction is the largest remaining source-copy cost; the sample
does not isolate every operation within that phase.

A later twelve-run alternating release sample, with each executable's first
two runs discarded, measured v4/v5 medians (ms) of 0.061/0.607 for source copy
(9.9×), 0.105/1.159 for map/filter (11.1×), 0.617/4.285 for GroupByKey
(6.9×), 11.261/16.297 for inner join (1.4×), and 3.800/10.466 for JSONL
round trip (2.8×). Counts and checksums matched in every run. Host load
changed absolute times, but the paired gap persists. A separate alternating
source probe compared the existing per-row SHA-256 length and payload updates
with a single update of a short stack frame. Source-construction medians were
0.519 ms and 0.597 ms respectively, so the trial was reverted. The digest
bytes and source identity contract remain unchanged.

On 2026-10-11, a second digest trial buffered length-prefixed element bytes
in 64 KiB blocks before SHA-256 updates. A digest test covered multiple
blocks and a value larger than a block. Twenty alternating release runs of
the original and trial binaries, discarding the first two runs of each,
measured source copy at 0.658/0.721 ms and map/filter at 1.201/1.274 ms;
GroupByKey was 4.602/4.622 ms. Output counts and checksums matched.
The trial regressed both source copy and map/filter, so it was reverted.
A separate local microbenchmark also found per-value BLAKE3 updates slower
than the existing hardware-accelerated SHA-256 path on this arm64 host.
No source digest algorithm or version changed.

The next trial defers `from_vec` source hashing for the built-in infallible
coders until a runner requests source identity. Direct only requests that
identity for checkpointed jobs; portable lowering still requests and receives
the original `sha256-v1` digest. Custom coders still encode during source
registration so their errors retain the original timing. In twenty alternating
release runs per binary at 100,000 rows, discarding the first two of each,
source copy fell from 0.605 to 0.104 ms and map/filter from 1.217 to 0.728 ms.
Source construction fell from 0.523 to 0.028 ms in source copy. GroupByKey
was 4.634/4.804 ms and did not improve. All cases kept identical output
counts and checksums. A separate fourteen-run alternating v4/v5 sample,
discarding the first two of each, measured 0.059/0.109 ms for source copy
(1.8×), 0.102/0.652 ms for map/filter (6.4×), 0.587/4.493 ms for
GroupByKey (7.7×), 10.987/15.206 ms for inner join (1.4×), and
3.846/10.376 ms for JSONL round trip (2.7×). These are local medians, not
release thresholds. The source-copy gap narrowed substantially; the
remaining transform, grouping, and I/O costs still need work.

A bounded-vector batch reader now feeds sequential typed transform routes when
checkpointing is disabled. It keeps the same batch limit, source attribution
for clone panics, cancellation checks, and source/output counters; checkpointed
routes still use the iterator to skip recovered input. In twenty-four
alternating release runs per binary at 100,000 rows, discarding the first two
of each, map/filter moved from 0.708 to 0.611 ms (about 14% faster).
Source copy was 0.105/0.106 ms and GroupByKey 4.582/4.581 ms. All three cases
retained identical output counts and checksums. This is a local sample; the
remaining transform overhead versus v4 is still substantial.

The paired corpus now has a repeatable driver in
`scripts/compare_v4_v5_benchmarks.py`. It alternates rebuilt release
executables, checks output row counts and checksums on every run, discards two
warmup pairs, and reports medians from eight pairs per case. An optional macOS
`/usr/bin/time -l` path measures each process's peak resident memory; optional
ratio arguments can enforce release limits once those limits are agreed.
A current 100,000-row run on 2026-10-11 measured:

```sh
cargo build --release --example v5_benchmark_corpus
cargo build --release -p ironbeam-compat --example v4_benchmark_corpus
python3 -B scripts/compare_v4_v5_benchmarks.py \
  --v4 target/release/examples/v4_benchmark_corpus \
  --v5 target/release/examples/v5_benchmark_corpus \
  --rows 100000 --warmups 2 --samples 8 --rss
```

Use the configured Mr Boxington Cargo wrapper for the two builds. The `--rss`
option uses macOS `/usr/bin/time -l`; omit it on other hosts. The driver
also accepts `--max-latency-ratio` and `--max-rss-ratio` for a future release
threshold.

| Case | v4 ms | v5 ms | v5/v4 | v4 peak MiB | v5 peak MiB |
| --- | ---: | ---: | ---: | ---: | ---: |
| Source copy | 0.060 | 0.115 | 1.91× | 3.41 | 3.87 |
| Map/filter | 0.105 | 0.328 | 3.12× | 3.83 | 3.55 |
| GroupByKey | 0.578 | 1.103 | 1.91× | 5.23 | 4.52 |
| Latest per key | 0.660 | 1.166 | 1.77× | 7.31 | 3.89 |
| Inner join | 11.109 | 12.651 | 1.14× | 21.50 | 47.53 |
| Duplicate-key join | 2.741 | 5.968 | 2.18× | 5.53 | 31.66 |
| JSONL round trip | 4.105 | 10.798 | 2.63× | 4.33 | 5.05 |

The process peaks include runtime setup and are not an allocation profile.
Host load can move absolute times; the paired comparisons identify the next
work: map/filter and JSONL latency, and join memory, especially duplicate
keys. No acceptable regression limits have been set, so this measurement
does not close the performance gate.

The Direct materialized path now preallocates its source envelope vector only
when the source is a registered vector with a known length, capped by the
configured memory budget. A targeted rerun with the same paired settings
measured v5 peak memory at 46.00 MiB for inner join and 29.91 MiB for
duplicate-key join, versus 47.53 and 31.66 MiB before the change. The v4
peaks remained 21.50 and 5.53 MiB. Output counts and checksums matched;
Direct tests, formatting, and strict lint passed. This modest gain leaves
the join memory gate open.

On 2026-10-11, an adjacent eight-sample release comparison of the v5 binary
before and after indexing the smaller join input measured the duplicate-key
case at **29.95 → 23.06 MiB** peak RSS and **5.833 → 5.830 ms** median latency.
The equal-size inner join remained on its existing path and measured
**46.01 → 46.82 MiB** and **12.201 → 13.155 ms** in the same run. Counts and
checksums matched. The new skewed-input Direct test checks all four join
variants, unmatched rows, timestamps, and windows; the v4/v5 differential
corpus still passes. This reduction does not yet establish an acceptable
release memory limit against v4.

The eager JSONL parser now reads borrowed line slices and parses directly
from bytes. The shared eager file source decodes the same bytes whose digest
it checked at execution, avoiding a third file read and a possible change
between validation and decoding. Default and all-feature JSONL tests pass,
including malformed UTF-8, Unicode blank lines, CRLF, and compressed files;
strict all-feature lint passes. An adjacent 12-sample release comparison
against the binary before both changes measured JSONL round trip at
**10.227 → 8.177 ms** and **5.06 → 4.44 MiB** peak RSS, with matching counts
and checksums. A fresh eight-sample paired v4/v5 comparison measured:

| Case (100,000 rows) | v4 ms | v5 ms | v5/v4 | v4 peak MiB | v5 peak MiB |
| --- | ---: | ---: | ---: | ---: | ---: |
| Source copy | 0.061 | 0.118 | 1.92× | 3.41 | 3.90 |
| Map/filter | 0.113 | 0.332 | 2.94× | 3.83 | 3.58 |
| GroupByKey | 0.560 | 1.091 | 1.95× | 5.05 | 4.52 |
| Latest per key | 0.654 | 1.239 | 1.89× | 7.31 | 3.84 |
| Inner join | 11.022 | 13.070 | 1.19× | 21.50 | 45.96 |
| Duplicate-key join | 2.742 | 5.719 | 2.09× | 5.53 | 21.42 |
| JSONL round trip | 3.983 | 8.016 | 2.01× | 4.33 | 4.45 |

Counts and checksums matched in all seven cases. These are local medians,
not release thresholds; join memory and JSONL latency still need work.

Equal-size and smaller-left Direct inner joins now retain only indexed
values and positions. Skewed smaller-right and outer joins keep their
existing bookkeeping. In a 12-sample adjacent release comparison of the
final code at 100,000 rows, equal-size inner join moved from
**45.97 → 39.54 MiB** peak RSS and **12.810 → 11.385 ms** median latency.
The duplicate-key case stayed near baseline at **22.20 → 21.52 MiB** and
**5.666 → 5.671 ms**.
Counts and checksums matched. Direct tests cover equal-size and skewed joins,
duplicate multiplicity, fixed-window isolation, and right-side output time;
core and Direct tests plus strict lint pass. The v5 join remains above v4's
measured memory use.

## Runner-specific assumptions and Dataflow dependency check

- `src/helpers/common.rs` `collect*` executes a terminal through the direct `Runner`; `src/runner.rs` uses `ThreadPoolBuilder::build_global()` on parallel paths, so requested thread counts are process-wide best effort.
- `src/node.rs` embeds type-erased `Partition`, `VecOps`, and local partition closures in graph nodes. `src/planner.rs` optimizes a requested terminal chain and uses source `VecOps::len()` hints. These belong to a direct physical plan, not the v5 logical graph.
- `src/helpers/joins.rs` snapshots each input as a linear subplan, creates a dummy source anchor, and expects internal downcasts. `flatten`, `tee`, and `wait_on` also need explicit multi-input/output and dependency semantics in the v5 graph.
- `from_custom_source` exposes a type-erased `VecOps` payload; format readers and `read_*_streaming` remain bounded, often shard-backed. Immediate `write_*` methods collect before writing. The current `Timestamped<T>` and `TimestampMs = u64` do not encode streaming progress.
- The separate Dataflow workspace depends on `ironbeam = "4"` with `coders`, and patches it to `../ironbeam`. Full offline Cargo metadata fails because that path does not contain the package in this local layout (`rustflow` is the package checkout). `--no-deps` metadata succeeds, confirming manifest structure only. No file was changed in the separate repository.
- Its graph translator accepts one source-rooted linear chain; its fingerprint hashes node IDs, kinds, operation counts, and edges but no closure, coder, or source configuration. The v5 graph and runner sketch specifies stricter cross-process verification.

## Phase 1 entry check

The [Phase 0 API inventory](V5_PHASE0_API_INVENTORY.md) remains the public-surface audit input. The historical graph sketch and initial workspace plans are available in Git history; current contract decisions and open work are in the [release gate status](V5_RELEASE_GATE_STATUS.md) and [replacement roadmap](V5_REPLACEMENT_ROADMAP.md). Numeric coverage gates and package-specific CI should use the portable measured report rather than the historical LCOV file.
