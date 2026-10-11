# Ironbeam 5.0.0 release gate status

Measured on `codex/ironbeam-v5-runner-core` through 2026-10-11. This is a working
audit, not a release declaration. The v5 packages remain at `0.1.0`, the root
facade defaults to v5 but is still versioned `4.1.1`, and publication is
staged locally.

## Verified in this checkout

- The separate Dataflow worker now validates connected branches and
  fixed-window assignment after a production unbounded SDF source. A live
  local Prism `Run` reached `DONE` across two residual bundles; three idempotent
  sinks verified six elementwise values, event timestamps, assigned intervals,
  and two grouped results, including a native GroupByKey that combined two
  production rows in one window. A Fn API test verifies both branches with
  one source open per bundle across a rebuilt graph. A separate Fn Data test
  runs the residual bundle in a new
  OS process with a newly built graph and verifies cursor resume. Killing the
  Prism process worker during a bundle makes this local Prism version fail the
  job rather than restart the worker; runner-managed process recovery is not
  claimed. Other SDF graph shapes remain open.
- Direct bounded linear sinks now write one source batch at a time instead of
  collecting the entire result before opening the sink. Typed sinks can opt in
  to `write_with_metadata` to receive event time, window, and assigned interval;
  existing value-only sinks keep their `write` implementation. A bounded
  source fixture verifies that a batch is written before the next is read, and
  tests cover metadata on linear and materialized paths, filtering, cancellation,
  and panic attribution. Sequential same-type map/filter routes over built-in
  vector sources now use typed batches before writing each sink batch; the
  sink test confirms the first batch is written before the next transform runs.
  Parallel or metadata-changing routes retain the envelope path. Full Direct
  tests and strict lint pass for this addition. Directly connected GroupByKey,
  global combine, and keyed combine folds can now drain completed results into
  their sinks one at a time, avoiding a second whole-result vector. Tests cover
  grouped values, sink write failure and abort, fixed-window metadata, empty
  global input, and keyed sums; core and Direct suites and strict lint pass.
  Fold state still retains groups, and other materialized sink routes
  still evaluate their full bounded graph before writing, so streaming and
  memory limits remain an open Direct gate. An alternating 16-run release sample of the
  100,000-row JSONL round trip measured 10.52 ms median after this change
  versus 10.74 ms in the preserved pre-change binary, with matching 100,000
  rows and checksum 4,999,950,000. This is a local performance sample, and the
  broader v4/v5 performance gate remains open. The fresh combined
  core/Direct/transforms coverage corpus passes the unchanged core and Direct
  gates: core 2,337/2,424 source locations and 348/396 branches; Direct
  1,638/1,734 source locations and 311/340 branches.
- The all-feature Dataflow workspace suite and the opt-in live local Prism
  fixture passed with the metadata-aware core contract. The fixture completed
  its multi-source, format, grouping, side-input, and finalized-sink job; the
  temporary Prism service was stopped afterward.
- Clean core and Direct coverage corpora now pass their unchanged CI ratios:
  core 2,194/2,282 source locations and 341/390 branch outcomes; Direct
  1,283/1,365 source locations and 238/258 branch outcomes. New Direct tests
  exercise fallible terminals, cancellation, lifted combine failures, and
  checkpoint recovery through flat-map output. That recovery test exposed an
  invalid assumption that output count cannot exceed source position. The
  checkpoint manifest now binds its progress fields and data digest with a
  canonical checksum, and its schema version is 1.1. Corrupt progress and frame counts
  are rejected; flat-map checkpoints recover successfully. Direct now aborts
  an open sink and reports cancellation if the job is cancelled while writing
  or immediately before commit. The typed source-only route now appends bounded
  output batches in one budget check when spill is disabled; exact-budget,
  spill, and panic-attribution tests pass.
- `bash scripts/check_v5_packages.sh` passed after the JSONL glob dependency cleanup:
  workspace tests, selected feature variants, all-feature
  documentation, and local package verification for all twenty-five v5 crates.
  No package was published.
- Root v5 tests no longer compile Testcontainers as an unconditional dev
  dependency. The legacy MongoDB/Postgres/MySQL suites use the opt-in
  `legacy-v4-service-tests` feature, which `--all-features` still enables in
  the live CI jobs. The default dependency tree excludes both service-test
  libraries; the v5 facade tests pass, and the all-feature legacy MongoDB
  target compiles and lists its thirty tests.
- The `cloud_io_demo` example now runs entirely through v5: it writes JSONL
  through a deferred URI object sink, reads and transforms the rows through a
  URI source, then copies the object between two registered schemes. URI glob
  sources also work; their child sources bind scheme and content identity.
  The URI sink binds its destination to the graph configuration. Its v5-only
  build and strict lint pass. The remaining legacy fake cloud-service
  families are still an API migration gap.
  Provider-neutral object access now offers atomic create-if-absent for
  immutable manifests. The local backend and the opt-in hosted adapter use
  the same contract; local and in-memory hosted-adapter tests verify that
  conflicting writes cannot replace the first object. The isolated cloud
  object coverage gate measures 260/269 source locations and 49/56 branches,
  above its unchanged CI baseline.
- `ironbeam-transform-regex` owns the v5 regex helpers and measures 45/45
  source locations in its isolated tests. The ordinary
  `ironbeam-transforms` dependency tree no longer includes `regex`. The
  root v5 prelude exposes `RegexExt` under `v5-regex`, and the compatibility
  prelude still exposes it directly. The extracted package, common transforms,
  and compatibility crates pass CI-strength pedantic and nursery lint.
- `ironbeam-transform-approx` owns KMV/HLL approximate distinct helpers. Its
  sketch merge and error-bound tests measure 108/108 source locations and
  12/12 branches. The v5 root prelude exposes them under `v5-approx`; common
  transforms no longer depends on `hyperloglogplus`. The tests caught and
  fixed a zero-error-bound acceptance bug in v5 HLL construction.
- `ironbeam-io-csv` is isolated from the common file lifecycle package. Its
  minimal and all-feature tests, package verification, strict lint, and
  85/86 source-location plus 8/8 branch coverage passed. Scalar and tuple
  writes fail before replacing a previous output file.
- `ironbeam-io-xml` is isolated from common I/O. Its all-feature tests measure
  42/43 source locations and 2/2 branches. Strict lint and local package
  verification pass; shared I/O's default tree has no `quick-xml` dependency.
- `ironbeam-io-msgpack` is isolated from common I/O. Its all-feature tests
  measure 63/66 source locations and 5/6 branches. The complete package gate
  passes. Shared I/O's default tree has no `rmp-serde` dependency.
- `ironbeam-io-cbor` is isolated from common I/O. Its all-feature tests
  measure 60/63 source locations and 5/6 branches. The full package gate
  passes. Shared I/O's default tree has no `ciborium` dependency.
- `ironbeam-io-jsonl` owns file and object adapters. Its all-feature tests
  now measure 342/361 source locations and 32/34 active branch outcomes,
  including append failure cleanup, four compressed object formats, and
  cloud glob behavior; both exceed its existing CI coverage ratios. Its
  all-feature dependency tree has no legacy `ironbeam-cloud` dependency.
  Cloud globs now use a focused wildcard matcher that preserves the existing
  `*`, `**`, `?`, literal, Unicode, and newline rules without pulling the
  regex engine into default v5 root tests. The package gate checks that both
  regex and Testcontainers stay out of the v5-only root dependency tree.
  Protobuf is in `ironbeam-io-protobuf` with a bundle-compatible append sink;
  it measures 136/146 source locations and 18/20 branches under all features,
  above its CI ratios. TFRecord has its own crate and a bundle-compatible
  append sink; its all-feature coverage is 209/226 source locations and 18/20
  branches, above the existing CI ratios. Shared lifecycle I/O measures 265/276
  locations and 25/26 branches through the extracted format packages.
  These measured baselines still leave uncovered error paths for release review.
- The separate Dataflow checkout has a Prism `Run` reaching `DONE` through a
  separately launched v5 worker for twenty-three bounded sources, including a keyed
  input shared by GroupByKey and an ordinary branch, native Flatten and
  GroupByKey, event-time assignment, native fixed-window assignment with
  offset, per-window GroupByKey, two-, three-, and ten-input CoGroupByKey,
  fixed-window CoGroupByKey, all four join variants with duplicate and
  unmatched keys, a fixed-window full join that retains input timestamps,
  a bounded side input fetched over Beam Fn State, including a side collection
  with values in two fixed windows that is globally materialized to match
  Direct's side-view behavior,
  native Reshuffle in global and fixed windows,
  and thirty terminal
  sinks. Six sinks commit pending fragments and accept them only after
  Prism's `FinalizeBundle` callback; the live proof verifies all three rows and
  leaves no pending fragments. The translator requires an immediate Reshuffle
  before a finalized sink, since a fused stage reached `DONE` without delivering
  that callback. User-defined records pass through both the extracted file JSONL
  adapter and a provider-neutral object-backed JSONL source in that live job.
  The latter reconstructs and verifies the same object digest in the separately
  launched worker. The live job also reads a native Avro container written
  with an explicit schema by v5 Direct and checks its transformed record.
  An extracted CSV source crosses the live Prism worker coder boundary.
  A length-delimited Protobuf file source decodes a typed v5 element, runs a
  transform in the worker, and writes the asserted result through a terminal sink.
  A separate Protobuf append sink writes length-delimited records in the worker;
  the v5 reader verifies the typed output. The append path needs a shared
  filesystem and may duplicate records on replay. A separate finalized
  Protobuf sink now publishes one validated length-delimited file after
  `DONE`; the v5 reader reopens all three original rows.
  A TFRecord source reads a v5 Direct-written record through the separately
  launched worker, decodes its Protobuf payload, and writes the asserted
  transformed row. A separate TFRecord append sink writes complete frames in
  the Prism worker; the v5 reader validates the output bytes and CRCs. This
  append path has at-least-once replay semantics and needs a shared filesystem.
  The original TFRecord whole-job replacement sink still needs the finalized
  fragment adapter when used through the portable worker.
  CSV globs also lower to multiple source nodes and native Flatten in the
  mock worker contract. The format-heavy live proof now lives in the
  unpublished `ironbeam-prism-proof` package, so the Dataflow runtime package
  no longer directly depends on those connector crates.
  The JSONL append sink uses a shared filesystem and may duplicate records on
  replay. This is a local runner proof of this bounded slice.
- The separate Dataflow checkout's graph translator, Fn API harness, and
  Dataflow runtime packages now have v5-only dependencies and protocol tests.
  The old v4 entrypoints, translator, handler, and example have been removed;
  the all-feature Dataflow workspace suite passes. The root `ironbeam` package
  gates its v4 modules and direct legacy dependencies behind `legacy-v4`, and now
  defaults to the v5 facade. The default build has only core, Direct,
  transforms, shared I/O, and JSONL as
  direct dependencies; it excludes the legacy cloud package. The v4 root
  integration tests are also gated by `legacy-v4`, allowing the complete
  root test target set to build in v5-only mode. The crate root and README
  now show a v5 quick start, with a passing doctest; legacy builds retain
  their old documentation. Four examples still require `legacy-v4`,
  including the v4 benchmark corpus. Advanced joins, CoGroupByKey, JSONL
  globbing, pluggable compression, the Direct plan explanation example, and
  the data-quality validation, windowing aggregation, combiners showcase, and
  multi-output, ETL, SQLite, and MongoDB examples now run through v5. The ETL
  example verifies grouping, traffic ranking, bot counts, and CSV and JSONL
  round trips. The database
  examples verify their sources, transforms, and deferred sinks against a
  temporary SQLite file and a temporary local MongoDB container, respectively.
  The seven-part testing example
  uses the separate `ironbeam-testing` crate through an opt-in `v5-testing`
  feature. Its package-local assertion, builder, fixture, mock I/O, and graph
  tests now measure 493/495 source locations and 30/30 active branch outcomes
  from a clean instrumented target. Those ratios are gated in CI. The tests
  caught unordered comparisons that ignored duplicate counts and a CSV mock
  writer that emitted a header when disabled; both are fixed. The two
  remaining uncovered source paths are failed graph snapshot and temporary
  CSV creation. The metrics example now uses optional `ironbeam-metrics` for custom
  counters, gauges, histograms, and JSON reports. Direct job configuration
  attaches that collector while `JobHandle` keeps the built-in execution
  counters. The `metrics` feature aliases `v5-metrics`; neither the optional
  crate nor its JSON dependency enters the default root dependency graph.
  Its isolated test corpus measures 215/216 source locations and 15/16
  active branch outcomes, and local Cargo packaging passes without publishing.
  Compression remains opt-in under
  `compression-gzip`. The multi-output example collects one classified bounded
  output per job and separates typed buckets locally. A counting-source test
  verifies one source open per partition job. Direct still lacks general
  graph-level side outputs that run independent downstream branches together;
  the multi-output runner gate remains open.
  Direct now also offers `run_collect_pair` for two bounded linear output
  branches in one job. It opens their shared source once, evaluates their
  common transforms once, preserves time and window metadata, and applies a
  combined memory budget with optional spilling. The split value needs a
  registered coder; fan-in, stateful routes, and checkpointed pair jobs are
  rejected before source access. Arbitrary multi-sink graph execution is
  still required to close the multi-output runner gate. Pair transform stages
  now reject element expansion beyond the configured buffer budget, and
  parallel pair jobs drain outputs after at most one worker-width chunk rather
  than materializing every result in a source batch. A user transform can
  still allocate its own result before returning it, so this is not a hard
  process memory bound. Eleven focused pair-job
  tests pass, and a clean core/Direct/transforms coverage run passes the CI
  thresholds: Direct 1,479/1,576 source locations and 267/292 branch outcomes;
  core 2,230/2,318 source locations and 341/390 branch outcomes.
  Direct now also runs two deferred sink branches from the same bounded linear
  source in one job. Their common transforms execute once, and each sink keeps
  event-time/window metadata. Both sinks must declare idempotent replay because
  commits happen in sequence: a failure in the second commit can leave the
  first committed. Tests cover single source execution, branch values and
  metadata, preflight rejection, write and factory failures, cancellation,
  both commit-failure positions, and source-batch backpressure. Sequential
  pair sinks write each element before moving to the next. The fresh combined
  coverage corpus passes the existing gates: core 2,340/2,427 source locations
  and 348/396 branches; Direct 1,781/1,893 source locations and 344/376
  branches. General multi-sink DAGs, atomic commit,
  checkpointed sink jobs, and unbounded execution remain open.
  Both v4 and v5 benchmark examples produce matching counts and checksums
  for four workloads at 1,000 rows. A single aligned 100,000-row release
  sample found substantial v5 latency regressions in all four cases; see
  `V5_PHASE0_BASELINE.md`. Exact encoded-length accounting reduced allocations
  and improved join latency in repeated release runs, but the performance gate
  remains open. Reusing the vector-source digest buffer reduced a 100,000-row
  scalar source probe from 4.13 ms to roughly 1.2 ms without changing its
  versioned digest; Direct execution remains the larger gap. The v4
  implementation remains in the repository. A dedicated single-result
  transform path then reduced the 100,000-row v5 map/filter median from
  24.572 ms to 14.058 ms, preserving filter metrics and flat-map behavior;
  streaming the single-worker results within the dedicated pool reduced the
  median further to 13.332 ms. Binding those transforms once per route then
  reduced the five-run median to 5.565 ms. This is still about 12 times the
  measured v4 median of 0.456 ms. Reserving source batches from the iterator
  size hint subsequently brought the warmed five-run map/filter median to
  4.786 ms, still roughly 10.5 times the v4 median. Direct now updates its
  output counters once per source batch rather than once per emitted row. In a
  warmed five-run 100,000-row sample, the median moved from 5.07 ms to 4.79 ms;
  this is a local sample, and the performance gate remains open. The v5 Direct
  source-only route now opens typed values without creating and downcasting an
  envelope for each row. Four warmed 100,000-row samples moved from
  2.48–2.62 ms before this change to 0.922–0.928 ms afterward; map/filter
  still uses the envelope route. Moving sequential Direct panic isolation to
  each source batch, while retaining the failing transform key, brought a
  warmed 100,000-row map/filter sample from about 4.0 ms to 3.2 ms with
  matching output counts and checksums. The v4 map/filter implementation is
  still substantially faster, so the performance gate remains open. Direct
  now keeps same-type sequential map/filter routes typed from source through
  output. A five-run 100,000-row release sample moved the map/filter run
  median from roughly 3.2 ms to 0.63 ms with the same 50,000 output rows and
  checksum. Type-changing, metadata-bearing, and parallel routes keep the
  envelope path; core, Direct, root workspace, and Dataflow workspace tests
  pass. The v4 map/filter path remains faster in local measurements, and the
  performance gate stays open. The two-element tuple coder now reuses its
  encoded buffer while preserving the wire digest. A warmed five-run v5
  corpus sample moved GroupByKey from 12.384 to 4.529 ms and inner join from
  33.240 to 19.233 ms at 100,000 rows; see `V5_PHASE0_BASELINE.md` for the
  paired v4 figures and phase breakdown. Higher-arity tuples, vectors, and
  options now reuse coder buffers with byte-compatibility tests; no paired
  latency number has been recorded for them yet. A later alternating
  five-run 100,000-row paired sample still measured v5/v4 ratios of 17.4×
  for source copy, 9.5× for map/filter, 7.1× for GroupByKey, 1.6× for inner
  join, and 2.9× for JSONL; counts and checksums matched. The performance
  gate remains open. The v5 Direct
  collection trait now accepts `PCollection` as well as `Output`; it builds
  an isolated terminal snapshot, so repeated v4-shaped `collect_seq` and `collect_par`
  calls leave the user's graph unchanged. Named `Output` terminals remain for
  portable and durable jobs. The root package
  version is still 4.1.1, until the replacement gates close.
- `ironbeam-cloud-objects` owns URI routing, registered provider handles, the
  object contract, and a filesystem-backed local adapter. Its isolated corpus
  measures 234/243 source locations and 46/54 active branch outcomes. The
  previous `ironbeam-cloud` crate reexports this API during migration. A JSONL
  graph source and sink round-trip through that adapter. Opt-in
  `ironbeam-cloud-object-store` builds S3, GCS,
  and Azure Blob clients from environment credentials. Its
  `object_stores_from_env()` constructor registers all enabled providers in
  one URI router, with bucket clients still initialized on first use. Default
  and all-provider tests and strict lint pass. `ObjectStores::copy_prefix`
  copies relative keys across URI schemes using a fixed listing snapshot;
  local and in-memory cross-provider tests pass. The common adapter's operations
  pass against Apache `object_store`'s in-memory backend. Hosted
  storage authentication and live service behavior remain unverified.
- `ironbeam-compression` owns codec detection and stream wrapping, with a
  bounded writer that finalizes object bytes without a second JSONL buffer.
  All four codecs pass
  source/sink round trips; its isolated all-feature corpus measures at least 140/157
  source locations and 29/34 active branch outcomes. Compression-only shared
  I/O depends on this crate without pulling in legacy cloud helpers. Shared
  I/O's `cloud` feature now reexports the provider-neutral object contract;
  its all-feature dependency tree also excludes the legacy cloud package.

## Open replacement gates

1. **Core coverage quality:** The old package-only CI baseline recorded 972/974
   source locations and 210/222 branches before core grew. It omitted paths
   exercised through Direct and transforms tests, so CI now gates the
   three-package corpus. An earlier local report combined stale instrumented
   generic and macro instances, yielding 2,121/2,656 source locations and a
   false threshold failure. Repeating the exact CI corpus with an isolated
   coverage target measured 1,841/1,917 source locations and 326/378 branches.
   Tests for malformed set/map headers, tuple parts, and result bytes then
   raised the clean corpus to 1,846/1,917 (96.30%) and 327/378 (86.51%),
   above both existing CI thresholds. Coverage runs must start from a clean
   instrumented target; the denominator is sensitive to cached generic
   instances. Remaining uncovered paths include impossible-length guards and
   failure branches for graph and runner contracts. Continue targeted coverage
   work rather than treating this ratio as complete unit test coverage.
   The common transforms package now has a reviewed isolated baseline of
   1,092/1,120 source locations (97.50%) and 82/90 active branch outcomes
   (91.11%). The old 89/89, zero-branch baseline predates the expanded helper
   corpus and did not measure branches. Tests now exercise partial accumulator
   merges, constant side-input variants, sampler output forms, malformed
   dead-letter coders, validation collection, t-digest merges, and custom
   window combiners. The last test exposed and fixed recursion in both custom
   window-combiner helpers. Remaining uncovered locations include allocation
   overflow guards that cannot be reached with ordinary 64-bit buffers, plus
   statistical and side-input edges. The new extension packages have separate
   passing coverage gates. A fresh rerun after registry dispatch and time
   metadata tests measured core at 2,049/2,127 source locations (96.33%) and
   332/384 branches (86.46%), passing both current CI ratios. This remains
   a coverage quality review gate rather than a claim of complete unit tests;
   untested validation and capability branches still need review.
   The finalization adapter and fail-closed tests raised the fresh combined
   core/Direct/transforms corpus to 2,297/2,384 source locations (96.35%) and
   346/394 branches (87.82%), above both unchanged CI thresholds.
2. **Portable feature corpus:** The v5 translator now supports bounded,
   global-window `group_by_key_named` through native Beam KV/iterable coders
   and a live Prism shuffle proof. Its keyed input can also feed an ordinary
   branch, verified by another sink in that Prism job. It still rejects
   some unbounded source graph shapes, update sinks, and
   whole-job final-result sinks. Core now registers typed split/reassembly
   coders for actual `group_by_key_named` nodes, distinct from
   `latest_per_key_named`, which uses the same local operation marker.
   `cogroup_by_key_2_named` through `cogroup_by_key_10_named` register
   typed tagged-value coder contracts for their inputs. Tests cover
   heterogeneous values, empty groups, and invalid tags. The translator now
   expands co-group nodes into tagged ParDos, native Flatten, and native
   GroupByKey. A live Prism job verifies two and three heterogeneous inputs,
   ten same-typed inputs, shared branches, and fixed-window groups. Intermediate arities use
   the same generated path but lack individual live fixtures. The worker
   also reconstructs per-key left and right events from Beam's tagged
   shuffle and invokes the same aggregate used by Direct for inner, left,
   right, and full joins. Tagged join values preserve each input timestamp;
   the live Prism corpus checks duplicate-key products, unmatched rows,
   interval windows, and timestamps.
   The worker decodes fixed and chunked inline iterables and can resolve
   Beam state-backed iterables through the Fn State client. Prefix, token,
   and fetched suffix assembly have focused tests, and a mock runner drives
   the actual Fn Data and Fn State RPCs into a typed grouped result. A Prism-emitted
   state-backed group still needs a live proof. A local Prism job now also
   groups 512 distinct 4 KiB values and verifies the count, total payload
   length, and full-byte SHA-256 after shuffle. The tested Prism build encodes
   GBK iterables inline, so this verifies large inline payloads rather than
   state-backed fetching. Bounded millisecond-aligned fixed windows lower
   to native Beam
   `WindowInto`; the worker carries interval metadata across Fn API boundaries.
   One live Prism job verifies assigned intervals and keyed counts in each
   fixed window. The same job verifies native Reshuffle with values and
   fixed-window metadata intact. Other window types and full time-metadata
   parity remain open. Core now exposes the same bounded side-view
   materialization and mapping functions to Direct and portable workers;
   materialized views are transferable across executor threads. The separate
   Dataflow harness uses a tested Fn State client with continuation and error
   handling. Beam side-input ParDo lowering, bundle validation, and worker
   materialization reach `DONE` in local Prism for bounded global and
   fixed-window views. For a fixed-window side collection, the translator
   inserts a global-window branch so Beam materializes the same whole view
   as Direct. The live fixture uses an order-independent materializer;
   iterable order across the portable side branch is not yet proven to match
   Direct for order-sensitive materializers. Stateful or unbounded side views
   remain outside this proof.
   Bounded `wait_on` now lowers through a completion side view, with a
   global-window mapping for a windowed signal. A live Prism job verifies that
   its sink receives main values and fixed-window metadata only after the
   signal source finishes; altered signal wiring fails contract validation.
3. **Remote I/O:** Provider clients can access hosted object storage from the
   bounded `ObjectIO` interface, but the local Prism worker still shares the
   driver's filesystem. Driver-local file paths are not staged to cloud
   workers, and root file sinks use whole-job final-result commit semantics
   that are unsafe across independent Fn API bundles. Remote worker staging,
   a runner-wide output commit protocol, and live provider tests remain.
4. **Root replacement:** The root package now defaults to v5, but the v4 modules,
   opt-in feature graph, examples, and compatibility paths remain. Continue
   API migration and the focused v4 removal gate in
   `V5_REPLACEMENT_ROADMAP.md` before setting version `5.0.0`.
   The separate Dataflow workspace's WordCount example now uses the v5 graph,
   typed sink, and `run_v5_from_env`; its Direct result and portable protobuf
   preparation pass, and its default dependency tree excludes the v4 root.
   The Dataflow runtime and graph translator now expose only v5 lowering;
   their v4 entrypoints, translation tests, and example have been replaced.
   The all-feature Dataflow workspace suite and strict package lint pass.
   The separate Dataflow harness has also removed its optional v4 root
   dependency and old DynOp execution path. Its remaining plan, control, and
   worker fixtures use the v5 DoFn wire contract; the planner rejects the
   old v4 URN. The all-feature Dataflow workspace tests and strict graphx
   and harness lint pass, and its lockfile no longer contains `ironbeam` v4.
   The full multi-source, format, grouping, and finalized-sink Prism fixture
   reached `DONE` again through a separate v5 worker after this cutover.
   The root package's own v4 modules and compatibility feature are still a
   release blocker before changing the root package to 5.0.0.
   `ironbeam-cloud` still contains legacy traits, fake services, readers,
   and helpers. Its compression and object APIs now reexport separate crates;
   v5 JSONL and shared I/O no longer depend on it. Remove
   the remaining legacy package from the v5 runtime and root feature graph
   during the v4 cutover. Its earlier isolated source corpus measured only
   37/1,695 locations after the object extraction, so reexports alone do not
   make the legacy package release-ready.
5. **Execution proof:** The user chose local Prism proof for now. On
   2026-10-10, the opt-in `ironbeam-prism-proof` test passed against a real
   local Prism JobService. It passed again on 2026-10-11 after the core coder
   and Direct changes, after the object source migrated to the URI adapter,
   and with an actual `FinalizeBundle` callback for a pending sink fragment;
   the Prism service was stopped afterward. That fixture
   checks completed jobs and captured
   values for its bounded sources and sinks, JSONL, Avro, CSV, protobuf,
   object-backed JSONL, Flatten, grouping, joins, side input, Reshuffle,
   event-time assignment, and fixed windows. Its object-backed fixture uses
   a shared local filesystem, so it does not prove remote staging.
   The v5 worker now serves compact Fn API monitoring metadata, and a live
   Prism run returns committed element counts for sources and intermediate
   collections. A transform-scoped user counter now records the finalized
   sink's input count; the live fixture verifies that this committed count
   equals the accepted fragment row total. Shared I/O now stages immutable,
   scoped, checksummed local fragments and accepts them only after the bundle
   callback. After `DONE`, the Prism driver verifies committed row accounting,
   validates the assembled framed records before visibility, atomically
   publishes one file, and confirms an idempotent retry. This is a live
   single-bundle local proof. A real TFRecord fragment sink now spools each
   bundle to disk, stages it through shared I/O, and publishes after `DONE`;
   the publisher checks every frame and CRC, and the v5 reader reopens the
   original three rows. A native-schema Avro fragment sink also publishes one
   Avro container after `DONE`; the v5 reader reopens all three rows, and a
   repeated publication produces identical bytes. MessagePack and CBOR now
   use a shared sequence-fragment lifecycle with format-specific streaming
   validation; both publish and reopen the same three rows after `DONE` in
   local Prism. Protobuf uses that lifecycle too, validating every
   length-delimited record before publication and reopening the same rows.
   JSONL now uses the same finalized fragment lifecycle: each record is
   validated as JSON with a complete newline before publication. The live
   local Prism job publishes and reopens all three rows after `DONE`, and
   retrying publication is idempotent. Local tests reject malformed and
   truncated fragments without replacing an existing result. Its isolated
   coverage is 380/403 source locations and 36/38 branches, above the
   unchanged package baseline.
   CSV now has a header-aware finalized bundle sink. The same live Prism
   job publishes one CSV header and reopens all three rows after `DONE`;
   local tests cover multiple bundles, quoted multiline values, mismatched
   headers, malformed rows, empty bundles, and retry recovery. Publication
   checks the runner's committed row count and is idempotent.
   The expanded CSV crate's isolated all-feature coverage is 329/355 source
   locations and 34/38 branches; this measured baseline replaces the former
   67/68 and 2/2 threshold from before bundle finalization existed.
   Portable CSV publication currently targets an uncompressed file; the
   existing Direct CSV sink retains its optional compressed output.
   XML now stages complete item elements per bundle. The live Prism fixture
   reads XML input, publishes a single validated XML document after `DONE`,
   and reopens all three output rows. Local tests cover escaped text,
   multiple fragments, empty output, malformed elements, row-count mismatch,
   and idempotent publication. Portable XML publication is uncompressed.
   Its isolated all-feature coverage is 160/169 source locations and 6/6
   branches, replacing the former 42/43 source baseline from before the
   fragment lifecycle was added.
   Parquet now stages typed coder frames per bundle and publishes one native
   Parquet file after `DONE`, decoding in batches of at most 1,024 rows.
   The live Prism fixture reads a Parquet source and reopens all three
   finalized output rows. Local tests cover multiple bundles, more than one
   record batch, empty output, corrupt frames, uncoded types, wrong counts,
   and idempotent publication. Its isolated all-feature coverage is 310/338
   source locations and 23/24 branches, above the existing package gate.
   Arrow IPC row output now uses the same accepted-bundle lifecycle and
   publishes one native file in batches of at most 1,024 decoded rows. Local
   tests cover multiple bundles, batch rollover, empty output, corrupt frames,
   uncoded types, wrong counts, and idempotent publication. A separate local
   Prism fixture reads native Arrow input, crosses Reshuffle, publishes after
   `DONE`, and reopens all three rows with the finalized sink as its only
   terminal. The initial lowering gave that terminal an unused Beam output;
   Prism reported `DONE` with zero Impulse elements. The Dataflow integration
   now lowers sinks without Beam outputs, and both the isolated final-only
   Arrow job and the larger multi-source Prism job pass with that contract.
   Arrow's isolated all-feature coverage is 560/618 source locations and
   39/44 branches, above the existing package gate; its strict lint and
   offline package verification pass.
   Native Arrow record-batch output now stages one coded batch per element,
   verifies every batch's schema and committed count, and publishes one Arrow
   IPC file after `DONE`. Unit tests cover multiple bundle fragments, schema
   disagreement, corrupt frames, empty output, and idempotent publication.
   The separate Prism job now also reads the input as record batches and
   reopens the finalized batch file with its three original rows.
   Publication also rejects pending or orphaned staging
   entries even if their row count is zero. Malformed MessagePack and CBOR
   fragments cannot replace an existing result, and failed serialization
   leaves no accepted fragment. The combined I/O coverage corpus measured
   shared I/O at 873/901 source locations and 102/106 branches, MessagePack
   at 112/116 and 8/8, CBOR at 100/104 and 7/8, Protobuf at 172/183 and
   18/20, and TFRecord at 398/422 and 35/38; all pass unchanged gates.
   Avro's gate is rebased to its expanded native fragment module at 367/387
   source locations and 27/28 branches. The untested lines are primarily
   filesystem and integer-overflow errors; malformed datum, wrong schema,
   wrong count, pending fragments, and repeated publication are exercised.
   Multi-bundle retry and worker-restart recovery remain unproved. A local
   two-source Flatten experiment reached `DONE`, but Prism coalesced its Avro
   output into one final sink bundle, so it did not close the multi-bundle gate.
   A later production SDF fixture closed the successful multi-bundle slice:
   its residual restriction produced at least two finalized framed fragments
   in a live Prism job. The driver published all three original values only
   after `DONE` with committed row-count validation. A runner retry or
   abandoned attempt and no-partial-publish proof on failure remain open.
   The worker now passes a stable bundle attempt ID to each sink before writes;
   the Prism probe uses it for deterministic fragment names and rejects a
   conflicting replay. This prepares durable manifest and publication work.
   Shared local fragment acceptance can now resume after a process stops
   between creating the accepted hard link and removing the pending name:
   a reopened scope verifies both complete files are byte-identical before
   removing the pending name. Conflicting bytes still fail closed. This fixes
   the fragment-store crash window. The worker can now record the exact sink
   set for each bundle in a local, identity-bound finalization journal before
   returning `ProcessBundleResponse`. A fresh registry recovers a Protobuf
   fragment using only that record, rejects unknown and missing fragments,
   and persists a completion marker for duplicate callbacks. The ten-sink
   local Prism job reached `DONE` with journal records and matching completion
   markers. A separate two-process test exits immediately after staging and
   reopens the journal in a fresh process to finalize and publish Protobuf.
   The worker also has an object-backed journal using the provider-neutral
   object API; a local object-provider test verifies exact multi-sink recovery.
   An actual worker kill/restart during a live Prism job and a hosted object
   storage proof remain open.
   The two-process Protobuf recovery test now rebuilds `V5BundleExecutor` in
   each process and invokes its bundle-finalization callback, rather than
   calling the journal registry directly. A changed worker build ID fails
   identity validation before recovery; a duplicate callback in another
   fresh process succeeds after the completion marker is durable. The full
   local Prism graph and Arrow fixture still reach `DONE` after the latest
   core changes. Prism's process environment does not itself restart a dead
   worker process, so a supervised live restart proof remains open.
   A transient Prism run also exposed an empty source-rooted bundle; the worker
   now closes its runner outputs without opening the user source for that case,
   with a focused Fn Data regression test and a subsequent live passing run.
   Recovery requires `IRONBEAM_V5_FINALIZATION_JOURNAL_DIR` to point to the
   same durable local directory after restart. Without that setting the
   worker uses its in-memory registry and cannot recover after process loss.
   The v5 worker now tracks progress across multiple sinks in one bundle:
   when a later finalizer fails, retry resumes at that sink without replaying
   earlier successes. A two-bundle registry test also verifies isolated and
   duplicate callbacks; the live Prism proof above covers successful
   multi-bundle completion.
   The larger opt-in Prism graph fixture now reaches `DONE` again. The stall
   was a concurrent Fn Data connection race: source bundles could open
   multiple streams for one endpoint before the worker cached one, leaving
   input on a discarded stream. Endpoint connection creation is now
   serialized and covered by a concurrent two-bundle test. The fixture also
   locks its shared capture files so concurrent append bundles cannot
   interleave record bytes. All four live `prism_v5_run` tests pass together;
   the full harness suite and strict harness/proof lint pass.
   No real Dataflow cloud job has reached `DONE` with the
   v5 worker; cloud execution remains unverified and is not the requested
   release proof at this stage.
6. **Performance and formatting:** Two adjacent paired 100,000-row source-copy
   samples measured v5 about 12.4–13.4× slower than v4, with matching output counts
   and checksums; other workload ratios are in `V5_PHASE0_BASELINE.md`.
   Source construction and Direct execution both contribute materially.
   Fixed-size wire-length accounting removes a per-value length call on
   source-only batches, but the adjacent timing change is too small to
   distinguish confidently from run noise. A bounded slice reader for
   `from_vec` then reduced an adjacent v5 source-copy median from 0.897 to
   0.768 ms; the performance gate remains open.
   Plain single-worker vector copies now collect inline without creating a
   Rayon pool; transformed, materialized, parallel, and sink jobs retain their
   job-owned pools. An alternating release sample moved source copy from
   0.770 to 0.602 ms with the same output checksum, while map/filter stayed
   near 1.15 ms. This does not close the v4 performance gap.
   A paired whole-corpus peak-RSS sample measured 25.8 MiB for v4 and
   54.4 MiB for v5. A per-operation allocation profile and a defined
   performance threshold are still needed before calling performance
   acceptable.
   Isolated case runs now show 100,000-row GroupByKey at 5.41 MiB peak RSS
   for v4 versus 14.16 MiB for v5, and inner join at 21.44 versus 50.48 MiB.
   Source copy, map/filter, and JSONL peaks are much closer. A source-vector
   reservation trial helped grouping but hurt joining and was reverted.
   Streaming linear GroupByKey input directly into its keyed accumulator then
   reduced an adjacent 100,000-row peak from 14.19 to 4.72 MiB, with matching
   results and the source memory-limit behavior preserved. Its median latency
   moved from 4.506 to 4.739 ms in that sample; the join memory gap remains.
   The root workspace and all-feature Dataflow suites pass with this change;
   the live local Prism fixture still reaches `DONE`. Fresh core/Direct/transforms
   coverage passes the unchanged gates: core 2,354/2,443 source locations and
   348/396 branches, Direct 1,779/1,891 and 349/380, and isolated common
   transforms 1,163/1,189 and 108/116. Additional validator boundary cases
   raised the isolated transforms branch ratio above its existing threshold.
   The local test, documentation, and package matrix verified all twenty-five
   v5 crates without publishing them.
   A subsequent join-index change stores one left row position inline per
   key and allocates a vector only for duplicate keys. An alternating
   100,000-row unique-key sample moved from 17.332 to 15.356 ms and from
   50.53 to 47.42 MiB peak RSS; a duplicate-key sample stayed near 7.7 ms
   and 31.61 MiB. Both retained matching output counts and checksums. The
   live Prism fixture still reaches `DONE`, and fresh core and Direct coverage
   pass their unchanged gates at 2,367/2,457 source locations and 348/396
   branches for core, and 1,779/1,891 and 349/380 for Direct. The larger
   root workspace and all-feature Dataflow suites pass, and the changed core
   and Direct crates verify as local packages. The larger join memory and
   latency gaps versus v4 remain release work.
   A later twelve-run alternating release sample still measured v5 at 9.9×
   v4 for source copy, 11.1× for map/filter, 6.9× for GroupByKey, 1.4× for
   inner join, and 2.8× for JSONL, with matching results. A short-frame
   SHA-256 update trial slowed source construction and was reverted; its
   paired timings are recorded in `V5_PHASE0_BASELINE.md`.
   Built-in vector source digests are now computed only when checkpointing or
   portable lowering requests source identity. The digest bytes are unchanged;
   custom coder failures remain eager. A paired local release sample moved
   source copy from 0.605 to 0.104 ms and map/filter from 1.217 to 0.728 ms
   at 100,000 rows, with matching output. A separate paired v4/v5 sample
   measured a 1.8× source-copy gap, while map/filter was still 6.4× and
   GroupByKey 7.7×. Core and Direct tests, strict lint, Dataflow graph
   translation tests, and unchanged coverage gates pass. This narrows the
   source-copy cost but does not close the broader performance gate.
   Sequential typed transform routes now read bounded vector batches directly
   when no checkpoint is active. A paired local sample moved map/filter from
   0.708 to 0.611 ms with identical output; source copy and GroupByKey were
   flat. The wider transform performance gap remains open.
   Typed Direct map/filter routes now append each completed batch through the
   same checked output-store path as source-only routes. A 22-pair alternating
   release sample at 100,000 rows moved the map/filter median from 0.620 to
   0.489 ms with identical rows and checksum; source copy stayed near 0.10 ms.
   Against a rebuilt, case-selected v4 benchmark, paired medians were v4
   0.137 ms and v5 0.490 ms for map/filter (3.59×), 0.056 and 0.102 ms for
   source copy (1.83×), and 0.616 and 4.608 ms for GroupByKey (7.49×).
   Direct tests, the root v5 facade tests, strict Direct lint, and exact
   transformed-output budget/spill regression tests pass. The grouping gap
   remains the larger performance gate.
   Built-in bounded vector sources can now feed a matching GroupByKey fold as
   typed batches without allocating an envelope per tuple. Sources carrying
   transform stages or other metadata retain the existing envelope path. A
   22-pair alternating release sample at 100,000 keyed rows measured v4
   0.605 ms and v5 2.472 ms (4.09×), versus 4.608 ms for v5 before this
   change. Deferring the built-in `(u32, u32)` vector source digest until
   checkpointing or portable lowering requests it preserved its exact coder
   digest and moved a further paired sample to v4 0.619 ms and v5 1.340 ms
   (2.16×). Group values, source memory limits, non-vector fallback, core,
   Direct, transforms, root facade, and Dataflow graph/worker tests pass;
   strict core and Direct lint passes. Grouping remains slower than v4.
   A clean core/Direct/transforms coverage run passes unchanged CI gates:
   core 2,420/2,515 source locations and 350/398 branches; Direct
   1,821/1,938 source locations and 373/408 branches. The added failure-path
   test verifies that vector source clone panics and grouping fold panics are
   attributed to their respective nodes.
   Separating the unassigned-window index lets a repeated key reuse its
   existing group without cloning the key. A further 22-pair alternating
   100,000-row release sample measured v4 0.609 ms and v5 1.045 ms (1.72×).
   Core, Direct, Dataflow graph/worker tests and strict core/Direct lint pass;
   a clean coverage rerun remains above the existing gates at core
   2,430/2,525 lines and 354/402 branches and Direct 1,821/1,938 lines and
   373/408 branches. The performance gap is still open.
   The typed map/filter route now accumulates rejected-row metrics within each
   batch and publishes one total, preserving transform order and cancellation
   checks. An alternating 40-pair release comparison against the same checkout
   before this change measured 100,000-row map/filter medians of 0.497 ms
   before and 0.483 ms after; at one million rows, 3.984 ms before and
   3.879 ms after. Output counts and checksums matched. Full Direct tests and
   strict Direct lint pass. This is a small improvement, not closure of the
   broader v4/v5 performance gate.
   Local checkpoint replacement now generates a recognizable data filename
   and validates the previous manifest before deleting its referenced data.
   Tests show that path traversal, a sibling file, `latest.json`, mismatched
   version or identity, and a bad progress checksum cannot trigger cleanup;
   valid replacement still removes the prior data file. This does not add
   aggregate-state recovery or v4 checkpoint policy parity. Full Direct tests,
   strict lint, and clean coverage pass: Direct 1,894/2,011 source locations
   and 384/420 branches, with core unchanged at 2,430/2,525 and 354/402.
   The current core and worker also passed the live local Prism multi-source
   execution fixture to `DONE`, and the root workspace all-feature test suite
   completed successfully after these changes.
   Direct can now checkpoint the completed output of a one-source bounded
   aggregate when the source is a built-in vector and the output has a coder.
   A second run recovers global-combine and GroupByKey results without opening
   the source; grouped output also recovers through the spill path. A changed
   source identity, partial source offset, and custom source fail closed.
   Checkpoints are written only after the aggregate finishes, so mid-aggregate
   restart and v4 barrier/time/node policies remain open. Full Direct tests,
   strict lint, Dataflow graph/worker tests, and clean coverage pass: Direct
   1,946/2,064 source locations and 399/436 branches, core 2,430/2,525 and
   354/402.
   Core now exposes an optional fold-state snapshot/restore contract.
   GroupByKey serializes its in-progress keyed values and first-element time
   metadata with an independently versioned, coder-bound state format, and restores its
   group indexes before accepting more input. Tests cover resumed values and
   windows, malformed coder bytes, duplicate groups, incompatible versions or coders,
   and folds without a state coder. At this stage Direct had not yet connected
   these snapshots to its checkpoint manager.
   Clean core/Direct/transforms coverage passes unchanged gates at core
   2,492/2,590 source locations and 370/420 branches, Direct 1,946/2,064
   and 399/436; strict core lint passes.
   Direct now persists and restores GroupByKey fold state between batches for
   the exact built-in vector source → GroupByKey → output route. An interrupted
   three-batch run resumes after its first committed batch and reads only the
   remaining source rows; a later run recovers the completed output without
   reading the source. Checkpoints bind the graph, source, output coder, binary
   build, data digest, progress, and checkpoint kind. Invalid fold payloads,
   altered manifests, offsets beyond the source, and state on a different
   route fail closed. Checkpoint interval and memory budget are covered. The
   local manifest schema is now 1.2; older v5 draft manifests are rejected.
   Arbitrary folds, transforms before grouping, custom and unbounded sources,
   and the v4 barrier/time/node policies still lack mid-run recovery. Fresh
   coverage clears the core and Direct gates: core 2,492/2,590 lines and
   370/420 branches; Direct 2,186/2,331 lines and 441/482 branches.
   Affected core/Direct tests and strict lint, the root default-feature facade
   tests, Dataflow graph/worker tests, and both live local Prism execution
   fixtures pass. The first full all-feature workspace build exhausted local
   disk space before tests started. After clearing generated build artifacts,
   the full all-feature workspace test and doctest suite passed with Mr
   Boxington enabled. This closes the workspace test gate for this change.
   The GroupByKey restart path now also accepts linear map, filter, flat-map,
   timestamp, and fixed-window stages after a built-in vector source. Its
   vector-slice envelope reader starts at the saved source offset, while the
   original typed-batch path remains for a direct vector → GroupByKey route.
   A cancelled map/filter fixture resumes without revisiting committed rows;
   a flat-map fixture preserves both outputs from committed source rows, and a
   fixed-window fixture restores one window and completes another. Saved
   state on a reshuffle route still fails closed. Source clone and transform
   panics retain their respective node attribution. Fresh coverage passes at
   core 2,509/2,607 lines and 370/420 branches, and Direct 2,259/2,407 lines
   and 450/492 branches. Multi-input, custom, and unbounded sources remain
   outside this mid-run checkpoint contract.
   The full all-feature workspace tests and doctests, strict core/Direct lint,
   separate Dataflow graph/worker tests, and both live local Prism fixtures
   pass after this change.
   Fold-state persistence and recovery now enforce the Direct job memory
   budget before writing state or loading saved state bytes. Oversized saves leave the
   previous valid checkpoint intact. Recovery rejects manifests over 1 MiB,
   and rejects symlinked or non-regular manifest and data files before loading
   the payload. Direct tests cover the manager checks and a grouped job whose
   snapshot exceeds its budget. V4's scheduling policies persist only progress
   markers and re-execute from the start; their v5 replacement still needs a
   restorable-state scheduling contract. Fresh core and Direct coverage pass
   unchanged gates at core 2,509/2,607 lines and 370/420 branches, and Direct
   2,366/2,515 lines and 463/506 branches; strict Direct lint and the full
   workspace all-feature test and doctest suite pass.
   Workspace-wide `cargo fmt --all --check` currently reports formatting
   changes in many untouched files. Changed files in this batch were formatted
   locally; a focused workspace formatting pass remains for the final cutover.

The core now registers typed TestStream fixtures with a stable source digest,
timestamped encoded elements, and monotonic watermark controls. The separate
Dataflow translator lowers these fixtures to Beam's unbounded TestStream
primitive. A local Prism `Run` reaches `DONE` through the v5 worker and
verifies two values in their fixed windows; the existing bounded live corpus
also passes. This closes the scripted unbounded fixture proof. The production
reader and checkpoint proof is described below. Remote Dataflow execution is
a later cloud integration proof, outside the requested local Prism gate.

The core also now registers a production unbounded reader factory with a
nonblocking poll result, bounded event batches, timestamped values, and a
checkpoint progress signal. Reopening passes the saved cursor to the factory
and validates split identity and nondecreasing watermark against the saved
signal. Core tests cover progress-only polls, resume, capacity and cursor
limits, and invalid source progress. Direct now runs one registered unbounded
source through a linear transform route to an idempotent append sink. Each
bounded poll batch receives a stable sink bundle ID, commits before its cursor
is checkpointed, and must advance the cursor when it carries data; cancellation,
restart, completed recovery, failed commits, and checkpoint integrity are
covered by Direct tests. `start_stream_sink_pair` now executes two linear
branches from the same source with one coded split and checkpoints only after
both idempotent append sinks commit. Tests verify source sharing, cancellation
and cursor resume, and replay after the second sink fails following the first
sink's commit. The checkpoint identity binds both sink digests and coders.
Direct still rejects stateful or multi-source streaming routes, non-idempotent
sinks, and sinks requiring bundle finalization. The
ordinary bounded `run_sink` path still rejects unbounded graphs. The separate
Dataflow production source polling is now covered by a local Prism run;
broader portable graph shapes remain release gates.
The separate Dataflow workspace now has a versioned SDF restriction payload
and a tested worker-side polling adapter that preserves reader state across
`Pending` and returns an encoded residual cursor. The Beam SDF transform
lowering now submits a splittable ParDo and SDF requirement for Prism to
expand into pair, split/size, and process stages. Public translation validates
the source before opening its reader.
The Fn API control loop now accepts split requests while a bundle runs, and a
live Prism TestStream run still reaches `DONE`. The Dataflow bundle planner
recognizes the three v5 SDF stages and validates their restriction-coder
payloads. The worker executes pair and split/size preparation over Fn Data,
both fused and across a runner boundary. Its process stage polls a registered
reader, emits timestamped values, and returns residual bundle roots. A two
bundle Fn Data test verifies resume from the saved cursor; a bad restriction
fails before opening the reader. Local Prism `Run` now reaches `DONE` with two
timestamped batches and an idempotent sink that checks each value and event
timestamp. The prior TestStream Prism proof still passes. An explicit worker
restart after an accepted batch, split-response roots, and additional portable
graph shapes remain unproved. Remote cloud execution is a later extension.
The new Direct stream and checkpoint modules have 605/665 source locations
covered in isolation. With the expanded failure-path tests, the package now
measures 2,860/3,132 source locations (91.32%) and 560/618 branches (90.61%).
The Direct CI floor is set to 91% source locations and 90% branches for this
larger runner surface; the former 93.74%/91.38% ratios were measured before
unbounded execution was added. This is a coverage floor, not a claim that
the remaining runner paths are complete.

The detailed architecture and exit criteria remain in
`WORKSPACE_ARCHITECTURE_PLAN.md`, `IRONBEAM_V5_EXECUTION_PLAN.md`, and
`V5_REPLACEMENT_ROADMAP.md`.
