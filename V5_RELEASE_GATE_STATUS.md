# Ironbeam 5.0.0 release gate status

Measured on `codex/ironbeam-v5-runner-core` on 2026-10-10. This is a working
audit, not a release declaration. The v5 packages remain at `0.1.0`, the root
facade defaults to v5 but is still versioned `4.1.1`, and publication is
staged locally.

## Verified in this checkout

- `bash scripts/check_v5_packages.sh` passed after the regex and approximate
  distinct extension splits:
  workspace tests, selected feature variants, all-feature
  documentation, and local package verification for all twenty-four v5 crates.
  No package was published.
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
  now measure 261/279 source locations and 20/22 active branch outcomes,
  including append failure cleanup, four compressed object formats, and
  cloud glob behavior; both exceed its existing CI coverage ratios. Its
  all-feature dependency tree has no legacy `ironbeam-cloud` dependency.
  Protobuf is now in
  `ironbeam-io-protobuf` with 93/100 source locations and 16/18 branches
  measured under all features. TFRecord now has its own crate with 164/179
  source locations and 16/18 branches. Shared lifecycle I/O measures 265/276
  locations and 25/26 branches through the extracted format packages.
  These measured baselines still leave uncovered error paths for release review.
- The separate Dataflow checkout has a Prism `Run` reaching `DONE` through a
  separately launched v5 worker for twenty-two bounded sources, including a keyed
  input shared by GroupByKey and an ordinary branch, native Flatten and
  GroupByKey, event-time assignment, native fixed-window assignment with
  offset, per-window GroupByKey, two-, three-, and ten-input CoGroupByKey,
  fixed-window CoGroupByKey, all four join variants with duplicate and
  unmatched keys, a fixed-window full join that retains input timestamps,
  a bounded side input fetched over Beam Fn State, including a side collection
  with values in two fixed windows that is globally materialized to match
  Direct's side-view behavior,
  native Reshuffle in global and fixed windows,
  and twenty-one terminal
  append sinks. User-defined records pass through both the extracted file JSONL
  adapter and a provider-neutral object-backed JSONL source in that live job.
  The latter reconstructs and verifies the same object digest in the separately
  launched worker. The live job also reads a native Avro container written
  with an explicit schema by v5 Direct and checks its transformed record.
  An extracted CSV source crosses the live Prism worker coder boundary.
  A length-delimited Protobuf file source decodes a typed v5 element, runs a
  transform in the worker, and writes the asserted result through a terminal sink.
  CSV globs also lower to multiple source nodes and native Flatten in the
  mock worker contract. The format-heavy live proof now lives in the
  unpublished `ironbeam-prism-proof` package, so the Dataflow runtime package
  no longer directly depends on those connector crates.
  The JSONL append sink uses a shared filesystem and may duplicate records on
  replay. This is a local runner proof of this bounded slice.
- The separate Dataflow checkout now defaults its graph translator, Fn API
  harness, and Dataflow runtime packages to v5-only dependencies. The old v4
  entrypoints, translator, handler, tests, and wordcount example remain behind
  an explicit `legacy-v4` feature. A package-scoped Dataflow test and a live
  Prism run pass with that feature absent; workspace tests also pass with the
  compatibility example enabling it. The root `ironbeam` package now gates
  its v4 modules and direct legacy dependencies behind `legacy-v4`, and now
  defaults to the v5 facade. The default build has only core, Direct,
  transforms, shared I/O, and JSONL as
  direct dependencies; it excludes the legacy cloud package. The v4 root
  integration tests are also gated by `legacy-v4`, allowing the complete
  root test target set to build in v5-only mode. The crate root and README
  now show a v5 quick start, with a passing doctest; legacy builds retain
  their old documentation. Twelve examples still require `legacy-v4`,
  including the v4 benchmark corpus. Advanced joins, CoGroupByKey, JSONL
  globbing, pluggable compression, the Direct plan explanation example, and
  the data-quality validation example now
  run through v5; compression remains opt-in under `compression-gzip`.
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
  performance gate stays open. The
  v5 Direct
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
2. **Portable feature corpus:** The v5 translator now supports bounded,
   global-window `group_by_key_named` through native Beam KV/iterable coders
   and a live Prism shuffle proof. Its keyed input can also feed an ordinary
   branch, verified by another sink in that Prism job. It still rejects
   unbounded sources, update sinks, and
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
   `ironbeam-cloud` still contains legacy traits, fake services, readers,
   and helpers. Its compression and object APIs now reexport separate crates;
   v5 JSONL and shared I/O no longer depend on it. Remove
   the remaining legacy package from the v5 runtime and root feature graph
   during the v4 cutover. Its earlier isolated source corpus measured only
   37/1,695 locations after the object extraction, so reexports alone do not
   make the legacy package release-ready.
5. **Execution proof:** The user chose local Prism proof for now. On
   2026-10-10, the opt-in `ironbeam-prism-proof` test passed against a real
   local Prism JobService. That fixture checks completed jobs and captured
   values for its bounded sources and sinks, JSONL, Avro, CSV, protobuf,
   object-backed JSONL, Flatten, grouping, joins, side input, Reshuffle,
   event-time assignment, and fixed windows. Its object-backed fixture uses
   a shared local filesystem, so it does not prove remote staging.
   No real Dataflow cloud job has reached `DONE` with the
   v5 worker; cloud execution remains unverified and is not the requested
   release proof at this stage.

The detailed architecture and exit criteria remain in
`WORKSPACE_ARCHITECTURE_PLAN.md`, `IRONBEAM_V5_EXECUTION_PLAN.md`, and
`V5_REPLACEMENT_ROADMAP.md`.
