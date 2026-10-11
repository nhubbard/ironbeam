# Ironbeam 5.0.0 release gate status

Measured on `codex/ironbeam-v5-runner-core` on 2026-10-10. This is a working
audit, not a release declaration. The v5 packages remain at `0.1.0`, the root
facade still defaults to the v4 implementation at `4.1.1`, and publication is
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
  separately launched v5 worker for twenty-one bounded sources, including a keyed
  input shared by GroupByKey and an ordinary branch, native Flatten and
  GroupByKey, event-time assignment, native fixed-window assignment with
  offset, per-window GroupByKey, two-, three-, and ten-input CoGroupByKey,
  fixed-window CoGroupByKey, all four join variants with duplicate and
  unmatched keys, a fixed-window full join that retains input timestamps,
  a bounded global-window side input fetched over Beam Fn State,
  native Reshuffle in global and fixed windows,
  and twenty terminal
  append sinks. User-defined records pass through both the extracted file JSONL
  adapter and a provider-neutral object-backed JSONL source in that live job.
  The latter reconstructs and verifies the same object digest in the separately
  launched worker. The live job also reads a native Avro container written
  with an explicit schema by v5 Direct and checks its transformed record.
  An extracted CSV source crosses the live Prism worker coder boundary.
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
  compatibility example enabling it. This reduces build coupling but does not
  remove the v4 root implementation from the main Ironbeam repository.
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
   passing coverage gates. Core coverage remains the release blocker here.
2. **Portable feature corpus:** The v5 translator now supports bounded,
   global-window `group_by_key_named` through native Beam KV/iterable coders
   and a live Prism shuffle proof. Its keyed input can also feed an ordinary
   branch, verified by another sink in that Prism job. It still rejects
   windowed side views, unbounded sources, update sinks, and
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
   The worker decodes fixed and chunked inline iterables but still rejects
   Beam state-backed iterables, which still need integration with the new
   Fn State client. Bounded millisecond-aligned fixed windows lower
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
   materialization reach `DONE` in local Prism for a bounded global view.
   Windowed views still need semantics that preserve Direct's whole-view
   materialization across windows; the translator rejects them explicitly.
3. **Remote I/O:** Provider clients can access hosted object storage from the
   bounded `ObjectIO` interface, but the local Prism worker still shares the
   driver's filesystem. Driver-local file paths are not staged to cloud
   workers, and root file sinks use whole-job final-result commit semantics
   that are unsafe across independent Fn API bundles. Remote worker staging,
   a runner-wide output commit protocol, and live provider tests remain.
4. **Root replacement:** The root package is still v4-first; the v4 modules,
   feature graph, examples, and compatibility paths remain. Continue format
   extraction, API migration, and the focused v4 removal gate in
   `V5_REPLACEMENT_ROADMAP.md` before setting version `5.0.0`.
   `ironbeam-cloud` still contains legacy traits, fake services, readers,
   and helpers. Its compression and object APIs now reexport separate crates;
   v5 JSONL and shared I/O no longer depend on it. Remove
   the remaining legacy package from the v5 runtime and root feature graph
   during the v4 cutover. Its earlier isolated source corpus measured only
   37/1,695 locations after the object extraction, so reexports alone do not
   make the legacy package release-ready.
5. **Execution proof:** The user chose local Prism proof for now. The existing
   Prism job reaches `DONE` for the bounded elementwise, Flatten, and
   global-window GroupByKey and fixed-window GroupByKey slices, including a
   JSONL connector round-trip.
   The remaining portable feature
   corpus and other source/sink data still need local Prism runs. The object-backed
   fixture uses a shared local filesystem, so it does not prove remote staging.
   No real Dataflow cloud job has reached `DONE` with the
   v5 worker; cloud execution remains unverified and is not the requested
   release proof at this stage.

The detailed architecture and exit criteria remain in
`WORKSPACE_ARCHITECTURE_PLAN.md`, `IRONBEAM_V5_EXECUTION_PLAN.md`, and
`V5_REPLACEMENT_ROADMAP.md`.
