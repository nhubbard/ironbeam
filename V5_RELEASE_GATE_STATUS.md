# Ironbeam 5.0.0 release gate status

Measured on `codex/ironbeam-v5-runner-core` on 2026-10-10. This is a working
audit, not a release declaration. The v5 packages remain at `0.1.0`, the root
facade still defaults to the v4 implementation at `4.1.1`, and publication is
staged locally.

## Verified in this checkout

- `bash scripts/check_v5_packages.sh` passed after the opt-in cloud provider
  package was added: workspace tests, selected feature variants, all-feature
  documentation, and local package verification for all twenty v5 crates.
  No package was published.
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
  measure 177/190 source locations and 12/14 branches. Protobuf is now in
  `ironbeam-io-protobuf` with 93/100 source locations and 16/18 branches
  measured under all features. TFRecord now has its own crate with 164/179
  source locations and 16/18 branches. Shared lifecycle I/O measures 265/276
  locations and 25/26 branches through the extracted format packages.
  These measured baselines still leave uncovered error paths for release review.
- The separate Dataflow checkout has a Prism `Run` reaching `DONE` through a
  separately launched v5 worker for three bounded sources, two branches,
  native Flatten, event-time assignment, and five terminal append sinks. A
  user-defined record passes through the extracted JSONL source and append
  sink in that live job. An extracted CSV source also crosses the mock worker
  coder contract; CSV globs lower to multiple source nodes and native Flatten.
  The JSONL append sink uses a shared filesystem and may duplicate records on
  replay. This is a local runner proof of this bounded slice.
- `ironbeam-cloud` now has URI routing, registered provider handles, and a
  filesystem-backed local adapter. A JSONL graph source and sink round-trip
  through that adapter. Opt-in `ironbeam-cloud-object-store` builds S3, GCS,
  and Azure Blob clients from environment credentials; the common adapter's
  operations pass against Apache `object_store`'s in-memory backend. Hosted
  storage authentication and live service behavior remain unverified.

## Open replacement gates

1. **Core coverage quality:** The old package-only CI baseline recorded 972/974
   source locations and 210/222 branches before core grew. It was red after
   expansion and omitted paths exercised through Direct and transforms tests.
   The three-package corpus covers the same core paths as the full workspace
   corpus. After adversarial coder tests, it measures 1,794/1,868 source
   locations (96.04%) and 324/376 branches (86.17%). CI now gates this
   three-package corpus at the measured baseline. The changed percentage is
   disclosed rather than treated as an unchanged quality level; 74 source
   locations and 52 branch outcomes remain to audit before release.
2. **Portable feature corpus:** The v5 translator still rejects grouping,
   joins, side inputs, window changes, unbounded sources, update sinks, and
   whole-job final-result sinks. Native GroupByKey needs typed key/value
   coders, runner shuffle output decoding, and state-backed or chunked
   iterable handling. The Prism proof does not cover these features.
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
5. **Execution proof:** The user chose local Prism proof for now. The existing
   Prism job reaches `DONE` for the bounded elementwise and Flatten slice,
   including a JSONL connector round-trip. The remaining portable feature
   corpus and other source/sink data still need local Prism runs. No real Dataflow cloud job has reached `DONE` with the
   v5 worker; cloud execution remains unverified and is not the requested
   release proof at this stage.

The detailed architecture and exit criteria remain in
`WORKSPACE_ARCHITECTURE_PLAN.md`, `IRONBEAM_V5_EXECUTION_PLAN.md`, and
`V5_REPLACEMENT_ROADMAP.md`.
