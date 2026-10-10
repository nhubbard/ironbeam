# Ironbeam 5.0.0 release gate status

Measured on `codex/ironbeam-v5-runner-core` on 2026-10-10. This is a working
audit, not a release declaration. The v5 packages remain at `0.1.0`, the root
facade still defaults to the v4 implementation at `4.1.1`, and publication is
staged locally.

## Verified in this checkout

- `bash scripts/check_v5_packages.sh` passed after XML extraction: workspace
  tests, selected feature variants, all-feature documentation, and local
  package verification for fourteen v5 crates. No package was published.
- `ironbeam-io-csv` is isolated from the common file lifecycle package. Its
  minimal and all-feature tests, package verification, strict lint, and
  85/86 source-location plus 8/8 branch coverage passed. Scalar and tuple
  writes fail before replacing a previous output file.
- `ironbeam-io-xml` is isolated from common I/O. Its all-feature tests measure
  42/43 source locations and 2/2 branches. Strict lint and local package
  verification pass; shared I/O's default tree has no `quick-xml` dependency.
- The separate Dataflow checkout has a Prism `Run` reaching `DONE` through a
  separately launched v5 worker for two sources, two branches, native Flatten,
  event-time assignment, and four terminal append sinks. An extracted CSV
  source crosses the mock worker coder contract; CSV globs lower to multiple
  source nodes and native Flatten. This is a local runner proof.

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
3. **Remote I/O:** The local Prism worker shares the driver's filesystem.
   Driver-local file paths are not staged to cloud workers, and the root file
   sinks use whole-job final-result commit semantics that are unsafe across
   independent Fn API bundles. A remote object source/sink and runner-wide
   output commit protocol must be implemented and tested.
4. **Root replacement:** The root package is still v4-first; the v4 modules,
   feature graph, examples, and compatibility paths remain. Continue format
   extraction, API migration, and the focused v4 removal gate in
   `V5_REPLACEMENT_ROADMAP.md` before setting version `5.0.0`.
5. **Cloud execution:** No real Dataflow cloud job has reached `DONE` with the
   v5 worker. A pinned Linux worker artifact, credentials, cloud storage paths,
   and cost-controlled integration run are still required. The local Prism
   result does not substitute for this gate.

The detailed architecture and exit criteria remain in
`WORKSPACE_ARCHITECTURE_PLAN.md`, `IRONBEAM_V5_EXECUTION_PLAN.md`, and
`V5_REPLACEMENT_ROADMAP.md`.
