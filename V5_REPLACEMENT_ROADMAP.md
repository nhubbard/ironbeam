# Replacing the v4 implementation on this branch

The target for `codex/ironbeam-v5-runner-core` is a v5-only, multi-crate
workspace. The root `ironbeam` package remains the familiar entry point, but
its implementation and default dependency graph must be v5. The checked-in v4
implementation under `src/` is temporary migration scaffolding. The published
v4 package may remain a dependency of `ironbeam-compat` for bounded migration
tests; no v5 runtime package may depend on it.

This is a replacement gate, not a claim that the current workspace has already
reached it. `V5_HELPER_PARITY_TRACKER.md` closes *bounded Direct helper* parity.
It does not close runner, durability, unbounded, Dataflow, or public-path parity.

## Current seams and the next extractions

The local Apache Beam checkout keeps SDK core, model, runners, testing, and I/O
modules separate (`sdks/java/core`, `model`, `runners`, `sdks/java/testing`,
`sdks/java/io`). Its I/O tree has independent modules for CSV, JSON, Parquet,
XML, JDBC, and MongoDB, among others. Ironbeam should use the same dependency
direction without creating a crate for every source file.

| Boundary | Current state | Next change and reason |
| --- | --- | --- |
| Public facade | Root `ironbeam` 4.1.1 defaults to v4 and exposes v5 under `v5-preview` | Make root exports and examples v5, then remove the v4 modules and old feature graph after migration gates pass. This removes the largest build and test coupling. |
| Logical graph and extension API | `ironbeam-core` owns `Pipeline`, `PCollection`, graph, `Element`, errors, typed source/sink registration | Keep these types together while extensions must construct graph nodes. Extract a runner-neutral `ironbeam-model` only if graph descriptors need an independently versioned serialization contract. An API-only crate that repeats these types would add an abstraction without isolating dependencies. |
| Local runner | `ironbeam-direct` owns planner, execution, metrics, checkpoint, and spill modules | Finish shared-upstream execution, physical scheduling, state, and recovery here. Extract a state/checkpoint backend only when it can be independently selected and tested by more than one runner. |
| Convenience transforms | `ironbeam-transforms` owns many unrelated extension traits; `regex` and HLL are unconditional dependencies | Keep ordinary helpers together for now; move regex and approximate-distinct/sketch helpers to focused extension crates or optional feature modules when their package-level tests and dependencies are separable. Preserve one prelude for users. |
| File formats | Avro, Parquet, Arrow IPC, CSV, XML, MessagePack, CBOR, JSONL, Protobuf, and TFRecord have independent crates; `ironbeam-io` now holds the shared file lifecycle helpers | Continue connector parity and move shared file helpers to a small lifecycle crate when a concrete split improves dependency or test isolation. No reexport-only aggregator is needed. |
| Databases | SQL and MongoDB already have independent crates and live-service tests | Keep them direct extensions of core. Do not pull their dependencies into a default facade build. |
| Cloud/object storage | `ironbeam-cloud` routes object URIs and includes a local adapter; opt-in `ironbeam-cloud-object-store` supplies S3, GCS, and Azure Blob clients through Apache `object_store`; `ironbeam-io-jsonl::cloud_io` accepts a registered provider handle | Test provider authentication and object operations against local emulators. Add remote runner staging and bundle-safe output commits before claiming cloud execution parity. Keep provider SDK dependencies out of the shared cloud contracts crate. |
| Coders and portable protocol | Core owns coder identity and registers each node's typed `Element` coder for worker access; Beam lowering remains in the separate Dataflow integration | Extend the protobuf path to all v4 graph shapes and I/O boundaries. Keep Beam protocol and harness dependencies in the separate Dataflow integration; test user-defined element types over actual Fn API streams. |
| Testing and compatibility | `ironbeam-testing` and `ironbeam-compat` are already leaf crates | Keep package-scoped fixtures and differential tests. Compatibility must remain optional and must never be a dependency of core, Direct, or the root v5 facade. |

CSV's v5 public path is now `ironbeam_io_csv::{csv_source, csv_sink, ...}` or
`ironbeam::v5::*` with `v5-io-csv`. The root `io-csv` flag still belongs to the
v4 facade until cutover. `ironbeam-io` keeps the common file lifecycle only;
its CSV codec dependency and implementation moved into `ironbeam-io-csv`.
Package coverage baselines were measured again after the default shared-I/O
feature set became file lifecycle plus parallel I/O. Shared-I/O coverage now
runs its own tests and the format packages that exercise that lifecycle; each
extracted format has its own source and branch gate. XML's v5 public path is
`ironbeam_io_xml::*` or `ironbeam::v5::*` with
`v5-io-xml`. The root `io-xml` flag still belongs to v4 until facade cutover.
MessagePack is similarly isolated as `ironbeam-io-msgpack` under the
`v5-io-msgpack` facade feature; the v4 `io-msgpack` flag remains separate.
CBOR is isolated as `ironbeam-io-cbor` under `v5-io-cbor`; `io-cbor` remains
the v4 feature until cutover. JSONL and its object-storage adapters moved to
`ironbeam-io-jsonl`, which the `v5-preview` facade includes by default. The
v4 `io-jsonl` flag remains separate.

Prefer `cargo test -p <changed-package>` and the directly affected downstream
packages while iterating. Keep the full workspace, all-feature, service, and
local packaging checks as release or replacement gates. Separate CI coverage
per package and per format after each extraction; a default-feature coverage
number for `ironbeam-io` does not measure opt-in format code. Cargo's shared
lockfile and feature unification still require minimal/default/all-feature
checks, so package boundaries reduce coupling but do not remove those checks.

## API similarity contract

Preserve v4 operation names, data shapes, and bounded Direct results where they
do not conflict with the v5 graph and lifecycle contracts. Keep fundamental
operations inherent on `PCollection`; reexport convenience extension traits in
the root prelude. Add v4-shaped aliases only when they preserve stable identity,
typed errors, and deferred execution. The migration guide must give a mechanical
edit for every changed public path in `V5_PHASE0_API_INVENTORY.md`.

| Aspect | v4 | Current v5 | Required disposition |
| --- | --- | --- | --- |
| Pipeline vocabulary | `Pipeline`, `PCollection<T>`, map/filter/key/group/combine and helpers | Same conceptual types and bounded helper families | Preserve familiar root imports and chaining when the facade flips. |
| Node identity | Implicit IDs; names often added afterward | Stable unique keys at construction | Intentional API break. Keep keys visible in examples and migration errors; do not silently invent recovery identities. |
| Construction errors | Many transforms return a collection directly | Node construction returns `Result` | Intentional break needed for graph validation. Document `?` at each stage. |
| Helper location | Most v4 helpers are inherent methods | Many v5 helpers are extension traits | Root prelude should make the usual method calls available with one import. |
| Execution | `collect_seq()`/`collect_par(...)` on collections; many immediate writers | Explicit output terminal and deferred sinks | Preserve convenient Direct terminal methods through traits, while retaining explicit job ownership and side-effect timing. |
| Time and coders | v4 timestamp wrapper and feature-dependent Serde bound | Envelope event time in signed nanoseconds; explicit element/coder identity | Intentional semantic and type break; document conversion and reject incompatible state. |
| Feature flags | Broad v4 defaults and `io-*` flags | Small core/Direct and separately gated extensions | Root v5 facade must map old feature names or provide precise replacements before deleting v4 paths. |

No percentage of source compatibility is asserted. The migration guide gives
the actual edits. Bounded result parity is stronger than source compatibility
today; unbounded and portable execution parity remains open.

## Gates before deleting `src/` v4 runtime modules

1. **Public API ledger:** Every v4 public type, helper, source, sink, feature,
   and example has a v5 owner and a migration outcome: equivalent or an
   intentional change with a concrete recipe. Any proposed omission requires
   an explicit user decision; a bounded helper row alone is insufficient.
2. **Behavior corpus:** Compare representative v4 and v5 bounded results,
   errors, ordering, window metadata, and I/O bytes where promised. Run the
   corpus against sequential and parallel Direct. Keep benchmark comparisons
   for latency, peak memory, and build/test time; no unexplained regression is
   accepted merely because it moved to another crate.
3. **Runtime contract:** Direct executes branches and multiple outputs once per
   job, supports the required complex graph shapes without accidental full
   materialization, and has tested checkpoint/restart, spill limits, metrics,
   cancellation, and sink commit behavior. Unbounded graphs either run with
   defined progress/window/state semantics or have an explicit, documented
   v5-only capability boundary accepted as the replacement contract.
4. **Dataflow contract:** The separate integration builds against the v5 core
   and lowers the full v4 feature corpus. A local Beam Prism `Run` must reach
   `DONE` through a separately launched v5 Fn API worker and verify values for
   user-defined types, source and sink formats, branches, grouping, side
   inputs, windows, and unbounded fixtures where v4 supports them. A Prism
   `Prepare` response alone is protocol acceptance, not execution parity.
   Unsupported operations fail validation before side effects. Runner support
   is recorded independently from Direct support.
5. **Package isolation:** Every extracted package passes tests, docs, minimal
   and default feature builds, and relevant all-feature checks. Root defaults
   exclude heavyweight format/database/remote dependencies. A change to one
   connector can be checked without building unrelated connectors.
6. **Facade cutover:** Root `ironbeam` is v5-only, with one prelude and documented
   feature aliases. All repository examples, docs, benchmarks, tests, and the
   release workflow use v5. Remove the local v4 modules, old root dependencies,
   and v4-specific CI jobs in a focused commit, then run the complete matrix.
   Keep `ironbeam-compat` as an optional leaf against published v4 if useful.

Work through these in reviewable commits: extract one connector or capability,
move its tests and facade export, verify its isolated dependency tree, then
update the ledger. Do not combine the v4 deletion with the format extractions.
Release remains staged locally until the user authorizes publication.
