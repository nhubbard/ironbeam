# Replacing the v4 implementation on this branch

The target for `codex/ironbeam-v5-runner-core` is a v5-only, multi-crate
workspace. The root `ironbeam` package remains the familiar entry point, but
its implementation and default dependency graph are now v5. The local v4
implementation under `src/` has been removed. The published
v4 package remains a dependency of `ironbeam-compat` for bounded migration
tests; no v5 runtime package may depend on it.

The bounded v4 behavior baseline, v4/v5 Direct differential tests, and
TFRecord wire-compatibility test now live in `ironbeam-compat` and run against
published Ironbeam 4.1.1. Their reference results remain available after the
root v4 implementation was deleted.
The v4 benchmark driver is also in that leaf crate; it retains the seven
workload names used by the v5 benchmark example.
The v4 mean, count-per-element, keys/values, and key/value-swap suites now run
there against the published package as well, removing another 90 root-owned
legacy test cases while retaining their behavior evidence.
The v4 `to_dict`, `to_hashmap`, and `to_display_string` suites add 58 more
reference cases in the same leaf crate.
The 49 basic-helper cases also run there against published v4, with the root
test module removed and CI retaining the leaf target.
The remaining 225 helper cases now run in the compatibility crate too, so the
root `tests/helpers` tree has been removed. Parquet's 15 cases use the opt-in
`v4-parquet` feature to keep the default compatibility dependency graph small.
The v4 combiner and testing reference suites have also moved into the leaf
crate. The CSV mock test uses the separate `v4-csv` feature.
The cloud-I/O, file-format, SQLite, and database-service reference families
have moved too. Format and connector features keep their optional dependencies
out of the default compatibility build; live database checks require the
`v4-service-tests` feature. The root combined legacy I/O target is gone.

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
| Public facade | Root `ironbeam` is v5-only, carries an unpublished `5.0.0` candidate version, and also exposes its API under `v5`; v4 reference tests and checkpoint-policy examples run against published 4.1.1 from `ironbeam-compat` | Finish release verification and ordered workspace publication before declaring 5.0.0 released. |
| Logical graph and extension API | `ironbeam-core` owns `Pipeline`, `PCollection`, graph, `Element`, errors, typed source/sink registration | Keep these types together while extensions must construct graph nodes. Extract a runner-neutral `ironbeam-model` only if graph descriptors need an independently versioned serialization contract. An API-only crate that repeats these types would add an abstraction without isolating dependencies. |
| Local runner | `ironbeam-direct` owns planner, execution, metrics, checkpoint, and spill modules | Finish shared-upstream execution, physical scheduling, state, and recovery here. Extract a state/checkpoint backend only when it can be independently selected and tested by more than one runner. |
| Custom metrics and reports | Built-in job counters stay in `ironbeam-direct`; optional `ironbeam-metrics` owns v4-shaped counters, gauges, histograms, custom metric registration, and JSON reporting | Attach the shared collector to `JobConfig` for a Direct run instead of storing it on a reusable `Pipeline`. Keep the `metrics` feature alias for source compatibility. |
| Convenience transforms | `ironbeam-transforms` owns common helpers. Regex lives in `ironbeam-transform-regex`; KMV and HLL approximate distinct helpers live in `ironbeam-transform-approx`. | Keep these opt-in extension dependencies out of common transforms. Preserve one conditional facade prelude for users. |
| File formats | Avro, Parquet, Arrow IPC, CSV, XML, MessagePack, CBOR, JSONL, Protobuf, and TFRecord have independent crates; `ironbeam-io` holds shared file lifecycle helpers; `ironbeam-compression` owns the codec boundary | Continue connector parity and move shared file helpers to a small lifecycle crate when a concrete split improves dependency or test isolation. No reexport-only aggregator is needed. |
| Databases | SQL and MongoDB already have independent crates and live-service tests | Keep them direct extensions of core. Do not pull their dependencies into a default facade build. |
| Cloud/object storage | `ironbeam-cloud-objects` owns the URI object contract and local adapter; `ironbeam-compression` owns codec streams; `ironbeam-cloud-service-api`, `ironbeam-cloud-fakes`, and `ironbeam-cloud-ops` own service contracts, in-memory services, and operation helpers; opt-in `ironbeam-cloud-object-store` supplies S3, GCS, and Azure Blob clients; `ironbeam-io-jsonl` owns eager and graph object JSONL. The legacy cloud package is retired. | Test provider authentication against local emulators, then add remote staging and bundle-safe output commits. Provider SDKs stay out of the object contract. |
| Coders and portable protocol | Core owns coder identity and registers each node's typed `Element` coder for worker access; Beam lowering remains in the separate Dataflow integration | Extend the protobuf path to all v4 graph shapes and I/O boundaries. Keep Beam protocol and harness dependencies in the separate Dataflow integration; test user-defined element types over actual Fn API streams. |
| Testing and compatibility | `ironbeam-testing` and `ironbeam-compat` are already leaf crates | Keep package-scoped fixtures and differential tests. Compatibility must remain optional and must never be a dependency of core, Direct, or the root v5 facade. |

CSV's v5 public path is now `ironbeam_io_csv::{csv_source, csv_sink, ...}` or
the root facade with `v5-io-csv` or its old `io-csv` alias. `ironbeam-io` keeps the common file lifecycle only;
its CSV codec dependency and implementation moved into `ironbeam-io-csv`.
Package coverage baselines were measured again after the default shared-I/O
feature set became file lifecycle plus parallel I/O. Shared-I/O coverage now
runs its own tests and the format packages that exercise that lifecycle; each
extracted format has its own source and branch gate. XML's v5 public path is
`ironbeam_io_xml::*` or `ironbeam::v5::*` with
`v5-io-xml` or the `io-xml` alias.
MessagePack is similarly isolated as `ironbeam-io-msgpack` under the
`v5-io-msgpack` facade feature and old alias. CBOR is isolated as
`ironbeam-io-cbor` under `v5-io-cbor` and its old alias. JSONL and its
object-storage adapters moved to `ironbeam-io-jsonl`, which the root v5
facade includes by default. Published v4 remains the reference for old paths.
The `glob_patterns` example now runs the v5 JSONL glob source and deferred
sink, including the same six partitioned records, three filtered errors, and
three batch files. The `compressed_io` example uses the isolated
`ironbeam-compression` codec API through the opt-in root `v5-compression`
feature; gzip and a registered custom codec both round-trip the sample rows.
The `explain_pipeline` example now uses v5 Direct's read-only plan summary for
elementwise, grouped, and fused paths. V4's detailed optimization decisions
remain a planner-parity gap.
The `sql_pipeline` example now uses the isolated v5 SQL extension and checks
its deferred read-transform-write path against a temporary SQLite database.
The `mongodb_pipeline` example uses its separate v5 extension and passed a
read-transform-write check against a temporary local MongoDB container.
The `cloud_io_demo` example now uses v5's provider-neutral URI router, two
local object providers, URI-level deferred JSONL sink and source adapters,
and a cross-provider prefix copy. It runs without hosted
provider SDKs. URI-based JSONL sources bind the scheme and content digest to
graph identity, and sinks bind the destination URI. The sink still has
whole-job final-result semantics. A separate object-backed JSONL fragment sink
now stages portable bundles and publishes only against the completed attempt
manifest; the local Prism production proof reopens that result. Local streaming publication is proved with a 64 MiB worker bundle limit;
hosted provider streaming and consistency remain extension work. The old fake
cache, queue, search, graph, compute, and inference families now have separate
provider-neutral contracts, fakes, and operation helpers. Hosted provider
adapters beyond object storage remain future extension work.
The `etl_pipeline` example now uses v5 grouping, global TopK, bot counts, and
the separate CSV and JSONL connectors with checked temporary round trips.
The v4 checkpoint examples now live in `ironbeam-compat`. V5 batch/progress
checkpoints replace their marker-only barrier, time, and node-based policies,
as recorded in the migration guide. V5 Direct recovers the
completed output of a coded aggregate from one bounded built-in vector source.
The `v5_checkpoint_recovery` example rebuilds the graph after a completed
aggregate and verifies that the second run reads zero source rows from its
versioned local checkpoint. It uses a temporary directory and does not claim
recovery after every graph shape.
For a bounded vector source followed by linear map, filter, flat-map,
timestamp, or fixed-window stages and then GroupByKey, it also restores
versioned fold state from the last committed source batch and reads only the
remaining source rows. Validated stateless stages after GroupByKey, including
keyed-latest selection and time/window projection, run after restoration;
a downstream flat-map failure can retry without reopening the source. Fixed-window
one-source groups and two-unbounded joins/co-groups also checkpoint coded open
state with source cursors. Broader graph recovery remains replacement work. The
local manifest schema is 1.2 and rejects older v5 draft checkpoints.
V4's barrier, node-count, time, and hybrid policies wrote progress markers;
its runner reopened those markers but re-executed from the start. V5 saves
restorable batch or progress state instead of retaining those schedules.
Regex helpers live in `ironbeam-transform-regex`; the root v5 prelude exposes
`RegexExt` when `v5-regex` is enabled. Plain `ironbeam-transforms` builds no
longer compile the regex engine. Published 4.1.1 remains the reference for
the removed v4 root helper paths.
Approximate distinct helpers live in `ironbeam-transform-approx`, exposed by
the root v5 prelude when `v5-approx` is enabled. Plain
`ironbeam-transforms` builds no longer compile `hyperloglogplus`. The
extracted package has its own KMV/HLL merge tests and coverage gate.

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
| Execution | `collect_seq()`/`collect_par(...)` on collections; many immediate writers | Direct collection traits now work on collections through a detached output; explicit output terminals and deferred sinks remain | Keep stable named terminals for portable jobs and durable identity while preserving the familiar bounded Direct call shape. |
| Time and coders | v4 timestamp wrapper and feature-dependent Serde bound | Envelope event time in signed nanoseconds; explicit element/coder identity; opt-in `ironbeam-coder-postcard` macro for Serde user records | Intentional semantic and type break; document conversion and reject incompatible state. |
| Feature flags | Broad v4 defaults and `io-*` flags | Small core/Direct and separately gated extensions | Root v5 facade must map old feature names or provide precise replacements before deleting v4 paths. |

No percentage of source compatibility is asserted. The migration guide gives
the actual edits. Bounded result parity is stronger than source compatibility
today; unbounded and portable execution parity remains open.

For every operation that two v5 runners both advertise, the release contract
targets the same element values, keyed/window grouping, event timestamps,
window intervals, typed error category, and committed output. A supplied
order or tie key also makes ordering and tie selection part of that contract.
The runner may choose its bundle size, parallel schedule, and retry timing,
but those choices must not alter those observations. A runner that lacks an
operation rejects it during graph validation before opening a source or sink.
Contract tests should exercise the same graph and input fixture on Direct and
the local Prism worker, including failing cases. The published v4 package
remains a separate compatibility reference. This shared-behavior target does
not claim that every operation is already implemented on every runner.

Ordered folds require an explicit user order key for identical Direct and
portable behavior. The existing encounter-order fold methods retain their
Direct compatibility behavior; Beam grouping does not preserve that encounter
order. The order-key variants for global and keyed folds sort within each
key/window before invoking the fold and reject duplicate order keys in a
key/window on both runners rather than depending on arrival order. Include
matching metadata and error behavior in the cross-runner corpus before
claiming parity. The order-key extractor is part of the rebuilt worker
registry and must match the driver binary build identity.
Explicit tie-key variants now provide deterministic global and keyed `latest`
selection across Direct and portable execution, including event-time/window
metadata and duplicate tie-key errors. The older first-arrival tie methods
retain Direct compatibility behavior.
The keyed `fold_values_ordered_by_named` variant now uses a reified order key,
Beam GroupByKey, and a sorted fold on both runners. The global
`fold_globally_ordered_by_named` variant uses the same constant-key Beam
grouping as other global combines, including an empty-input seed. Live Prism
jobs match Direct keyed values, event times, and windows; global values and
empty identity; and rejection of duplicate global and keyed order keys. Add
the remaining behavior corpus before closing this gate.

## Remaining gates after the v5-only root cutover

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
   The first shared-source slices are `run_collect_pair` and `run_sink_pair`
   for two bounded linear branches with a coded split. The sink pair requires
   idempotent replay because commits are sequential. The unbounded
   `start_stream_sink_pair` route now checkpoints one coded split and two
   idempotent append sinks after both commits, with a replay test for failure
   after the first commit. `run_collect_pair` now checkpoints two coded linear
   output branches together and recovers accepted batches without re-running
   their transforms. Bounded `run_sinks` accepts any number of named,
   idempotent sink branches from bounded sources, including `Flatten` fan-in,
   shared stateless stages, and nested fan-out; it opens each source once,
   runs each shared stage once, and copies branch values through registered
   coders. Sink commits remain sequential, with explicit retry semantics if a
   later commit fails. Stateful branches, non-`Flatten` fan-in, atomic
   multi-sink commit, and bounded sink checkpointing remain open.
4. **Dataflow contract:** The separate integration builds against the v5 core
   and lowers the full v4 feature corpus. A local Beam Prism `Run` must reach
   `DONE` through a separately launched v5 Fn API worker and verify values for
   user-defined types, source and sink formats, branches, grouping, side
   inputs, windows, and unbounded fixtures where v4 supports them. A Prism
   `Prepare` response alone is protocol acceptance, not execution parity.
   Unsupported operations fail validation before side effects. Runner support
   is recorded independently from Direct support. The intended stable v5
   contract is equivalent values, errors, ordering where an order key is
   supplied, and time metadata on every supported runner. Direct's existing
   encounter-order compatibility methods remain a documented exception until
   they can be expressed by a portable order key.
5. **Package isolation:** Every extracted package passes tests, docs, minimal
   and default feature builds, and relevant all-feature checks. Root defaults
   exclude heavyweight format/database/remote dependencies. A change to one
   connector can be checked without building unrelated connectors.
6. **Facade release:** Root `ironbeam` is v5-only, with one prelude and documented
   feature aliases. All repository examples, docs, benchmarks, tests, and the
   release workflow must use v5. The unused root dependencies and old CI paths
   are removed; run the complete matrix after further changes. Keep `ironbeam-compat` as an
   optional leaf against published v4 for the behavior corpus.

Work through these in reviewable commits: extract one connector or capability,
move its tests and facade export, verify its isolated dependency tree, then
update the ledger. Do not combine the v4 deletion with the format extractions.
Release remains staged locally until the user authorizes publication.
