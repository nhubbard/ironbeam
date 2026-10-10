# Ironbeam workspace and runner architecture plan

Draft for discussion, based on `main` at `d64c26a` (October 10, 2026). This plan records the direction for Ironbeam 5 as a maintainable Cargo workspace while keeping its typed Rust pipeline API central. Version 5 may break the version 4 API where a clearer crate boundary warrants it; a separate compatibility package can help users migrate and exchange materialized data. It also identifies the contracts needed for optional Dataflow and Rust ecosystem integrations. The reviewable work sequence and coverage gates are in the [execution plan](IRONBEAM_V5_EXECUTION_PLAN.md).

## Current position

Ironbeam 4.1.1 is one published crate. `Pipeline` stores a graph of `Node`s; `PCollection<T>` builds that graph; the planner turns a requested terminal into an optimized local plan; and `Runner` executes it sequentially or with Rayon. Formats, cloud abstractions, metrics, coders, checkpointing, spilling, test utilities, and most user-facing transforms all live in the same crate.

The separate `ironbeam-dataflow` workspace translates a graph snapshot into Beam's portable pipeline format and supplies a worker harness. Its current translator accepts one linear source chain and a subset of node kinds. Broader Dataflow support needs a clearer graph and execution contract, especially for branching, sources, sinks, coders, and worker reconstruction. Beam protobuf should be a backend format, not Ironbeam's sole internal representation.

The old `rustflow` folder name does not determine the Cargo package name. The `main` branch is the planning baseline; the merged dependency changes are already present there.

## Goals and constraints

- Keep `ironbeam` as the primary package and entry point, while allowing an intentional version 5 API redesign.
- Make ownership and dependency direction visible: graph and typed operations below runners; optional I/O and integrations above them.
- Keep the local runner useful independently of cloud services and large dataframe dependencies.
- Design the v5 core as a runner-independent logical graph from its first vertical slice.
- Let backends report unsupported operations explicitly. Local support and portable support are separate capabilities.
- Avoid a forced migration to async Rust or Beam protobufs for ordinary batch pipelines.
- Document v4-to-v5 API changes and provide bounded compatibility helpers where they are technically sound.
- Treat Cargo feature behavior, documentation paths, and release tooling as part of the conversion.

The workspace conversion can change package organization and the public API together, but it should not silently change runner results. Streaming semantics and Dataflow feature coverage remain separate projects.

## Proposed dependency shape

```text
                          ironbeam (v5 facade)
                         /       |        \
              ironbeam-direct  ironbeam-io  optional adapters
                         \       |        /
                          ironbeam-core

      ironbeam-dataflow -> ironbeam-core + Beam protocol/harness crates
      ironbeam-compat   -> ironbeam v4 + ironbeam v5
```

The names are provisional. Cargo members are valuable when they enforce a useful boundary, not simply because a module has many lines.

| Package | Intended responsibility | Initial placement |
| --- | --- | --- |
| `ironbeam` | Version 5 user-facing exports, prelude, feature selection, and examples | Keep the existing package name, but permit deliberate API and default-feature changes in the major release. |
| `ironbeam-core` | `Pipeline`, `PCollection<T>`, graph IDs and nodes, element and combine traits, transform construction, coder metadata, names, and runner-neutral graph validation | Make this an early extraction once a narrow external construction API is designed. |
| `ironbeam-direct` | Current planner, optimizer, Rayon runner, local collect methods, cache placement, metrics execution, checkpoint and spill orchestration | Keep tightly coupled parts together until the core/runner contract is proven. |
| `ironbeam-io` | Low-level format readers and writers plus bounded source and sink adapters | First extract format mechanics; move graph-facing helpers after a public source/sink construction API exists. Subdivide by format only if dependency or ownership pressure warrants it. |
| `ironbeam-testing` | `TestPipeline`, fixtures, assertions, and local-runner test support | Candidate leaf crate after its dependency on the direct runner is explicit. |
| `ironbeam-arrow`, `ironbeam-datafusion`, `ironbeam-polars` | Optional batch/columnar interoperability | Prototypes first; publish only after the interchange type and ownership model settle. |
| `ironbeam-object-store` | Optional local/cloud object storage source and sink support | Candidate once source/sink lifecycle and error semantics are specified. |
| `ironbeam-compat` | Migration helpers and materialized-data bridges between published Ironbeam 4 and Ironbeam 5 | Optional leaf package; neither core nor the v5 facade should depend on it. |
| `ironbeam-dataflow` | Beam translation, driver, harness, and GCP submission | Continue as a separate project initially. Align dependencies and contracts before deciding whether to move its repository into this workspace. |

`ironbeam-core` must not depend on `ironbeam-direct`, a format package, DataFusion, Polars, or Dataflow. Optional packages may depend on core. The facade may depend on selected packages and re-export their APIs. That direction prevents cycles and keeps a small core build possible.

## The core API boundary

The intended core is a *runner-independent pipeline description and typed construction API*, not a local execution engine. It should own:

- `Pipeline`, `PCollection<T>`, `NodeId`, graph edges, names, and snapshots.
- `Element`, `CombineFn`, stateless operator abstractions, and the standard transforms that construct nodes.
- A stable way for external packages to register a typed source or sink without touching `pub(crate)` fields.
- Enough metadata for a runner to validate element types, coders, boundedness, and capabilities.

Runner-specific behavior belongs behind explicit backend capability and lowering hooks. A runner can decline an operation with a typed diagnostic, but the core must not use silent no-op stubs or panic placeholders. The current `Node` enum's partition closures and nested subplans are local physical-plan details to separate from the v5 logical graph.

The current source construction path calls `Pipeline::insert_node`, `connect`, and `set_coder`, then constructs `PCollection` using crate-private fields. A separate I/O crate cannot use that path. Before extracting graph-facing I/O helpers, define a narrow public builder API or source/sink trait that preserves the type and coder checks. Exposing the entire mutable graph would make later runner work harder.

Many helpers also provide **inherent** `PCollection` methods from modules inside the present crate. Rust does not allow a sibling crate to add inherent methods to a type owned by `ironbeam-core`. Version 5 can deliberately move optional helpers to extension traits or free functions while keeping fundamental transforms inherent. The facade can re-export extension traits through a prelude so common chaining remains ergonomic. Explicit imports and method resolution will change; the migration guide and compatibility package should identify those changes precisely.

### Which transforms remain inherent

The core should own the graph-building primitives and a small set of shared vocabulary transforms. A method belongs directly on `PCollection` when it either creates a fundamental graph operation, is a building block used throughout other core transforms, or provides essential graph metadata. A method's popularity alone is insufficient, and merely calling `map` internally does not make every wrapper a core primitive. The core should not depend on optional formats, cloud services, a particular runner, or an ecosystem adapter.

| Proposed home | Examples from v4 | Reason |
| --- | --- | --- |
| Inherent `PCollection` methods in core | `map`, `filter`, `flat_map`, custom transform application, `with_name`, `node_id` | Basic typed graph construction and identification. |
| Inherent keyed and aggregate methods in core | `key_by`, `map_values`, `filter_values`, `group_by_key`, `combine_values`, `combine_globally` | Shared vocabulary for many other transforms; grouping and combines also create fundamental node kinds. |
| Core graph operations, using a method or free function as fits their inputs | `flatten` and a co-group/fan-in primitive once its graph representation is settled | They represent graph shape rather than a convenience wrapper. `flatten` already takes several collections, so it need not become a method. |
| Extension traits or functions outside core | `filter_eq`, count/sum/top-k convenience methods, regex helpers, format readers and writers, Arrow/DataFusion/Polars adapters | These compose the core vocabulary or require optional dependencies. |
| Decide after semantic review | `map_batches`, joins, side inputs, window helpers, `tee`, `wait_on`, `reshuffle` | Some build special nodes or affect runner behavior; their v5 meaning should be specified before assigning crate ownership. |

This is an initial call graph and semantics rule, not a final list. For example, v4 `key_by` is implemented with `map`, but many core operations use keyed data, so keeping it inherent is reasonable. `filter_eq` is a thin wrapper over `filter` and can move out. `combine_values` and `combine_globally` each construct a distinct node kind and belong in core. A separate review should classify every public `PCollection` method, including its v4 caller path and v5 import requirement.

`collect_seq`, `collect_par`, and immediate `write_*` methods currently execute work from `PCollection`. Version 5 may put direct-runner terminals behind an extension trait or explicit runner call. Deferred source and sink nodes are a separate design task: immediate writers currently collect a whole result before writing, which is unsuitable as the only sink model for large or remote pipelines.

The `coders` feature currently changes the universal `Element` bound to require Serde and attaches Postcard coders during construction. Version 5 can reconsider that bound, but the choice must be explicit because it affects every `PCollection<T>` and the Dataflow harness. The wire contract should identify codec and version clearly; it should not rely only on a Rust type name or closure shape.

## Placement of existing features

| Current area | Proposed owner | Boundary note |
| --- | --- | --- |
| `pipeline`, `collection`, `node`, `node_id`, `type_token`, `extensions` | Core | Extract together only after the builder API is designed. |
| `combiners`, typed transforms, joins, side inputs, window helpers, validation | Core primitives plus optional transform packages | Apply the inherent-method rule above; convenience methods may move to extension traits. Current windows are bounded-data helpers, not streaming progress semantics. |
| `planner`, `runner`, local CSE, execution metrics | Direct runner | The optimized linear plan is a local physical plan; backends should consume the logical graph or a runner-neutral lowering. |
| `checkpoint`, `spill`, `spill_integration` | Direct runner support | Keep current local behavior separate from future distributed checkpoint contracts. |
| `coders` | Core metadata plus a codec package if useful | Core must know how to associate a coder with a node; concrete Postcard encoding can be isolated later. |
| `io/*`, format-specific `helpers/*`, compression, globbing | I/O package | Split low-level codecs first. Graph-facing readers need the public source API; fluent writers need the method-ownership decision. |
| `io/cloud/*` | Optional storage adapter | Compare its abstractions with `object_store` before preserving a parallel cloud interface. |
| `testing/*` and `testing.rs` | Testing package | Some assertions execute via the direct runner. |
| `beam-proto`, graph translator, harness, Dataflow driver | Dataflow project | Keep GCP, tonic, and protobuf dependencies outside the default local build. |

Inventory the current Cargo feature names (`io-*`, compression options, `parallel-io`, `metrics`, `checkpointing`, `spilling`, and `coders`) and map each to its version 5 package or document its replacement. A major release permits a smaller default feature set and clearer names. Check default, minimal, individual, and all-feature builds; feature unification across workspace members must not accidentally enable a heavy integration in a minimal consumer build.

## Version 4 and version 5 compatibility

The compatibility package should serve two distinct needs. It offers a familiar API surface and data handoff, not ABI compatibility; v4 and v5 Rust types remain separate compiled types.

| Need | Feasible approach | Limit |
| --- | --- | --- |
| Help v4 callers adopt v5 names and method locations | Export migration traits, helper functions, and a v4-to-v5 API guide | A sibling crate cannot recreate v4 inherent methods on v5's `PCollection`; users may need an import or call-site change. |
| Exchange data between an already built v4 pipeline and a v5 pipeline | Depend on both major versions under distinct Cargo dependency names; collect a bounded v4 result into `Vec<T>`, then create a v5 source, and offer the reverse direction where type bounds allow | This materializes data and uses the local runner. It does not preserve laziness, graph optimization, metrics, checkpoints, or distributed execution across the boundary. |

An arbitrary v4 `Pipeline` or `PCollection<T>` cannot be losslessly converted into a v5 graph: v4 nodes hold type-erased payloads and compiled closures, and their IDs, coders, and runner assumptions are version-specific. Avoid a graph conversion API unless a narrowly defined operation can be proven equivalent. A compatibility crate can coexist with both major versions because Cargo supports multiple versions of the same package under distinct dependency names; the `ironbeam` v5 package must not depend back on compatibility code.

The migration guide should map public types, transform methods, terminal calls, feature flags, and behavior changes. Compatibility tests should compile small v4-style examples against the adapters and verify data-bridge boundaries explicitly.

## Rust ecosystem integrations

| Candidate | First useful boundary | Main question |
| --- | --- | --- |
| Apache Arrow | Exchange `RecordBatch`es without an extra file round trip | Is a batch-native collection or operator path needed to avoid row conversion costs? |
| Apache DataFusion | Run a schema-aware SQL/relational segment over Arrow batches | Where does Ironbeam's plan end and DataFusion's plan begin? Arbitrary Rust closures cannot be translated into expressions. |
| Polars | Hand a bounded batch or subpipeline to a `LazyFrame`/`DataFrame` adapter | Which planner owns operations on either side of the handoff? |
| `object_store` | Implement location-independent bounded sources and sinks | How are async I/O, retries, atomic writes, and credentials surfaced to a synchronous runner? |
| Timely/Differential Dataflow | Explore a distinct progress-aware runner | Which typed Ironbeam operations have equivalent incremental semantics? |

Arrow interoperability is the strongest first prototype because Ironbeam already has Arrow IPC support. DataFusion and Polars can follow through explicit batch handoffs. A direct Timely or Differential runner is a research project; their progress and update models should not be hidden behind a thin adapter.

## Dataflow runner direction

An optional Dataflow package should lower the logical graph into Beam's portable pipeline representation. The local direct runner should remain a separate implementation. Initial parity is a named subset, not every Ironbeam operation.

The major contracts to resolve are:

1. **Graph shape:** stable handling of branches, fan-in, multiple outputs, and terminal sinks. The current translator's single-chain assumption is the immediate limit.
2. **Executable operations:** how a worker identifies compiled Rust operations, verifies it rebuilt the same graph, and detects incompatible binaries or coders. A structural fingerprint alone does not prove closures and source configuration are equivalent.
3. **Sources and sinks:** deferred work, boundedness, splitting, retries, and commit behavior. `from_vec` plus a logging transform is useful for a prototype but not a general cloud I/O contract.
4. **Capabilities:** explicit backend rejection when an operation has no valid Dataflow lowering or harness implementation.
5. **Repository and release alignment:** the integration is currently a separate Apache-2.0 workspace with its own release cadence and a local `../ironbeam` patch. Its ownership, licensing, dependency versions, and release process need agreement before any repository move. Fixing local path assumptions is independent of merging codebases.

## Streaming and feature parity track

File APIs named `read_*_streaming` perform lazy, **bounded** shard reads. Beam-style streaming means an unbounded source and progress semantics. A synchronous worker-thread engine is possible; async Rust is helpful for concurrent network I/O but does not itself provide watermarks, state, or recovery.

Prioritize missing Beam concepts by the contracts they establish:

| Feature | Why it matters | Relative complexity |
| --- | --- | --- |
| Deferred sources and sinks | Enables large local jobs and real backend I/O | High |
| Splittable source work with checkpoint/resume | Supports dynamic work balancing and portable file sources | Very high |
| Schema-aware records and field operations | Improves Arrow/DataFusion interop and portable transforms | Medium to high |
| Transform lifecycle and execution context | Defines resource setup, teardown, metrics, and worker behavior | Medium |
| Unbounded source plus bounded queues and backpressure | Establishes a minimal local streaming runner | Very high |
| Watermarks, full window triggers, lateness, and `TestStream` | Defines when results become final and how to test progress | Very high |
| Keyed state, timers, and recoverable sink commits | Makes stateful streaming reliable | Very high |

Keep two support lists for each feature: local direct runner and portable Dataflow backend. Existing transforms need explicit semantics for an unbounded collection; some global operations may require a window or remain batch-only.

## Conversion sequence and checkpoints

1. **Record the v4 baseline.** Inventory public paths, inherent methods, feature combinations, examples, tests, documentation links, and release behavior on `main`. Capture representative downstream builds, including the Dataflow workspace. Mark intentional v5 breaks in a migration table.
2. **Create the workspace shell.** Keep the existing `ironbeam` package as the public entry point while introducing a workspace manifest and package-level release configuration. This isolates build and publishing changes from API extraction.
3. **Define the v5 core contract.** Design typed source/sink construction, graph validation, and the ownership of core transforms. Extract `ironbeam-core` and move the direct planner and executor behind a one-way dependency. Allow extension-trait changes where they simplify the boundary.
4. **Extract I/O and support packages.** Move format mechanics, graph-facing sources/sinks, and test support once their dependencies are explicit. Map v4 feature flags to v5 packages or document replacements.
5. **Build migration support.** Publish a v4-to-v5 guide and a small `ironbeam-compat` package for source migration and bounded materialized-data bridges. Do not promise graph-level conversion.
6. **Add one integration prototype.** Use Arrow batches at a single explicit handoff point. Decide whether batch-native execution belongs in core before adding both DataFusion and Polars adapters.
7. **Align the Dataflow project.** Test it against the workspace packages through a normal dependency path, specify capability checks, and expand from a single chain to selected graph shapes and deferred I/O. Decide repository placement with its contributor before moving its crates.
8. **Explore streaming separately.** Build a deterministic in-process test source and fixed-window runner before network connectors or distributed state.

At every extraction, check `cargo test`, documentation, examples, default and minimal feature builds, and downstream API examples. Before publishing multiple crates, check package metadata, dependency versions, publish order, and release automation. The existing release configuration currently assumes a single root `Cargo.toml` and `Cargo.lock`.

## Decisions to settle before code movement

1. Which methods in the provisional inherent-method table have semantics strong enough to be shared by every runner?
2. Should v5's default build remain broad, or should format and distributed integrations become opt-in?
3. Should Arrow batches become first-class Ironbeam elements/operators, or remain an explicit adapter boundary?
4. Should the Dataflow integration stay in its contributor's repository with path/registry dependencies, or eventually join the Ironbeam repository and release process?

Recommended starting point: keep the `ironbeam` package name, define the v5 core and source/sink boundary, and permit a documented method reorganization. Build compatibility around v4-style calls and bounded data exchange rather than attempting to transplant v4 graphs.
