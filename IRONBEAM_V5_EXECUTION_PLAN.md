# Ironbeam 5 workspace conversion execution plan

Planning branch: `codex/ironbeam-v5-runner-core`, created from `main` at `d64c26a` on October 10, 2026. This plan turns the [workspace architecture proposal](WORKSPACE_ARCHITECTURE_PLAN.md) into a sequence of reviewable changes. Ironbeam 5 may change source-level API; the separate compatibility package will provide familiar calls and bounded data exchange, not ABI or arbitrary graph compatibility.

## Outcome and non-negotiable contracts

Ironbeam 5 has a runner-independent core that describes a typed logical dataflow graph. A direct runner and a Dataflow backend consume that graph through explicit validation and lowering contracts. A backend may support a subset of operations, but it must reject unsupported operations with a typed error naming the node and missing capability before starting work. No no-op stubs or runtime panics stand in for missing runner implementations.

The v5 graph describes *what* an operation means; each runner chooses *how* to partition, schedule, shuffle, retry, and materialize it. The core does not import Rayon, Tokio, GCP, Beam protobufs, a format-specific library, or a physical-plan type. Beam protobufs are produced by the Dataflow backend. The existing local runner remains the reference executor while the new contract is established.

No streaming claim follows from this conversion. Source boundedness and event-time metadata should be represented from the start so a later streaming runner can use them, but watermarks, triggers, keyed state, and timers need their own semantics and tests.

## Architecture to establish first

### Logical graph and executable functions

The current `Node` enum includes `VecOps`, boxed `Any` payloads, `Partition`-level closures, and nested local subplans for some joins and flatten operations. Those are useful direct-runner machinery but are not a neutral graph model. The v5 design needs separate layers:

1. **Logical graph:** stable node IDs, typed input/output relationships, fan-in/fan-out edges, operation kind, names, coder/schema requirements, boundedness, and declared capabilities. It must represent explicit sources and sinks. A graph snapshot should be immutable and validated before lowering.
2. **Executable function registry:** typed user code and source/sink factories associated with logical nodes by stable handles. A direct runner invokes them locally; the Dataflow harness can rebuild the same registry from the user's binary. The graph need not serialize Rust closures. Stable handles alone are insufficient for cross-process equivalence, so the backend also verifies the binary, graph, coder, and relevant source configuration.
3. **Physical plan:** private to each runner. The direct runner may retain fusion, partitions, Rayon, and local subplans internally. Dataflow lowers to Beam transforms, coders, and environments.

This layering is a design target, not a demand to discard working local algorithms. A first vertical slice can carry `from_vec → map → filter → sink/collect` through the new graph and direct runner while the remaining v4 operations continue to work during migration.

### Runner extension contract

The core should define a small backend interface for graph validation, capability reporting, lowering, and job submission or execution. Operation-specific hooks may be dynamic traits or a registry keyed by semantic operation kind. They must expose enough information for third-party runners without asking core to know runner internals. The interface must permit both synchronous local completion and a remote job handle; it must not force every caller onto Tokio.

Candidate capabilities include bounded/unbounded sources, elementwise transforms, keyed shuffle, combine, fan-in, side inputs, windowing, timers/state, and sink commit semantics. Each runner publishes its capabilities and validates the entire requested graph before side effects. A transform with runner-specific semantics either declares those semantics as a capability or lives in a runner-specific extension package. A core operation may have multiple valid physical implementations; a missing implementation produces `UnsupportedOperation`, not a placeholder result.

The first interface review must settle: how terminal outputs are selected, whether a pipeline has one or many sinks, how typed results relate to an object-safe backend, and how a runner supplies diagnostics and cancellation. Do not freeze a public `Runner` trait until the direct runner and one Dataflow lowering prototype both exercise it.

### Core `PCollection` surface

Keep the graph primitives and shared vocabulary inherent: `map`, `filter`, `flat_map`, custom transform application, `key_by`, keyed mapping/filtering, grouping, and combining. Keep node identity and naming near the graph. Multi-input graph operations such as flatten can remain free functions. Convenience transforms built from these operations move to extension traits or transform packages. The method-by-method inventory is a required Phase 0 artifact; names alone do not decide ownership. `collect_*` is a direct-runner terminal, while format writers should become deferred sink options as well as any retained immediate convenience methods.

## Work packages and exit criteria

Each numbered package should be a reviewable change or small series of changes. Every package must leave the workspace compiling and its applicable tests green. Record API changes as they happen rather than postponing a migration guide until the end.

| Phase | Deliverable | Exit criterion |
| --- | --- | --- |
| 0. Baseline and API inventory | Fresh v4 coverage report, public API and feature inventory, representative pipelines, current Dataflow dependency check, and v4-to-v5 migration ledger | A reviewer can identify current behavior, coverage gaps, and every method proposed to move. CI numbers are recorded from `main`, not inferred from an old report. |
| 1. Workspace shell and test harness | Cargo workspace with the `ironbeam` facade still usable, per-package CI jobs, coverage collection and thresholds, and a runner contract test crate | Existing examples and tests still pass; minimal and all-feature builds work; coverage reports distinguish core, runner, I/O, and integration code. |
| 2. Runner-neutral core vertical slice | `ironbeam-core` graph, validation, typed builder API, function registry, capability/error model; v5 facade exports | `from_vec → map → filter → output` executes through the direct runner; malformed graphs and unsupported capabilities fail before execution; core has no runner or I/O dependencies. |
| 3. Direct runner migration | Move planner, optimizer, partition execution, local metrics, spilling, and checkpointing behind the new graph contract | Golden and differential tests show specified v4/v5 behavior for the operation matrix in sequential and parallel modes; no old local-plan objects leak into core. |
| 4. Transforms, sources, and sinks | Move remaining core primitives, convenience transforms, bounded I/O, and terminal APIs to their owning packages | Branching, flatten, joins, grouping, combines, side inputs, and each enabled format have contract tests; every unsupported backend path is explicit. |
| 5. Compatibility and release preparation | `ironbeam-compat`, v4-to-v5 guide, feature mapping, package metadata, and multi-crate release workflow | Representative v4-style programs migrate with documented changes; bounded v4/v5 data bridges pass; each crate passes a publish dry run. |
| 6. Dataflow alignment | Update the separate backend to consume v5 graph contracts, then extend a selected operation subset | Translator and harness agree on coders, IDs, and capability failures in local/mock tests; a real-cloud smoke job is a separate opt-in gate with credentials and cost controls. |
| 7. Optional Rust integrations | Arrow batch boundary first, then DataFusion, Polars, or `object_store` adapters as justified by prototypes | Conversion cost and ownership are measured; adapters have independent feature/build tests and do not enter the minimal core dependency graph. |

The Dataflow repository should stay separately owned until its contributor agrees on placement and release responsibilities. The current `../ironbeam` patch must be updated for local testing; no repository move is required to prove the v5 backend contract.

## Test and coverage policy from Phase 0

The current CI runs `cargo llvm-cov --workspace --all-features --branch` and uploads the result, but does not fail on a coverage threshold. A saved `lcov.info` modified September 19, 2026 contains 9,658 covered of 11,229 instrumented lines (86.01%) and 802 covered of 1,046 branches (76.67%). It is a historical reference, not a fresh `main` baseline. Phase 0 reruns coverage and records module-level exclusions before setting gates.

**Coverage target:** every reachable new line and branch in the logical graph, validation, capability dispatch, coder selection, and runner lowering paths should be exercised. Uncovered paths need a test or a reviewed explanation; generated protocol code and genuinely unreachable defensive paths may be excluded explicitly. No phase may lower the measured baseline for touched packages. Add a CI threshold ratchet based on the fresh baseline, then raise it as migrated modules become fully covered. A single overall percentage never substitutes for an operation-level test matrix.

Required layers:

1. **Core unit and property tests:** graph insertion, stable IDs, type/coder relationships, fan-in/out, cycle and missing-input rejection, named scopes, deterministic snapshots, and invalid capability combinations. Use generated DAGs where they reveal invariants that example tests miss.
2. **Runner contract suite:** the same small graph cases run against every backend's validator. Assert supported results and typed unsupported errors; test that validation happens before a source or sink side effect. A backend can mark a capability unsupported, but cannot skip the case.
3. **Direct-runner differential tests:** run representative v4 and v5 pipelines over the same bounded inputs. Compare values, order where promised, empty/singleton behavior, failure propagation, sequential and parallel modes, and checkpoint/spill paths. Lock down intended behavior changes separately.
4. **Serialization and Dataflow tests:** verify coder round trips and deterministic graph/function references; compare translated Beam structures and exercise harness bundle execution with mock services. Test mismatched binary/graph/coder configuration as failures rather than log-only warnings.
5. **I/O and recovery tests:** source splitting, partial read, retry, sink commit or abort, empty inputs, large keys/batches, and optional format gates. Tests needing Docker, cloud access, or paid services run in a separate, clearly named tier.
6. **Consumer tests:** compile public examples, explicit-import examples, the compatibility crate with both major versions, and the separate Dataflow workspace. Test each package independently so workspace feature unification does not hide missing dependencies.

Per-phase local and CI checks include formatting, Clippy with the existing pedantic/nursery policy, `cargo doc --no-deps`, default/no-default/all-feature builds, `cargo-hack` each-feature checks, the relevant test suites, and `cargo llvm-cov --branch`. Run the full matrix before merging each phase. The coverage report must be saved and reviewed for the affected packages, including new error branches. Add mutation or fault-injection tests where a green test could still accept a wrong result, especially in graph validation, shuffle/combines, and sink commit logic.

## Change discipline and release criteria

- Keep the v4 behavior corpus and v5 implementation side by side only as long as needed to compare them. Do not let old and new graph types silently mix.
- Design and test one vertical slice through core and direct runner before bulk-moving modules. Each later operation gets a direct-runner test, a backend capability test, and a documented v4/v5 mapping.
- Make intentional changes explicit: API signature, default feature, output ordering, error behavior, coder/wire format, or performance assumptions.
- Keep the `ironbeam` v5 facade free of a dependency on `ironbeam-compat`; compatibility may depend on separately named v4 and v5 packages.
- Treat a release as ready only when package publishing order, documentation, migration examples, direct-runner regression suite, backend contract suite, and coverage gates all pass. Dataflow cloud support is advertised only for the operations verified there.

## Immediate branch tasks

1. Run the fresh baseline coverage command on `main` or an untouched checkout and record line/branch counts by module. Confirm which tests require external services and separate those from the default suite.
2. Produce a method inventory of all public `PCollection` methods and free transform functions, with their dependencies and proposed v5 owner. Flag operations that embed local-runner behavior.
3. Write a short v5 logical graph and runner contract sketch with one direct-runner and one Dataflow lowering example for the same small pipeline. Resolve terminal selection and capability errors before making the interface public.
4. Implement Phase 1 only after the contract sketch and coverage gate are reviewable. Keep each later phase independently mergeable.
