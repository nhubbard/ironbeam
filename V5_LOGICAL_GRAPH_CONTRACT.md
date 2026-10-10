# Ironbeam 5 logical graph and runner contract sketch

Status: Phase 0 design fixture, **not** a published Rust trait. The first core/direct/Dataflow prototype may refine names without changing these invariants.

## One graph, two lowerings

Use `from_vec([1,2,3,4]) -> map(double) -> filter(even) -> output` as the first bounded slice. Its logical descriptor has a bounded source, two versioned semantic operations, three typed collection edges, and a declared output port. `map` and `filter` preserve event metadata. Expected output is `[2,4,6,8]` in the sequential direct case; Dataflow promises the same multiset, with no global ordering guarantee.

| Logical node | Direct lowering | Dataflow lowering |
| --- | --- | --- |
| Bounded in-memory source | Owned local source and partition producer; retain v4 `from_vec` for the migration slice | Prototype materialized impulse/source payload with explicit coder and size limit; reject general `from_vec` shipping until a portable source contract exists |
| `map(double)`, `filter(even)` | Fusable stateless operations over bounded buffers | Two named ParDo transforms whose worker operation handles resolve in the rebuilt registry |
| Output port | Typed `Vec<i32>` result request on the direct runner | Explicit deferred sink or result export; reject a driver-side `Vec` request without a supported materialization capability |

The backend validates the **entire requested subgraph** and all output/sink capabilities before opening a source or sink. The graph may contain branches and fan-in; each collection has its own producer port and consumer edges. The first Dataflow prototype may reject a shape it cannot lower, with a typed diagnostic, but the graph representation must preserve that shape. The present Dataflow translator (`ironbeam-graphx/src/translate.rs`) requires one linear source chain and is therefore only a comparison point.

## Descriptor and executable registry

An immutable graph snapshot contains ordered node/port descriptors, directed edges, semantic operation ID plus version, type/schema/coder ID plus version, boundedness, names, time/window declarations, source progress declarations, sink mode/recovery declarations, and required capabilities. Source and sink construction goes through a typed builder that checks ports and coders; it does not expose mutable graph internals. Runtime closures, `Any`, `VecOps`, and direct physical partitions stay outside the serialized descriptor.

`NodeId` is a build-local index. Portable identity is a canonical byte encoding of the semantic descriptor: sort nodes by stable user path/operation key and ports, normalize options, sort edges by those keys, and hash with an explicitly named algorithm/version. Duplicate stable paths are validation errors. Randomized map iteration, debug strings, Rust type names alone, closure addresses, and local node IDs cannot affect the hash. A registry maps semantic operation handles to compiled typed functions and source/sink factories; cross-process startup verifies descriptor hash, registry manifest hash, binary/build identity, coder manifest, and source configuration digest **before** bundle processing. A match is required; mismatch is permanent incompatibility. The existing Dataflow structural fingerprint hashes node kinds and edges only, so it does not establish executable equivalence.

Graph descriptor, coder wire formats, and checkpoint/state schemas have independent major/minor versions. Readers accept supported minor versions and explicit migrators only; unknown major versions and unsupported older forms fail before execution. A state snapshot binds graph and state schema identities. The graph descriptor never serializes Rust closures.

## Runner boundary and lifecycle

The initial interface is conceptually `validate(snapshot, requested_outputs, capabilities) -> ValidatedPlan`, `lower(validated, registry, job_config) -> BackendPlan`, and `submit(plan) -> JobHandle`. Keep physical plans and registries private while the two prototype lowerings prove the interface. A pipeline may have zero or many declared sinks and independently named output ports; submission requires at least one sink or requested result. Typed `collect<T>` is a direct-only bounded convenience that registers one result request, submits, waits, checks its type/coder, and takes the result. The object-safe backend sees erased output descriptors, not a generic `Vec<T>` method.

`JobHandle` has a stable job ID, status (`Submitted`, `Running`, `Draining`, `Succeeded`, `Failed`, `Cancelled`), progress snapshot, cancellation request, wait/observe, and a terminal typed error. It may live indefinitely. Cancellation is idempotent and does not imply successful sink commit. A synchronous direct runner can complete before `submit` returns; remote and streaming runners need not. No Tokio runtime is imposed on core callers.

Each submission owns a `JobConfig` with parallelism, memory and buffer budgets, cancellation token, clock, and telemetry context. The direct runner uses a job-owned Rayon pool instead of `build_global`; two jobs with different settings must coexist. Core records resource requirements only when they affect capability validation, not scheduler choices.

Errors are typed as graph construction/validation, unsupported capability, incompatible descriptor/binary/coder, execution, I/O, and recovery. They carry backend, stable node path plus local ID when available, operation/capability, source cause, and retry class (`Permanent`, `Transient`, `Unknown`). Invalid public input and unsupported operations return errors; internal invariant assertions remain internal. Validation is side-effect free and precedes source setup or sink creation.

Third-party operations declare a namespaced semantic ID and major/minor version, input/output and time behavior, and capabilities. A runner without a registered lowering returns `UnsupportedOperation` naming the node and semantic ID. Physical-plan and registry internals are not a public plugin ABI yet.

## Unbounded-source contract fixture

Design-only pseudocode for Phase 2's compile-time fixture:

```rust,ignore
let events = p.source::<Event>(SourceSpec {
    key: "events", boundedness: Unbounded,
    split_identity: Stable, replay: AtLeastOnce,
    progress: EventTimeProgress,
    coder: EventCoderV1,
})?;
let prepared = events.map_named("normalize/v1", normalize)?; // preserves timestamp/window
let windowed = prepared.assign_fixed_windows(Duration::minutes(5))?;
windowed.sink(SinkSpec::append("events-out", ReplayPolicy::Idempotent))?;
let graph = p.snapshot()?;
assert_eq!(DirectRunner::validate(&graph).unwrap_err().capability(), UnboundedSource);
```

This must be constructible with core APIs even if no runner can execute it. All collections carry boundedness: elementwise transforms preserve it; fan-in is unbounded if any input is; finite-only global group/combine, `take`, and joins/side inputs without explicit window/time rules are rejected on unbounded input. Default window is global. Event time is a checked signed 64-bit count of nanoseconds from the Unix epoch, with explicit pre-epoch support and overflow errors; an internal envelope carries timestamp, window set, and future pane metadata separately from user `T`. Stateless elementwise transforms preserve the envelope by default. Assignment changes windows explicitly. Progress messages are separate from data, identify source and split, carry replay position and optional event-time progress, and bounded splits emit completion. Fan-in retains input/split provenance.

Stateful nodes reserve key/window scope, state schema version, and timer domain metadata and require runner capabilities; no empty state/timer API is exposed. Deferred sinks declare append, update, or final-result mode, replay/idempotency, and commit/abort behavior. Exactly-once claims require a coordinated source-position/sink-commit protocol; the initial slice does not claim one. Bounded buffers and backpressure belong to runner physical plans, not core `Vec<T>` edges. Watermarks, triggers, late panes, virtual-time test streams, and recovery execution remain later streaming-runner work.

## Phase 2 contract tests required before freezing an API

Canonical snapshot determinism and identity mismatch; missing/duplicate ports and cycle rejection; coder/type/version incompatibility; branching and fan-in; unsupported custom operation and unbounded fixture rejected before side effects; timestamp/window preservation; finite-only operation rejection; independent per-job settings and cancellation; a fake handle remaining `Running`; Direct and Dataflow lowering of the same bounded slice. The v4 behavior corpus in `tests/v5_behavior_corpus.rs` supplies differential values and order expectations.
