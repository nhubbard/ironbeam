# Phase 2: runner-neutral core vertical slice

Status: implemented on `codex/ironbeam-v5-runner-core` as an additive `v5-preview` API. The v4 facade and its default features retain their existing behavior. This is a graph and bounded execution slice, not a streaming runner or a release-ready v5 API.

## What is concrete

- `ironbeam-core` owns immutable graph snapshots, typed `Pipeline`/`PCollection<T>` construction, the compiled-function registry, versioned descriptors, canonical JSON and SHA-256 graph fingerprints, typed validation/capability errors, boundedness, time/window metadata, source progress and sink declarations. It has no runner, Rayon, Tokio, I/O, or Beam dependency.
- `ironbeam-direct` validates the entire graph before opening sources, then executes a requested bounded `from_vec → map → filter → output` path. Each submission builds its own Rayon pool. The bounded path preserves result order for this source and elementwise operations. The direct runner currently materializes a `Vec<Envelope>` at each stage; that is a private physical choice, not a core edge contract.
- The existing facade exposes these packages as `ironbeam::v5` only with `v5-preview`. A facade smoke test compiles that route. Other v4 operations have not been moved.
- The test-only Dataflow prototype lowers **the same bounded descriptor** to materialized-source, ParDo, ParDo, and result-export labels. It does not ship data, construct Beam protobufs, or claim cloud support. The separate `ironbeam-dataflow` repository remains untouched until Phase 6.

## Settled contracts

| Concern | Phase 2 choice |
| --- | --- |
| Identity | A build-local `local_id` is excluded from canonical JSON. Stable node keys and explicit semantic element/coder IDs enter the sorted descriptor and `sha256-v1` fingerprint. `Element` requires owners to declare those IDs. Driver/worker compatibility compares graph, build, registry, coder, and source-configuration identities; a mismatch is permanent. |
| Versions | Graph, coder, and checkpoint schema versions are independent. Unknown major or newer minor versions reject before lowering. No automatic migration exists yet. |
| Errors | Shared `ErrorKind` covers graph, unsupported capability, incompatibility, execution, I/O, recovery, and cancellation. Errors carry node/backend, message, and retry class. Validation is side-effect free. |
| Resources | `JobConfig` owns parallelism, memory/buffer declarations, and cancellation token. Distinct jobs get distinct pools. This slice validates nonzero budgets; memory/buffer enforcement belongs to Phase 3 physical scheduling. |
| Time | Signed `i64` nanoseconds allow pre-epoch timestamps. User values stay separate from envelope metadata. Default window is global; fixed window assignment uses checked arithmetic and rejects overflow. Map/filter preserve metadata. |
| Boundedness | Stateless operations preserve source boundedness. An unbounded global aggregation marker fails graph construction. The unbounded-source fixture with map, five-minute window, and append sink is representable; Direct rejects `unbounded-source` at `events` before execution. |
| Source/sink | Source progress declares stable split identity, replay, and event-time reporting. A separate `ProgressSignal` carries split, position, time, and completion. Deferred sinks declare mode, replay, and commit requirement. These are descriptors only in Phase 2. |
| Lifecycle | Direct returns a job ID, status, processed count, and idempotent cancellation handle. A fixture proves a handle can remain `Running` indefinitely. The object-safe public `Runner` trait remains unfrozen pending real Dataflow lowering. |
| Terminal | `Output<T>` selects a typed bounded result. A graph can describe multiple outputs or sinks; the direct convenience runs one requested output path. A remote result export needs an explicit materialization capability. |

## Verification and limits

The core tests cover canonical identity, malformed edges/cycles, versions, coder/type mismatches, boundedness/time propagation, fixed windows including pre-epoch and overflow, capabilities, and custom-operation rejection. Direct tests cover bounded values, empty/filter cases, independent parallelism, cancellation, resource validation, and rejection before source work. The shared contract package checks the test-only Dataflow lowering against the same graph used by Direct. CI measures `ironbeam-core` and `ironbeam-direct` independently and ratchets their line and branch coverage.

`from_vec` is a local bounded fixture; a future remote source must have a portable source configuration and registered coder implementation. Fixed windows and sinks are descriptive but unsupported by the current Direct executor. There is no watermark advancement, live source polling, state/timers, recovery execution, or production Dataflow submission. Phase 3 can now migrate direct physical planning without changing the logical edge type to a collection-sized vector.
