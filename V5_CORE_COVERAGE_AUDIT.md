# V5 core test and coverage audit

Measured on the Phase 2 branch with `cargo llvm-cov -p ironbeam-core --branch`: **534/545 lines (97.98%)** and **89/100 branches (89.00%)**. The package-scoped CI gate now uses these counts. Tests live in `crates/ironbeam-core/tests/graph.rs` and run independently of the facade, Direct, and Dataflow prototype.

The suite exercises canonical graph identity across insertion order and branching; graph/coder/checkpoint version rejection; stable semantic IDs; missing, duplicate, cyclic, and mistyped edges; explicit `map` input type/coder validation; boundedness and time propagation; pre-epoch and overflowing fixed windows; source progress and deferred sink declarations; custom and unbounded capability rejection; runtime registry lookup/type failures; and incompatible driver/worker identities. The first Phase 3 differential case compares the v4 and v5 bounded elementwise corpus in sequential and parallel modes.

The required input-edge descriptor changes the preview graph format to **major version 2**. Built-in operation versions remain independent at 1.0. A version 1 graph is rejected; no migrator is available for this unreleased preview format.

The remaining uncovered lines are mostly defensive lock-poison and impossible formatting/serialization errors for the current descriptor types, plus private builder failure paths that public typed construction cannot reach. Branch coverage is below 100% partly because short-circuit conditions compile into separate branches. These are reviewed gaps, not evidence that all future graph operations are implemented.

**Still missing from the core contract:** a multi-input/fan-in descriptor and typed builder, portable source configuration and registered coder implementations for remote execution, and executable window/sink/recovery behavior. The current graph represents unary branching but not fan-in. Phase 3 must continue differential tests as direct planner, partition execution, metrics, spilling, and checkpointing move behind the graph; Phase 4 adds multi-input operations and format ownership. This audit does not label the entire new core “completely tested.”
