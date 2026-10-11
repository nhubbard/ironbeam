# V5 core test and coverage audit

## 2026-10-10 release-gate remeasurement

The current CI corpus is `cargo llvm-cov -p ironbeam-core -p ironbeam-direct
-p ironbeam-transforms --branch --json` with an isolated, clean target. It
counts source locations once across generic instantiations. Before the latest
registry and time metadata tests it measured 2,032/2,127 locations (95.53%)
and 329/384 branch outcomes (85.68%), below the checked-in baseline ratios.
Tests now cover missing portable coder registrations, single versus multi
transform dispatch, zero sized offset windows, and nonincreasing window
intervals. A clean rerun measured **2,049/2,127 locations (96.33%)** and
**332/384 branches (86.46%)**, above the current 96.04% and 86.17% CI
thresholds. The raw LLVM report contains 2,822/3,025 covered lines and
310/406 branches because generic instantiations are counted separately there.
This passes the configured gate, but is not complete unit coverage: untested
branches remain in graph validation, impossible-length guards, and some
backend capability combinations.

The coder registry now captures each `Element` implementation at graph
construction and gives remote workers type-erased encode/decode access by node
key. The Dataflow mock worker uses it for user-defined element types. Remote Fn
API execution, arbitrary graph lowering, and source/sink parity remain open.

## Earlier Phase 2 measurement

Measured on the Phase 2 branch with `cargo llvm-cov -p ironbeam-core --branch`: **534/545 lines (97.98%)** and **89/100 branches (89.00%)**. This is historical evidence, not the current release gate. Tests live in `crates/ironbeam-core/tests/graph.rs` and run independently of the facade, Direct, and Dataflow prototype.

The suite exercises canonical graph identity across insertion order and branching; graph/coder/checkpoint version rejection; stable semantic IDs; missing, duplicate, cyclic, and mistyped edges; explicit `map` input type/coder validation; boundedness and time propagation; pre-epoch and overflowing fixed windows; source progress and deferred sink declarations; custom and unbounded capability rejection; runtime registry lookup/type failures; and incompatible driver/worker identities. The first Phase 3 differential case compares the v4 and v5 bounded elementwise corpus in sequential and parallel modes.

The required input-edge descriptor changes the preview graph format to **major version 2**. Built-in operation versions remain independent at 1.0. A version 1 graph is rejected; no migrator is available for this unreleased preview format.

The remaining uncovered lines are mostly defensive lock-poison and impossible formatting/serialization errors for the current descriptor types, plus private builder failure paths that public typed construction cannot reach. Branch coverage is below 100% partly because short-circuit conditions compile into separate branches. These are reviewed gaps, not evidence that all future graph operations are implemented.

**Still missing from the core contract:** a multi-input/fan-in descriptor and typed builder, portable source configuration and registered coder implementations for remote execution, and executable window/sink/recovery behavior. The current graph represents unary branching but not fan-in. Phase 3 must continue differential tests as direct planner, partition execution, metrics, spilling, and checkpointing move behind the graph; Phase 4 adds multi-input operations and format ownership. This audit does not label the entire new core “completely tested.”
