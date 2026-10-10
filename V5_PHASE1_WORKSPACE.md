# Phase 1 workspace and test harness

The workspace now has two members: the existing published `ironbeam` package and the unpublished `ironbeam-runner-contract-tests` package. The root package remains the default member, so ordinary `cargo test`, examples, and the v4 public API keep their prior entry point. This phase does not extract `ironbeam-core` or publish a runner trait; those follow the first Phase 2 vertical slice.

The contract package is an adapter test harness. Its bounded case is the same `from_vec([1,2,3,4]) -> map(double) -> filter(even) -> output` graph in the v5 sketch. A v4 direct adapter proves the expected values. A second fixture declares an unbounded source, signed pre-epoch event time, a fixed window, and a sink; the batch adapter reports `unbounded-source` at `events` before starting a job. The fixture types are **test-only** and are not production graph or runner APIs. A Phase 2 direct adapter must replace the v4 adapter with validation against the actual v5 snapshot; the Dataflow adapter will use the same cases when its lowering is ready.

CI selects each package independently. The facade runs default and minimal tests, each-feature builds, all-feature build and portable coverage. The contract package has its own test, Clippy, and coverage job. Docker-backed MongoDB and SQL tests are a manually dispatched service tier. Documentation builds across the workspace; all existing examples build. The benchmark example is built but not timed in CI, because host load makes timing unsuitable as a pass/fail gate.

`coverage-baselines.json` stores the measured Phase 0 facade floor (9,382/11,229 lines and 785/1,046 branches) and the new contract package floor (47/54 lines and 4/4 branches). `scripts/check_coverage.py` compares exact coverage ratios, fails on a regression or missing measurement, and reports facade code as core/transforms, direct runner, I/O, and testing plus the separate integration contract package. The category labels describe the current single-crate layout; they will become package boundaries as code moves. The facade report skips `io::mongodb` and `io::sql_containers` because they require Docker, but those instrumented source lines remain in the coverage denominator. Each CI coverage job uses a separate build directory, avoiding mixed feature-profile measurements.

The full workspace test run exposed a pre-existing race among spill tests that reset the process-wide `MemoryTracker`. The affected tests now use the same test mutex as the rest of that file. This changes test isolation only.

After that fix, the default workspace suite passed 1,440 tests and the portable all-feature workspace suite passed 1,638 tests. The Docker service tier was not run locally because this host cannot connect to Docker.
All non-benchmark examples also built and ran with all features locally.

Local checks for this change:

```text
cargo fmt --all --check
cargo test -p ironbeam --lib --tests
cargo test -p ironbeam --no-default-features --lib --tests
cargo test -p ironbeam-runner-contract-tests
cargo build -p ironbeam --no-default-features
cargo build -p ironbeam --all-features
cargo hack check -p ironbeam --each-feature --no-dev-deps --keep-going
cargo doc --workspace --all-features --no-deps
cargo llvm-cov -p ironbeam --all-features --branch --json --summary-only --output-path coverage-ironbeam.json -- --skip io::mongodb --skip io::sql_containers
cargo llvm-cov -p ironbeam-runner-contract-tests --branch --json --summary-only --output-path coverage-contract.json
```

The separate Dataflow repository still has a local `../ironbeam` path assumption. Its dependency and release alignment remain Phase 6 work; it is not a member of this workspace.
