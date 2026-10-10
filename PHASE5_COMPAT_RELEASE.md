# Phase 5: compatibility and release preparation

The `ironbeam-compat` package builds against published `ironbeam` 4.1.1 and the local v5 packages. Its `from_vec` helper and prelude support source migration; `v4_to_v5` and `v5_to_v4` exchange finite in-memory values by running the source pipeline locally. Integration tests cover empty, singleton, and multirow handoff, an equivalent v4/v5 program, convenience trait imports, and bridge error classification. The v5 facade does not depend on compatibility.

The [migration guide](V4_TO_V5_MIGRATION.md) maps public types, common transforms, terminals, file I/O, features, and behavior changes. The full v4 method inventory remains in [the Phase 0 inventory](V5_PHASE0_API_INVENTORY.md). Deferred adapters, including MongoDB, SQL, TFRecord, and Arrow IPC, are listed as v4-only rather than advertised as v5 capabilities.

## Package order and release gate

| Order | Package | Registry dependencies |
| --- | --- | --- |
| 1 | `ironbeam-core` 0.1.0 | None of the new packages |
| 2 | `ironbeam-direct` 0.1.0 | `ironbeam-core` 0.1.0 |
| 3 | `ironbeam-transforms` 0.1.0 | `ironbeam-core` 0.1.0; Direct is a dev dependency |
| 4 | `ironbeam-io` 0.1.0 | `ironbeam-core` 0.1.0; Direct is a dev dependency |
| 5 | `ironbeam-compat` 0.1.0 | Published `ironbeam` 4.1.1 and the v5 core, Direct, and transforms packages |

Each path dependency now has a registry version, and publishable packages have a repository URL and license. `scripts/release_v5_packages.sh check` runs workspace tests and docs, inspects every package file list, and performs the first crates.io publish dry run. The separately dispatched `V5 Package Release` workflow has a protected `publish` job. It tests, verifies cleanliness, performs each package's publish dry run, then publishes in dependency order, retrying dry runs while newly published dependencies propagate to the index. The existing semantic-release workflow remains responsible for the root `ironbeam` package and must not be used to publish these five packages as one crate.

The root package remains 4.1.1 with `v5-preview`. Its future 5.0.0 release is outside this package sequence and requires the facade/API decision, documentation, and version bump. Because its manifest now gives the optional v5 dependencies registry versions, any future root publish must follow the leaf-package sequence. No crate has been published by this phase's preparation work. A crates.io lookup found that none of the five `0.1.0` package versions is currently present.

## Local evidence

`ironbeam-compat` integration tests pass against the published 4.1.1 crate. Its source-location coverage is 23/23 lines with no instrumented branches. Workspace Clippy with pedantic/nursery warnings denied passes. Default workspace tests, all-feature workspace tests including Rancher Desktop's MongoDB and SQL containers, and workspace documentation all pass. The `ironbeam-core` publish dry run passes with crates.io access. Cargo correctly rejects `ironbeam-direct`'s publish dry run before `ironbeam-core` 0.1.0 exists in the registry; the later package dry runs are therefore gated on the protected publication sequence, not on local compilation.
