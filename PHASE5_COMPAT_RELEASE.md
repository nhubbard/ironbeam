# Phase 5: compatibility and release preparation

The `ironbeam-compat` package builds against published `ironbeam` 4.1.1 and the local v5 packages. Its `from_vec` helper and prelude support source migration; `v4_to_v5` and `v5_to_v4` exchange finite in-memory values by running the source pipeline locally. Integration tests cover empty, singleton, and multirow handoff, an equivalent v4/v5 program, convenience trait imports, and bridge error classification. The v5 facade does not depend on compatibility.

The [migration guide](V4_TO_V5_MIGRATION.md) maps public types, common transforms, terminals, file I/O, features, and behavior changes. The full v4 method inventory remains in [the Phase 0 inventory](V5_PHASE0_API_INVENTORY.md). MongoDB, SQL, TFRecord, and Arrow IPC now have opt-in bounded Direct adapters. The provider-neutral cloud contracts, utilities, and compressed JSONL object readers/writers have moved into a shared package used by both v4 and v5.

## Package order and release gate

| Order | Package | Registry dependencies |
| --- | --- | --- |
| 1 | `ironbeam-cloud` 0.1.0 | None of the new packages |
| 2 | `ironbeam-core` 0.1.0 | None of the new packages |
| 3 | `ironbeam-direct` 0.1.0 | `ironbeam-core` 0.1.0 |
| 4 | `ironbeam-transforms` 0.1.0 | `ironbeam-core` 0.1.0; Direct is a dev dependency |
| 5 | `ironbeam-io-sql` 0.1.0 | `ironbeam-core` 0.1.0; Direct is a dev dependency |
| 6 | `ironbeam-io-mongodb` 0.1.0 | `ironbeam-core` 0.1.0; Direct is a dev dependency |
| 7 | `ironbeam-io` 0.1.0 | `ironbeam-core` 0.1.0 and optional `ironbeam-cloud` 0.1.0; Direct is a dev dependency |
| 8 | `ironbeam-testing` 0.1.0 | `ironbeam-core` and `ironbeam-direct` 0.1.0 |
| 9 | `ironbeam-compat` 0.1.0 | Published `ironbeam` 4.1.1 and the v5 core, Direct, and transforms packages |

Each path dependency now has a registry version, and publishable packages have a repository URL and license. `scripts/check_v5_packages.sh` runs workspace tests and docs, then builds and verifies every distributable tarball with Cargo. Its temporary `patch.crates-io` arguments resolve unpublished v5 dependencies to this workspace. The `V5 Package Check` workflow runs this local verification on pull requests or manual dispatch. It has no registry token or publish action. The existing semantic-release workflow remains responsible for the root `ironbeam` package and must not be used to publish these nine packages as one crate.

The root package remains 4.1.1 with `v5-preview`. Its future 5.0.0 release is outside this package sequence and requires the facade/API decision, documentation, and version bump. Because its manifest now gives the optional v5 dependencies registry versions, any future root publish must follow the leaf-package sequence. No crate has been published by this phase's preparation work. When publication is authorized later, perform real crates.io publish dry runs and releases in the order above; the local patches must not be used for that release gate.

## Local evidence

`ironbeam-compat` integration tests pass against the published 4.1.1 crate. Its source-location coverage is 23/23 lines with no instrumented branches. Workspace Clippy with pedantic/nursery warnings denied passed before the cloud extraction; the focused cloud package Clippy gate passes after it. Default workspace tests and docs pass after the cloud extraction. The opt-in v5 MongoDB live test passed with Rancher Desktop before the connector split. SQL and MongoDB now build as independent packages, and minimal `ironbeam-io` has no normal SQLx, MongoDB, BSON, or testcontainers dependencies. Local Cargo package assembly and verification cover all nine v5 packages using workspace patches. The `ironbeam-core` crates.io publish dry run also passed before the release check was changed to local-only. Cargo correctly rejects `ironbeam-direct`'s crates.io publish dry run before `ironbeam-core` 0.1.0 exists in the registry; that remote release gate remains pending by choice.
