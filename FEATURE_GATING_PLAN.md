# Feature-Gating Clean-Fix Plan

## Context

The README promises consumers can disable features they don't need
(`default-features = false`, then opt back in). In practice that has never
worked: because CI only ever builds with **all features on**, a large web of
latent coupling went unnoticed. Adding the opt-in `io-msgpack` connector (the
first I/O backend excluded from `default`) surfaced it — the plain `cargo build`
broke until `helpers::msgpack` was gated.

A `cargo hack --feature-powerset` run (8192 combinations, ~1.2M log lines)
confirms the blast radius but also bounds it: the log contains **only two
distinct error codes**, so every failing permutation traces to exactly **two
root causes**.

| Error  | Count  | Root cause |
|--------|--------|------------|
| `E0432` unresolved import | 53,248 | `helpers/{avro,csv,jsonl,parquet,xml}.rs` (and their `pub mod` / `pub use` in `helpers/mod.rs`) `use crate::io::{format}` unconditionally, but `io/mod.rs` gates each `pub mod {format}` on its `io-*` feature. Disable the feature → the module vanishes → the helper fails to compile. |
| `E0308` mismatched types | 8,192 | `runner.rs:197`, inside the `#[cfg(not(feature = "checkpointing"))]` branch, calls `exec_par::<T>(chain, …)` passing `chain` by value where `exec_par` wants `&[Node]`. Only compiles today because `checkpointing` is in `default`. |

Goal: make the **entire feature powerset compile**, restore the
"disable-what-you-don't-need" promise, and add a regression guard so this can't
silently return.

## Design: always-available ABI + runtime stub

Disabling a feature should break **runtime, not compilation**. The public API
surface (functions, types, trait impls) stays present in every build; only the
*bodies* are feature-gated. The optional dependency stays out of disabled builds
because only the real body references it.

### The pattern, per I/O module (`src/io/{format}.rs`)

1. **Dependency imports** (`use rmp_serde`, `apache_avro`, `quick_xml`, `csv`,
   `parquet`, `arrow`, `serde_arrow`) → gate `#[cfg(feature = "io-{format}")]`.
2. **Private helpers that touch the dependency** (`open_*_reader`,
   `*_read_loop`, `*_count_records`, `make_*_shards`, `is_clean_eof`, …) → gate
   `#[cfg(feature = "io-{format}")]` (only compiled when the feature is on; no
   dead-code warnings when off).
3. **Public leaf functions** keep their signature unconditionally and split the
   body:
   ```rust
   pub fn read_msgpack_vec<T: DeserializeOwned>(path: impl AsRef<Path>) -> Result<Vec<T>> {
       #[cfg(feature = "io-msgpack")]
       { let path = path.as_ref(); let rdr = open_msgpack_reader(path)?; msgpack_read_loop(rdr, path) }
       #[cfg(not(feature = "io-msgpack"))]
       { let _ = path.as_ref(); anyhow::bail!("`io-msgpack` feature is not enabled") }
   }
   ```
4. **Plain-data types** (`MsgpackShards`, `AvroShards`, `*VecOps<T>`, …) are
   dependency-free → define unconditionally, no change.
5. **`VecOps` trait impls** need **no body change**: their methods call the
   now-always-present leaf functions (e.g. `read_*_range`), whose stub returns
   `Err`, so the existing `.ok()?` naturally yields `None`. A disabled format's
   source can never be constructed anyway (step 3 bails in `build_*_shards`).

**Stub convention:** `Result`-returning fns → `bail!("`io-{format}` feature is
not enabled")`. Genuinely non-`Result` fns that cannot operate → `unimplemented!`.
(In practice every leaf I/O fn returns `Result`, so `bail!` dominates;
constructors like `*VecOps::new()` are dependency-free and need no stub.)

### ABI exceptions (must stay compile-gated)

A function whose **signature** names an external type cannot live in the ABI
without its dependency. The only cases:

- `src/io/avro.rs`: `write_avro_vec_with_schema(.., schema: &Schema)`,
  `read_avro_vec_with_schema_obj(.., schema: &Schema)`,
  `build_avro_shards_with_schema_obj(.., schema: &Schema)` — `apache_avro::Schema`.

These keep their `#[cfg(feature = "io-avro")]` gate (and stay gated in `lib.rs`
re-exports). The string-schema variants (`schema: impl AsRef<str>`) are
dependency-free in signature and convert to the stub pattern normally.
parquet/xml/csv/jsonl/msgpack public signatures are **all** generic/dep-free —
no exceptions there (verified: `parquet` `RecordBatch` use is internal only).

### Behavior features stay as compile gates

`parallel-io`, `metrics`, `spilling`, `checkpointing`, `compression-*` are **not**
part of this refactor. They already gate definition *and* use consistently
(e.g. `write_*_par` and every caller share `#[cfg(all(io-*, parallel-io))]`;
compression codecs degrade at runtime via the codec registry). The powerset log
shows no errors attributable to them **except** the one `checkpointing`/`chain`
bug, fixed directly below.

## Concrete changes

1. **`src/io/{avro,csv,jsonl,parquet,xml,msgpack}.rs`** — apply the pattern
   above (gate dep imports + dep-using private helpers; split public leaf-fn
   bodies). `msgpack` is the reference implementation to copy.
2. **`src/io/mod.rs`** — remove `#[cfg(feature = "io-*")]` from each
   `pub mod {format};` (modules now compile unconditionally). Keep the
   `#[cfg_attr(docsrs, doc(cfg(...)))]` doc annotations.
3. **`src/helpers/{avro,csv,jsonl,parquet,xml,msgpack}.rs`** — these need no body
   changes once the io leaf fns are always present; remove the now-unnecessary
   per-item `#[cfg(feature = "io-*")]` on entry points that are dependency-free
   in signature (leave `parallel-io` gates intact; leave avro `Schema` items
   gated).
4. **`src/helpers/mod.rs`** — ungate `pub mod {format};` and `pub use {format}::*;`
   for all I/O formats (revert the `io-msgpack` gate added as the stop-gap), so
   the helper layer is uniform and always compiles.
5. **`src/lib.rs`** — ungate the I/O re-exports (`read_*`, `write_*`,
   `*_vec`, `*_streaming`) so `ironbeam::read_msgpack` etc. exist in every build;
   keep the avro `&Schema` re-exports and the `parallel-io`-gated `write_*_par`
   re-exports gated as they require.
6. **`src/runner.rs:197`** — change `exec_par::<T>(chain, parts, limit)` to
   `exec_par::<T>(&chain, parts, limit)` in the
   `#[cfg(not(feature = "checkpointing"))]` branch (match the checkpointing
   branch at line 174).

## Cross-feature internal-call audit

The user's specific concern: an **always-on** function silently calling a
**disabled** function's stub. Mitigation + verification:

- Grep every `crate::io::{format}` / `*_vec` / `*_range` / `*_shards` call site
  and confirm each is reachable **only** through that same format's public entry
  point (which itself bails when disabled). I/O leaf fns are consumed exclusively
  by (a) that format's helpers and (b) that format's `VecOps` impl — both behind
  the same feature's user-facing surface. There is no always-on, non-I/O caller.
- The runner reaches a format's `VecOps` only via a `Source` node, which can only
  be created by `read_{format}_streaming` — already stubbed to `bail!` when the
  feature is off. So a disabled format is unreachable at runtime past its own
  entry point; the failure is a clean `Err` at source construction.

## Tests

- Keep all existing tests (they run under `--all-features`, unchanged).
- Add a small `tests/io/feature_disabled.rs` (gated `#[cfg(not(feature = "io-msgpack"))]`,
  exercised by the `--each-feature` CI lane) asserting a representative stub
  returns `Err` with the expected "feature is not enabled" message, so the
  runtime-stub contract is covered rather than merely compiled.

## Regression guard (CI)

A full powerset is intractable in CI (8192 builds). Use **`cargo hack
--each-feature`** instead — each feature alone + none + all = N+2 builds, which
catches exactly this class of coupling.

- Add a job to `.github/workflows/ci.yml`:
  ```yaml
  feature-matrix:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@<pinned-sha>
      - uses: dtolnay/rust-toolchain@<pinned-sha>   # match repo's pinning policy
      - uses: taiki-e/install-action@<pinned-sha>
        with: { tool: cargo-hack }
      - run: cargo hack check --each-feature --no-dev-deps --keep-going
  ```
  (Follow the repo's existing SHA-pinning convention from the CI hardening work.)
- Document the heavier local sweep in CONTRIBUTING/README:
  `cargo hack check --feature-powerset --depth 2 --no-dev-deps` for pairwise, or
  full `--feature-powerset` for an exhaustive (slow) check.

## Verification

1. `cargo build` (default, no `io-msgpack`) and `cargo build --all-features` — both pass.
2. `cargo hack check --each-feature --no-dev-deps --keep-going` — **zero** failures
   (previously: every non-default lane failed).
3. Spot-check the two original signatures:
   `cargo check --no-default-features` and
   `cargo check --no-default-features --features checkpointing` — clean (E0432/E0308 gone).
4. `cargo test --all-features` — full suite green.
5. `cargo clippy --all-targets --all-features -- -D warnings -W clippy::pedantic -W clippy::nursery`
   and `RUSTDOCFLAGS="-D warnings" cargo doc --no-deps --all-features` — clean.
6. Per Rules of Engagement: `cargo llvm-cov --workspace --all-features --html --branch`
   — coverage not regressed vs. the pre-change report.

## Out of scope

- Behavior-feature (`metrics`/`spilling`/`parallel-io`/`compression-*`) API
  redesign — they are already self-consistent.
- The 5.2–5.8 Tier-5 connectors (CBOR, Arrow IPC, protobuf, parquet-rows,
  TFRecord, SQL, MongoDB) — when implemented, they adopt this same pattern.
