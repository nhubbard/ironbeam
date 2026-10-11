# ironbeam-compat

Bounded in-process data handoff between Ironbeam 4 collections and the Ironbeam 5 graph/Direct runner. The package also offers a `from_vec` helper with v4-like spelling and a prelude for v5 convenience transforms and local terminals.

Each bridge runs its source pipeline and materializes all values in memory before building the target source. It is intended for finite local data that fits in memory. It does not convert a graph, preserve laziness, transfer event-time metadata, carry checkpoints or metrics, or make a remote backend execute v4 code.

The `v4_baseline`, `differential`, TFRecord wire, and v4 helper tests use published Ironbeam 4.1.1
as their reference. They live in this leaf crate so the root package can
eventually remove its checked-in v4 implementation without losing parity
evidence.
The `v4_benchmark_corpus` example measures published v4 with the same workload
names as the root v5 benchmark example.
The basic-helper reference suite retains 49 v4 cases for global and keyed
summaries, collection materializers, and top/bottom selection.
The remaining helper reference families also live here: batching, cloud,
distinct, joins, regex, timestamp reification, reshuffle, side input,
statistics, value operations, windowed combine, and windowing. Parquet's
15 cases run with `cargo test -p ironbeam-compat --features v4-parquet --test
v4_helper_parquet`, keeping its heavy dependency out of the default build.
The v4 combiner and testing suites live here as separate reference targets.
The mock I/O target runs its JSONL and temporary-path checks by default; its
CSV check uses `cargo test -p ironbeam-compat --features v4-csv --test
v4_testing_mock_io`.

See [the migration guide](https://github.com/nhubbard/ironbeam/blob/main/V4_TO_V5_MIGRATION.md) for call-site examples and feature mapping.
