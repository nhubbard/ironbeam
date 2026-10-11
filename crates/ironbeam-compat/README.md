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
The v4 core collection, node identity, custom extension, and utility suites
also run against published 4.1.1 here; they contain 38 reference cases.
The batching, enhanced filtering, sampling, tee, and wait-on suites add 114
published-v4 cases without requiring optional format or service features.
The dead-letter, logging, named-operation, multi-output, validation, and
testing-utility suites add 122 more default compatibility cases.
CoGroupByKey, graph explanation, ordering, and planner references add 113
more cases against published v4.
Pipeline and runner references add 54 default cases. Enabling
`v4-metrics,v4-checkpointing` runs 67 cases, including the optional branches.
The mock I/O target runs its JSONL and temporary-path checks by default; its
CSV check uses `cargo test -p ironbeam-compat --features v4-csv --test
v4_testing_mock_io`.
The v4 cloud-I/O suites cover its provider traits, fake services, utilities,
and readers. Compressed reader checks use `v4-gzip,v4-zstd` in a separate CI
target, leaving codec dependencies out of default compatibility builds.
The v4 file-format reference suites also live here. Run
`bash scripts/check_v4_format_reference.sh` from the repository root to test
each format with its own features; the script accepts `IRONBEAM_CARGO` for a
Cargo wrapper. The 21 SQLite reference tests run with `v4-sql` and need no
database service. PostgreSQL, MySQL, and MongoDB reference tests run with
`v4-service-tests` and their connector features in the manual Docker CI job.
These published-v4 checks preserve reference behavior while
the separate v5 format crates carry their implementation tests.

See [the migration guide](https://github.com/nhubbard/ironbeam/blob/main/V4_TO_V5_MIGRATION.md) for call-site examples and feature mapping.
