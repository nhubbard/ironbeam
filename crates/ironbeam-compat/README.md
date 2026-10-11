# ironbeam-compat

Bounded in-process data handoff between Ironbeam 4 collections and the Ironbeam 5 graph/Direct runner. The package also offers a `from_vec` helper with v4-like spelling and a prelude for v5 convenience transforms and local terminals.

Each bridge runs its source pipeline and materializes all values in memory before building the target source. It is intended for finite local data that fits in memory. It does not convert a graph, preserve laziness, transfer event-time metadata, carry checkpoints or metrics, or make a remote backend execute v4 code.

The `v4_baseline`, `differential`, and TFRecord wire tests use published Ironbeam 4.1.1
as their reference. They live in this leaf crate so the root package can
eventually remove its checked-in v4 implementation without losing parity
evidence.

See [the migration guide](https://github.com/nhubbard/ironbeam/blob/main/V4_TO_V5_MIGRATION.md) for call-site examples and feature mapping.
