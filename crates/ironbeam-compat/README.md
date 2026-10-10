# ironbeam-compat

Bounded in-process data handoff between Ironbeam 4 collections and the Ironbeam 5 graph/Direct runner. The package also offers a `from_vec` helper with v4-like spelling and a prelude for v5 convenience transforms and local terminals.

Each bridge runs its source pipeline and materializes all values in memory before building the target source. It is intended for finite local data that fits in memory. It does not convert a graph, preserve laziness, transfer event-time metadata, carry checkpoints or metrics, or make a remote backend execute v4 code.

See [the migration guide](https://github.com/nhubbard/ironbeam/blob/main/V4_TO_V5_MIGRATION.md) for call-site examples and feature mapping.
