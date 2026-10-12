# Migration from Ironbeam 4

Ironbeam 5 retains the pipeline and collection model but changes how graph identity, errors, and execution are expressed. The root crate on this branch is v5-only. Ironbeam 4.1.1 remains the reference for compatibility tests; it is not embedded in the v5 facade.

## A practical sequence

1. Upgrade the dependency to the local v5 workspace candidate and import `ironbeam::{Pipeline, prelude::*}`.
2. Give every source, transform, and terminal a stable, unique key. Handle the `Result` from graph construction.
3. Replace v4 collection methods with their named v5 equivalents. Start with a bounded Direct test and compare element results.
4. Select only the file format features the application uses. Each format has its own crate.
5. Convert timestamp values from milliseconds to nanoseconds when they become event-time metadata.
6. Revisit ordered reductions and batches. Portable ordered operations need explicit order keys, and v5 batches have explicit limits rather than v4 physical partition boundaries.
7. Replace v4 progress-marker checkpoints with v5 batch/progress checkpoints. Do not reuse v4 checkpoint files.
8. If a pipeline is portable, run the local Prism fixture and review capability errors before considering hosted execution.

The [API mapping](api.md) compares common calls. [Operational changes](operations.md) cover checkpoints, I/O, and portable behavior. The full repository [migration inventory](https://github.com/nhubbard/ironbeam/blob/main/V4_TO_V5_MIGRATION.md) contains less common helpers.
