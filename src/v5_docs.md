# Ironbeam 5

Ironbeam builds typed data pipelines with stable graph keys. The `v5` module
combines the small core, Direct runner, common transforms, and separately
versioned I/O crates. Import the prelude for convenience methods.

```rust
use ironbeam::v5::{Pipeline, prelude::*};

# fn main() -> Result<(), Box<dyn std::error::Error>> {
let pipeline = Pipeline::default();
let values = pipeline.from_vec("numbers", vec![1_i32, 2, 3, 4])?
    .map_named("double", |value| value * 2)?
    .filter_named("large", |value| *value > 4)?;
assert_eq!(values.collect_seq()?, vec![6, 8]);
# Ok(())
# }
```

Every graph node has a stable unique key. Construction returns `Result`;
Direct collection creates a private terminal when called on a collection.
Use an explicit `.output("result")?` for a named portable or durable terminal. Enable
`v5-io-*` features only for the formats an application needs. The
`V4_TO_V5_MIGRATION.md` guide maps familiar v4 calls to their v5 equivalents.

The core, Direct runner, common transforms, and JSONL facade remain available
with `--no-default-features`. Other formats and helpers are opt-in. The
transitional `v5-preview` feature now only enables the default cloud and JSON
validation extras; it no longer gates the v5 API.

The root facade is v5-only. Published Ironbeam 4.1.1 remains available through
the separate compatibility reference crate for migration checks.
