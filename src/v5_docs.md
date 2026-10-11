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

This branch defaults to the v5 root facade. The old root exports require
`legacy-v4`; the final 5.0.0 cutover will remove that implementation.
