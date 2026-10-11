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
    .filter_named("large", |value| *value > 4)?
    .output("result")?;
assert_eq!(values.collect_seq()?, vec![6, 8]);
# Ok(())
# }
```

Every source, transform, and terminal has a stable unique key. Construction
returns `Result`, and execution starts at an explicit terminal. Enable
`v5-io-*` features only for the formats an application needs. The
`V4_TO_V5_MIGRATION.md` guide maps familiar v4 calls to their v5 equivalents.

This branch still enables the old root exports by default through
`legacy-v4`. A v5-only facade build uses `--no-default-features --features
v5-preview`; the final 5.0.0 cutover will make v5 the root default and remove
the legacy implementation.
