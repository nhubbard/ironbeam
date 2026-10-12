# Getting started

Ironbeam 5 requires Rust 1.94 or newer. Until the 5.0.0 packages are published, use a checkout of the repository as a path dependency:

```toml
[dependencies]
ironbeam = { path = "/path/to/ironbeam" }
```

For a package inside this workspace, `cargo run --example explain_pipeline` runs an existing example. The following program builds an in-memory source, applies two transforms, and collects the result with the Direct runner:

```rust
use ironbeam::{Pipeline, prelude::*};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let pipeline = Pipeline::default();
    let result = pipeline
        .from_vec("numbers", vec![1_i32, 2, 3, 4])?
        .map_named("double", |value| value * 2)?
        .filter_named("large", |value| *value > 4)?
        .collect_seq()?;
    assert_eq!(result, vec![6, 8]);
    Ok(())
}
```

The strings `numbers`, `double`, and `large` are stable graph keys. Construction can fail, so each operation returns `Result`. `collect_seq` runs locally and creates a private output for this call. A pipeline intended for portable execution or durable identity should use an explicit named output:

```rust
let output = pipeline
    .from_vec("numbers", vec![1_i32, 2, 3])?
    .map_named("double", |n| n * 2)?
    .output("result")?;
```

The [runner guide](concepts/runners.md) explains how to execute that output. The [migration guide](migration/index.md) covers the API changes from Ironbeam 4.
