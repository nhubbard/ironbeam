# Testing pipelines

The optional `ironbeam-testing` crate provides test data builders and assertions. The root facade exposes it with `v5-testing`. A bounded Direct test can compare element results without starting an external service.

```toml
[dev-dependencies]
ironbeam = { path = "/path/to/ironbeam", features = ["v5-testing"] }
```

```rust
use ironbeam::v5::{Pipeline, prelude::*, testing::assert_collections_equal};

fn main() -> Result<(), ironbeam::v5::Error> {
    let values = Pipeline::default()
        .from_vec("test-input", vec![1_i32, 2, 3])?
        .map_named("double", |n| n * 2)?
        .collect_seq()?;
    assert_collections_equal(&values, &[2, 4, 6]);
    Ok(())
}
```

The [testing example](https://github.com/nhubbard/ironbeam/blob/main/examples/testing_pipeline.rs) demonstrates builders, keyed assertions, and fixtures. This workspace keeps unit tests in each crate's `tests/` directory. Runner agreement tests compare values and observable errors for graph shapes accepted by both backends. For grouped or shuffled results, comparisons should account for ordering only where the pipeline defines an explicit order key.
