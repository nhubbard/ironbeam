# Testing pipelines

The optional `ironbeam-testing` crate provides test data builders and assertions. The root facade exposes it with `v5-testing`.

```toml
[dev-dependencies]
ironbeam = { path = "/path/to/ironbeam", features = ["v5-testing"] }
```

```rust
use ironbeam::v5::{Pipeline, prelude::*, testing::assert_collections_equal};
let values = Pipeline::default()
    .from_vec("test-input", vec![1_i32, 2, 3])?
    .map_named("double", |n| n * 2)?
    .collect_seq()?;
assert_collections_equal(&values, &[2, 4, 6]);
# Ok::<(), ironbeam::v5::Error>(())
```

The [testing example](https://github.com/nhubbard/ironbeam/blob/main/examples/testing_pipeline.rs) demonstrates builders, keyed assertions, and fixtures. Keep unit tests in a crate's `tests/` directory. Runner agreement tests should compare values and observable errors on both backends for graph shapes both accept.
