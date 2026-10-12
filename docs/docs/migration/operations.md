# Operational migration

## Checkpoints

Ironbeam 4 policies saved progress markers and reran graph work during recovery. Version 5 Direct checkpoints coded batch or fold state and source progress at supported boundaries. Set `JobConfig::checkpoint` to `Some(CheckpointConfig)`; keep graph keys, coder IDs, source identity, and binary build ID stable for a compatible restart. A v4 checkpoint cannot be loaded by v5.

```rust
use ironbeam::{CheckpointConfig, JobConfig};
let config = JobConfig {
    checkpoint: Some(CheckpointConfig {
        directory: "./checkpoints/job-a".into(),
        interval_batches: 1,
        binary_build_id: "my-app-5.0.0".into(),
        ..CheckpointConfig::default()
    }),
    ..JobConfig::default()
};
```

The complete [checkpoint recovery example](https://github.com/nhubbard/ironbeam/blob/main/examples/v5_checkpoint_recovery.rs) runs a job twice and checks that the second run recovers its result.

## Batches and ordering

V4 custom transforms could see physical partition boundaries. V5 batch transforms use an explicit batch size and semantic version. Results should be compared at the element level unless the application defines an explicit order key. Portable ordered folds require an explicit order key because a shuffle does not preserve encounter order.

## File formats and side effects

Use the relevant `v5-io-*` feature or individual format crate. The old `io-*` feature names select matching v5 crates, but they do not restore v4 method signatures. Direct-only deferred append sinks do not provide automatic replay safety for external databases. Where a portable final result is required, use the supported fragment and publication protocol for that format.

## Portability boundary

The local Prism proof checks supported graph shapes and a separate worker. It does not establish hosted Dataflow credentials, deployment, or remote object-store guarantees. Keep a Direct result test and a portable fixture for any pipeline that must run on both.
