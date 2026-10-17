# Direct checkpoints

A checkpoint is a durable record of execution state for a Direct job. Ironbeam 5 stores coded batch or fold state and source progress at supported boundaries. Ironbeam 4 saved progress markers and reran graph work after recovery; its checkpoint files are not compatible with version 5.

## Configuration

`JobConfig` supplies a checkpoint policy when the runner starts a job. This configuration requests a checkpoint after each eligible batch:

```rust
use ironbeam::{CheckpointConfig, JobConfig};

let config = JobConfig {
    checkpoint: Some(CheckpointConfig {
        directory: "./checkpoints/orders".into(),
        interval_batches: 1,
        binary_build_id: "orders-app-5.0.0".into(),
        ..CheckpointConfig::default()
    }),
    ..JobConfig::default()
};
```

The directory contains state for one job identity. Recovery depends on compatible graph keys, source configuration, coder IDs, and `binary_build_id`. An incompatible executable or serialized representation needs a new build ID. Independent jobs require separate checkpoint directories.

## Recovery and side effects

A named output gives a resumed job a stable terminal identity. Direct validates saved state before continuing; it does not reinterpret a v4 checkpoint as v5 state. The [checkpoint recovery example](https://github.com/nhubbard/ironbeam/blob/main/examples/v5_checkpoint_recovery.rs) runs a job twice and checks the recovered result.

Checkpointing records runner state. It does not by itself make an external append operation idempotent. A retried append sink can produce duplicate database rows unless the sink or application supplies a replay-safe contract. [Input and output](io.md) describes sink behavior, and [operational migration](../migration/operations.md) compares the versions.

Direct can checkpoint a fixed-window graph with two unbounded sources feeding several stateful joins when the branches share the same source routes and merge through `Flatten` into one idempotent sink. The source cursors and open window rows are saved together. A restarted process restores the unfinished window, and a failed sink commit replays the bundle under the same identity. Graphs with different stateful source routes remain outside this execution path.
