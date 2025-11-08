//! Demonstration of automatic checkpointing for fault tolerance.
//!
//! This example shows how to use rustflow's checkpointing feature to make
//! long-running batch jobs resilient to failures. Checkpoints are automatically
//! created at configurable intervals, and the pipeline can resume from the last
//! checkpoint on restart.
//!
//! Run with:
//! ```bash
//! cargo run --example checkpointing_demo --features checkpointing
//! ```

use rustflow::*;

#[cfg(feature = "checkpointing")]
use rustflow::checkpoint::{CheckpointConfig, CheckpointPolicy};

#[cfg(feature = "checkpointing")]
fn main() -> anyhow::Result<()> {
    println!("=== Rustflow Checkpointing Demo ===\n");

    // Create a pipeline with a large dataset to simulate long-running job
    let p = Pipeline::default();

    println!("Building pipeline with 100,000 records...");
    let data = from_vec(&p, (0..100_000).collect::<Vec<i32>>());

    // Perform some transformations that would take time in a real scenario
    let transformed = data
        .map(|x: &i32| x * 2)
        .filter(|x: &i32| x % 3 == 0)
        .key_by(|x: &i32| x % 100) // Group by modulo 100
        .map_values(|x: &i32| x / 2)
        .combine_values(Sum::<i32>::default()); // Sum values per key

    // Configure checkpointing
    let checkpoint_config = CheckpointConfig {
        enabled: true,
        directory: "./rustflow_checkpoints".into(),
        policy: CheckpointPolicy::AfterEveryBarrier,
        auto_recover: true,
        max_checkpoints: Some(5),
    };

    println!("\nCheckpoint Configuration:");
    println!("  Directory: {:?}", checkpoint_config.directory);
    println!("  Policy: AfterEveryBarrier");
    println!("  Auto-recovery: enabled");
    println!("  Max checkpoints: 5");

    // Create runner with checkpointing
    let runner = Runner {
        mode: ExecMode::Sequential,
        checkpoint_config: Some(checkpoint_config),
        ..Default::default()
    };

    println!("\nExecuting pipeline with checkpointing...");
    println!("(Watch for checkpoint messages during execution)\n");

    // Execute the pipeline
    let result = runner.run_collect::<(i32, i32)>(&p, transformed.node_id())?;

    println!("\n=== Execution Complete ===");
    println!("Total results: {}", result.len());
    println!("Sample results (first 10):");
    for (k, v) in result.iter().take(10) {
        println!("  Key {}: Sum = {}", k, v);
    }

    println!("\n=== Checkpointing Summary ===");
    println!("Checkpoints were automatically created after each barrier node.");
    println!("If this pipeline had failed, it could resume from the last checkpoint.");
    println!("Since execution completed successfully, all checkpoints were cleaned up.");

    Ok(())
}

#[cfg(not(feature = "checkpointing"))]
fn main() {
    println!("This example requires the 'checkpointing' feature.");
    println!("Run with: cargo run --example checkpointing_demo --features checkpointing");
}
