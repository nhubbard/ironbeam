//! Demonstration of automatic checkpointing for fault tolerance.
//!
//! This example shows v4 progress-marker checkpointing for a batch job.
//! The v4 runner reloads checkpoint metadata, then re-executes the graph from
//! the start on restart.
//!
//! Run with:
//! ```bash
//! cargo run -p ironbeam-compat --example v4_checkpointing_demo --features v4-checkpointing
//! ```

use anyhow::Result;
use ironbeam_v4::{ExecMode, Pipeline, Runner, Sum, from_vec};

#[cfg(feature = "v4-checkpointing")]
use ironbeam_v4::checkpoint::{CheckpointConfig, CheckpointPolicy};

#[cfg(feature = "v4-checkpointing")]
fn main() -> Result<()> {
    println!("=== Ironbeam Checkpointing Demo ===\n");

    // Create a pipeline with a large dataset to simulate long-running jobs
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
        directory: "./Ironbeam_checkpoints".into(),
        policy: CheckpointPolicy::AfterEveryBarrier,
        auto_recover: true,
        max_checkpoints: Some(5),
    };

    println!("\nCheckpoint Configuration:");
    println!("  Directory: {:?}", checkpoint_config.directory.display());
    println!("  Policy: AfterEveryBarrier");
    println!("  Progress-marker reload: enabled");
    println!("  Max checkpoints: 5");

    // Create a runner with checkpointing
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
        println!("  Key {k}: Sum = {v}");
    }

    println!("\n=== Checkpointing Summary ===");
    println!("Checkpoints were automatically created after each barrier node.");
    println!("V4 can reload its progress marker after failure, but re-executes from the start.");
    println!("Since execution completed successfully, all checkpoints were cleaned up.");

    Ok(())
}

#[cfg(not(feature = "v4-checkpointing"))]
fn main() {
    println!("This example requires the 'v4-checkpointing' feature.");
    println!(
        "Run with: cargo run -p ironbeam-compat --example v4_checkpointing_demo --features v4-checkpointing"
    );
}
