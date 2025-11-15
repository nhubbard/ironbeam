//! Advanced checkpointing demo showing different policies and recovery scenarios.
//!
//! This example demonstrates:
//! - Different checkpoint policies (`AfterEveryBarrier`, `EveryNNodes`, `TimeInterval`)
//! - Checkpoint recovery on failure
//! - Checkpoint management and cleanup
//!
//! Run with:
//! ```bash
//! cargo run --example checkpoint_recovery_demo --features checkpointing
//! ```

use anyhow::Result;
use ironbeam::{AverageF64, Count, ExecMode, Pipeline, Runner, Sum, from_vec};
use std::env::args;

#[cfg(feature = "checkpointing")]
use ironbeam::checkpoint::{CheckpointConfig, CheckpointPolicy};

#[cfg(feature = "checkpointing")]
fn run_with_time_based_checkpoints() -> Result<()> {
    println!("=== Time-Based Checkpointing ===\n");

    let p = Pipeline::default();
    let data = from_vec(&p, (0..50_000).collect::<Vec<i32>>());

    let result_collection = data
        .map(|x: &i32| x * 2)
        .filter(|x: &i32| x % 5 == 0)
        .key_by(|x: &i32| x % 50)
        .combine_values(Count);

    let checkpoint_config = CheckpointConfig {
        enabled: true,
        directory: "./checkpoints_time_based".into(),
        policy: CheckpointPolicy::TimeInterval(2), // Every 2 seconds
        auto_recover: true,
        max_checkpoints: Some(3),
    };

    println!("Policy: Checkpoint every 2 seconds");
    println!("Max checkpoints: 3 (oldest will be deleted)\n");

    let runner = Runner {
        mode: ExecMode::Sequential,
        checkpoint_config: Some(checkpoint_config),
        ..Default::default()
    };

    let result = runner.run_collect::<(i32, u64)>(&p, result_collection.node_id())?;

    println!("\nCompleted! Results: {} groups", result.len());
    Ok(())
}

#[cfg(feature = "checkpointing")]
fn run_with_node_based_checkpoints() -> Result<()> {
    println!("=== Node-Based Checkpointing ===\n");

    let p = Pipeline::default();
    let data = from_vec(&p, (0..30_000).collect::<Vec<i32>>());

    let result_collection = data
        .map(|x: &i32| x + 1)
        .map(|x: &i32| x * 3)
        .filter(|x: &i32| *x > 100)
        .key_by(|x: &i32| x % 20)
        .map_values(|x: &i32| f64::from(*x))
        .combine_values(AverageF64);

    let checkpoint_config = CheckpointConfig {
        enabled: true,
        directory: "./checkpoints_node_based".into(),
        policy: CheckpointPolicy::EveryNNodes(2), // Every 2 nodes
        auto_recover: true,
        max_checkpoints: Some(5),
    };

    println!("Policy: Checkpoint every 2 nodes");
    println!("This creates more frequent checkpoints for fine-grained recovery\n");

    let runner = Runner {
        mode: ExecMode::Sequential,
        checkpoint_config: Some(checkpoint_config),
        ..Default::default()
    };

    let result = runner.run_collect::<(i32, f64)>(&p, result_collection.node_id())?;

    println!("\nCompleted! Results: {} groups", result.len());
    println!("Sample averages:");
    for (k, v) in result.iter().take(5) {
        println!("  Key {k}: Average = {v:.2}");
    }
    Ok(())
}

#[cfg(feature = "checkpointing")]
fn run_with_hybrid_checkpoints() -> Result<()> {
    println!("=== Hybrid Checkpointing ===\n");

    let p = Pipeline::default();
    let data = from_vec(&p, (0..40_000).collect::<Vec<i32>>());

    let result_collection = data
        .map(|x: &i32| x % 1000)
        .key_by(|x: &i32| *x)
        .map_values(|_: &i32| 1u64)
        .combine_values(Sum::<u64>::default())
        .map(|(k, v): &(i32, u64)| (k / 10, *v))
        .key_by(|(k, _): &(i32, u64)| *k)
        .map_values(|(_, v): &(i32, u64)| *v)
        .combine_values(Sum::<u64>::default());

    let checkpoint_config = CheckpointConfig {
        enabled: true,
        directory: "./checkpoints_hybrid".into(),
        policy: CheckpointPolicy::Hybrid {
            barriers: true,
            interval_secs: 3,
        },
        auto_recover: true,
        max_checkpoints: Some(10),
    };

    println!("Policy: Checkpoint after barriers OR every 3 seconds");
    println!("This provides the most aggressive checkpointing\n");

    let runner = Runner {
        mode: ExecMode::Sequential,
        checkpoint_config: Some(checkpoint_config),
        ..Default::default()
    };

    let result = runner.run_collect::<(i32, u64)>(&p, result_collection.node_id())?;

    println!("\nCompleted! Results: {} groups", result.len());
    Ok(())
}

#[cfg(feature = "checkpointing")]
fn main() -> Result<()> {
    let args: Vec<String> = args().collect();

    if args.len() > 1 {
        match args[1].as_str() {
            "time" => run_with_time_based_checkpoints()?,
            "node" => run_with_node_based_checkpoints()?,
            "hybrid" => run_with_hybrid_checkpoints()?,
            _ => {
                println!("Usage: {} [time|node|hybrid]", args[0]);
                println!("\nAvailable modes:");
                println!("  time   - Time-based checkpointing (every N seconds)");
                println!("  node   - Node-based checkpointing (every N nodes)");
                println!("  hybrid - Hybrid checkpointing (barriers + time)");
                return Ok(());
            }
        }
    } else {
        println!("Running all checkpoint policy demos...\n");

        run_with_time_based_checkpoints()?;
        println!("\n{}\n", "=".repeat(60));

        run_with_node_based_checkpoints()?;
        println!("\n{}\n", "=".repeat(60));

        run_with_hybrid_checkpoints()?;
    }

    println!("\n=== Summary ===");
    println!("All checkpoint demos completed successfully!");
    println!("\nCheckpoint directories created:");
    println!("  - ./checkpoints_time_based/");
    println!("  - ./checkpoints_node_based/");
    println!("  - ./checkpoints_hybrid/");
    println!("\nCheckpoints are automatically cleaned up on successful completion.");
    println!("To test recovery, interrupt a run (Ctrl+C) and restart it.");

    Ok(())
}

#[cfg(not(feature = "checkpointing"))]
fn main() {
    println!("This example requires the 'checkpointing' feature.");
    println!("Run with: cargo run --example checkpoint_recovery_demo --features checkpointing");
}
