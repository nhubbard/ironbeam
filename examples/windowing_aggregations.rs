//! Time-based windowing and aggregations example.
//!
//! Demonstrates:
//! - Tumbling windows for time-series data
//! - Per-window aggregations
//! - Keyed windowing (per-user, per-window)
//! - Statistical combiners on windowed data
//!
//! Run with: `cargo run --example windowing_aggregations`

use anyhow::Result;
use rustflow::{Pipeline, from_vec, Timestamped, OrdF64, Min, Max, AverageF64};

#[allow(clippy::cast_precision_loss)]
fn main() -> Result<()> {
    println!("⏰ Windowing and Aggregations Example\n");

    let pipeline = Pipeline::default();

    // Simulate streaming sensor data with timestamps
    let sensor_data = [
        ("sensor_1", 1000, 22.5), // Window 1
        ("sensor_1", 2000, 23.0),
        ("sensor_2", 2500, 21.8),
        ("sensor_1", 11000, 24.2), // Window 2
        ("sensor_2", 12000, 22.1),
        ("sensor_1", 21000, 23.8), // Window 3
        ("sensor_2", 21500, 22.5),
        ("sensor_2", 22000, 23.2),
    ];

    // Create timestamped events
    let events = from_vec(
        &pipeline,
        sensor_data
            .iter()
            .map(|(sensor, ts, temp)| (sensor.to_string(), Timestamped::new(*ts, *temp)))
            .collect::<Vec<_>>(),
    );

    // =============================================================================
    // EXAMPLE 1: Global windowing (all sensors combined)
    // =============================================================================
    println!("📊 Example 1: Global 10-second tumbling windows");

    let global_windows = events
        .clone()
        .map(|(_, timestamped)| timestamped.clone())
        .group_by_window(10_000, 0);

    let window_stats = global_windows.map(|(window, temps)| {
        let count = temps.len();
        let sum: f64 = temps.iter().copied().sum();
        let avg = sum / count as f64;
        let max = temps.iter().copied().fold(f64::NEG_INFINITY, f64::max);
        let min = temps.iter().copied().fold(f64::INFINITY, f64::min);

        (*window, count, avg, min, max)
    });

    let mut results = window_stats.collect_seq()?;
    results.sort_by_key(|(window, _, _, _, _)| *window);
    println!("\nWindow | Count | Avg Temp | Min | Max");
    println!("{:-<50}", "");
    for (win, count, avg, min, max) in results {
        println!(
            "{win:?} | {count} | {avg:.2}°C | {min:.2}°C | {max:.2}°C"
        );
    }

    // =============================================================================
    // EXAMPLE 2: Per-sensor windowing
    // =============================================================================
    println!("\n📊 Example 2: Per-sensor 10-second windows");

    let per_sensor_windows = events.clone().group_by_key_and_window(10_000, 0);

    let sensor_window_avgs = per_sensor_windows.map(|((sensor, window), temps)| {
        let avg: f64 = temps.iter().copied().sum::<f64>() / temps.len() as f64;
        (sensor.clone(), *window, avg)
    });

    let mut sensor_results = sensor_window_avgs.collect_seq()?;
    sensor_results.sort_by_key(|(sensor, window, _)| (sensor.clone(), *window));
    println!("\nSensor | Window | Avg Temp");
    println!("{:-<45}", "");
    for (sensor, win, avg) in sensor_results {
        println!("{sensor} | {win:?} | {avg:.2}°C");
    }

    // =============================================================================
    // EXAMPLE 3: Using combiners on windowed data
    // =============================================================================
    println!("\n📊 Example 3: Statistical aggregations per sensor");

    let sensor_temps = events.map_values(|ts| ts.value);

    // Min, Max, Average per sensor
    let min_temps = sensor_temps
        .clone()
        .map_values(|&v| OrdF64(v))
        .combine_values(Min::<OrdF64>::new());
    let max_temps = sensor_temps
        .clone()
        .map_values(|&v| OrdF64(v))
        .combine_values(Max::<OrdF64>::new());
    let avg_temps = sensor_temps.combine_values(AverageF64);

    println!("\nSensor Statistics:");
    println!("{:-<40}", "");

    let mins = min_temps.collect_seq_sorted()?;
    let maxs = max_temps.collect_seq_sorted()?;
    let mut avgs = avg_temps.collect_seq()?;
    avgs.sort_by_key(|(sensor, _)| sensor.clone());

    for i in 0..mins.len() {
        println!(
            "{}: Min={:.2}°C, Max={:.2}°C, Avg={:.2}°C",
            mins[i].0, mins[i].1.0, maxs[i].1.0, avgs[i].1
        );
    }

    println!("\n✅ Windowing Complete!");
    println!("\nKey Concepts:");
    println!("  • Tumbling windows: Non-overlapping time buckets");
    println!("  • group_by_window: Window unkeyed streams");
    println!("  • group_by_key_and_window: Window per-key streams");
    println!("  • Combiners work on windowed data");

    Ok(())
}
