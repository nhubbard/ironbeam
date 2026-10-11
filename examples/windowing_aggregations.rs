//! Time-based windowing and aggregations with the v5 graph.
//!
//! Run with `cargo run --example windowing_aggregations`.

use anyhow::Result;
use ironbeam::v5::{OrdF64, Pipeline, prelude::*};

const WINDOW_NS: i64 = 10_000_000_000;

#[allow(clippy::cast_precision_loss, clippy::too_many_lines)]
fn main() -> Result<()> {
    println!("⏰ Windowing and Aggregations Example\n");
    let sensor_data = [
        ("sensor_1", 1000_i64, 22.5_f64),
        ("sensor_1", 2000, 23.0),
        ("sensor_2", 2500, 21.8),
        ("sensor_1", 11000, 24.2),
        ("sensor_2", 12000, 22.1),
        ("sensor_1", 21000, 23.8),
        ("sensor_2", 21500, 22.5),
        ("sensor_2", 22000, 23.2),
    ];
    // v4 Timestamped used milliseconds; v5 event-time metadata uses nanoseconds.
    let pipeline = Pipeline::default();
    let events = pipeline.from_vec(
        "sensor-events",
        sensor_data
            .into_iter()
            .map(|(sensor, timestamp_ms, temp)| {
                (sensor.to_string(), (timestamp_ms * 1_000_000, temp))
            })
            .collect(),
    )?;

    println!("📊 Example 1: Global 10-second tumbling windows");
    let all_temps = events
        .map_named("global-timestamp-pairs", |(_, timestamp_and_temp)| {
            timestamp_and_temp
        })?
        .timestamps_from_pairs_named("global-event-time")?;
    let global_windows = all_temps.group_by_window_named("global-groups", WINDOW_NS, 0)?;
    let mut window_stats = global_windows
        .map_named("global-stats", |(window, temps)| {
            let count = u64::try_from(temps.len()).unwrap_or(u64::MAX);
            let sum: f64 = temps.iter().sum();
            let min = temps.iter().copied().fold(f64::INFINITY, f64::min);
            let max = temps.iter().copied().fold(f64::NEG_INFINITY, f64::max);
            (window, count, sum / count as f64, min, max)
        })?
        .collect_seq()?;
    window_stats.sort_by_key(|(window, _, _, _, _)| window.start_ns);
    assert_eq!(
        window_stats
            .iter()
            .map(|(_, count, _, _, _)| *count)
            .collect::<Vec<_>>(),
        [3, 2, 3]
    );
    println!("Window | Count | Avg Temp | Min | Max");
    for (window, count, average, min, max) in &window_stats {
        println!(
            "{}..{} | {count} | {average:.2}°C | {min:.2}°C | {max:.2}°C",
            window.start_ns, window.end_ns
        );
    }

    println!("\n📊 Example 2: Per-sensor 10-second windows");
    let keyed_temps = events
        .map_named("keyed-timestamp-pairs", |(sensor, (timestamp, temp))| {
            (timestamp, (sensor, temp))
        })?
        .timestamps_from_pairs_named("keyed-event-time")?;
    let mut sensor_windows = keyed_temps
        .group_by_key_and_window_named("sensor-groups", WINDOW_NS, 0)?
        .map_named("sensor-window-average", |((sensor, window), temps)| {
            let average = temps.iter().sum::<f64>() / temps.len() as f64;
            (sensor, window, average)
        })?
        .collect_seq()?;
    sensor_windows.sort_by_key(|(sensor, window, _)| (sensor.clone(), window.start_ns));
    assert_eq!(sensor_windows.len(), 6);
    for (sensor, window, average) in &sensor_windows {
        println!(
            "{sensor} | {}..{} | {average:.2}°C",
            window.start_ns, window.end_ns
        );
    }

    println!("\n📊 Example 3: Statistical aggregations per sensor");
    let mins = keyed_temps
        .map_values_named("ordered-min-input", OrdF64)?
        .min_per_key_named("min-group", "min-result")?
        .collect_seq()?;
    let maxs = keyed_temps
        .map_values_named("ordered-max-input", OrdF64)?
        .max_per_key_named("max-group", "max-result")?
        .collect_seq()?;
    let avgs = keyed_temps
        .average_per_key_named("average-group", "average-result")?
        .collect_seq()?;
    let min_by_sensor: std::collections::BTreeMap<_, _> = mins.into_iter().collect();
    let max_by_sensor: std::collections::BTreeMap<_, _> = maxs.into_iter().collect();
    let avg_by_sensor: std::collections::BTreeMap<_, _> = avgs.into_iter().collect();
    assert_eq!(min_by_sensor.len(), 2);
    assert_eq!(min_by_sensor["sensor_1"].0, 22.5);
    assert_eq!(max_by_sensor["sensor_1"].0, 24.2);
    assert!((avg_by_sensor["sensor_1"] - 23.375).abs() < 1e-9);
    assert_eq!(min_by_sensor["sensor_2"].0, 21.8);
    assert_eq!(max_by_sensor["sensor_2"].0, 23.2);
    assert!((avg_by_sensor["sensor_2"] - 22.4).abs() < 1e-9);
    for (sensor, average) in &avg_by_sensor {
        println!(
            "{sensor}: Min={:.2}°C, Max={:.2}°C, Avg={average:.2}°C",
            min_by_sensor[sensor].0, max_by_sensor[sensor].0
        );
    }
    println!("\n✅ Windowing Complete!");
    Ok(())
}
