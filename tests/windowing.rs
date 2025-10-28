// tests/windowing.rs
use rustflow::window::{Timestamped, Window};
use rustflow::*;

fn mk_ts(i: u64) -> u64 {
    i
} // convenience: using raw millis in tests

#[test]
fn tumbling_non_keyed_counts() -> anyhow::Result<()> {
    let p = Pipeline::default();

    // Create events at timestamps 0..30 step 5; window size 10 → windows [0,10), [10,20), [20,30)
    let events: Vec<Timestamped<&'static str>> = (0..30)
        .step_by(5)
        .map(|t| Timestamped::new(mk_ts(t), "x"))
        .collect();

    let pc = from_vec(&p, events)
        .group_by_window(10, 0) // 10ms windows, zero offset
        .map_values(|vs| vs.len() as u64)
        .collect_par_sorted_by_key(None, None)?; // sort by Window (Ord)

    // Expect: each 10ms bucket gets 2 events at t={0,5}, {10,15}, {20,25}
    let expected = vec![
        (Window::new(0, 10), 2u64),
        (Window::new(10, 20), 2u64),
        (Window::new(20, 30), 2u64),
    ];

    assert_eq!(pc, expected);
    Ok(())
}

#[test]
fn tumbling_keyed_counts() -> anyhow::Result<()> {
    let p = Pipeline::default();

    // Two keys: "a" and "b", over two windows; arrange unbalanced counts.
    let rows: Vec<(String, Timestamped<u8>)> = vec![
        ("a".into(), Timestamped::new(mk_ts(1), 1)),
        ("a".into(), Timestamped::new(mk_ts(3), 2)),
        ("b".into(), Timestamped::new(mk_ts(7), 1)),
        ("a".into(), Timestamped::new(mk_ts(12), 1)),
        ("b".into(), Timestamped::new(mk_ts(14), 1)),
    ];

    let out = from_vec(&p, rows)
        .key_by_window(10, 0) // ((K, Window), V)
        .group_by_key() // → ((K, Window), Vec<V>)
        .map_values(|vs| vs.len() as u32)
        .collect_par_sorted_by_key(None, None)?;

    // Expected bucket counts:
    // window [0,10):  "a" has 2, "b" has 1
    // window [10,20): "a" has 1, "b" has 1
    let w0 = Window::new(0, 10);
    let w1 = Window::new(10, 20);
    let expected = vec![
        (("a".to_string(), w0), 2u32),
        (("a".to_string(), w1), 1u32),
        (("b".to_string(), w0), 1u32),
        (("b".to_string(), w1), 1u32),
    ];

    assert_eq!(out, expected);
    Ok(())
}

#[test]
fn attach_timestamps_then_window() -> anyhow::Result<()> {
    let p = Pipeline::default();

    // Start from raw payloads + separate timestamp field
    #[derive(Clone, serde::Serialize, serde::Deserialize)]
    struct Row {
        ts: u64,
        val: u32,
    }

    let rows: Vec<Row> = vec![
        Row { ts: 5, val: 1 },
        Row { ts: 8, val: 1 },
        Row { ts: 11, val: 1 },
        Row { ts: 15, val: 1 },
    ];

    let out = from_vec(&p, rows)
        .attach_timestamps(|r| r.ts) // Timestamped<Row>
        .key_by_window(10, 0) // (Window, Row)
        .group_by_key()
        .map_values(|vs| vs.len() as u8) // counts per window
        .collect_par_sorted_by_key(None, None)?;

    let expected = vec![
        (Window::new(0, 10), 2u8),  // ts=5,8
        (Window::new(10, 20), 2u8), // ts=11,15
    ];
    assert_eq!(out, expected);
    Ok(())
}
