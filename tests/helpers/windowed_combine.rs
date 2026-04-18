use anyhow::Result;
use ironbeam::*;

// ───────────────────────── helpers ───────────────────────────

fn w(start: u64, end: u64) -> Window {
    Window::new(start, end)
}

// ──────────────────── combine_per_window (generic) ────────────────────────

#[test]
fn combine_per_window_sum_basic() -> Result<()> {
    let p = Pipeline::default();
    let events = from_vec(
        &p,
        vec![
            Timestamped::new(1_000u64, 10u32),
            Timestamped::new(5_000u64, 20u32),
            Timestamped::new(12_000u64, 5u32),
        ],
    );

    let mut result = events
        .combine_per_window(10_000, 0, Sum::<u32>::new())
        .collect_seq()?;
    result.sort_by_key(|(win, _)| win.start);

    assert_eq!(
        result,
        vec![(w(0, 10_000), 30u32), (w(10_000, 20_000), 5u32)]
    );
    Ok(())
}

#[test]
fn combine_per_window_single_window() -> Result<()> {
    let p = Pipeline::default();
    let events = from_vec(
        &p,
        vec![
            Timestamped::new(0u64, 1u32),
            Timestamped::new(4_999u64, 2u32),
        ],
    );

    let result = events
        .combine_per_window(5_000, 0, Sum::<u32>::new())
        .collect_seq()?;

    assert_eq!(result.len(), 1);
    assert_eq!(result[0], (w(0, 5_000), 3u32));
    Ok(())
}

#[test]
fn combine_per_window_three_windows() -> Result<()> {
    let p = Pipeline::default();
    let events = from_vec(
        &p,
        vec![
            Timestamped::new(1_000u64, 1u32),
            Timestamped::new(11_000u64, 2u32),
            Timestamped::new(21_000u64, 3u32),
        ],
    );

    let mut result = events
        .combine_per_window(10_000, 0, Sum::<u32>::new())
        .collect_seq()?;
    result.sort_by_key(|(win, _)| win.start);

    assert_eq!(
        result,
        vec![
            (w(0, 10_000), 1u32),
            (w(10_000, 20_000), 2u32),
            (w(20_000, 30_000), 3u32),
        ]
    );
    Ok(())
}

// ──────────────────────── sum_per_window ─────────────────────────────────

#[test]
fn sum_per_window_basic() -> Result<()> {
    let p = Pipeline::default();
    let events = from_vec(
        &p,
        vec![
            Timestamped::new(1_000u64, 1u64),
            Timestamped::new(5_000u64, 2u64),
            Timestamped::new(11_000u64, 4u64),
        ],
    );

    let mut result = events.sum_per_window(10_000, 0).collect_seq()?;
    result.sort_by_key(|(win, _)| win.start);

    assert_eq!(
        result,
        vec![(w(0, 10_000), 3u64), (w(10_000, 20_000), 4u64)]
    );
    Ok(())
}

#[test]
fn sum_per_window_single_element_each() -> Result<()> {
    let p = Pipeline::default();
    let events = from_vec(
        &p,
        vec![
            Timestamped::new(500u64, 7u32),
            Timestamped::new(1_500u64, 3u32),
        ],
    );

    let mut result = events.sum_per_window(1_000, 0).collect_seq()?;
    result.sort_by_key(|(win, _)| win.start);

    assert_eq!(result, vec![(w(0, 1_000), 7u32), (w(1_000, 2_000), 3u32)]);
    Ok(())
}

#[test]
fn sum_per_window_many_in_one_window() -> Result<()> {
    let p = Pipeline::default();
    let events = from_vec(
        &p,
        (0..5u32)
            .map(|i| Timestamped::new(u64::from(i) * 100, i))
            .collect::<Vec<_>>(),
    );

    let result = events.sum_per_window(10_000, 0).collect_seq()?;
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].1, 1 + 2 + 3 + 4);
    Ok(())
}

// ────────────────────────── count_per_window ─────────────────────────────

#[test]
fn count_per_window_basic() -> Result<()> {
    let p = Pipeline::default();
    let events = from_vec(
        &p,
        vec![
            Timestamped::new(1_000u64, "a"),
            Timestamped::new(5_000u64, "b"),
            Timestamped::new(11_000u64, "c"),
            Timestamped::new(14_000u64, "d"),
        ],
    );

    let mut result = events.count_per_window(10_000, 0).collect_seq()?;
    result.sort_by_key(|(win, _)| win.start);

    assert_eq!(
        result,
        vec![(w(0, 10_000), 2u64), (w(10_000, 20_000), 2u64)]
    );
    Ok(())
}

#[test]
fn count_per_window_single_window() -> Result<()> {
    let p = Pipeline::default();
    let events = from_vec(
        &p,
        vec![
            Timestamped::new(100u64, "x"),
            Timestamped::new(200u64, "y"),
            Timestamped::new(300u64, "z"),
        ],
    );

    let result = events.count_per_window(1_000, 0).collect_seq()?;
    assert_eq!(result, vec![(w(0, 1_000), 3u64)]);
    Ok(())
}

#[test]
fn count_per_window_one_per_window() -> Result<()> {
    let p = Pipeline::default();
    let events = from_vec(
        &p,
        vec![
            Timestamped::new(0u64, 1i32),
            Timestamped::new(10_000u64, 2i32),
        ],
    );

    let mut result = events.count_per_window(10_000, 0).collect_seq()?;
    result.sort_by_key(|(win, _)| win.start);

    assert_eq!(
        result,
        vec![(w(0, 10_000), 1u64), (w(10_000, 20_000), 1u64)]
    );
    Ok(())
}

// ─────────────────────────── min_per_window ──────────────────────────────

#[test]
fn min_per_window_basic() -> Result<()> {
    let p = Pipeline::default();
    let events = from_vec(
        &p,
        vec![
            Timestamped::new(1_000u64, 5u32),
            Timestamped::new(5_000u64, 2u32),
            Timestamped::new(11_000u64, 8u32),
        ],
    );

    let mut result = events.min_per_window(10_000, 0).collect_seq()?;
    result.sort_by_key(|(win, _)| win.start);

    assert_eq!(
        result,
        vec![(w(0, 10_000), 2u32), (w(10_000, 20_000), 8u32)]
    );
    Ok(())
}

#[test]
fn min_per_window_single_per_window() -> Result<()> {
    let p = Pipeline::default();
    let events = from_vec(
        &p,
        vec![
            Timestamped::new(500u64, 42u32),
            Timestamped::new(1_500u64, 7u32),
        ],
    );

    let mut result = events.min_per_window(1_000, 0).collect_seq()?;
    result.sort_by_key(|(win, _)| win.start);

    assert_eq!(result, vec![(w(0, 1_000), 42u32), (w(1_000, 2_000), 7u32)]);
    Ok(())
}

#[test]
fn min_per_window_all_same() -> Result<()> {
    let p = Pipeline::default();
    let events = from_vec(
        &p,
        vec![
            Timestamped::new(0u64, 5u32),
            Timestamped::new(1_000u64, 5u32),
            Timestamped::new(2_000u64, 5u32),
        ],
    );

    let result = events.min_per_window(10_000, 0).collect_seq()?;
    assert_eq!(result, vec![(w(0, 10_000), 5u32)]);
    Ok(())
}

// ─────────────────────────── max_per_window ──────────────────────────────

#[test]
fn max_per_window_basic() -> Result<()> {
    let p = Pipeline::default();
    let events = from_vec(
        &p,
        vec![
            Timestamped::new(1_000u64, 5u32),
            Timestamped::new(5_000u64, 2u32),
            Timestamped::new(11_000u64, 8u32),
        ],
    );

    let mut result = events.max_per_window(10_000, 0).collect_seq()?;
    result.sort_by_key(|(win, _)| win.start);

    assert_eq!(
        result,
        vec![(w(0, 10_000), 5u32), (w(10_000, 20_000), 8u32)]
    );
    Ok(())
}

#[test]
fn max_per_window_multiple_in_window() -> Result<()> {
    let p = Pipeline::default();
    let events = from_vec(
        &p,
        vec![
            Timestamped::new(0u64, 3u32),
            Timestamped::new(1_000u64, 1u32),
            Timestamped::new(2_000u64, 9u32),
            Timestamped::new(3_000u64, 4u32),
        ],
    );

    let result = events.max_per_window(10_000, 0).collect_seq()?;
    assert_eq!(result, vec![(w(0, 10_000), 9u32)]);
    Ok(())
}

// ─────────────────────────── average_per_window ──────────────────────────

#[test]
fn average_per_window_basic() -> Result<()> {
    let p = Pipeline::default();
    let events = from_vec(
        &p,
        vec![
            Timestamped::new(1_000u64, 1u32),
            Timestamped::new(5_000u64, 3u32),
            Timestamped::new(11_000u64, 10u32),
        ],
    );

    let mut result = events.average_per_window(10_000, 0).collect_seq()?;
    result.sort_by_key(|(win, _)| win.start);

    assert!(
        (result[0].1 - 2.0).abs() < 1e-12,
        "window 0 avg should be 2.0"
    );
    assert!(
        (result[1].1 - 10.0).abs() < 1e-12,
        "window 1 avg should be 10.0"
    );
    Ok(())
}

#[test]
fn average_per_window_single_element() -> Result<()> {
    let p = Pipeline::default();
    let events = from_vec(&p, vec![Timestamped::new(0u64, 42u32)]);

    let result = events.average_per_window(10_000, 0).collect_seq()?;
    assert_eq!(result.len(), 1);
    assert!((result[0].1 - 42.0).abs() < 1e-12);
    Ok(())
}

#[test]
fn average_per_window_three_windows() -> Result<()> {
    // Each window: [0,10s)=6 elements summing to 15, so avg=2.5
    //              [10s,20s)=4 elements summing to 16, so avg=4.0
    //              [20s,30s)=1 element =100, avg=100.0
    let mut events = Vec::new();
    for i in 0..6u32 {
        events.push(Timestamped::new(u64::from(i) * 1_000, i));
    }
    for i in 0..4u32 {
        events.push(Timestamped::new(10_000 + u64::from(i) * 1_000, i * 4));
    }
    events.push(Timestamped::new(20_000, 100u32));

    let p2 = Pipeline::default();
    let mut result = from_vec(&p2, events)
        .average_per_window(10_000, 0)
        .collect_seq()?;
    result.sort_by_key(|(win, _)| win.start);

    // window 0: 0+1+2+3+4+5 = 15, n=6, avg=2.5
    assert!((result[0].1 - 2.5).abs() < 1e-9);
    // window 1: 0+4+8+12 = 24, n=4, avg=6.0
    assert!((result[1].1 - 6.0).abs() < 1e-9);
    // window 2: 100, n=1, avg=100.0
    assert!((result[2].1 - 100.0).abs() < 1e-9);
    Ok(())
}

// ────────────────── combine_per_key_and_window (generic) ─────────────────

#[test]
fn combine_per_key_and_window_sum_basic() -> Result<()> {
    let p = Pipeline::default();
    let events = from_vec(
        &p,
        vec![
            ("sensor_a".to_string(), Timestamped::new(1_000u64, 10u32)),
            ("sensor_a".to_string(), Timestamped::new(5_000u64, 20u32)),
            ("sensor_b".to_string(), Timestamped::new(3_000u64, 5u32)),
            ("sensor_a".to_string(), Timestamped::new(12_000u64, 3u32)),
        ],
    );

    let mut result = events
        .combine_per_key_and_window(10_000, 0, Sum::<u32>::new())
        .collect_seq()?;
    result.sort();

    assert_eq!(
        result,
        vec![
            (("sensor_a".to_string(), w(0, 10_000)), 30u32),
            (("sensor_a".to_string(), w(10_000, 20_000)), 3u32),
            (("sensor_b".to_string(), w(0, 10_000)), 5u32),
        ]
    );
    Ok(())
}

#[test]
fn combine_per_key_and_window_single_key() -> Result<()> {
    let p = Pipeline::default();
    let events = from_vec(
        &p,
        vec![
            ("k".to_string(), Timestamped::new(0u64, 1u32)),
            ("k".to_string(), Timestamped::new(1_000u64, 2u32)),
        ],
    );

    let result = events
        .combine_per_key_and_window(10_000, 0, Sum::<u32>::new())
        .collect_seq()?;

    assert_eq!(result, vec![(("k".to_string(), w(0, 10_000)), 3u32)]);
    Ok(())
}

// ──────────────────── sum_per_key_and_window ─────────────────────────────

#[test]
fn sum_per_key_and_window_basic() -> Result<()> {
    let p = Pipeline::default();
    let events = from_vec(
        &p,
        vec![
            ("k1".to_string(), Timestamped::new(1_000u64, 1u32)),
            ("k1".to_string(), Timestamped::new(5_000u64, 2u32)),
            ("k2".to_string(), Timestamped::new(3_000u64, 10u32)),
        ],
    );

    let mut result = events.sum_per_key_and_window(10_000, 0).collect_seq()?;
    result.sort();

    assert_eq!(
        result,
        vec![
            (("k1".to_string(), w(0, 10_000)), 3u32),
            (("k2".to_string(), w(0, 10_000)), 10u32),
        ]
    );
    Ok(())
}

#[test]
fn sum_per_key_and_window_two_windows() -> Result<()> {
    let p = Pipeline::default();
    let events = from_vec(
        &p,
        vec![
            ("a".to_string(), Timestamped::new(0u64, 1u32)),
            ("a".to_string(), Timestamped::new(10_000u64, 9u32)),
        ],
    );

    let mut result = events.sum_per_key_and_window(10_000, 0).collect_seq()?;
    result.sort();

    assert_eq!(
        result,
        vec![
            (("a".to_string(), w(0, 10_000)), 1u32),
            (("a".to_string(), w(10_000, 20_000)), 9u32),
        ]
    );
    Ok(())
}

// ──────────────────── count_per_key_and_window ───────────────────────────

#[test]
fn count_per_key_and_window_basic() -> Result<()> {
    let p = Pipeline::default();
    let events = from_vec(
        &p,
        vec![
            ("k1".to_string(), Timestamped::new(1_000u64, "a")),
            ("k1".to_string(), Timestamped::new(5_000u64, "b")),
            ("k2".to_string(), Timestamped::new(3_000u64, "c")),
        ],
    );

    let mut result = events.count_per_key_and_window(10_000, 0).collect_seq()?;
    result.sort();

    assert_eq!(
        result,
        vec![
            (("k1".to_string(), w(0, 10_000)), 2u64),
            (("k2".to_string(), w(0, 10_000)), 1u64),
        ]
    );
    Ok(())
}

#[test]
fn count_per_key_and_window_multiple_windows() -> Result<()> {
    let p = Pipeline::default();
    let events = from_vec(
        &p,
        vec![
            ("x".to_string(), Timestamped::new(0u64, 1i32)),
            ("x".to_string(), Timestamped::new(5_000u64, 2i32)),
            ("x".to_string(), Timestamped::new(15_000u64, 3i32)),
        ],
    );

    let mut result = events.count_per_key_and_window(10_000, 0).collect_seq()?;
    result.sort();

    assert_eq!(
        result,
        vec![
            (("x".to_string(), w(0, 10_000)), 2u64),
            (("x".to_string(), w(10_000, 20_000)), 1u64),
        ]
    );
    Ok(())
}

// ──────────────────── min_per_key_and_window ─────────────────────────────

#[test]
fn min_per_key_and_window_basic() -> Result<()> {
    let p = Pipeline::default();
    let events = from_vec(
        &p,
        vec![
            ("k1".to_string(), Timestamped::new(1_000u64, 5u32)),
            ("k1".to_string(), Timestamped::new(5_000u64, 2u32)),
            ("k1".to_string(), Timestamped::new(11_000u64, 8u32)),
        ],
    );

    let mut result = events.min_per_key_and_window(10_000, 0).collect_seq()?;
    result.sort();

    assert_eq!(
        result,
        vec![
            (("k1".to_string(), w(0, 10_000)), 2u32),
            (("k1".to_string(), w(10_000, 20_000)), 8u32),
        ]
    );
    Ok(())
}

#[test]
fn min_per_key_and_window_two_keys() -> Result<()> {
    let p = Pipeline::default();
    let events = from_vec(
        &p,
        vec![
            ("a".to_string(), Timestamped::new(0u64, 10u32)),
            ("a".to_string(), Timestamped::new(1_000u64, 3u32)),
            ("b".to_string(), Timestamped::new(0u64, 7u32)),
            ("b".to_string(), Timestamped::new(2_000u64, 1u32)),
        ],
    );

    let mut result = events.min_per_key_and_window(10_000, 0).collect_seq()?;
    result.sort();

    assert_eq!(
        result,
        vec![
            (("a".to_string(), w(0, 10_000)), 3u32),
            (("b".to_string(), w(0, 10_000)), 1u32),
        ]
    );
    Ok(())
}

// ──────────────────── max_per_key_and_window ─────────────────────────────

#[test]
fn max_per_key_and_window_basic() -> Result<()> {
    let p = Pipeline::default();
    let events = from_vec(
        &p,
        vec![
            ("k1".to_string(), Timestamped::new(1_000u64, 5u32)),
            ("k1".to_string(), Timestamped::new(5_000u64, 2u32)),
            ("k1".to_string(), Timestamped::new(11_000u64, 8u32)),
        ],
    );

    let mut result = events.max_per_key_and_window(10_000, 0).collect_seq()?;
    result.sort();

    assert_eq!(
        result,
        vec![
            (("k1".to_string(), w(0, 10_000)), 5u32),
            (("k1".to_string(), w(10_000, 20_000)), 8u32),
        ]
    );
    Ok(())
}

#[test]
fn max_per_key_and_window_two_keys() -> Result<()> {
    let p = Pipeline::default();
    let events = from_vec(
        &p,
        vec![
            ("a".to_string(), Timestamped::new(0u64, 3u32)),
            ("a".to_string(), Timestamped::new(1_000u64, 9u32)),
            ("b".to_string(), Timestamped::new(0u64, 100u32)),
        ],
    );

    let mut result = events.max_per_key_and_window(10_000, 0).collect_seq()?;
    result.sort();

    assert_eq!(
        result,
        vec![
            (("a".to_string(), w(0, 10_000)), 9u32),
            (("b".to_string(), w(0, 10_000)), 100u32),
        ]
    );
    Ok(())
}

// ─────────────────── average_per_key_and_window ──────────────────────────

#[test]
fn average_per_key_and_window_basic() -> Result<()> {
    let p = Pipeline::default();
    let events = from_vec(
        &p,
        vec![
            ("k1".to_string(), Timestamped::new(1_000u64, 1u32)),
            ("k1".to_string(), Timestamped::new(5_000u64, 3u32)),
            ("k1".to_string(), Timestamped::new(11_000u64, 10u32)),
        ],
    );

    let mut result = events.average_per_key_and_window(10_000, 0).collect_seq()?;
    result.sort_by(|a, b| a.0.cmp(&b.0));

    assert!(
        (result[0].1 - 2.0).abs() < 1e-12,
        "window 0 avg should be 2.0"
    );
    assert!(
        (result[1].1 - 10.0).abs() < 1e-12,
        "window 1 avg should be 10.0"
    );
    Ok(())
}

#[test]
fn average_per_key_and_window_two_keys() -> Result<()> {
    let p = Pipeline::default();
    let events = from_vec(
        &p,
        vec![
            ("a".to_string(), Timestamped::new(0u64, 2u32)),
            ("a".to_string(), Timestamped::new(1_000u64, 4u32)),
            ("b".to_string(), Timestamped::new(0u64, 10u32)),
        ],
    );

    let mut result = events.average_per_key_and_window(10_000, 0).collect_seq()?;
    result.sort_by(|a, b| a.0.cmp(&b.0));

    assert!((result[0].1 - 3.0).abs() < 1e-12, "a avg should be 3.0");
    assert!((result[1].1 - 10.0).abs() < 1e-12, "b avg should be 10.0");
    Ok(())
}

// ───────────────────── integration / chaining ────────────────────────────

#[test]
fn windowed_combine_chain_with_map_values() -> Result<()> {
    let p = Pipeline::default();
    // Sum per window, then convert window to (start, sum) tuple
    let events = from_vec(
        &p,
        vec![
            Timestamped::new(0u64, 1u32),
            Timestamped::new(1_000u64, 2u32),
            Timestamped::new(10_000u64, 10u32),
        ],
    );

    let mut result = events
        .sum_per_window(10_000, 0)
        .map(|(win, total): &(Window, u32)| (win.start, *total))
        .collect_seq()?;
    result.sort_unstable();

    assert_eq!(result, vec![(0u64, 3u32), (10_000u64, 10u32)]);
    Ok(())
}

#[test]
fn windowed_combine_keyed_then_filter() -> Result<()> {
    let p = Pipeline::default();
    let events = from_vec(
        &p,
        vec![
            ("a".to_string(), Timestamped::new(0u64, 1u32)),
            ("a".to_string(), Timestamped::new(1_000u64, 1u32)),
            ("b".to_string(), Timestamped::new(0u64, 100u32)),
        ],
    );

    // Sum per key and window, then keep only entries where sum > 50
    let mut result = events
        .sum_per_key_and_window(10_000, 0)
        .filter(|(_, total): &((String, Window), u32)| *total > 50)
        .collect_seq()?;
    result.sort();

    assert_eq!(result, vec![(("b".to_string(), w(0, 10_000)), 100u32)]);
    Ok(())
}

#[test]
fn windowed_combine_count_matches_explicit_group_by() -> Result<()> {
    let p1 = Pipeline::default();
    let p2 = Pipeline::default();

    let make_events = || {
        vec![
            Timestamped::new(0u64, "x"),
            Timestamped::new(5_000u64, "y"),
            Timestamped::new(11_000u64, "z"),
        ]
    };

    // Approach 1: convenience helper
    let mut helper_result = from_vec(&p1, make_events())
        .count_per_window(10_000, 0)
        .collect_seq()?;
    helper_result.sort_by_key(|(win, _)| win.start);

    // Approach 2: explicit three-step pipeline
    let mut explicit_result = from_vec(&p2, make_events())
        .group_by_window(10_000, 0)
        .map_values(|vs: &Vec<&'static str>| vs.len() as u64)
        .collect_seq()?;
    explicit_result.sort_by_key(|(win, _)| win.start);

    assert_eq!(helper_result, explicit_result);
    Ok(())
}
