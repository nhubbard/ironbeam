//! Tests for the `Latest` combiner.
//!
//! This module tests the `Latest` combiner functionality including:
//! - Selecting latest value per key
//! - Selecting latest value globally
//! - Handling ties (same timestamp)
//! - Convenience methods (`latest_per_key`, `latest_globally`)
//! - Integration with liftable combiner API

use anyhow::Result;
use ironbeam::combiners::Latest;
use ironbeam::window::Timestamped;
use ironbeam::*;

#[test]
fn test_latest_per_key_basic() -> Result<()> {
    let p = Pipeline::default();
    let data = from_vec(
        &p,
        vec![
            ("a", Timestamped::new(100, "old")),
            ("a", Timestamped::new(200, "new")),
            ("b", Timestamped::new(150, "single")),
        ],
    );

    let latest = data.combine_values(Latest::new()).collect_seq_sorted()?;

    assert_eq!(latest, vec![("a", "new"), ("b", "single")]);
    Ok(())
}

#[test]
fn test_latest_per_key_multiple_updates() -> Result<()> {
    let p = Pipeline::default();
    let data = from_vec(
        &p,
        vec![
            ("user1", Timestamped::new(100, "login")),
            ("user1", Timestamped::new(200, "click")),
            ("user2", Timestamped::new(150, "purchase")),
            ("user1", Timestamped::new(180, "view")),
            ("user2", Timestamped::new(300, "logout")),
        ],
    );

    let latest = data.combine_values(Latest::new()).collect_seq_sorted()?;

    assert_eq!(
        latest,
        vec![
            ("user1", "click"),   // timestamp 200 is latest
            ("user2", "logout")   // timestamp 300 is latest
        ]
    );
    Ok(())
}

#[test]
fn test_latest_per_key_out_of_order() -> Result<()> {
    let p = Pipeline::default();
    let data = from_vec(
        &p,
        vec![
            ("a", Timestamped::new(300, "newest")),
            ("a", Timestamped::new(100, "oldest")),
            ("a", Timestamped::new(200, "middle")),
        ],
    );

    let latest = data.combine_values(Latest::new()).collect_seq()?;

    assert_eq!(latest, vec![("a", "newest")]);
    Ok(())
}

#[test]
fn test_latest_per_key_single_value() -> Result<()> {
    let p = Pipeline::default();
    let data = from_vec(&p, vec![("key", Timestamped::new(100, "only"))]);

    let latest = data.combine_values(Latest::new()).collect_seq()?;

    assert_eq!(latest, vec![("key", "only")]);
    Ok(())
}

#[test]
fn test_latest_per_key_many_keys() -> Result<()> {
    let p = Pipeline::default();
    let data = from_vec(
        &p,
        vec![
            ("a", Timestamped::new(100, "a1")),
            ("b", Timestamped::new(200, "b1")),
            ("c", Timestamped::new(150, "c1")),
            ("a", Timestamped::new(250, "a2")),
            ("b", Timestamped::new(180, "b2")),
        ],
    );

    let mut latest = data.combine_values(Latest::new()).collect_seq()?;
    latest.sort_unstable();

    assert_eq!(
        latest,
        vec![("a", "a2"), ("b", "b1"), ("c", "c1")]
    );
    Ok(())
}

#[test]
fn test_latest_globally_basic() -> Result<()> {
    let p = Pipeline::default();
    let data = from_vec(
        &p,
        vec![
            Timestamped::new(100, "event1"),
            Timestamped::new(300, "event2"),
            Timestamped::new(200, "event3"),
        ],
    );

    let latest = data.combine_globally(Latest::new(), None).collect_seq()?;

    assert_eq!(latest, vec!["event2"]);
    Ok(())
}

#[test]
fn test_latest_globally_single_value() -> Result<()> {
    let p = Pipeline::default();
    let data = from_vec(&p, vec![Timestamped::new(100, "only")]);

    let latest = data.combine_globally(Latest::new(), None).collect_seq()?;

    assert_eq!(latest, vec!["only"]);
    Ok(())
}

#[test]
fn test_latest_globally_many_values() -> Result<()> {
    let p = Pipeline::default();
    let data = from_vec(
        &p,
        (0_u64..1000)
            .map(|i| Timestamped::new(i, format!("event{i}")))
            .collect::<Vec<_>>(),
    );

    let latest = data.combine_globally(Latest::new(), None).collect_seq()?;

    assert_eq!(latest, vec!["event999"]);
    Ok(())
}

#[test]
fn test_latest_per_key_convenience_method() -> Result<()> {
    let p = Pipeline::default();
    let data = from_vec(
        &p,
        vec![
            ("a", Timestamped::new(100, 1)),
            ("a", Timestamped::new(200, 2)),
            ("b", Timestamped::new(150, 3)),
        ],
    );

    let latest = data.latest_per_key().collect_seq_sorted()?;

    assert_eq!(latest, vec![("a", 2), ("b", 3)]);
    Ok(())
}

#[test]
fn test_latest_globally_convenience_method() -> Result<()> {
    let p = Pipeline::default();
    let data = from_vec(
        &p,
        vec![
            Timestamped::new(100, "a"),
            Timestamped::new(300, "b"),
            Timestamped::new(200, "c"),
        ],
    );

    let latest = data.latest_globally().collect_seq()?;

    assert_eq!(latest, vec!["b"]);
    Ok(())
}

#[test]
fn test_latest_with_numeric_values() -> Result<()> {
    let p = Pipeline::default();
    let data = from_vec(
        &p,
        vec![
            ("sensor", Timestamped::new(1000, 23.5)),
            ("sensor", Timestamped::new(2000, 24.1)),
            ("sensor", Timestamped::new(3000, 22.8)),
        ],
    );

    let latest = data.combine_values(Latest::new()).collect_seq()?;

    assert_eq!(latest, vec![("sensor", 22.8)]);
    Ok(())
}

#[test]
fn test_latest_with_struct_values() -> Result<()> {
    #[derive(Clone, Debug, PartialEq, Eq)]
    struct Event {
        id: u32,
        status: String,
    }

    let p = Pipeline::default();
    let data = from_vec(
        &p,
        vec![
            (
                "order",
                Timestamped::new(
                    100,
                    Event {
                        id: 1,
                        status: "pending".to_string(),
                    },
                ),
            ),
            (
                "order",
                Timestamped::new(
                    200,
                    Event {
                        id: 1,
                        status: "shipped".to_string(),
                    },
                ),
            ),
            (
                "order",
                Timestamped::new(
                    300,
                    Event {
                        id: 1,
                        status: "delivered".to_string(),
                    },
                ),
            ),
        ],
    );

    let latest = data.combine_values(Latest::new()).collect_seq()?;

    assert_eq!(
        latest,
        vec![(
            "order",
            Event {
                id: 1,
                status: "delivered".to_string()
            }
        )]
    );
    Ok(())
}

#[test]
fn test_latest_with_large_timestamps() -> Result<()> {
    let p = Pipeline::default();
    let data = from_vec(
        &p,
        vec![
            ("key", Timestamped::new(1_700_000_000_000, "old")),
            ("key", Timestamped::new(1_700_000_001_000, "new")),
        ],
    );

    let latest = data.combine_values(Latest::new()).collect_seq()?;

    assert_eq!(latest, vec![("key", "new")]);
    Ok(())
}

#[test]
fn test_latest_preserves_order_of_keys() -> Result<()> {
    let p = Pipeline::default();
    let data = from_vec(
        &p,
        vec![
            ("z", Timestamped::new(100, "z1")),
            ("a", Timestamped::new(200, "a1")),
            ("m", Timestamped::new(150, "m1")),
        ],
    );

    let latest = data.combine_values(Latest::new()).collect_seq_sorted()?;

    // Keys should be in sorted order
    assert_eq!(latest, vec![("a", "a1"), ("m", "m1"), ("z", "z1")]);
    Ok(())
}

#[test]
fn test_latest_with_combine_values_lifted() -> Result<()> {
    let p = Pipeline::default();
    let data = from_vec(
        &p,
        vec![
            ("a", Timestamped::new(100, 1)),
            ("a", Timestamped::new(200, 2)),
            ("b", Timestamped::new(150, 3)),
        ],
    );

    let grouped = data.group_by_key();
    let latest = grouped.combine_values_lifted(Latest::new());
    let result = latest.collect_seq_sorted()?;

    assert_eq!(result, vec![("a", 2), ("b", 3)]);
    Ok(())
}

#[test]
fn test_latest_timestamp_tie() -> Result<()> {
    // When multiple values have the same timestamp, one is chosen arbitrarily
    let p = Pipeline::default();
    let data = from_vec(
        &p,
        vec![
            ("key", Timestamped::new(100, "first")),
            ("key", Timestamped::new(100, "second")),
            ("key", Timestamped::new(100, "third")),
        ],
    );

    let latest = data.combine_values(Latest::new()).collect_seq()?;

    // Just verify we got one of them
    assert_eq!(latest.len(), 1);
    assert_eq!(latest[0].0, "key");
    // The value should be one of the three, but which one is implementation-defined
    assert!(["first", "second", "third"].contains(&latest[0].1));
    Ok(())
}
