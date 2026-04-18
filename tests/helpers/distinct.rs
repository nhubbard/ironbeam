//! Tests for `distinct_by`: deduplication by a computed key projection.

use anyhow::Result;
use ironbeam::*;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct Event {
    user_id: u32,
    payload: String,
}

// ─────────────────────────────── distinct_by ─────────────────────────────────

#[test]
fn distinct_by_removes_duplicates_by_field() -> Result<()> {
    let p = Pipeline::default();
    let events = from_vec(
        &p,
        vec![
            Event { user_id: 1, payload: "first".into() },
            Event { user_id: 1, payload: "second".into() },
            Event { user_id: 2, payload: "only".into() },
        ],
    );
    let mut result = events.distinct_by(|e| e.user_id).collect_seq()?;
    result.sort();
    assert_eq!(result.len(), 2);
    assert_eq!(result[0].user_id, 1);
    assert_eq!(result[1].user_id, 2);
    Ok(())
}

#[test]
fn distinct_by_empty_collection() -> Result<()> {
    let p = Pipeline::default();
    let result = from_vec::<Event>(&p, vec![])
        .distinct_by(|e| e.user_id)
        .collect_seq()?;
    assert!(result.is_empty());
    Ok(())
}

#[test]
fn distinct_by_single_element() -> Result<()> {
    let p = Pipeline::default();
    let events = from_vec(&p, vec![Event { user_id: 42, payload: "hello".into() }]);
    let result = events.distinct_by(|e| e.user_id).collect_seq()?;
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].user_id, 42);
    Ok(())
}

#[test]
fn distinct_by_all_same_key_returns_one() -> Result<()> {
    let p = Pipeline::default();
    let events = from_vec(
        &p,
        vec![
            Event { user_id: 7, payload: "a".into() },
            Event { user_id: 7, payload: "b".into() },
            Event { user_id: 7, payload: "c".into() },
        ],
    );
    let result = events.distinct_by(|e| e.user_id).collect_seq()?;
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].user_id, 7);
    Ok(())
}

#[test]
fn distinct_by_all_unique_keys_passes_all_through() -> Result<()> {
    let p = Pipeline::default();
    let events = from_vec(
        &p,
        vec![
            Event { user_id: 1, payload: "a".into() },
            Event { user_id: 2, payload: "b".into() },
            Event { user_id: 3, payload: "c".into() },
        ],
    );
    let mut result = events.distinct_by(|e| e.user_id).collect_seq()?;
    result.sort_by_key(|e| e.user_id);
    assert_eq!(result.len(), 3);
    assert_eq!(result[0].user_id, 1);
    assert_eq!(result[1].user_id, 2);
    assert_eq!(result[2].user_id, 3);
    Ok(())
}

#[test]
fn distinct_by_string_key_projection() -> Result<()> {
    let p = Pipeline::default();
    // Deduplicate by first character of a string
    let words = from_vec(&p, vec!["apple", "avocado", "banana", "blueberry", "cherry"]);
    let result_count = words
        .distinct_by(|w: &&str| w.chars().next().unwrap())
        .collect_seq()?
        .len();
    // 'a', 'b', 'c' → 3 distinct groups
    assert_eq!(result_count, 3);
    Ok(())
}

#[test]
fn distinct_by_computed_projection() -> Result<()> {
    let p = Pipeline::default();
    // Deduplicate integers by their value modulo 3
    let nums = from_vec(&p, vec![0u32, 3, 6, 1, 4, 7, 2, 5, 8]);
    let result_count = nums.distinct_by(|n| n % 3).collect_seq()?.len();
    // mod-3 buckets: 0, 1, 2 → 3 groups
    assert_eq!(result_count, 3);
    Ok(())
}

#[test]
fn distinct_by_large_collection() -> Result<()> {
    let p = Pipeline::default();
    // 1000 elements, 10 distinct keys (0..10 cycling)
    let data: Vec<u32> = (0..1000).collect();
    let result_count = from_vec(&p, data).distinct_by(|n| n % 10).collect_seq()?.len();
    assert_eq!(result_count, 10);
    Ok(())
}

#[test]
fn distinct_by_retains_full_element_not_just_key() -> Result<()> {
    let p = Pipeline::default();
    let events = from_vec(
        &p,
        vec![
            Event { user_id: 5, payload: "payload_a".into() },
            Event { user_id: 5, payload: "payload_b".into() },
        ],
    );
    let result = events.distinct_by(|e| e.user_id).collect_seq()?;
    assert_eq!(result.len(), 1);
    // The result is a full Event, not just the user_id
    assert_eq!(result[0].user_id, 5);
    assert!(!result[0].payload.is_empty());
    Ok(())
}
