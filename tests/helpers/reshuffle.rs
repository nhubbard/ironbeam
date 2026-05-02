//! Tests for [`PCollection::reshuffle`].

use anyhow::Result;
use ironbeam::*;

// --- Correctness: all elements preserved ----------------------------------

#[test]
fn reshuffle_empty_collection() -> Result<()> {
    let p = Pipeline::default();
    let result = from_vec::<u32>(&p, vec![]).reshuffle().collect_seq()?;
    assert!(result.is_empty());
    Ok(())
}

#[test]
fn reshuffle_single_element() -> Result<()> {
    let p = Pipeline::default();
    let result = from_vec(&p, vec![42u32]).reshuffle().collect_seq()?;
    assert_eq!(result, vec![42u32]);
    Ok(())
}

#[test]
fn reshuffle_preserves_all_elements() -> Result<()> {
    let p = Pipeline::default();
    let mut result = from_vec(&p, vec![1u32, 2, 3, 4, 5])
        .reshuffle()
        .collect_seq()?;
    result.sort_unstable();
    assert_eq!(result, vec![1u32, 2, 3, 4, 5]);
    Ok(())
}

#[test]
fn reshuffle_no_duplicates_introduced() -> Result<()> {
    let p = Pipeline::default();
    let input: Vec<u32> = (0..100).collect();
    let mut result = from_vec(&p, input).reshuffle().collect_seq()?;
    assert_eq!(result.len(), 100);
    result.sort_unstable();
    result.dedup();
    assert_eq!(result.len(), 100);
    Ok(())
}

#[test]
fn reshuffle_duplicate_input_values_all_preserved() -> Result<()> {
    let p = Pipeline::default();
    let result = from_vec(&p, vec![7u32, 7, 7, 7])
        .reshuffle()
        .collect_seq()?;
    assert_eq!(result.len(), 4);
    assert!(result.iter().all(|&v| v == 7));
    Ok(())
}

// --- Generic element types ------------------------------------------------

#[test]
fn reshuffle_string_elements() -> Result<()> {
    let p = Pipeline::default();
    let input = vec!["alpha".to_string(), "beta".to_string(), "gamma".to_string()];
    let mut result = from_vec(&p, input.clone()).reshuffle().collect_seq()?;
    result.sort_unstable();
    let mut expected = input;
    expected.sort_unstable();
    assert_eq!(result, expected);
    Ok(())
}

#[test]
fn reshuffle_struct_elements() -> Result<()> {
    #[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
    struct Record {
        id: u32,
        label: String,
    }

    let p = Pipeline::default();
    let input = vec![
        Record {
            id: 1,
            label: "a".into(),
        },
        Record {
            id: 2,
            label: "b".into(),
        },
        Record {
            id: 3,
            label: "c".into(),
        },
    ];
    let mut result = from_vec(&p, input.clone()).reshuffle().collect_seq()?;
    result.sort_unstable();
    let mut expected = input;
    expected.sort_unstable();
    assert_eq!(result, expected);
    Ok(())
}

// --- Scalability ----------------------------------------------------------

#[test]
fn reshuffle_large_collection() -> Result<()> {
    let p = Pipeline::default();
    let input: Vec<u64> = (0..1_000).collect();
    let mut result = from_vec(&p, input.clone()).reshuffle().collect_seq()?;
    assert_eq!(result.len(), 1_000);
    result.sort_unstable();
    assert_eq!(result, input);
    Ok(())
}

// --- Composition ----------------------------------------------------------

#[test]
fn reshuffle_chainable_with_map() -> Result<()> {
    let p = Pipeline::default();
    let mut result = from_vec(&p, vec![1u32, 2, 3, 4, 5])
        .reshuffle()
        .map(|x: &u32| x * 2)
        .collect_seq()?;
    result.sort_unstable();
    assert_eq!(result, vec![2u32, 4, 6, 8, 10]);
    Ok(())
}

#[test]
fn reshuffle_after_map() -> Result<()> {
    let p = Pipeline::default();
    let mut result = from_vec(&p, vec![1i32, 2, 3])
        .map(|x: &i32| x + 10)
        .reshuffle()
        .collect_seq()?;
    result.sort_unstable();
    assert_eq!(result, vec![11i32, 12, 13]);
    Ok(())
}

#[test]
fn reshuffle_twice() -> Result<()> {
    let p = Pipeline::default();
    let input: Vec<u32> = (0..10).collect();
    let mut result = from_vec(&p, input.clone())
        .reshuffle()
        .reshuffle()
        .collect_seq()?;
    result.sort_unstable();
    assert_eq!(result, input);
    Ok(())
}

#[test]
fn reshuffle_after_group_by_key_restores_elements() -> Result<()> {
    // After GBK (which collapses to 1 partition), reshuffle redistributes elements
    // so that downstream stateless stages can run in parallel on all cores.
    let p = Pipeline::default();
    let input = vec![
        ("a".to_string(), 1u32),
        ("b".to_string(), 2),
        ("a".to_string(), 3),
    ];
    let mut result = from_vec(&p, input)
        .group_by_key()
        .flat_map(|(k, vs): &(String, Vec<u32>)| {
            vs.iter().map(|&v| (k.clone(), v)).collect::<Vec<_>>()
        })
        .reshuffle()
        .map(|(k, v): &(String, u32)| format!("{k}:{v}"))
        .collect_seq()?;
    result.sort_unstable();
    assert_eq!(result, vec!["a:1", "a:3", "b:2"]);
    Ok(())
}

// --- Parallel execution ---------------------------------------------------

#[test]
fn reshuffle_parallel_collect_preserves_elements() -> Result<()> {
    // Forces the n > 1 path inside the reshuffle closure.
    let p = Pipeline::default();
    let input: Vec<u32> = (0..200).collect();
    let mut result = from_vec(&p, input.clone())
        .reshuffle()
        .collect_par(None, Some(4))?;
    result.sort_unstable();
    assert_eq!(result, input);
    Ok(())
}
