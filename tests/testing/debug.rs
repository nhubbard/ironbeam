//! Tests for debug utilities in testing module.

use ironbeam::testing::PCollectionDebugExt;
use ironbeam::*;

#[test]
fn test_debug_inspect() {
    let p = Pipeline::default();
    let result = from_vec(&p, vec![1, 2, 3])
        .debug_inspect("test")
        .collect_seq()
        .unwrap();

    assert_eq!(result, vec![1, 2, 3]);
}

#[test]
fn test_debug_inspect_with_custom_fn() {
    let p = Pipeline::default();
    let result = from_vec(&p, vec![1, 2, 3])
        .debug_inspect_with("custom", |x: &i32| {
            // Custom inspection logic
            assert!(*x > 0);
        })
        .collect_seq()
        .unwrap();

    assert_eq!(result, vec![1, 2, 3]);
}

#[test]
fn test_debug_count() {
    let p = Pipeline::default();
    let result = from_vec(&p, vec![1, 2, 3, 4, 5])
        .debug_count("count_test")
        .collect_seq()
        .unwrap();

    assert_eq!(result, vec![1, 2, 3, 4, 5]);
}

#[test]
fn test_debug_sample() {
    let p = Pipeline::default();
    let result = from_vec(&p, (1..=100).collect::<Vec<_>>())
        .debug_sample(5, "sample_test")
        .collect_seq()
        .unwrap();

    assert_eq!(result.len(), 100);
}

#[test]
fn test_debug_with_empty_collection() {
    let p = Pipeline::default();
    let result = from_vec(&p, Vec::<i32>::new())
        .debug_inspect("empty")
        .collect_seq()
        .unwrap();

    assert_eq!(result, Vec::<i32>::new());
}

#[test]
fn test_debug_sample_with_large_n() {
    let p = Pipeline::default();
    let result = from_vec(&p, vec![1, 2, 3])
        .debug_sample(100, "large_n") // Sample more than available
        .collect_seq()
        .unwrap();

    assert_eq!(result, vec![1, 2, 3]);
}

#[test]
fn test_debug_chaining() {
    let p = Pipeline::default();
    let result = from_vec(&p, vec![1, 2, 3, 4, 5])
        .debug_inspect("start")
        .map(|x: &i32| x * 2)
        .debug_count("after_map")
        .filter(|x: &i32| x > &5)
        .debug_sample(2, "filtered")
        .collect_seq()
        .unwrap();

    assert_eq!(result, vec![6, 8, 10]);
}

#[test]
fn test_debug_with_strings() {
    let p = Pipeline::default();
    let result = from_vec(&p, vec!["a".to_string(), "b".to_string(), "c".to_string()])
        .debug_inspect("strings")
        .collect_seq()
        .unwrap();

    assert_eq!(result, vec!["a", "b", "c"]);
}

#[test]
fn test_debug_inspect_with_many_elements() {
    let p = Pipeline::default();
    // Test with more than 10 elements to trigger the truncation logic
    let result = from_vec(&p, (1..=20).collect::<Vec<_>>())
        .debug_inspect("many_elements")
        .collect_seq()
        .unwrap();

    assert_eq!(result.len(), 20);
}

#[test]
fn test_debug_count_zero() {
    let p = Pipeline::default();
    let result = from_vec(&p, Vec::<i32>::new())
        .debug_count("zero")
        .collect_seq()
        .unwrap();

    assert_eq!(result.len(), 0);
}

#[test]
fn test_debug_sample_zero() {
    let p = Pipeline::default();
    let result = from_vec(&p, vec![1, 2, 3])
        .debug_sample(0, "zero_sample")
        .collect_seq()
        .unwrap();

    assert_eq!(result, vec![1, 2, 3]);
}
