//! Comprehensive tests for `to_hashmap` transform.
//!
//! This test suite validates the conversion of `PCollection<(K, V)>` to `HashMap<K, V>`,
//! including edge cases, duplicate key handling, and various data types.

use ironbeam::*;

/// Test basic `to_hashmap` with string keys and numeric values
#[test]
fn test_to_hashmap_basic() {
    let p = Pipeline::default();
    let pairs = from_vec(&p, vec![
        ("a".to_string(), 1u32),
        ("b".to_string(), 2),
        ("c".to_string(), 3),
    ]);

    let map = pairs.to_hashmap().unwrap();

    assert_eq!(map.len(), 3);
    assert_eq!(map.get("a"), Some(&1));
    assert_eq!(map.get("b"), Some(&2));
    assert_eq!(map.get("c"), Some(&3));
}

/// Test `to_hashmap` with numeric keys and string values
#[test]
fn test_to_hashmap_numeric_keys() {
    let p = Pipeline::default();
    let pairs = from_vec(&p, vec![
        (1, "one".to_string()),
        (2, "two".to_string()),
        (3, "three".to_string()),
    ]);

    let map = pairs.to_hashmap().unwrap();

    assert_eq!(map.len(), 3);
    assert_eq!(map.get(&1), Some(&"one".to_string()));
    assert_eq!(map.get(&2), Some(&"two".to_string()));
    assert_eq!(map.get(&3), Some(&"three".to_string()));
}

/// Test `to_hashmap` with duplicate keys (last value wins)
#[test]
fn test_to_hashmap_duplicate_keys() {
    let p = Pipeline::default();
    let pairs = from_vec(&p, vec![
        ("a".to_string(), 1),
        ("b".to_string(), 2),
        ("a".to_string(), 10), // Duplicate key, should override
        ("b".to_string(), 20), // Duplicate key, should override
    ]);

    let map = pairs.to_hashmap().unwrap();

    // Only 2 keys, with last values
    assert_eq!(map.len(), 2);
    assert_eq!(map.get("a"), Some(&10));
    assert_eq!(map.get("b"), Some(&20));
}

/// Test `to_hashmap` with empty collection
#[test]
fn test_to_hashmap_empty() {
    let p = Pipeline::default();
    let pairs: Vec<(String, i32)> = vec![];
    let pcoll = from_vec(&p, pairs);

    let map = pcoll.to_hashmap().unwrap();

    assert_eq!(map.len(), 0);
    assert!(map.is_empty());
}

/// Test `to_hashmap` with single element
#[test]
fn test_to_hashmap_single_element() {
    let p = Pipeline::default();
    let pairs = from_vec(&p, vec![("only".to_string(), 42)]);

    let map = pairs.to_hashmap().unwrap();

    assert_eq!(map.len(), 1);
    assert_eq!(map.get("only"), Some(&42));
}

/// Test `to_hashmap` after `key_by`
#[test]
fn test_to_hashmap_after_key_by() {
    let p = Pipeline::default();
    let words = from_vec(&p, vec![
        "cat".to_string(),
        "dog".to_string(),
        "bird".to_string(),
        "ant".to_string(),
    ]);

    let keyed = words.key_by(|s: &String| s.len());
    let map = keyed.to_hashmap().unwrap();

    // Note: duplicate keys (length 3) - last value wins
    assert_eq!(map.len(), 2); // lengths 3 and 4
    assert!(map.contains_key(&3));
    assert!(map.contains_key(&4));
    assert_eq!(map.get(&4), Some(&"bird".to_string()));
}

/// Test `to_hashmap` with tuple keys
#[test]
fn test_to_hashmap_tuple_keys() {
    let p = Pipeline::default();
    let pairs = from_vec(&p, vec![
        ((1, "a"), 100),
        ((2, "b"), 200),
        ((1, "b"), 150),
    ]);

    let map = pairs.to_hashmap().unwrap();

    assert_eq!(map.len(), 3);
    assert_eq!(map.get(&(1, "a")), Some(&100));
    assert_eq!(map.get(&(2, "b")), Some(&200));
    assert_eq!(map.get(&(1, "b")), Some(&150));
}

/// Test `to_hashmap` with struct values
#[test]
fn test_to_hashmap_struct_values() {
    #[derive(Clone, Debug, PartialEq)]
    struct Person {
        name: String,
        age: u32,
    }

    let p = Pipeline::default();
    let pairs = from_vec(&p, vec![
        (1, Person { name: "Alice".into(), age: 25 }),
        (2, Person { name: "Bob".into(), age: 30 }),
        (3, Person { name: "Carol".into(), age: 35 }),
    ]);

    let map = pairs.to_hashmap().unwrap();

    assert_eq!(map.len(), 3);
    assert_eq!(map.get(&1).unwrap().name, "Alice");
    assert_eq!(map.get(&2).unwrap().age, 30);
    assert_eq!(map.get(&3).unwrap().name, "Carol");
}

/// Test `to_hashmap` with large collection
#[test]
fn test_to_hashmap_large_collection() {
    let p = Pipeline::default();
    let pairs: Vec<(i32, i32)> = (0..1000).map(|i| (i, i * 2)).collect();
    let pcoll = from_vec(&p, pairs);

    let map = pcoll.to_hashmap().unwrap();

    assert_eq!(map.len(), 1000);
    assert_eq!(map.get(&0), Some(&0));
    assert_eq!(map.get(&500), Some(&1000));
    assert_eq!(map.get(&999), Some(&1998));
}

/// Test `to_hashmap` preserves all unique keys
#[test]
fn test_to_hashmap_unique_keys() {
    let p = Pipeline::default();
    let pairs = from_vec(&p, vec![
        ("key1", 10),
        ("key2", 20),
        ("key3", 30),
        ("key4", 40),
        ("key5", 50),
    ]);

    let map = pairs.to_hashmap().unwrap();

    assert_eq!(map.len(), 5);
    assert_eq!(map.get(&"key1"), Some(&10));
    assert_eq!(map.get(&"key2"), Some(&20));
    assert_eq!(map.get(&"key3"), Some(&30));
    assert_eq!(map.get(&"key4"), Some(&40));
    assert_eq!(map.get(&"key5"), Some(&50));
}

/// Test `to_hashmap` with Option values
#[test]
fn test_to_hashmap_option_values() {
    let p = Pipeline::default();
    let pairs = from_vec(&p, vec![
        ("a", Some(1)),
        ("b", None),
        ("c", Some(3)),
    ]);

    let map = pairs.to_hashmap().unwrap();

    assert_eq!(map.len(), 3);
    assert_eq!(map.get(&"a"), Some(&Some(1)));
    assert_eq!(map.get(&"b"), Some(&None));
    assert_eq!(map.get(&"c"), Some(&Some(3)));
}

/// Test `to_hashmap` with Result values
#[test]
fn test_to_hashmap_result_values() {
    let p = Pipeline::default();
    let pairs = from_vec(&p, vec![
        (1, Ok::<i32, String>(10)),
        (2, Err::<i32, String>("error".into())),
        (3, Ok(30)),
    ]);

    let map = pairs.to_hashmap().unwrap();

    assert_eq!(map.len(), 3);
    assert_eq!(map.get(&1), Some(&Ok(10)));
    assert!(map.get(&2).unwrap().is_err());
    assert_eq!(map.get(&3), Some(&Ok(30)));
}

/// Test `to_hashmap` with Vec values
#[test]
fn test_to_hashmap_vec_values() {
    let p = Pipeline::default();
    let pairs = from_vec(&p, vec![
        ("a", vec![1, 2, 3]),
        ("b", vec![4, 5]),
        ("c", vec![6]),
    ]);

    let map = pairs.to_hashmap().unwrap();

    assert_eq!(map.len(), 3);
    assert_eq!(map.get(&"a"), Some(&vec![1, 2, 3]));
    assert_eq!(map.get(&"b"), Some(&vec![4, 5]));
    assert_eq!(map.get(&"c"), Some(&vec![6]));
}

/// Test `to_hashmap` after filtering
#[test]
fn test_to_hashmap_after_filter() {
    let p = Pipeline::default();
    let pairs = from_vec(&p, vec![
        ("a", 1),
        ("b", 2),
        ("c", 3),
        ("d", 4),
        ("e", 5),
    ]);

    let filtered = pairs.filter(|(_, v)| *v % 2 == 0);
    let map = filtered.to_hashmap().unwrap();

    assert_eq!(map.len(), 2);
    assert_eq!(map.get(&"b"), Some(&2));
    assert_eq!(map.get(&"d"), Some(&4));
    assert_eq!(map.get(&"a"), None);
}

/// Test `to_hashmap` after map transformation
#[test]
fn test_to_hashmap_after_map() {
    let p = Pipeline::default();
    let pairs = from_vec(&p, vec![
        ("a".to_string(), 1),
        ("b".to_string(), 2),
        ("c".to_string(), 3),
    ]);

    let transformed = pairs.map(|(k, v)| (k.clone(), v * 10));
    let map = transformed.to_hashmap().unwrap();

    assert_eq!(map.len(), 3);
    assert_eq!(map.get("a"), Some(&10));
    assert_eq!(map.get("b"), Some(&20));
    assert_eq!(map.get("c"), Some(&30));
}

/// Test `to_hashmap` with composite struct keys
#[test]
fn test_to_hashmap_composite_keys() {
    #[derive(Clone, Debug, PartialEq, Eq, Hash)]
    struct CompositeKey {
        region: String,
        category: u32,
    }

    let p = Pipeline::default();
    let pairs = from_vec(&p, vec![
        (CompositeKey { region: "US".into(), category: 1 }, 100),
        (CompositeKey { region: "EU".into(), category: 2 }, 200),
        (CompositeKey { region: "US".into(), category: 2 }, 150),
    ]);

    let map = pairs.to_hashmap().unwrap();

    assert_eq!(map.len(), 3);
    assert_eq!(
        map.get(&CompositeKey { region: "US".into(), category: 1 }),
        Some(&100)
    );
    assert_eq!(
        map.get(&CompositeKey { region: "EU".into(), category: 2 }),
        Some(&200)
    );
}

/// Test `to_hashmap` with boolean keys
#[test]
fn test_to_hashmap_boolean_keys() {
    let p = Pipeline::default();
    let pairs = from_vec(&p, vec![
        (true, "yes".to_string()),
        (false, "no".to_string()),
    ]);

    let map = pairs.to_hashmap().unwrap();

    assert_eq!(map.len(), 2);
    assert_eq!(map.get(&true), Some(&"yes".to_string()));
    assert_eq!(map.get(&false), Some(&"no".to_string()));
}

/// Test `to_hashmap` with char keys
#[test]
fn test_to_hashmap_char_keys() {
    let p = Pipeline::default();
    let pairs = from_vec(&p, vec![
        ('a', 1),
        ('b', 2),
        ('c', 3),
    ]);

    let map = pairs.to_hashmap().unwrap();

    assert_eq!(map.len(), 3);
    assert_eq!(map.get(&'a'), Some(&1));
    assert_eq!(map.get(&'b'), Some(&2));
    assert_eq!(map.get(&'c'), Some(&3));
}

/// Test `to_hashmap` duplicate keys with different order
#[test]
fn test_to_hashmap_duplicate_ordering() {
    let p = Pipeline::default();
    let pairs = from_vec(&p, vec![
        ("x", 1),
        ("y", 2),
        ("x", 3),
        ("y", 4),
        ("x", 5),
    ]);

    let map = pairs.to_hashmap().unwrap();

    // Last values for each key
    assert_eq!(map.len(), 2);
    assert_eq!(map.get(&"x"), Some(&5));
    assert_eq!(map.get(&"y"), Some(&4));
}

/// Test `to_hashmap` after `with_constant_key`
#[test]
fn test_to_hashmap_constant_key() {
    let p = Pipeline::default();
    let numbers = from_vec(&p, vec![1, 2, 3, 4, 5]);

    let keyed = numbers.with_constant_key("all");
    let map = keyed.to_hashmap().unwrap();

    // Only one key, last value wins
    assert_eq!(map.len(), 1);
    assert_eq!(map.get(&"all"), Some(&5));
}

/// Test `to_hashmap` with floating point values
#[test]
fn test_to_hashmap_float_values() {
    let p = Pipeline::default();
    let pairs = from_vec(&p, vec![
        ("pi", 3.14159),
        ("e", 2.71828),
        ("phi", 1.61803),
    ]);

    let map = pairs.to_hashmap().unwrap();

    assert_eq!(map.len(), 3);
    assert_eq!(map.get(&"pi"), Some(&3.14159));
    assert_eq!(map.get(&"e"), Some(&2.71828));
    assert_eq!(map.get(&"phi"), Some(&1.61803));
}

/// Test `to_hashmap` maintains all values when keys are distinct
#[test]
fn test_to_hashmap_distinct_keys_preserves_all() {
    let p = Pipeline::default();
    let pairs: Vec<(i32, String)> = (1..=100)
        .map(|i| (i, format!("value_{}", i)))
        .collect();
    let pcoll = from_vec(&p, pairs);

    let map = pcoll.to_hashmap().unwrap();

    assert_eq!(map.len(), 100);
    for i in 1..=100 {
        assert_eq!(map.get(&i), Some(&format!("value_{}", i)));
    }
}

/// Test `to_hashmap` integration with parallel collection
#[test]
fn test_to_hashmap_parallel() {
    let p = Pipeline::default();
    let pairs: Vec<(i32, i32)> = (0..1000).map(|i| (i, i * i)).collect();
    let pcoll = from_vec(&p, pairs);

    // Note: to_hashmap internally uses collect_seq, but we can still test it
    let map = pcoll.to_hashmap().unwrap();

    assert_eq!(map.len(), 1000);
    assert_eq!(map.get(&10), Some(&100));
    assert_eq!(map.get(&50), Some(&2500));
    assert_eq!(map.get(&99), Some(&9801));
}

/// Test `to_hashmap` with string slice keys
#[test]
fn test_to_hashmap_str_keys() {
    let p = Pipeline::default();
    let pairs = from_vec(&p, vec![
        ("alpha", 1),
        ("beta", 2),
        ("gamma", 3),
    ]);

    let map = pairs.to_hashmap().unwrap();

    assert_eq!(map.len(), 3);
    assert_eq!(map.get(&"alpha"), Some(&1));
    assert_eq!(map.get(&"beta"), Some(&2));
    assert_eq!(map.get(&"gamma"), Some(&3));
}
