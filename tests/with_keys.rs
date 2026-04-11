//! Comprehensive tests for `WithKeys` transform (feature 1.4).
//!
//! This test suite validates the keying operations including:
//! - `key_by()` for deriving keys from elements
//! - `with_constant_key()` for global grouping
//! - `with_keys()` as an alias for Apache Beam familiarity

use ironbeam::*;
use std::collections::HashMap;

/// Test basic `key_by` functionality with string length
#[test]
fn test_key_by_string_length() {
    let p = Pipeline::default();
    let words = from_vec(
        &p,
        vec![
            "cat".to_string(),
            "dog".to_string(),
            "bird".to_string(),
            "elephant".to_string(),
            "ant".to_string(),
        ],
    );

    let keyed = words.key_by(|s: &String| s.len());
    let mut result = keyed.collect_seq().unwrap();
    result.sort_by_key(|k| k.0);

    assert_eq!(result.len(), 5);
    assert_eq!(result[0], (3, "cat".to_string()));
    assert_eq!(result[1], (3, "dog".to_string()));
    assert_eq!(result[2], (3, "ant".to_string()));
    assert_eq!(result[3], (4, "bird".to_string()));
    assert_eq!(result[4], (8, "elephant".to_string()));
}

/// Test `key_by` with numeric data
#[test]
fn test_key_by_modulo() {
    let p = Pipeline::default();
    let numbers = from_vec(&p, vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);

    // Key by modulo 3
    let keyed = numbers.key_by(|n: &i32| n % 3);
    let grouped = keyed.group_by_key();
    let result = grouped.collect_seq().unwrap();

    // Convert to HashMap for easier testing
    let map: HashMap<_, _> = result.into_iter().collect();

    assert_eq!(map.len(), 3);

    let mut key0 = map.get(&0).unwrap().clone();
    key0.sort_unstable();
    assert_eq!(key0, vec![3, 6, 9]);

    let mut key1 = map.get(&1).unwrap().clone();
    key1.sort_unstable();
    assert_eq!(key1, vec![1, 4, 7, 10]);

    let mut key2 = map.get(&2).unwrap().clone();
    key2.sort_unstable();
    assert_eq!(key2, vec![2, 5, 8]);
}

/// Test `with_constant_key` for global grouping
#[test]
fn test_with_constant_key_global_grouping() {
    let p = Pipeline::default();
    let numbers = from_vec(&p, vec![1, 2, 3, 4, 5]);

    let keyed = numbers.with_constant_key("all");
    let grouped = keyed.group_by_key();
    let result = grouped.collect_seq().unwrap();

    assert_eq!(result.len(), 1);
    assert_eq!(result[0].0, "all");

    let mut values = result[0].1.clone();
    values.sort_unstable();
    assert_eq!(values, vec![1, 2, 3, 4, 5]);
}

/// Test `with_constant_key` with empty collection
#[test]
fn test_with_constant_key_empty() {
    let p = Pipeline::default();
    let empty: Vec<i32> = vec![];
    let numbers = from_vec(&p, empty);

    let keyed = numbers.with_constant_key(42);
    let result = keyed.collect_seq().unwrap();

    assert_eq!(result.len(), 0);
}

/// Test `with_constant_key` with single element
#[test]
fn test_with_constant_key_single_element() {
    let p = Pipeline::default();
    let single = from_vec(&p, vec![100]);

    let keyed = single.with_constant_key("key");
    let grouped = keyed.group_by_key();
    let result = grouped.collect_seq().unwrap();

    assert_eq!(result.len(), 1);
    assert_eq!(result[0], ("key", vec![100]));
}

/// Test `with_constant_key` for global sum aggregation
#[test]
fn test_with_constant_key_global_sum() {
    let p = Pipeline::default();
    let numbers = from_vec(&p, vec![10, 20, 30, 40, 50]);

    let keyed = numbers.with_constant_key(0);
    let summed = keyed.map_values(|v| *v).combine_values(Sum::default());
    let result = summed.collect_seq().unwrap();

    assert_eq!(result.len(), 1);
    assert_eq!(result[0], (0, 150));
}

/// Test `with_keys` as alias for `key_by`
#[test]
fn test_with_keys_alias() {
    let p = Pipeline::default();
    let words = from_vec(
        &p,
        vec![
            "apple".to_string(),
            "apricot".to_string(),
            "banana".to_string(),
        ],
    );

    // Use with_keys instead of key_by
    let keyed = words.with_keys(|s: &String| s.chars().next().unwrap());
    let grouped = keyed.group_by_key();
    let result = grouped.collect_seq().unwrap();

    let map: HashMap<_, _> = result.into_iter().collect();

    assert_eq!(map.len(), 2);
    assert_eq!(map.get(&'a').unwrap().len(), 2);
    assert_eq!(map.get(&'b').unwrap().len(), 1);
}

/// Test `key_by` with struct field extraction
#[test]
fn test_key_by_struct_field() {
    #[derive(Clone, Debug, PartialEq)]
    struct Person {
        name: String,
        age: u32,
        city: String,
    }

    let p = Pipeline::default();
    let people = from_vec(
        &p,
        vec![
            Person {
                name: "Alice".into(),
                age: 25,
                city: "NYC".into(),
            },
            Person {
                name: "Bob".into(),
                age: 30,
                city: "LA".into(),
            },
            Person {
                name: "Carol".into(),
                age: 35,
                city: "NYC".into(),
            },
            Person {
                name: "David".into(),
                age: 40,
                city: "LA".into(),
            },
        ],
    );

    // Key by city
    let by_city = people.key_by(|p| p.city.clone());
    let grouped = by_city.group_by_key();
    let result = grouped.collect_seq().unwrap();

    let map: HashMap<_, _> = result.into_iter().collect();

    assert_eq!(map.len(), 2);
    assert_eq!(map.get("NYC").unwrap().len(), 2);
    assert_eq!(map.get("LA").unwrap().len(), 2);
}

/// Test `key_by` with computed value
#[test]
fn test_key_by_computed_value() {
    #[derive(Clone, Debug)]
    #[allow(dead_code)]
    struct Product {
        name: String,
        price: f64,
    }

    let p = Pipeline::default();
    let products = from_vec(
        &p,
        vec![
            Product {
                name: "Book".into(),
                price: 15.99,
            },
            Product {
                name: "Pen".into(),
                price: 2.50,
            },
            Product {
                name: "Laptop".into(),
                price: 899.99,
            },
            Product {
                name: "Mouse".into(),
                price: 25.00,
            },
        ],
    );

    // Key by price category (cheap < 10, mid < 100, expensive >= 100)
    let by_category = products.key_by(|p| {
        if p.price < 10.0 {
            "cheap"
        } else if p.price < 100.0 {
            "mid"
        } else {
            "expensive"
        }
    });

    let grouped = by_category.group_by_key();
    let result = grouped.collect_seq().unwrap();

    let map: HashMap<_, _> = result.into_iter().collect();

    assert_eq!(map.len(), 3);
    assert_eq!(map.get(&"cheap").unwrap().len(), 1);
    assert_eq!(map.get(&"mid").unwrap().len(), 2);
    assert_eq!(map.get(&"expensive").unwrap().len(), 1);
}

/// Test `key_by` followed by counting per key
#[test]
fn test_key_by_with_count_per_key() {
    let p = Pipeline::default();
    let data = from_vec(&p, vec!["a", "b", "a", "c", "a", "b", "d"]);

    let keyed = data.key_by(|s: &&str| s.to_string());
    let counts = keyed.map_values(|_| 1u64).combine_values(Sum::default());
    let result = counts.collect_seq().unwrap();

    let map: HashMap<_, _> = result.into_iter().collect();

    assert_eq!(map.len(), 4);
    assert_eq!(*map.get("a").unwrap(), 3);
    assert_eq!(*map.get("b").unwrap(), 2);
    assert_eq!(*map.get("c").unwrap(), 1);
    assert_eq!(*map.get("d").unwrap(), 1);
}

/// Test `key_by` with Option values
#[test]
fn test_key_by_option() {
    #[derive(Clone)]
    #[allow(dead_code)]
    struct Record {
        id: u32,
        category: Option<String>,
    }

    let p = Pipeline::default();
    let records = from_vec(
        &p,
        vec![
            Record {
                id: 1,
                category: Some("A".into()),
            },
            Record {
                id: 2,
                category: None,
            },
            Record {
                id: 3,
                category: Some("B".into()),
            },
            Record {
                id: 4,
                category: Some("A".into()),
            },
            Record {
                id: 5,
                category: None,
            },
        ],
    );

    // Key by category, treating None as "unknown"
    let keyed = records.key_by(|r| r.category.clone().unwrap_or_else(|| "unknown".to_string()));

    let grouped = keyed.group_by_key();
    let result = grouped.collect_seq().unwrap();

    let map: HashMap<_, _> = result.into_iter().collect();

    assert_eq!(map.len(), 3);
    assert_eq!(map.get("A").unwrap().len(), 2);
    assert_eq!(map.get("B").unwrap().len(), 1);
    assert_eq!(map.get("unknown").unwrap().len(), 2);
}

/// Test `with_constant_key` with different key types
#[test]
fn test_with_constant_key_various_types() {
    let p = Pipeline::default();

    // With string key
    let data1 = from_vec(&p, vec![1, 2, 3]);
    let keyed1 = data1.with_constant_key("global");
    let result1 = keyed1.collect_seq().unwrap();
    assert_eq!(result1.len(), 3);
    assert_eq!(result1[0].0, "global");

    // With numeric key
    let data2 = from_vec(&p, vec!["a", "b", "c"]);
    let keyed2 = data2.with_constant_key(42u64);
    let result2 = keyed2.collect_seq().unwrap();
    assert_eq!(result2.len(), 3);
    assert_eq!(result2[0].0, 42);

    // With tuple key
    let data3 = from_vec(&p, vec![1.0, 2.0, 3.0]);
    let keyed3 = data3.with_constant_key(("region", 1));
    let result3 = keyed3.collect_seq().unwrap();
    assert_eq!(result3.len(), 3);
    assert_eq!(result3[0].0, ("region", 1));
}

/// Test `key_by` with composite keys (tuples)
#[test]
fn test_key_by_composite_key() {
    #[derive(Clone, Debug)]
    #[allow(dead_code)]
    struct Event {
        user_id: u32,
        region: String,
        value: i32,
    }

    let p = Pipeline::default();
    let events = from_vec(
        &p,
        vec![
            Event {
                user_id: 1,
                region: "US".into(),
                value: 10,
            },
            Event {
                user_id: 1,
                region: "EU".into(),
                value: 20,
            },
            Event {
                user_id: 2,
                region: "US".into(),
                value: 15,
            },
            Event {
                user_id: 1,
                region: "US".into(),
                value: 5,
            },
        ],
    );

    // Key by (user_id, region) tuple
    let keyed = events.key_by(|e| (e.user_id, e.region.clone()));
    let grouped = keyed.group_by_key();
    let result = grouped.collect_seq().unwrap();

    let map: HashMap<_, _> = result.into_iter().collect();

    assert_eq!(map.len(), 3);
    assert_eq!(map.get(&(1, "US".to_string())).unwrap().len(), 2);
    assert_eq!(map.get(&(1, "EU".to_string())).unwrap().len(), 1);
    assert_eq!(map.get(&(2, "US".to_string())).unwrap().len(), 1);
}

/// Test `key_by` preserves all values correctly
#[test]
fn test_key_by_preserves_values() {
    let p = Pipeline::default();
    let data = from_vec(&p, vec![1, 2, 3, 4, 5, 6]);

    // Key by even/odd
    let keyed = data.key_by(|n: &i32| n % 2);
    let grouped = keyed.group_by_key();
    let result = grouped.collect_seq().unwrap();

    let map: HashMap<_, _> = result.into_iter().collect();

    let mut evens = map.get(&0).unwrap().clone();
    evens.sort_unstable();
    assert_eq!(evens, vec![2, 4, 6]);

    let mut odds = map.get(&1).unwrap().clone();
    odds.sort_unstable();
    assert_eq!(odds, vec![1, 3, 5]);
}

/// Test parallel execution with keying operations
#[test]
fn test_key_by_parallel() {
    let p = Pipeline::default();
    let numbers = from_vec(&p, (1..=1000).collect::<Vec<i32>>());

    let keyed = numbers.key_by(|n: &i32| n % 10);
    let grouped = keyed.group_by_key();
    let result = grouped.collect_par(Some(4), Some(4)).unwrap();

    let map: HashMap<_, _> = result.into_iter().collect();

    // Should have 10 keys (0-9)
    assert_eq!(map.len(), 10);

    // Each key should have 100 values
    for i in 0..10 {
        assert_eq!(map.get(&i).unwrap().len(), 100);
    }
}

/// Test `with_constant_key` parallel execution
#[test]
fn test_with_constant_key_parallel() {
    let p = Pipeline::default();
    let numbers = from_vec(&p, (1..=100).collect::<Vec<i32>>());

    let keyed = numbers.with_constant_key(0);
    let result = keyed.collect_par(Some(4), Some(4)).unwrap();

    assert_eq!(result.len(), 100);
    // All should have the same key
    assert!(result.iter().all(|(k, _)| *k == 0));
}

/// Test chaining `key_by` operations
#[test]
fn test_key_by_chaining() {
    #[derive(Clone, Debug)]
    struct Item {
        category: String,
        subcategory: String,
        value: i32,
    }

    let p = Pipeline::default();
    let items = from_vec(
        &p,
        vec![
            Item {
                category: "A".into(),
                subcategory: "X".into(),
                value: 1,
            },
            Item {
                category: "A".into(),
                subcategory: "Y".into(),
                value: 2,
            },
            Item {
                category: "B".into(),
                subcategory: "X".into(),
                value: 3,
            },
        ],
    );

    // First key by category, then by subcategory
    let by_category = items.key_by(|i| i.category.clone());
    let nested = by_category.map_values(|item| (item.subcategory.clone(), item.value));

    let result = nested.group_by_key().collect_seq().unwrap();
    let map: HashMap<_, _> = result.into_iter().collect();

    assert_eq!(map.len(), 2);
    assert_eq!(map.get("A").unwrap().len(), 2);
    assert_eq!(map.get("B").unwrap().len(), 1);
}

/// Test `key_by` with enum discriminant
#[test]
fn test_key_by_enum() {
    #[derive(Clone, Debug)]
    enum Status {
        Pending,
        Active,
        Complete,
    }

    #[derive(Clone)]
    #[allow(dead_code)]
    struct Task {
        id: u32,
        status: Status,
    }

    let p = Pipeline::default();
    let tasks = from_vec(
        &p,
        vec![
            Task {
                id: 1,
                status: Status::Pending,
            },
            Task {
                id: 2,
                status: Status::Active,
            },
            Task {
                id: 3,
                status: Status::Pending,
            },
            Task {
                id: 4,
                status: Status::Complete,
            },
        ],
    );

    // Key by status discriminant
    let keyed = tasks.key_by(|t| match t.status {
        Status::Pending => 0,
        Status::Active => 1,
        Status::Complete => 2,
    });

    let grouped = keyed.group_by_key();
    let result = grouped.collect_seq().unwrap();

    let map: HashMap<_, _> = result.into_iter().collect();

    assert_eq!(map.len(), 3);
    assert_eq!(map.get(&0).unwrap().len(), 2); // Pending
    assert_eq!(map.get(&1).unwrap().len(), 1); // Active
    assert_eq!(map.get(&2).unwrap().len(), 1); // Complete
}

/// Test that keys are properly cloned
#[test]
fn test_key_by_cloning() {
    let p = Pipeline::default();
    let data = from_vec(&p, vec!["hello".to_string(), "world".to_string()]);

    // Use a String key which requires cloning
    let keyed = data.key_by(|s: &String| s.chars().next().unwrap().to_string());
    let result = keyed.collect_seq().unwrap();

    assert_eq!(result.len(), 2);
    assert_eq!(result[0].0, "h".to_string());
    assert_eq!(result[1].0, "w".to_string());
}
