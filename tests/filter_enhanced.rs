//! Comprehensive tests for enhanced filter operations.
//!
//! This test suite validates the convenience filter methods added in feature 1.3,
//! including comparison operators and the `filter_by` method for struct fields.

use ironbeam::*;

/// Test basic equality filtering
#[test]
fn test_filter_eq() {
    let p = Pipeline::default();
    let numbers = from_vec(&p, vec![1, 2, 3, 2, 1, 4, 2]);
    let twos = numbers.filter_eq(&2);
    let result = twos.collect_seq().unwrap();
    assert_eq!(result, vec![2, 2, 2]);
}

/// Test inequality filtering
#[test]
fn test_filter_ne() {
    let p = Pipeline::default();
    let numbers = from_vec(&p, vec![1, 2, 3, 2, 1]);
    let not_twos = numbers.filter_ne(&2);
    let mut result = not_twos.collect_seq().unwrap();
    result.sort_unstable();
    assert_eq!(result, vec![1, 1, 3]);
}

/// Test less-than filtering with integers
#[test]
fn test_filter_lt_integers() {
    let p = Pipeline::default();
    let numbers = from_vec(&p, vec![1, 5, 10, 15, 20, 25]);
    let small = numbers.filter_lt(&10);
    let mut result = small.collect_seq().unwrap();
    result.sort_unstable();
    assert_eq!(result, vec![1, 5]);
}

/// Test less-than filtering with floats
#[test]
fn test_filter_lt_floats() {
    let p = Pipeline::default();
    let numbers = from_vec(&p, vec![1.5, 5.5, 10.0, 15.5, 20.0]);
    let small = numbers.filter_lt(&10.0);
    let mut result = small.collect_seq().unwrap();
    result.sort_by(|a, b| a.partial_cmp(b).unwrap());
    assert_eq!(result, vec![1.5, 5.5]);
}

/// Test less-than-or-equal filtering
#[test]
fn test_filter_le() {
    let p = Pipeline::default();
    let numbers = from_vec(&p, vec![1, 5, 10, 15, 20]);
    let small = numbers.filter_le(&10);
    let mut result = small.collect_seq().unwrap();
    result.sort_unstable();
    assert_eq!(result, vec![1, 5, 10]);
}

/// Test greater-than filtering
#[test]
fn test_filter_gt() {
    let p = Pipeline::default();
    let numbers = from_vec(&p, vec![1, 5, 10, 15, 20, 25]);
    let large = numbers.filter_gt(&10);
    let mut result = large.collect_seq().unwrap();
    result.sort_unstable();
    assert_eq!(result, vec![15, 20, 25]);
}

/// Test greater-than-or-equal filtering
#[test]
fn test_filter_ge() {
    let p = Pipeline::default();
    let numbers = from_vec(&p, vec![1, 5, 10, 15, 20]);
    let large = numbers.filter_ge(&10);
    let mut result = large.collect_seq().unwrap();
    result.sort_unstable();
    assert_eq!(result, vec![10, 15, 20]);
}

/// Test range filtering [min, max)
#[test]
fn test_filter_range() {
    let p = Pipeline::default();
    let numbers = from_vec(&p, vec![1, 5, 10, 15, 20, 25]);
    let mid = numbers.filter_range(&10, &20);
    let mut result = mid.collect_seq().unwrap();
    result.sort_unstable();
    assert_eq!(result, vec![10, 15]);
}

/// Test inclusive range filtering [min, max]
#[test]
fn test_filter_range_inclusive() {
    let p = Pipeline::default();
    let numbers = from_vec(&p, vec![1, 5, 10, 15, 20, 25]);
    let mid = numbers.filter_range_inclusive(&10, &20);
    let mut result = mid.collect_seq().unwrap();
    result.sort_unstable();
    assert_eq!(result, vec![10, 15, 20]);
}

/// Test range filtering with edge cases
#[test]
fn test_filter_range_edge_cases() {
    let p = Pipeline::default();
    let numbers = from_vec(&p, vec![1, 2, 3, 4, 5]);

    // Empty range
    let empty = numbers.clone().filter_range(&5, &5);
    assert_eq!(empty.collect_seq().unwrap().len(), 0);

    // Single element range
    let single = numbers.clone().filter_range(&3, &4);
    assert_eq!(single.collect_seq().unwrap(), vec![3]);

    // Full range
    let all = numbers.filter_range(&0, &10);
    let mut result = all.collect_seq().unwrap();
    result.sort_unstable();
    assert_eq!(result, vec![1, 2, 3, 4, 5]);
}

/// Test string filtering
#[test]
fn test_filter_strings() {
    let p = Pipeline::default();
    let words = from_vec(&p, vec!["apple", "banana", "cherry", "date"]);

    // Test equality
    let bananas = words.clone().filter_eq(&"banana");
    assert_eq!(bananas.collect_seq().unwrap(), vec!["banana"]);

    // Test less-than (lexicographic)
    let before_cherry = words.clone().filter_lt(&"cherry");
    let mut result = before_cherry.collect_seq().unwrap();
    result.sort_unstable();
    assert_eq!(result, vec!["apple", "banana"]);

    // Test greater-than
    let after_banana = words.filter_gt(&"banana");
    let mut result = after_banana.collect_seq().unwrap();
    result.sort_unstable();
    assert_eq!(result, vec!["cherry", "date"]);
}

/// Test filtering with custom struct - basic field access
#[test]
fn test_filter_by_struct_field() {
    #[derive(Clone, Debug, PartialEq)]
    struct Person {
        name: String,
        age: u32,
    }

    let p = Pipeline::default();
    let people = from_vec(&p, vec![
        Person { name: "Alice".into(), age: 25 },
        Person { name: "Bob".into(), age: 17 },
        Person { name: "Carol".into(), age: 30 },
        Person { name: "David".into(), age: 15 },
    ]);

    // Filter adults (age >= 18)
    let adults = people.filter_by(|person| person.age, |age| *age >= 18);
    let result = adults.collect_seq().unwrap();
    assert_eq!(result.len(), 2);
    assert_eq!(result[0].name, "Alice");
    assert_eq!(result[1].name, "Carol");
}

/// Test filtering with computed value (string length)
#[test]
fn test_filter_by_computed_value() {
    let p = Pipeline::default();
    let words = from_vec(&p, vec![
        "hi".to_string(),
        "hello".to_string(),
        "world".to_string(),
        "rust".to_string(),
        "programming".to_string(),
    ]);

    // Find words with length > 4
    let long_words = words.filter_by(String::len, |len| *len > 4);
    let mut result = long_words.collect_seq().unwrap();
    result.sort_unstable();
    assert_eq!(result, vec!["hello", "programming", "world"]);
}

/// Test filtering with complex struct
#[test]
fn test_filter_by_complex_struct() {
    #[derive(Clone, Debug)]
    struct Product {
        name: String,
        price: f64,
        in_stock: bool,
    }

    let p = Pipeline::default();
    let products = from_vec(&p, vec![
        Product { name: "Book".into(), price: 15.99, in_stock: true },
        Product { name: "Pen".into(), price: 2.50, in_stock: true },
        Product { name: "Laptop".into(), price: 899.99, in_stock: false },
        Product { name: "Mouse".into(), price: 25.00, in_stock: true },
    ]);

    // Find affordable products in stock (price < 20 and in_stock)
    let affordable_in_stock = products
        .filter_by(|p| p.in_stock, |in_stock| *in_stock)
        .filter_by(|p| p.price, |price| *price < 20.0);

    let result = affordable_in_stock.collect_seq().unwrap();
    assert_eq!(result.len(), 2);

    // Verify the correct products
    let names: Vec<String> = result.iter().map(|p| p.name.clone()).collect();
    assert!(names.contains(&"Book".to_string()));
    assert!(names.contains(&"Pen".to_string()));
}

/// Test chaining multiple filter operations
#[test]
fn test_filter_chaining() {
    let p = Pipeline::default();
    let numbers = from_vec(&p, vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);

    // Chain multiple filters: > 3 and < 8 and != 5
    let result = numbers
        .filter_gt(&3)
        .filter_lt(&8)
        .filter_ne(&5);

    let mut values = result.collect_seq().unwrap();
    values.sort_unstable();
    assert_eq!(values, vec![4, 6, 7]);
}

/// Test filter with empty collection
#[test]
fn test_filter_empty_collection() {
    let p = Pipeline::default();
    let empty: Vec<i32> = vec![];
    let numbers = from_vec(&p, empty);

    let result = numbers.filter_gt(&5);
    assert_eq!(result.collect_seq().unwrap().len(), 0);
}

/// Test filter that matches nothing
#[test]
fn test_filter_matches_nothing() {
    let p = Pipeline::default();
    let numbers = from_vec(&p, vec![1, 2, 3, 4, 5]);

    let result = numbers.filter_gt(&100);
    assert_eq!(result.collect_seq().unwrap().len(), 0);
}

/// Test filter that matches everything
#[test]
fn test_filter_matches_everything() {
    let p = Pipeline::default();
    let numbers = from_vec(&p, vec![1, 2, 3, 4, 5]);

    let result = numbers.filter_gt(&0);
    let mut values = result.collect_seq().unwrap();
    values.sort_unstable();
    assert_eq!(values, vec![1, 2, 3, 4, 5]);
}

/// Test `filter_by` with Option extraction
#[test]
fn test_filter_by_option_field() {
    #[derive(Clone)]
    struct Record {
        id: u32,
        value: Option<i32>,
    }

    let p = Pipeline::default();
    let records = from_vec(&p, vec![
        Record { id: 1, value: Some(10) },
        Record { id: 2, value: None },
        Record { id: 3, value: Some(20) },
        Record { id: 4, value: Some(5) },
    ]);

    // Filter records with Some value > 7
    let filtered = records.filter_by(
        |r| r.value,
        |opt| opt.is_some() && opt.unwrap() > 7
    );

    let result = filtered.collect_seq().unwrap();
    assert_eq!(result.len(), 2);
    assert_eq!(result[0].id, 1);
    assert_eq!(result[1].id, 3);
}

/// Test `filter_by` with nested struct
#[test]
fn test_filter_by_nested_struct() {
    #[derive(Clone)]
    struct Address {
        city: String,
        zip: u32,
    }

    #[derive(Clone)]
    struct Customer {
        name: String,
        address: Address,
    }

    let p = Pipeline::default();
    let customers = from_vec(&p, vec![
        Customer {
            name: "Alice".into(),
            address: Address { city: "NYC".into(), zip: 10001 }
        },
        Customer {
            name: "Bob".into(),
            address: Address { city: "LA".into(), zip: 90001 }
        },
        Customer {
            name: "Carol".into(),
            address: Address { city: "NYC".into(), zip: 10002 }
        },
    ]);

    // Find customers in NYC
    let nyc_customers = customers.filter_by(
        |c| c.address.city.clone(),
        |city| city == "NYC"
    );

    let result = nyc_customers.collect_seq().unwrap();
    assert_eq!(result.len(), 2);
    assert_eq!(result[0].name, "Alice");
    assert_eq!(result[0].address.zip, 10001);
    assert_eq!(result[1].name, "Carol");
    assert_eq!(result[1].address.zip, 10002);
}

/// Test combining `filter_by` with regular filter
#[test]
fn test_filter_by_with_regular_filter() {
    #[derive(Clone)]
    struct Score {
        player: String,
        points: u32,
    }

    let p = Pipeline::default();
    let scores = from_vec(&p, vec![
        Score { player: "Alice".into(), points: 100 },
        Score { player: "Bob".into(), points: 50 },
        Score { player: "Carol".into(), points: 150 },
        Score { player: "David".into(), points: 75 },
    ]);

    // Filter by points > 60 and name starts with 'A' or 'C'
    let filtered = scores
        .filter_by(|s| s.points, |p| *p > 60)
        .filter(|s| s.player.starts_with('A') || s.player.starts_with('C'));

    let result = filtered.collect_seq().unwrap();
    assert_eq!(result.len(), 2);
}

/// Test filter operations work correctly in parallel mode
#[test]
fn test_filter_parallel_execution() {
    let p = Pipeline::default();
    let numbers = from_vec(&p, (1..=1000).collect::<Vec<i32>>());

    let even = numbers.filter_by(|n| n % 2, |remainder| *remainder == 0);
    let large_even = even.filter_gt(&500);

    let mut result = large_even.collect_par(Some(4), Some(4)).unwrap();
    result.sort_unstable();

    // Should have 250 even numbers from 502 to 1000
    assert_eq!(result.len(), 250);
    assert_eq!(result[0], 502);
    assert_eq!(result[result.len() - 1], 1000);
}

/// Test filter methods preserve type safety
#[test]
fn test_filter_type_safety() {
    let p = Pipeline::default();

    // This should compile - i32 implements PartialOrd
    let numbers = from_vec(&p, vec![1, 2, 3]);
    let _ = numbers.filter_gt(&2);

    // This should compile - &str implements PartialOrd and PartialEq
    let strings = from_vec(&p, vec!["a", "b", "c"]);
    let _ = strings.filter_eq(&"b");
}

/// Test `filter_range` with negative numbers
#[test]
fn test_filter_range_negative() {
    let p = Pipeline::default();
    let numbers = from_vec(&p, vec![-10, -5, 0, 5, 10]);

    let result = numbers.filter_range(&-7, &7);
    let mut values = result.collect_seq().unwrap();
    values.sort_unstable();
    assert_eq!(values, vec![-5, 0, 5]);
}

/// Test filter operations with tuples
#[test]
fn test_filter_tuples() {
    let p = Pipeline::default();
    let data = from_vec(&p, vec![
        ("apple", 5),
        ("banana", 3),
        ("cherry", 8),
        ("date", 2),
    ]);

    // Filter by second element (count) > 3
    let popular = data.filter_by(|item| item.1, |count| *count > 3);
    let result = popular.collect_seq().unwrap();
    assert_eq!(result.len(), 2);
    assert_eq!(result[0].0, "apple");
    assert_eq!(result[1].0, "cherry");
}
