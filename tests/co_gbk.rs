//! Tests for multi-way `CoGroupByKey` operations

use ironbeam::*;
use anyhow::Result;

#[test]
fn test_cogroup_2_way() -> Result<()> {
    let p = Pipeline::default();

    let c1: PCollection<(String, u32)> = from_vec(
        &p,
        vec![
            ("a".to_string(), 1),
            ("a".to_string(), 2),
            ("b".to_string(), 3),
        ],
    );

    let c2: PCollection<(String, String)> = from_vec(
        &p,
        vec![
            ("a".to_string(), "x".to_string()),
            ("c".to_string(), "y".to_string()),
        ],
    );

    let result = cogroup_by_key!(c1, c2);

    let mut output = result.collect_seq()?;
    output.sort_by(|a, b| a.0.cmp(&b.0));

    assert_eq!(output.len(), 3);

    // Key "a" has values from both collections
    assert_eq!(output[0].0, "a");
    let (ref v1s, ref v2s) = output[0].1;
    assert_eq!(v1s.len(), 2);
    assert!(v1s.contains(&1));
    assert!(v1s.contains(&2));
    assert_eq!(v2s.len(), 1);
    assert!(v2s.contains(&"x".to_string()));

    // Key "b" has values only from first collection
    assert_eq!(output[1].0, "b");
    let (ref v1s, ref v2s) = output[1].1;
    assert_eq!(v1s.len(), 1);
    assert!(v1s.contains(&3));
    assert_eq!(v2s.len(), 0);

    // Key "c" has values only from second collection
    assert_eq!(output[2].0, "c");
    let (ref v1s, ref v2s) = output[2].1;
    assert_eq!(v1s.len(), 0);
    assert_eq!(v2s.len(), 1);
    assert!(v2s.contains(&"y".to_string()));

    Ok(())
}

#[test]
fn test_cogroup_3_way() -> Result<()> {
    let p = Pipeline::default();

    let users: PCollection<(String, String)> = from_vec(
        &p,
        vec![
            ("user1".to_string(), "Alice".to_string()),
            ("user2".to_string(), "Bob".to_string()),
        ],
    );

    let orders: PCollection<(String, u32)> = from_vec(
        &p,
        vec![
            ("user1".to_string(), 100),
            ("user1".to_string(), 200),
            ("user3".to_string(), 300),
        ],
    );

    let addresses: PCollection<(String, String)> = from_vec(
        &p,
        vec![
            ("user1".to_string(), "123 Main St".to_string()),
            ("user2".to_string(), "456 Oak Ave".to_string()),
        ],
    );

    let result = cogroup_by_key!(users, orders, addresses);

    let mut output = result.collect_seq()?;
    output.sort_by(|a, b| a.0.cmp(&b.0));

    assert_eq!(output.len(), 3);

    // user1 has values in all three collections
    assert_eq!(output[0].0, "user1");
    let (ref users_vals, ref orders_vals, ref addrs_vals) = output[0].1;
    assert_eq!(users_vals.len(), 1);
    assert_eq!(orders_vals.len(), 2);
    assert_eq!(addrs_vals.len(), 1);

    // user2 has values only in users and addresses
    assert_eq!(output[1].0, "user2");
    let (ref users_vals, ref orders_vals, ref addrs_vals) = output[1].1;
    assert_eq!(users_vals.len(), 1);
    assert_eq!(orders_vals.len(), 0);
    assert_eq!(addrs_vals.len(), 1);

    // user3 has values only in orders
    assert_eq!(output[2].0, "user3");
    let (ref users_vals, ref orders_vals, ref addrs_vals) = output[2].1;
    assert_eq!(users_vals.len(), 0);
    assert_eq!(orders_vals.len(), 1);
    assert_eq!(addrs_vals.len(), 0);

    Ok(())
}

#[test]
fn test_cogroup_4_way() -> Result<()> {
    let p = Pipeline::default();

    let c1: PCollection<(i32, String)> = from_vec(&p, vec![(1, "a".to_string())]);
    let c2: PCollection<(i32, String)> = from_vec(&p, vec![(1, "b".to_string())]);
    let c3: PCollection<(i32, String)> = from_vec(&p, vec![(1, "c".to_string())]);
    let c4: PCollection<(i32, String)> = from_vec(&p, vec![(1, "d".to_string())]);

    let result = cogroup_by_key!(c1, c2, c3, c4);

    let output = result.collect_seq()?;

    assert_eq!(output.len(), 1);
    assert_eq!(output[0].0, 1);

    let (ref v1, ref v2, ref v3, ref v4) = output[0].1;
    assert_eq!(v1.len(), 1);
    assert_eq!(v2.len(), 1);
    assert_eq!(v3.len(), 1);
    assert_eq!(v4.len(), 1);

    Ok(())
}

#[test]
fn test_cogroup_5_way() -> Result<()> {
    let p = Pipeline::default();

    let c1: PCollection<(i32, u32)> = from_vec(&p, vec![(1, 10)]);
    let c2: PCollection<(i32, u32)> = from_vec(&p, vec![(1, 20)]);
    let c3: PCollection<(i32, u32)> = from_vec(&p, vec![(1, 30)]);
    let c4: PCollection<(i32, u32)> = from_vec(&p, vec![(1, 40)]);
    let c5: PCollection<(i32, u32)> = from_vec(&p, vec![(1, 50)]);

    let result = cogroup_by_key!(c1, c2, c3, c4, c5);

    let output = result.collect_seq()?;

    assert_eq!(output.len(), 1);
    assert_eq!(output[0].0, 1);

    let (ref v1, ref v2, ref v3, ref v4, ref v5) = output[0].1;
    assert_eq!(v1[0], 10);
    assert_eq!(v2[0], 20);
    assert_eq!(v3[0], 30);
    assert_eq!(v4[0], 40);
    assert_eq!(v5[0], 50);

    Ok(())
}

#[test]
fn test_cogroup_6_way() -> Result<()> {
    let p = Pipeline::default();

    let c1: PCollection<(i32, u32)> = from_vec(&p, vec![(1, 10)]);
    let c2: PCollection<(i32, u32)> = from_vec(&p, vec![(1, 20)]);
    let c3: PCollection<(i32, u32)> = from_vec(&p, vec![(1, 30)]);
    let c4: PCollection<(i32, u32)> = from_vec(&p, vec![(1, 40)]);
    let c5: PCollection<(i32, u32)> = from_vec(&p, vec![(1, 50)]);
    let c6: PCollection<(i32, u32)> = from_vec(&p, vec![(1, 60)]);

    let result = cogroup_by_key!(c1, c2, c3, c4, c5, c6);

    let output = result.collect_seq()?;

    assert_eq!(output.len(), 1);
    let (ref v1, ref v2, ref v3, ref v4, ref v5, ref v6) = output[0].1;
    assert_eq!(v1[0], 10);
    assert_eq!(v2[0], 20);
    assert_eq!(v3[0], 30);
    assert_eq!(v4[0], 40);
    assert_eq!(v5[0], 50);
    assert_eq!(v6[0], 60);

    Ok(())
}

#[test]
fn test_cogroup_with_custom_join_logic() -> Result<()> {
    let p = Pipeline::default();

    let users: PCollection<(String, String)> = from_vec(
        &p,
        vec![
            ("user1".to_string(), "Alice".to_string()),
            ("user2".to_string(), "Bob".to_string()),
        ],
    );

    let orders: PCollection<(String, u32)> = from_vec(
        &p,
        vec![
            ("user1".to_string(), 100),
            ("user1".to_string(), 200),
        ],
    );

    let addresses: PCollection<(String, String)> = from_vec(
        &p,
        vec![("user1".to_string(), "123 Main St".to_string())],
    );

    let cogroup = cogroup_by_key!(users, orders, addresses);

    // Custom join logic: only users with both orders and addresses
    let results = cogroup.flat_map(|(user_id, (users, orders, addrs))| {
        if !users.is_empty() && !orders.is_empty() && !addrs.is_empty() {
            let user_name = &users[0];
            let total_orders = orders.iter().sum::<u32>();
            let address = &addrs[0];
            vec![format!(
                "{}: {} at {} with {} total orders",
                user_id, user_name, address, total_orders
            )]
        } else {
            vec![]
        }
    });

    let output = results.collect_seq()?;

    assert_eq!(output.len(), 1);
    assert_eq!(
        output[0],
        "user1: Alice at 123 Main St with 300 total orders"
    );

    Ok(())
}

#[test]
fn test_cogroup_empty_collections() -> Result<()> {
    let p = Pipeline::default();

    let c1: PCollection<(String, u32)> = from_vec(&p, vec![]);
    let c2: PCollection<(String, String)> = from_vec(&p, vec![]);

    let result = cogroup_by_key!(c1, c2);

    let output = result.collect_seq()?;

    assert_eq!(output.len(), 0);

    Ok(())
}

#[test]
fn test_cogroup_with_multiple_values_per_key() -> Result<()> {
    let p = Pipeline::default();

    let c1: PCollection<(i32, String)> = from_vec(
        &p,
        vec![
            (1, "a1".to_string()),
            (1, "a2".to_string()),
            (1, "a3".to_string()),
        ],
    );

    let c2: PCollection<(i32, String)> = from_vec(
        &p,
        vec![
            (1, "b1".to_string()),
            (1, "b2".to_string()),
        ],
    );

    let result = cogroup_by_key!(c1, c2);

    let output = result.collect_seq()?;

    assert_eq!(output.len(), 1);
    assert_eq!(output[0].0, 1);

    let (ref v1s, ref v2s) = output[0].1;
    assert_eq!(v1s.len(), 3);
    assert_eq!(v2s.len(), 2);

    Ok(())
}

#[test]
fn test_cogroup_10_way() -> Result<()> {
    let p = Pipeline::default();

    let c1: PCollection<(i32, u32)> = from_vec(&p, vec![(1, 10)]);
    let c2: PCollection<(i32, u32)> = from_vec(&p, vec![(1, 20)]);
    let c3: PCollection<(i32, u32)> = from_vec(&p, vec![(1, 30)]);
    let c4: PCollection<(i32, u32)> = from_vec(&p, vec![(1, 40)]);
    let c5: PCollection<(i32, u32)> = from_vec(&p, vec![(1, 50)]);
    let c6: PCollection<(i32, u32)> = from_vec(&p, vec![(1, 60)]);
    let c7: PCollection<(i32, u32)> = from_vec(&p, vec![(1, 70)]);
    let c8: PCollection<(i32, u32)> = from_vec(&p, vec![(1, 80)]);
    let c9: PCollection<(i32, u32)> = from_vec(&p, vec![(1, 90)]);
    let c10: PCollection<(i32, u32)> = from_vec(&p, vec![(1, 100)]);

    let result = cogroup_by_key!(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10);

    let output = result.collect_seq()?;

    assert_eq!(output.len(), 1);
    let (ref v1, ref v2, ref v3, ref v4, ref v5, ref v6, ref v7, ref v8, ref v9, ref v10) =
        output[0].1;

    assert_eq!(v1[0], 10);
    assert_eq!(v2[0], 20);
    assert_eq!(v3[0], 30);
    assert_eq!(v4[0], 40);
    assert_eq!(v5[0], 50);
    assert_eq!(v6[0], 60);
    assert_eq!(v7[0], 70);
    assert_eq!(v8[0], 80);
    assert_eq!(v9[0], 90);
    assert_eq!(v10[0], 100);

    Ok(())
}
