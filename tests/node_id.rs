use rustflow::NodeId;
use std::collections::{HashMap, HashSet};

#[test]
fn test_node_id_creation() {
    let id1 = NodeId::new(1);
    let id2 = NodeId::new(2);

    assert_eq!(id1.raw(), 1);
    assert_eq!(id2.raw(), 2);
}

#[test]
fn test_node_id_equality() {
    let id_1 = NodeId::new(1);
    let id_2 = NodeId::new(1);
    let id_3 = NodeId::new(2);

    assert_eq!(id_1, id_2);
    assert_ne!(id_1, id_3);
}

#[test]
fn test_node_id_clone() {
    let id1 = NodeId::new(42);
    let id2 = id1;

    assert_eq!(id1, id2);
    assert_eq!(id1.raw(), id2.raw());
}

#[test]
fn test_node_id_copy() {
    let id1 = NodeId::new(100);
    let id2 = id1; // Copy, not move

    assert_eq!(id1, id2);
    assert_eq!(id1.raw(), 100);
    assert_eq!(id2.raw(), 100);
}

#[test]
fn test_node_id_debug() {
    let id = NodeId::new(123);
    let debug_str = format!("{id:?}");

    assert!(debug_str.contains("NodeId"));
    assert!(debug_str.contains("123"));
}

#[test]
fn test_node_id_hashable() {
    let id1 = NodeId::new(1);
    let id2 = NodeId::new(2);
    let id3 = NodeId::new(1);

    let mut set = HashSet::new();
    set.insert(id1);
    set.insert(id2);
    set.insert(id3); // Should not add duplicate

    assert_eq!(set.len(), 2);
    assert!(set.contains(&id1));
    assert!(set.contains(&id2));
    assert!(set.contains(&id3)); // Same as id1
}

#[test]
fn test_node_id_as_map_key() {
    let id1 = NodeId::new(10);
    let id2 = NodeId::new(20);
    let id3 = NodeId::new(30);

    let mut map = HashMap::new();
    map.insert(id1, "node1");
    map.insert(id2, "node2");
    map.insert(id3, "node3");

    assert_eq!(map.get(&id1), Some(&"node1"));
    assert_eq!(map.get(&id2), Some(&"node2"));
    assert_eq!(map.get(&id3), Some(&"node3"));
    assert_eq!(map.len(), 3);
}

#[test]
fn test_node_id_ordering() {
    let id1 = NodeId::new(1);
    let id2 = NodeId::new(2);
    let id3 = NodeId::new(1);

    assert_eq!(id1, id3);
    assert_ne!(id1, id2);
}

#[test]
fn test_node_id_large_values() {
    let id_max = NodeId::new(u64::MAX);
    let id_large = NodeId::new(u64::MAX - 1);

    assert_eq!(id_max.raw(), u64::MAX);
    assert_eq!(id_large.raw(), u64::MAX - 1);
    assert_ne!(id_max, id_large);
}

#[test]
fn test_node_id_zero() {
    let id_zero = NodeId::new(0);
    assert_eq!(id_zero.raw(), 0);
}

#[test]
fn test_node_id_sequential() {
    // Simulate sequential ID generation
    let ids: Vec<NodeId> = (0..100).map(NodeId::new).collect();

    for (i, id) in ids.iter().enumerate() {
        assert_eq!(id.raw(), i as u64);
    }

    // All should be unique
    let unique: HashSet<_> = ids.iter().collect();
    assert_eq!(unique.len(), 100);
}
