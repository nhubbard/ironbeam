use rustflow::node::DynOp;
use rustflow::Partition;
use std::sync::Arc;

/// Test implementation of `DynOp` for testing trait defaults
struct TestDynOp;

impl DynOp for TestDynOp {
    fn apply(&self, input: Partition) -> Partition {
        input
    }
}

/// Test implementation with custom flags
struct KeyPreservingOp;

impl DynOp for KeyPreservingOp {
    fn apply(&self, input: Partition) -> Partition {
        input
    }

    fn key_preserving(&self) -> bool {
        true
    }
}

struct ValueOnlyOp;

impl DynOp for ValueOnlyOp {
    fn apply(&self, input: Partition) -> Partition {
        input
    }

    fn value_only(&self) -> bool {
        true
    }

    fn reorder_safe_with_value_only(&self) -> bool {
        true
    }
}

struct CostHintOp;

impl DynOp for CostHintOp {
    fn apply(&self, input: Partition) -> Partition {
        input
    }

    fn cost_hint(&self) -> u8 {
        5
    }
}

#[test]
fn test_dynop_default_trait_methods() {
    let op = TestDynOp;

    // Test default implementations
    assert!(
        !op.key_preserving(),
        "default key_preserving should be false"
    );
    assert!(!op.value_only(), "default value_only should be false");
    assert!(
        !op.reorder_safe_with_value_only(),
        "default reorder_safe_with_value_only should be false"
    );
    assert_eq!(op.cost_hint(), 10, "default cost_hint should be 10");
}

#[test]
fn test_dynop_key_preserving() {
    let op = KeyPreservingOp;

    assert!(op.key_preserving());
    assert!(!op.value_only());
    assert!(!op.reorder_safe_with_value_only());
    assert_eq!(op.cost_hint(), 10);
}

#[test]
fn test_dynop_value_only() {
    let op = ValueOnlyOp;

    assert!(!op.key_preserving());
    assert!(op.value_only());
    assert!(op.reorder_safe_with_value_only());
    assert_eq!(op.cost_hint(), 10);
}

#[test]
fn test_dynop_cost_hint() {
    let op = CostHintOp;

    assert!(!op.key_preserving());
    assert!(!op.value_only());
    assert!(!op.reorder_safe_with_value_only());
    assert_eq!(op.cost_hint(), 5);
}

#[test]
fn test_dynop_apply() {
    let op = TestDynOp;
    let data = vec![1, 2, 3, 4, 5];
    let partition: Partition = Box::new(data.clone());

    let result = op.apply(partition);
    let result_vec = *result.downcast::<Vec<i32>>().unwrap();

    assert_eq!(result_vec, data);
}

#[test]
fn test_dynop_in_arc() {
    // Test that DynOp can be used in Arc (common usage pattern)
    let op: Arc<dyn DynOp> = Arc::new(TestDynOp);

    assert!(!op.key_preserving());
    assert!(!op.value_only());
    assert_eq!(op.cost_hint(), 10);

    let data = vec![10, 20, 30];
    let partition: Partition = Box::new(data.clone());
    let result = op.apply(partition);
    let result_vec = *result.downcast::<Vec<i32>>().unwrap();

    assert_eq!(result_vec, data);
}

#[test]
fn test_dynop_multiple_implementations() {
    // Test that we can have multiple DynOp implementations with different behaviors
    let ops: Vec<Box<dyn DynOp>> = vec![
        Box::new(TestDynOp),
        Box::new(KeyPreservingOp),
        Box::new(ValueOnlyOp),
        Box::new(CostHintOp),
    ];

    assert_eq!(ops[0].cost_hint(), 10);
    assert!(ops[1].key_preserving());
    assert!(ops[2].value_only());
    assert_eq!(ops[3].cost_hint(), 5);
}
