// Unit tests for src/sampling.rs.
use super::Ranked;
use std::cmp::Ordering;

#[test]
fn equal_priorities_keep_earlier_input_first() {
    let first = Ranked {
        priority_bits: 7,
        index: 0,
        value: "first",
    };
    let second = Ranked {
        priority_bits: 7,
        index: 1,
        value: "second",
    };
    let same_rank = Ranked {
        priority_bits: 7,
        index: 0,
        value: "other",
    };
    assert!(first == same_rank);
    assert!(first != second);
    assert_eq!(first.cmp(&second), Ordering::Greater);
    assert_eq!(first.partial_cmp(&second), Some(Ordering::Greater));
}
