use ironbeam::testing::PAssert;

// ── is_empty ──────────────────────────────────────────────────────────────────

#[test]
fn test_is_empty_passes_for_empty_collection() {
    let empty: Vec<i32> = vec![];
    PAssert::that(&empty).is_empty().unwrap();
}

#[test]
fn test_is_empty_fails_for_non_empty_collection() {
    let data = vec![1, 2, 3];
    let err = PAssert::that(&data).is_empty().unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("3"), "error should mention element count");
}

// ── has_count ─────────────────────────────────────────────────────────────────

#[test]
fn test_has_count_passes_with_correct_count() {
    let data = vec![10, 20, 30];
    PAssert::that(&data).has_count(3).unwrap();
}

#[test]
fn test_has_count_passes_for_empty_collection() {
    let empty: Vec<i32> = vec![];
    PAssert::that(&empty).has_count(0).unwrap();
}

#[test]
fn test_has_count_fails_when_too_few_elements() {
    let data = vec![1];
    let err = PAssert::that(&data).has_count(5).unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains('5'), "error should mention expected count");
    assert!(msg.contains('1'), "error should mention actual count");
}

#[test]
fn test_has_count_fails_when_too_many_elements() {
    let data = vec![1, 2, 3, 4, 5];
    let err = PAssert::that(&data).has_count(2).unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains('2'), "error should mention expected count");
    assert!(msg.contains('5'), "error should mention actual count");
}

// ── all_match ─────────────────────────────────────────────────────────────────

#[test]
fn test_all_match_passes_when_all_satisfy_predicate() {
    let data = vec![2, 4, 6, 8];
    PAssert::that(&data).all_match(|x| x % 2 == 0).unwrap();
}

#[test]
fn test_all_match_passes_for_empty_collection() {
    let empty: Vec<i32> = vec![];
    PAssert::that(&empty).all_match(|_| false).unwrap();
}

#[test]
fn test_all_match_fails_for_first_violating_element() {
    let data = vec![2, 3, 4];
    let err = PAssert::that(&data).all_match(|x| x % 2 == 0).unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains('3'), "error should mention the failing element");
    assert!(msg.contains('1'), "error should mention the index");
}

#[test]
fn test_all_match_fails_on_last_element() {
    let data = vec![2, 4, 7];
    let err = PAssert::that(&data).all_match(|x| x % 2 == 0).unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains('7'), "error should mention the failing element");
}

// ── contains_in_any_order ─────────────────────────────────────────────────────

#[test]
fn test_contains_in_any_order_passes_for_same_order() {
    let data = vec![1, 2, 3];
    PAssert::that(&data)
        .contains_in_any_order(&[1, 2, 3])
        .unwrap();
}

#[test]
fn test_contains_in_any_order_passes_for_different_order() {
    let data = vec![3, 1, 2];
    PAssert::that(&data)
        .contains_in_any_order(&[1, 2, 3])
        .unwrap();
}

#[test]
fn test_contains_in_any_order_passes_for_empty_collections() {
    let empty: Vec<i32> = vec![];
    PAssert::that(&empty)
        .contains_in_any_order(&[])
        .unwrap();
}

#[test]
fn test_contains_in_any_order_passes_with_duplicates() {
    let data = vec![1, 1, 2];
    PAssert::that(&data)
        .contains_in_any_order(&[1, 2, 1])
        .unwrap();
}

#[test]
fn test_contains_in_any_order_fails_for_missing_element() {
    let data = vec![1, 2];
    let err = PAssert::that(&data)
        .contains_in_any_order(&[1, 2, 3])
        .unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains('3'), "error should mention the missing element");
}

#[test]
fn test_contains_in_any_order_fails_for_extra_element() {
    let data = vec![1, 2, 3, 4];
    let err = PAssert::that(&data)
        .contains_in_any_order(&[1, 2, 3])
        .unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains('4'), "error should mention the extra element");
}

#[test]
fn test_contains_in_any_order_fails_for_wrong_duplicate_count() {
    // actual has two 1s; expected has only one 1
    let data = vec![1, 1, 2];
    let err = PAssert::that(&data)
        .contains_in_any_order(&[1, 2])
        .unwrap_err();
    let msg = err.to_string();
    assert!(!msg.is_empty());
}

#[test]
fn test_contains_in_any_order_fails_for_completely_different_elements() {
    let data = vec![4, 5, 6];
    let err = PAssert::that(&data)
        .contains_in_any_order(&[1, 2, 3])
        .unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("Missing"), "error should mention missing elements");
    assert!(msg.contains("Extra"), "error should mention extra elements");
}

// ── chaining ──────────────────────────────────────────────────────────────────

#[test]
fn test_chaining_multiple_assertions() {
    let data = vec![2, 4, 6];
    PAssert::that(&data)
        .has_count(3)
        .unwrap()
        .contains_in_any_order(&[6, 4, 2])
        .unwrap()
        .all_match(|x| x % 2 == 0)
        .unwrap();
}

#[test]
fn test_chaining_with_question_mark_operator() -> anyhow::Result<()> {
    let data = vec![10, 20, 30];
    PAssert::that(&data)
        .has_count(3)?
        .contains_in_any_order(&[30, 10, 20])?
        .all_match(|x| *x > 0)?;
    Ok(())
}

#[test]
fn test_chaining_short_circuits_on_first_failure() {
    let data = vec![1, 2, 3];
    // has_count(5) fails immediately; contains_in_any_order is never reached
    let err = PAssert::that(&data)
        .has_count(5)
        .and_then(|pa| pa.contains_in_any_order(&[1, 2, 3]))
        .unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains('5'));
}

// ── string collections ────────────────────────────────────────────────────────

#[test]
fn test_passert_works_with_string_slices() {
    let data = vec!["foo", "bar", "baz"];
    PAssert::that(&data)
        .has_count(3)
        .unwrap()
        .contains_in_any_order(&["baz", "foo", "bar"])
        .unwrap();
}

#[test]
fn test_passert_works_with_owned_strings() {
    let data = vec![String::from("hello"), String::from("world")];
    PAssert::that(&data)
        .has_count(2)
        .unwrap()
        .contains_in_any_order(&[String::from("world"), String::from("hello")])
        .unwrap();
}
