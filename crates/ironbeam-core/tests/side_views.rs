use ironbeam_core::Pipeline;
use std::collections::HashMap;

#[test]
fn singleton_side_view_requires_exactly_one_value() {
    let pipeline = Pipeline::default();
    let main = pipeline.from_vec("main", vec![1_i32]).unwrap();
    let side = pipeline.from_vec("side", vec![10_i32]).unwrap();
    main.map_with_singleton_side_named("single", &side, |value, side| value + side)
        .unwrap();
    let (_, registry) = pipeline.snapshot().unwrap();
    for count in [0, 2] {
        let values = (0..count).map(|value| Box::new(value) as _).collect();
        assert!(
            registry
                .materialize_side_input("single", values)
                .unwrap_err()
                .to_string()
                .contains("exactly one")
        );
    }
    let view = registry
        .materialize_side_input("single", vec![Box::new(10_i32)])
        .unwrap();
    assert_eq!(view.downcast_ref::<i32>(), Some(&10));
}

#[test]
fn map_side_view_rejects_duplicate_keys() {
    let pipeline = Pipeline::default();
    let main = pipeline.from_vec("main", vec!["a".to_owned()]).unwrap();
    let side = pipeline
        .from_vec("side", vec![("a".to_owned(), 1_i32)])
        .unwrap();
    main.map_with_map_side_named("map", &side, |key, view| {
        view.get(&key).copied().unwrap_or_default()
    })
    .unwrap();
    let (_, registry) = pipeline.snapshot().unwrap();
    let values = vec![
        Box::new(("a".to_owned(), 1_i32)) as _,
        Box::new(("a".to_owned(), 2_i32)) as _,
    ];
    assert!(
        registry
            .materialize_side_input("map", values)
            .unwrap_err()
            .to_string()
            .contains("duplicate key")
    );
}

#[test]
fn multimap_side_view_retains_all_values_for_a_key() {
    let pipeline = Pipeline::default();
    let main = pipeline.from_vec("main", vec!["a".to_owned()]).unwrap();
    let side = pipeline
        .from_vec("side", vec![("a".to_owned(), 1_i32)])
        .unwrap();
    main.map_with_multimap_side_named("multimap", &side, |key, view| {
        i32::try_from(view.get(&key).map_or(0, Vec::len)).unwrap()
    })
    .unwrap();
    let (_, registry) = pipeline.snapshot().unwrap();
    let values = vec![
        Box::new(("a".to_owned(), 2_i32)) as _,
        Box::new(("a".to_owned(), 1_i32)) as _,
    ];
    let view = registry.materialize_side_input("multimap", values).unwrap();
    let view = view.downcast_ref::<HashMap<String, Vec<i32>>>().unwrap();
    let mut values = view["a"].clone();
    values.sort_unstable();
    assert_eq!(values, [1, 2]);
}
