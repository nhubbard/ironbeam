use ironbeam_compat::{BridgeError, from_vec, prelude::*, v4_to_v5, v5_to_v4};
use ironbeam_core::Pipeline as V5Pipeline;
use ironbeam_direct::JobConfig;
use ironbeam_v4::{Pipeline as V4Pipeline, from_vec as v4_from_vec};

#[test]
fn representative_v4_program_migrates_with_named_v5_nodes() {
    let v4 = V4Pipeline::default();
    let old = v4_from_vec(&v4, vec![1_i32, 2, 3, 4])
        .map(|value| value * 2)
        .filter(|value| value % 4 == 0)
        .collect_seq()
        .unwrap();

    let v5 = V5Pipeline::default();
    let new = from_vec(&v5, "numbers", vec![1_i32, 2, 3, 4])
        .unwrap()
        .map_named("double", |value| value * 2)
        .unwrap()
        .filter_named("divisible-by-four", |value| value % 4 == 0)
        .unwrap()
        .output("result")
        .unwrap()
        .collect_seq()
        .unwrap();
    assert_eq!(new, old);
    assert_eq!(new, vec![4, 8]);
}

#[test]
fn bridges_exchange_bounded_values_in_both_directions() {
    for input in [vec![], vec![7_i32], vec![1, 2, 3, 4]] {
        let v4 = V4Pipeline::default();
        let old = v4_from_vec(&v4, input.clone()).map(|value| value * 2);
        let v5 = V5Pipeline::default();
        let migrated = v4_to_v5(old, &v5, "from-v4")
            .unwrap()
            .filter_named("positive", |value| *value > 0)
            .unwrap();
        let expected: Vec<_> = input.iter().map(|value| value * 2).collect();
        assert_eq!(
            migrated.output("v5-output").unwrap().collect_seq().unwrap(),
            expected
        );

        let destination = V4Pipeline::default();
        let restored = v5_to_v4(
            &migrated,
            &destination,
            "bridge-output",
            &JobConfig::default(),
        )
        .unwrap();
        assert_eq!(restored.collect_seq().unwrap(), expected);
    }
}

#[test]
fn bridge_failures_preserve_the_v5_error_type() {
    let v4 = V4Pipeline::default();
    let old = v4_from_vec(&v4, vec![1_i32]);
    let v5 = V5Pipeline::default();
    v5.from_vec("taken", vec![0_i32]).unwrap();
    assert!(matches!(
        v4_to_v5(old, &v5, "taken"),
        Err(BridgeError::V5(_))
    ));

    let source = v5.from_vec("new", vec![2_i32]).unwrap();
    let destination = V4Pipeline::default();
    let cancelled = JobConfig::default();
    cancelled
        .cancellation
        .store(true, std::sync::atomic::Ordering::Release);
    assert!(matches!(
        v5_to_v4(&source, &destination, "output", &cancelled),
        Err(BridgeError::V5(_))
    ));
}

#[test]
fn bridge_error_reports_source_side() {
    let old = BridgeError::V4(anyhow::anyhow!("old runner failed"));
    assert!(old.to_string().contains("Ironbeam 4"));
    assert!(std::error::Error::source(&old).is_some());

    let v5 = V5Pipeline::default();
    v5.from_vec("taken", vec![0_i32]).unwrap();
    let new = BridgeError::V5(v5.from_vec("taken", vec![1_i32]).err().unwrap());
    assert!(new.to_string().contains("Ironbeam 5"));
    assert!(std::error::Error::source(&new).is_some());
}

#[test]
fn migration_prelude_enables_convenience_methods() {
    let v5 = V5Pipeline::default();
    let counts = from_vec(&v5, "words", vec!["a".to_string(), "a".into()])
        .unwrap()
        .count_per_element_named("counts")
        .unwrap()
        .output("result")
        .unwrap()
        .collect_seq()
        .unwrap();
    assert_eq!(counts, vec![("a".to_string(), 2)]);
    let top = from_vec(&v5, "numbers", vec![3_i32, 1, 4, 2])
        .unwrap()
        .top_k_globally_named("top", 2)
        .unwrap()
        .output("top-out")
        .unwrap()
        .collect_seq()
        .unwrap();
    assert_eq!(top, vec![vec![4, 3]]);
}
