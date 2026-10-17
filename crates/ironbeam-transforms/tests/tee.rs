use ironbeam_core::{Element, Pipeline};
use ironbeam_direct::{DirectRunner, JobConfig};
use ironbeam_transforms::TeeExt;

struct NonClone(i32);
impl Element for NonClone {
    const TYPE_ID: &'static str = "test/non-clone";
    const CODER_ID: &'static str = "test/non-clone";
}

#[test]
fn tee_clones_graph_handles_without_cloning_elements() {
    let pipeline = Pipeline::default();
    let input = pipeline
        .from_vec("input", vec![1_i32, 2])
        .unwrap()
        .map_named("non-clone", NonClone)
        .unwrap();
    assert!(input.tee_n(0).is_empty());
    assert_eq!(input.tee_n(3).len(), 3);
    let (left, right) = input.tee();
    let left_output = left
        .map_named("left", |value| value.0 + 1)
        .unwrap()
        .output("left-out")
        .unwrap();
    let right_output = right
        .map_named("right", |value| value.0 * 2)
        .unwrap()
        .output("right-out")
        .unwrap();
    assert_eq!(
        DirectRunner
            .run_collect(&left_output, &JobConfig::default())
            .unwrap()
            .0,
        [2, 3]
    );
    assert_eq!(
        DirectRunner
            .run_collect(&right_output, &JobConfig::default())
            .unwrap()
            .0,
        [2, 4]
    );
}
