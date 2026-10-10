use ironbeam_direct::JobConfig;
use ironbeam_testing::{PAssert, PCollectionDebugExt, TestDataBuilder, TestPipeline};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

#[test]
fn test_pipeline_and_debug_helpers_use_v5_direct() {
    let pipeline = TestPipeline::new();
    let values = TestDataBuilder::new()
        .add_value(1_i32)
        .add_repeated(2, 2)
        .build();
    let input = pipeline.from_vec("values", values).unwrap();
    let observed = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&observed);
    let debugged = input
        .debug_inspect_with_named("inspect", "values", move |_| {
            counter.fetch_add(1, Ordering::Relaxed);
        })
        .unwrap()
        .debug_sample_named("sample", "first", 2)
        .unwrap();
    assert_eq!(pipeline.node_count().unwrap(), 3);
    assert_eq!(pipeline.edge_count().unwrap(), 2);
    assert_eq!(observed.load(Ordering::Relaxed), 0);
    let output = debugged
        .collect_debug_count_named("out", "count", &JobConfig::default())
        .unwrap();
    PAssert::that(&output)
        .has_count(3)
        .unwrap()
        .contains_in_any_order(&[1, 2, 2])
        .unwrap();
    assert_eq!(observed.load(Ordering::Relaxed), 3);
}
