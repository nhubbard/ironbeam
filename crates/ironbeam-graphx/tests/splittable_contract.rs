use ironbeam_core::{OffsetRange, Pipeline, Version};
use ironbeam_graphx::v5::translate_v5;

#[test]
fn user_splittable_transform_does_not_lower_as_an_ordinary_pardo() {
    let pipeline = Pipeline::default();
    pipeline
        .from_vec("input", vec![2_i32])
        .unwrap()
        .splittable_flat_map_named(
            "offsets",
            "tests/portable-offsets",
            Version { major: 1, minor: 0 },
            |_| OffsetRange::new(0, 2),
            |_, context| {
                for offset in 0..2 {
                    assert!(context.try_claim(offset)?);
                    context.emit(offset, offset * 1_000_000)?;
                }
                Ok(())
            },
        )
        .unwrap()
        .output("out")
        .unwrap();
    let error = translate_v5(&pipeline, "example.test/ironbeam:v5", "test-build")
        .err()
        .expect("worker execution is required before portable lowering");
    assert!(error.to_string().contains("splittable-pardo"), "{error}");
}
