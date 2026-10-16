use ironbeam_core::{OffsetRange, Pipeline, Version};
use ironbeam_direct::{DirectRunner, JobConfig};

const VERSION: Version = Version { major: 1, minor: 0 };

#[test]
fn bounded_offset_function_emits_claimed_positions() {
    let pipeline = Pipeline::default();
    let output = pipeline
        .from_vec("input", vec![3_i32, 2_i32])
        .unwrap()
        .splittable_flat_map_named(
            "offsets",
            "tests/claimed-offsets",
            VERSION,
            |value| OffsetRange::new(0, i64::from(*value)),
            |value, context| {
                for offset in 0..i64::from(*value) {
                    if !context.try_claim(offset)? {
                        break;
                    }
                    context.emit((*value, offset), offset * 1_000_000)?;
                }
                Ok(())
            },
        )
        .unwrap()
        .output("out")
        .unwrap();
    let (mut rows, _) = DirectRunner
        .run_collect(&output, &JobConfig::default())
        .unwrap();
    rows.sort_unstable();
    assert_eq!(rows, [(2, 0), (2, 1), (3, 0), (3, 1), (3, 2)]);
}

#[test]
fn incomplete_work_and_output_without_a_claim_fail_the_job() {
    for emit_without_claim in [false, true] {
        let pipeline = Pipeline::default();
        let output = pipeline
            .from_vec("input", vec![3_i32])
            .unwrap()
            .splittable_flat_map_named(
                "offsets",
                "tests/incomplete-offsets",
                VERSION,
                |_| OffsetRange::new(0, 3),
                move |_, context| {
                    if emit_without_claim {
                        context.emit(7_i32, 0)?;
                    } else {
                        assert!(context.try_claim(0)?);
                        context.emit(7_i32, 0)?;
                    }
                    Ok(())
                },
            )
            .unwrap()
            .output("out")
            .unwrap();
        assert!(
            DirectRunner
                .run_collect(&output, &JobConfig::default())
                .is_err()
        );
    }
}

#[test]
fn a_local_split_cannot_silently_drop_its_residual() {
    let pipeline = Pipeline::default();
    let output = pipeline
        .from_vec("input", vec![3_i32])
        .unwrap()
        .splittable_flat_map_named(
            "offsets",
            "tests/local-offset-split",
            VERSION,
            |_| OffsetRange::new(0, 3),
            |_, context| {
                assert!(context.try_claim(0)?);
                context.emit(0_i32, 0)?;
                let (_, residual) = context.tracker().try_split(0.0)?.unwrap();
                assert_eq!(residual, OffsetRange::new(1, 3)?);
                Ok(())
            },
        )
        .unwrap()
        .output("out")
        .unwrap();
    assert!(
        DirectRunner
            .run_collect(&output, &JobConfig::default())
            .is_err()
    );
}
