use ironbeam_core::{Capabilities, OffsetRange, Pipeline, Version};

fn graph_for(version: Version) -> ironbeam_core::Graph {
    let pipeline = Pipeline::default();
    pipeline
        .from_vec("source", vec![2_i32])
        .unwrap()
        .splittable_flat_map_named(
            "offsets",
            "tests/offset-transform",
            version,
            |_| OffsetRange::new(0, 2),
            |_, context| {
                for offset in 0..2 {
                    assert!(context.try_claim(offset)?);
                    context.emit(offset, offset * 1_000_000)?;
                }
                Ok(())
            },
        )
        .unwrap();
    pipeline.snapshot().unwrap().0
}

#[test]
fn semantic_version_changes_graph_identity_and_runner_capability() {
    let first = graph_for(Version { major: 1, minor: 0 });
    let second = graph_for(Version { major: 1, minor: 1 });
    assert_ne!(first.fingerprint().unwrap(), second.fingerprint().unwrap());
    assert!(
        first
            .validate_for("portable", &Capabilities::default())
            .is_err()
    );
    let mut capabilities = Capabilities {
        splittable_pardo: true,
        ..Capabilities::default()
    };
    assert!(first.validate_for("direct", &capabilities).is_ok());
    capabilities.splittable_pardo = false;
    assert!(first.validate_for("direct", &capabilities).is_err());
}

#[test]
fn splittable_handler_requires_an_application_namespace() {
    let pipeline = Pipeline::default();
    let input = pipeline.from_vec("source", vec![2_i32]).unwrap();
    assert!(
        input
            .splittable_flat_map_named::<i64>(
                "offsets",
                "ironbeam/reserved",
                Version { major: 1, minor: 0 },
                |_| OffsetRange::new(0, 2),
                |_, _| Ok(()),
            )
            .is_err()
    );
}
