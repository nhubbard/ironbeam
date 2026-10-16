use ironbeam_core::{
    Capabilities, Element, OffsetRange, OffsetRangeTracker, OffsetWatermarkEstimator, Pipeline,
    Version,
};

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

#[test]
fn worker_registry_reconstructs_the_versioned_coded_callback() {
    let pipeline = Pipeline::default();
    pipeline
        .from_vec("source", vec![2_i32])
        .unwrap()
        .splittable_flat_map_named(
            "offsets",
            "tests/coded-offsets",
            Version { major: 1, minor: 0 },
            |value| OffsetRange::new(0, i64::from(*value)),
            |_, context| {
                for offset in 0..2 {
                    assert!(context.try_claim(offset)?);
                    context.emit(offset, offset * 1_000_000)?;
                }
                context.update_watermark(1_000_000)?;
                Ok(())
            },
        )
        .unwrap();
    let (_, registry) = pipeline.snapshot().unwrap();
    assert!(registry.has_splittable("offsets"));
    let input = 2_i32.encode().unwrap();
    let restriction = registry
        .initial_splittable_restriction("offsets", &input)
        .unwrap();
    assert_eq!(restriction, OffsetRange::new(0, 2).unwrap());
    let estimator = OffsetWatermarkEstimator::new(None);
    let output = registry
        .process_splittable_restriction(
            "offsets",
            &input,
            OffsetRangeTracker::new(restriction).unwrap(),
            estimator.clone(),
        )
        .unwrap();
    assert_eq!(
        output
            .iter()
            .map(|row| (i64::decode(&row.value).unwrap(), row.timestamp_ns))
            .collect::<Vec<_>>(),
        [(0, 0), (1, 1_000_000)]
    );
    assert_eq!(estimator.current().unwrap(), Some(1_000_000));
}
