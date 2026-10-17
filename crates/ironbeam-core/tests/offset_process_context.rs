use ironbeam_core::{
    OffsetProcessContext, OffsetRange, OffsetRangeTracker, OffsetWatermarkEstimator,
};

#[test]
fn split_primary_and_residual_emit_each_offset_once_with_restored_watermark() {
    let restriction = OffsetRange::new(0, 4).unwrap();
    let tracker = OffsetRangeTracker::new(restriction).unwrap();
    let estimator = OffsetWatermarkEstimator::new(Some(5));
    let mut primary = OffsetProcessContext::new(tracker, estimator);
    assert!(primary.try_claim(0).unwrap());
    primary.emit("zero", 6).unwrap();
    primary.update_watermark(6).unwrap();
    let (_, residual) = primary.tracker().try_split(0.0).unwrap().unwrap();
    assert_eq!(residual, OffsetRange::new(1, 4).unwrap());
    assert_eq!(primary.estimator().current().unwrap(), Some(6));
    assert!(!primary.try_claim(1).unwrap());
    assert!(primary.emit("duplicate", 7).is_err());
    let first = primary.finish().unwrap();
    assert_eq!(first.len(), 1);
    assert_eq!((first[0].value, first[0].timestamp_ns), ("zero", 6));

    let tracker = OffsetRangeTracker::new(residual).unwrap();
    let estimator = OffsetWatermarkEstimator::new(Some(6));
    let mut resumed = OffsetProcessContext::new(tracker, estimator);
    for position in residual.start..residual.stop {
        assert!(resumed.try_claim(position).unwrap());
        resumed.emit(position, position + 6).unwrap();
    }
    resumed.update_watermark(9).unwrap();
    let output = resumed.finish().unwrap();
    assert_eq!(
        output.into_iter().map(|row| row.value).collect::<Vec<_>>(),
        [1, 2, 3]
    );
}

#[test]
fn no_output_without_a_claim_and_no_early_primary_completion() {
    let tracker = OffsetRangeTracker::new(OffsetRange::new(2, 4).unwrap()).unwrap();
    let mut context = OffsetProcessContext::new(tracker, OffsetWatermarkEstimator::new(None));
    assert!(context.emit(2, 2).is_err());
    assert!(context.try_claim(2).unwrap());
    context.emit(2, 2).unwrap();
    assert!(context.finish().is_err());
}

#[test]
fn watermark_updates_are_monotonic_across_shared_handles() {
    let estimator = OffsetWatermarkEstimator::new(Some(-10));
    let other = estimator.clone();
    other.update(-5).unwrap();
    assert_eq!(estimator.current().unwrap(), Some(-5));
    assert!(estimator.update(-6).is_err());
    assert_eq!(other.current().unwrap(), Some(-5));
}
