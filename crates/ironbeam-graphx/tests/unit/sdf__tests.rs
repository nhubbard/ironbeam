// Unit tests for src/sdf.rs.
use super::*;

fn progress() -> ProgressSignal {
    ProgressSignal {
        source_key: "events".into(),
        split_id: "split-a".into(),
        checkpoint_position: Some(vec![1, 2, 3]),
        event_time_ns: Some(10),
        completed: false,
    }
}

#[test]
fn initial_and_residual_roundtrip() {
    let initial = SourceRestriction::initial("events");
    assert_eq!(
        SourceRestriction::decode("events", &initial.encode().unwrap()).unwrap(),
        initial
    );
    let residual = SourceRestriction::resume(progress()).unwrap();
    assert_eq!(
        SourceRestriction::decode("events", &residual.encode().unwrap()).unwrap(),
        residual
    );
    assert_eq!(residual.encode().unwrap(), residual.encode().unwrap());
}

#[test]
fn wrong_identity_version_and_cursor_are_rejected() {
    let residual = SourceRestriction::resume(progress()).unwrap();
    assert!(matches!(
        SourceRestriction::decode("other", &residual.encode().unwrap()),
        Err(RestrictionError::Identity)
    ));
    let mut invalid = residual.clone();
    invalid.version += 1;
    assert!(matches!(invalid.encode(), Err(RestrictionError::Identity)));
    let mut invalid_progress = progress();
    invalid_progress.checkpoint_position = None;
    assert!(matches!(
        SourceRestriction::resume(invalid_progress),
        Err(RestrictionError::Progress)
    ));
    let mut invalid_progress = progress();
    invalid_progress.completed = true;
    assert!(matches!(
        SourceRestriction::resume(invalid_progress),
        Err(RestrictionError::Progress)
    ));
    let mut invalid_progress = progress();
    invalid_progress.checkpoint_position = Some(vec![0; MAX_RESTRICTION_BYTES + 1]);
    assert!(matches!(
        SourceRestriction::resume(invalid_progress),
        Err(RestrictionError::Progress)
    ));
    assert!(matches!(
        SourceRestriction::decode("events", &vec![0; MAX_RESTRICTION_BYTES + 1]),
        Err(RestrictionError::TooLarge)
    ));
    assert!(matches!(
        SourceRestriction::decode("events", b"bad json"),
        Err(RestrictionError::Encoding(_))
    ));
}
