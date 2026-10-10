use ironbeam_core::{ErrorKind, Pipeline};
use ironbeam_direct::{DirectRunner, JobConfig};
use ironbeam_transforms::{
    ErrorCollector, KeyedValidationExt, Validate, ValidationError, ValidationExt, ValidationMode,
    ValidationResult,
};
use std::sync::{Arc, Mutex};

#[derive(Clone, Debug)]
struct Row {
    score: i32,
}
impl ironbeam_core::Element for Row {
    const TYPE_ID: &'static str = "test/validation-row";
    const CODER_ID: &'static str = "test/validation-row";
}
impl Validate for Row {
    fn validate(&self) -> ValidationResult {
        if self.score >= 0 {
            Ok(())
        } else {
            Err(vec![
                ValidationError::field("score", "must be positive").with_code("negative"),
            ])
        }
    }
}

#[test]
fn skip_log_and_fail_modes_match_v4_values() {
    let pipeline = Pipeline::default();
    let rows = pipeline
        .from_vec(
            "rows",
            vec![Row { score: 1 }, Row { score: -1 }, Row { score: 2 }],
        )
        .unwrap();
    let collector = Arc::new(Mutex::new(ErrorCollector::new()));
    let logged = rows
        .validate_with_mode_named(
            "logged",
            ValidationMode::LogAndContinue,
            Some(Arc::clone(&collector)),
        )
        .unwrap()
        .output("logged-out")
        .unwrap();
    let skipped = rows
        .validate_skip_invalid_named("skipped")
        .unwrap()
        .output("skipped-out")
        .unwrap();
    let failed = rows
        .validate_fail_fast_named("failed")
        .unwrap()
        .output("failed-out")
        .unwrap();
    assert_eq!(
        DirectRunner
            .run_collect(&logged, &JobConfig::default())
            .unwrap()
            .0
            .into_iter()
            .map(|row| row.score)
            .collect::<Vec<_>>(),
        [1, 2]
    );
    let errors = collector.lock().unwrap();
    assert_eq!(errors.error_count(), 1);
    assert_eq!(
        errors.errors()[0].errors[0].to_string(),
        "[score] must be positive (code: negative)"
    );
    drop(errors);
    assert_eq!(
        DirectRunner
            .run_collect(&skipped, &JobConfig::default())
            .unwrap()
            .0
            .into_iter()
            .map(|row| row.score)
            .collect::<Vec<_>>(),
        [1, 2]
    );
    assert_eq!(
        DirectRunner
            .run_collect(&failed, &JobConfig::default())
            .err()
            .unwrap()
            .kind,
        ErrorKind::Execution
    );
}

#[test]
fn keyed_validation_preserves_keys() {
    let pipeline = Pipeline::default();
    let rows = pipeline
        .from_vec(
            "keyed",
            vec![
                ("good".to_string(), Row { score: 1 }),
                ("bad".into(), Row { score: -1 }),
            ],
        )
        .unwrap();
    let output = rows
        .validate_values_skip_invalid_named("validate")
        .unwrap()
        .output("out")
        .unwrap();
    let values = DirectRunner
        .run_collect(&output, &JobConfig::default())
        .unwrap()
        .0;
    assert_eq!(values.len(), 1);
    assert_eq!(values[0].0, "good");
}
