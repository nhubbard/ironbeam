use ironbeam_core::{ErrorKind, Pipeline};
use ironbeam_direct::{DirectFallibleExt, DirectRunner, JobConfig};
use ironbeam_transforms::TryProcessExt;

#[test]
fn per_element_errors_are_values_until_terminal_policy() {
    let p = Pipeline::default();
    let raw = p
        .from_vec("raw", vec!["10".to_string(), "oops".into(), "42".into()])
        .unwrap();
    let parsed = raw
        .try_map_named("parse", |s| s.parse::<i32>().map_err(|e| e.to_string()))
        .unwrap();
    let output = parsed.output("inspect").unwrap();
    let values = DirectRunner
        .run_collect(&output, &JobConfig::default())
        .unwrap()
        .0;
    assert_eq!(values[0], Ok(10));
    assert!(values[1].is_err());
    assert_eq!(values[2], Ok(42));

    let error = parsed
        .collect_fail_fast_named("fail-fast", &JobConfig::default())
        .unwrap_err();
    assert_eq!(error.kind, ErrorKind::Execution);
    assert_eq!(error.node.as_deref(), Some("fail-fast"));
    assert!(error.message.contains("element failed"));
}

#[test]
fn fallible_expansion_keeps_one_result_per_input_like_v4() {
    let p = Pipeline::default();
    let raw = p
        .from_vec("raw", vec!["1,2".to_string(), "bad".into()])
        .unwrap();
    let parsed = raw
        .try_flat_map_named("parse-many", |s| {
            s.split(',')
                .map(|part| part.parse::<i32>().map_err(|e| e.to_string()))
                .collect::<Result<Vec<_>, _>>()
        })
        .unwrap()
        .output("out")
        .unwrap();
    let values = DirectRunner
        .run_collect(&parsed, &JobConfig::default())
        .unwrap()
        .0;
    assert_eq!(values[0], Ok(vec![1, 2]));
    assert!(values[1].is_err());
}
