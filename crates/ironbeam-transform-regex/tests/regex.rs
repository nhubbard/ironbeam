use ironbeam_core::{ErrorKind, Pipeline};
use ironbeam_direct::{DirectRunner, JobConfig};
use ironbeam_transform_regex::RegexExt;

#[test]
fn regex_helpers_match_v4_results() {
    let p = Pipeline::default();
    let input = p
        .from_vec(
            "input",
            vec!["abc 123".to_string(), "xyz 456".into(), "none".into()],
        )
        .unwrap();
    let matched = input
        .regex_matches_named("matches", r"\d+")
        .unwrap()
        .output("matched-out")
        .unwrap();
    let extracted = input
        .regex_extract_named("extract", r"([a-z]+) (\d+)", 2)
        .unwrap()
        .output("extract-out")
        .unwrap();
    let pairs = input
        .regex_extract_kv_named("pairs", r"([a-z]+) (\d+)", 1, 2)
        .unwrap()
        .output("pairs-out")
        .unwrap();
    let found = input
        .regex_find_named("find", r"\d+")
        .unwrap()
        .output("find-out")
        .unwrap();
    let replaced = input
        .regex_replace_all_named("replace", r"\d+", "N")
        .unwrap()
        .output("replace-out")
        .unwrap();
    let split = input
        .regex_split_named("split", r"\s+")
        .unwrap()
        .output("split-out")
        .unwrap();
    let cfg = JobConfig::default();
    assert_eq!(
        DirectRunner.run_collect(&matched, &cfg).unwrap().0,
        ["abc 123", "xyz 456"]
    );
    assert_eq!(
        DirectRunner.run_collect(&extracted, &cfg).unwrap().0,
        ["123", "456"]
    );
    assert_eq!(
        DirectRunner.run_collect(&pairs, &cfg).unwrap().0,
        [("abc".into(), "123".into()), ("xyz".into(), "456".into())]
    );
    assert_eq!(
        DirectRunner.run_collect(&found, &cfg).unwrap().0,
        ["123", "456"]
    );
    assert_eq!(
        DirectRunner.run_collect(&replaced, &cfg).unwrap().0,
        ["abc N", "xyz N", "none"]
    );
    assert_eq!(
        DirectRunner.run_collect(&split, &cfg).unwrap().0,
        [
            vec!["abc".to_string(), "123".into()],
            vec!["xyz".into(), "456".into()],
            vec!["none".into()]
        ]
    );
}

#[test]
fn invalid_regex_returns_graph_error_at_construction() {
    let p = Pipeline::default();
    let input = p.from_vec("input", vec!["data".to_string()]).unwrap();
    let error = input.regex_matches_named("bad", "[").err().unwrap();
    assert_eq!(error.kind, ErrorKind::Graph);
    assert_eq!(error.node.as_deref(), Some("bad"));
}
