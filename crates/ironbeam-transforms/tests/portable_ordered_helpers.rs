use ironbeam_core::{
    Error, ErrorKind, Pipeline, Replay, SinkMode, SinkSpec, TestStreamEvent, TypedSink,
};
use ironbeam_direct::{DirectRunner, JobConfig};
use ironbeam_transforms::ElementwiseExt;

fn rows() -> Vec<(u8, String)> {
    [(3, "b"), (1, "a"), (4, "a"), (2, "c")]
        .into_iter()
        .map(|(rank, label)| (rank, label.to_owned()))
        .collect()
}

#[test]
fn ordered_helpers_use_explicit_order_and_distinct_identity() {
    let pipeline = Pipeline::default();
    let input = pipeline.from_vec("input", rows()).unwrap();
    let list = input
        .to_list_ordered_by_named("list", |row| row.0)
        .unwrap()
        .output("list-output")
        .unwrap();
    let take = input
        .take_ordered_by_named("take", 2, |row| row.0)
        .unwrap()
        .output("take-output")
        .unwrap();
    let first = input
        .first_ordered_by_named("first", |row| row.0)
        .unwrap()
        .output("first-output")
        .unwrap();
    let distinct = input
        .distinct_by_ordered_by_named("distinct", |row| row.1.clone(), |row| row.0)
        .unwrap()
        .output("distinct-output")
        .unwrap();
    let runner = DirectRunner;
    let config = JobConfig::default();
    let (lists, _) = runner.run_collect(&list, &config).unwrap();
    assert_eq!(
        lists,
        vec![vec![
            (1, "a".into()),
            (2, "c".into()),
            (3, "b".into()),
            (4, "a".into())
        ]]
    );
    let (mut taken, _) = runner.run_collect(&take, &config).unwrap();
    taken.sort_by_key(|row| row.0);
    assert_eq!(taken, [(1, "a".into()), (2, "c".into())]);
    let (first, _) = runner.run_collect(&first, &config).unwrap();
    assert_eq!(first, [(1, "a".into())]);
    let (mut unique, _) = runner.run_collect(&distinct, &config).unwrap();
    unique.sort_by_key(|row| row.0);
    assert_eq!(unique, [(1, "a".into()), (2, "c".into()), (3, "b".into())]);
}

#[test]
fn ordered_helpers_reject_duplicate_order_keys() {
    let pipeline = Pipeline::default();
    let output = pipeline
        .from_vec("input", vec![(1_u8, "a".to_owned()), (1, "b".to_owned())])
        .unwrap()
        .first_ordered_by_named("first", |row| row.0)
        .unwrap()
        .output("output")
        .unwrap();
    let error = DirectRunner
        .run_collect(&output, &JobConfig::default())
        .err()
        .unwrap();
    assert_eq!(error.kind, ErrorKind::Execution);
    assert!(error.message.contains("duplicate ordered fold key"));
}

struct DiscardRows;

impl TypedSink<(u8, String)> for DiscardRows {
    fn write(&mut self, _value: (u8, String)) -> Result<(), Error> {
        Ok(())
    }

    fn commit(&mut self) -> Result<(), Error> {
        Ok(())
    }

    fn abort(&mut self) -> Result<(), Error> {
        Ok(())
    }
}

#[test]
fn streaming_ordered_helper_rejects_duplicate_order_keys() {
    let pipeline = Pipeline::default();
    let sink = pipeline
        .test_stream_source(
            "input",
            vec![
                TestStreamEvent::Elements(vec![
                    ((1_u8, "a".to_owned()), 1_000_000),
                    ((1, "b".to_owned()), 2_000_000),
                ]),
                TestStreamEvent::AdvanceWatermark(10_000_000),
            ],
        )
        .unwrap()
        .fixed_windows("window", 10_000_000)
        .unwrap()
        .first_ordered_by_named("first", |row| row.0)
        .unwrap()
        .sink_named_with_config(
            "discard",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            "discard-ordered-duplicates-v1".into(),
            || Ok(Box::new(DiscardRows)),
        )
        .unwrap();
    let error = DirectRunner
        .start_stream_sink(&sink, &JobConfig::default())
        .unwrap()
        .wait()
        .err()
        .unwrap();
    assert_eq!(error.kind, ErrorKind::Execution);
    assert!(error.message.contains("duplicate ordered fold key"));
}
