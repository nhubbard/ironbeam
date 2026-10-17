use ironbeam_core::{
    Error, PaneTiming, Pipeline, Replay, SinkMode, SinkSpec, TestStreamEvent, TypedSink,
    WindowClosingBehavior, WindowOnTimeBehavior,
};
use ironbeam_direct::{DirectRunner, JobConfig};
use std::collections::HashSet;
use std::sync::{Arc, Mutex};

struct Capture {
    rows: Arc<Mutex<Vec<String>>>,
    accepted: Arc<Mutex<HashSet<String>>>,
    bundle: Option<String>,
    staged: Vec<String>,
}

impl TypedSink<String> for Capture {
    fn begin_bundle(&mut self, instruction_id: &str) -> Result<(), Error> {
        self.bundle = Some(instruction_id.to_owned());
        Ok(())
    }
    fn write(&mut self, value: String) -> Result<(), Error> {
        self.staged.push(value);
        Ok(())
    }
    fn commit(&mut self) -> Result<(), Error> {
        if self
            .accepted
            .lock()
            .unwrap()
            .insert(self.bundle.take().unwrap())
        {
            self.rows.lock().unwrap().append(&mut self.staged);
        }
        Ok(())
    }
    fn abort(&mut self) -> Result<(), Error> {
        self.staged.clear();
        Ok(())
    }
}

#[test]
fn bounded_group_emits_final_empty_pane_after_early_firing() {
    for on_time in [
        WindowOnTimeBehavior::FireIfNonempty,
        WindowOnTimeBehavior::FireAlways,
    ] {
        let pipeline = Pipeline::default();
        let output = pipeline
            .from_vec("source", vec![(1_000_000_i64, ("a".to_owned(), 2_i32))])
            .unwrap()
            .timestamps_from_pairs_named("event-time")
            .unwrap()
            .fixed_windows("window", 10_000_000)
            .unwrap()
            .allowed_lateness_named("lateness", 5_000_000)
            .unwrap()
            .closing_behavior_named("closing", WindowClosingBehavior::EmitAlways)
            .unwrap()
            .early_count_trigger_named("early", 1)
            .unwrap()
            .on_time_behavior_named("on-time", on_time)
            .unwrap()
            .group_by_key_named("group")
            .unwrap()
            .reify_panes_named("pane")
            .unwrap()
            .output("result")
            .unwrap();
        let (rows, _) = DirectRunner
            .run_collect(&output, &JobConfig::default())
            .unwrap();
        let timings = rows.iter().map(|(pane, _)| pane.timing).collect::<Vec<_>>();
        assert_eq!(
            timings,
            if on_time == WindowOnTimeBehavior::FireAlways {
                vec![PaneTiming::Early, PaneTiming::OnTime, PaneTiming::Late]
            } else {
                vec![PaneTiming::Early, PaneTiming::Late]
            }
        );
        assert_eq!(rows[0].1, ("a".to_owned(), vec![2]));
        assert_eq!(rows.last().unwrap().1, ("a".to_owned(), vec![]));
        assert!(rows.last().unwrap().0.is_last);
    }
}

#[test]
fn streaming_group_keeps_empty_final_pane_across_watermarks() {
    let rows = Arc::new(Mutex::new(Vec::new()));
    let accepted = Arc::new(Mutex::new(HashSet::new()));
    let pipeline = Pipeline::default();
    let sink = pipeline
        .test_stream_source(
            "source",
            vec![
                TestStreamEvent::Elements(vec![((1_i32, 7_i32), 1_000_000)]),
                TestStreamEvent::AdvanceWatermark(10_000_000),
                TestStreamEvent::AdvanceWatermark(15_000_000),
            ],
        )
        .unwrap()
        .fixed_windows("window", 10_000_000)
        .unwrap()
        .allowed_lateness_named("lateness", 5_000_000)
        .unwrap()
        .closing_behavior_named("closing", WindowClosingBehavior::EmitAlways)
        .unwrap()
        .early_count_trigger_named("early", 1)
        .unwrap()
        .group_by_key_named("group")
        .unwrap()
        .reify_panes_named("pane")
        .unwrap()
        .map_named("row", |(pane, (key, values))| {
            format!("{key}:{values:?}:{:?}:{}", pane.timing, pane.index)
        })
        .unwrap()
        .sink_named_with_config(
            "sink",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            "closing-stream-test-v1".into(),
            {
                let rows = Arc::clone(&rows);
                let accepted = Arc::clone(&accepted);
                move || {
                    Ok(Box::new(Capture {
                        rows: Arc::clone(&rows),
                        accepted: Arc::clone(&accepted),
                        bundle: None,
                        staged: Vec::new(),
                    }))
                }
            },
        )
        .unwrap();
    DirectRunner
        .start_stream_sink(&sink, &JobConfig::default())
        .unwrap()
        .wait()
        .unwrap();
    assert_eq!(
        rows.lock().unwrap().as_slice(),
        ["1:[7]:Early:0", "1:[]:Late:1"]
    );
}
