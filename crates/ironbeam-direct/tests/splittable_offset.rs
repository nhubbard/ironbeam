use ironbeam_core::{
    Error, OffsetRange, Pipeline, Replay, SinkMode, SinkSpec, TestStreamEvent, TypedSink, Version,
};
use ironbeam_direct::{DirectRunner, JobConfig};
use std::collections::HashSet;
use std::sync::{Arc, Mutex};

const VERSION: Version = Version { major: 1, minor: 0 };

struct StreamCapture {
    committed: Arc<Mutex<HashSet<(i32, i64)>>>,
    staged: Vec<(i32, i64)>,
}

impl TypedSink<(i32, i64)> for StreamCapture {
    fn write(&mut self, value: (i32, i64)) -> Result<(), Error> {
        self.staged.push(value);
        Ok(())
    }

    fn commit(&mut self) -> Result<(), Error> {
        self.committed.lock().unwrap().extend(self.staged.drain(..));
        Ok(())
    }

    fn abort(&mut self) -> Result<(), Error> {
        self.staged.clear();
        Ok(())
    }
}

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
fn finite_stream_applies_offset_restriction_to_each_element() {
    let pipeline = Pipeline::default();
    let committed = Arc::new(Mutex::new(HashSet::new()));
    let captured = Arc::clone(&committed);
    let sink = pipeline
        .test_stream_source(
            "events",
            vec![
                TestStreamEvent::Elements(vec![(2_i32, 0)]),
                TestStreamEvent::AdvanceWatermark(1_000_000),
                TestStreamEvent::Elements(vec![(3_i32, 1_000_000)]),
                TestStreamEvent::AdvanceWatermark(3_000_000),
            ],
        )
        .unwrap()
        .splittable_flat_map_named(
            "offsets",
            "tests/stream-offsets",
            VERSION,
            |value| OffsetRange::new(0, i64::from(*value)),
            |value, context| {
                for offset in 0..i64::from(*value) {
                    assert!(context.try_claim(offset)?);
                    context.emit((*value, offset), offset * 1_000_000)?;
                }
                Ok(())
            },
        )
        .unwrap()
        .sink_named_with_config(
            "out",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            "tests/stream-offset-capture".into(),
            move || {
                Ok(Box::new(StreamCapture {
                    committed: Arc::clone(&captured),
                    staged: Vec::new(),
                }))
            },
        )
        .unwrap();
    DirectRunner
        .start_stream_sink(&sink, &JobConfig::default())
        .unwrap()
        .wait()
        .unwrap();
    let mut rows = committed
        .lock()
        .unwrap()
        .iter()
        .copied()
        .collect::<Vec<_>>();
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
                    if !emit_without_claim {
                        assert!(context.try_claim(0)?);
                    }
                    context.emit(7_i32, 0)?;
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
fn direct_processes_callback_residuals_with_restored_watermarks() {
    let pipeline = Pipeline::default();
    let output = pipeline
        .from_vec("input", vec![3_i32])
        .unwrap()
        .splittable_flat_map_named(
            "offsets",
            "tests/local-offset-split",
            VERSION,
            |_| OffsetRange::new(0, 4),
            |_, context| {
                let restriction = context.tracker().restriction()?;
                if restriction.start > 0 {
                    assert_eq!(
                        context.estimator().current()?,
                        Some(restriction.start * 1_000_000)
                    );
                }
                for offset in restriction.start..restriction.stop {
                    if !context.try_claim(offset)? {
                        break;
                    }
                    context.emit(i32::try_from(offset).unwrap(), offset * 1_000_000)?;
                    if offset < 2 {
                        context.tracker().try_split(0.0)?;
                        context.update_watermark((offset + 1) * 1_000_000)?;
                        break;
                    }
                }
                Ok(())
            },
        )
        .unwrap()
        .output("out")
        .unwrap();
    let (rows, _) = DirectRunner
        .run_collect(&output, &JobConfig::default())
        .unwrap();
    assert_eq!(rows, [0, 1, 2, 3]);
}

#[test]
fn direct_rejects_a_residual_that_makes_no_progress() {
    let pipeline = Pipeline::default();
    let output = pipeline
        .from_vec("input", vec![1_i32])
        .unwrap()
        .splittable_flat_map_named::<i32>(
            "offsets",
            "tests/no-progress-offset-split",
            VERSION,
            |_| OffsetRange::new(0, 2),
            |_, context| {
                context.tracker().try_split(0.0)?;
                Ok(())
            },
        )
        .unwrap()
        .output("out")
        .unwrap();
    let error = DirectRunner
        .run_collect(&output, &JobConfig::default())
        .err()
        .expect("a repeated zero-progress split must fail");
    assert!(error.to_string().contains("made no progress"));
}
