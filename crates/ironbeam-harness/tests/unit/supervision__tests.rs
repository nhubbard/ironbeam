use super::*;
use ironbeam_core::{Error, SinkMode, SinkSpec, TypedSink};

struct Ordinary;

impl TypedSink<i32> for Ordinary {
    fn write(&mut self, _: i32) -> Result<(), Error> {
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
fn supervisor_rejects_a_sink_that_can_duplicate_on_replay() {
    let pipeline = Pipeline::default();
    pipeline
        .from_vec("source", vec![1_i32])
        .unwrap()
        .sink_named_with_config(
            "unsafe-append",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::AtLeastOnce,
                commit: true,
            },
            "supervision-test".into(),
            || Ok(Box::new(Ordinary)),
        )
        .unwrap();
    let error = validate_pipeline(&pipeline).unwrap_err().to_string();
    assert!(
        error.contains("unsafe-append is not replay-safe"),
        "{error}"
    );
}

#[test]
fn supervisor_accepts_an_idempotent_sink() {
    let pipeline = Pipeline::default();
    pipeline
        .from_vec("source", vec![1_i32])
        .unwrap()
        .sink_named_with_config(
            "safe-append",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            "supervision-test".into(),
            || Ok(Box::new(Ordinary)),
        )
        .unwrap();
    validate_pipeline(&pipeline).unwrap();
}

#[tokio::test]
async fn supervised_split_reaches_a_registered_bundle() {
    let runner = SupervisedV5Runner::new(PbdCache::new(), "test-worker".into()).unwrap();
    let directory = tempfile::tempdir().unwrap();
    let signal = directory.path().join("split-requested");
    runner
        .active
        .lock()
        .unwrap()
        .insert("bundle-1".into(), signal.clone());
    runner
        .split_bundle(&ProcessBundleSplitRequest {
            instruction_id: "bundle-1".into(),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(std::fs::read(signal).unwrap(), b"split");
}
