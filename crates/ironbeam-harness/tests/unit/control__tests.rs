// Unit tests for src/control.rs.
use super::*;
use async_trait::async_trait;
use beam_proto::fn_execution_v1::ProcessBundleResponse;
use std::sync::Mutex;

#[derive(Default)]
struct FinalizingRunner(Mutex<Vec<String>>);

#[async_trait]
impl BundleRunner for FinalizingRunner {
    async fn run(&self, _: &str, _: &bundle::BundlePlan) -> Result<ProcessBundleResponse> {
        unreachable!("this test only sends finalization")
    }

    async fn finalize_bundle(&self, instruction_id: &str) -> Result<()> {
        self.0.lock().unwrap().push(instruction_id.into());
        Ok(())
    }
}

#[tokio::test]
async fn finalization_uses_the_original_bundle_id() {
    let runner = FinalizingRunner::default();
    let resp = finalize_response("finalize-1".into(), "bundle-1", &runner).await;
    assert_eq!(resp.instruction_id, "finalize-1");
    assert!(resp.error.is_empty());
    assert!(matches!(resp.response, Some(InstrResp::FinalizeBundle(_))));
    assert_eq!(*runner.0.lock().unwrap(), ["bundle-1"]);

    let missing = finalize_response("finalize-2".into(), "", &runner).await;
    assert!(missing.response.is_none());
    assert!(missing.error.contains("no bundle instruction id"));
    assert_eq!(runner.0.lock().unwrap().len(), 1);
}

struct NoFinalizer;

#[async_trait]
impl BundleRunner for NoFinalizer {
    async fn run(&self, _: &str, _: &bundle::BundlePlan) -> Result<ProcessBundleResponse> {
        unreachable!("this test only sends finalization")
    }
}

#[tokio::test]
async fn executor_without_finalizer_returns_an_error() {
    let resp = finalize_response("finalize-3".into(), "bundle-3", &NoFinalizer).await;
    assert!(resp.response.is_none());
    assert!(resp.error.contains("unsupported for bundle-3"));
}

#[tokio::test]
async fn split_requires_a_bundle_id() {
    let resp = split_response(
        "split-1".into(),
        ProcessBundleSplitRequest::default(),
        &NoFinalizer,
    )
    .await;
    assert_eq!(resp.instruction_id, "split-1");
    assert!(resp.response.is_none());
    assert!(resp.error.contains("no bundle instruction id"));
}

struct PanickingRunner;

#[async_trait]
impl BundleRunner for PanickingRunner {
    async fn run(&self, _: &str, _: &bundle::BundlePlan) -> Result<ProcessBundleResponse> {
        panic!("validation failed in user transform")
    }

    async fn split_bundle(
        &self,
        _: &ProcessBundleSplitRequest,
    ) -> Result<beam_proto::fn_execution_v1::ProcessBundleSplitResponse> {
        panic!("split callback failed")
    }

    async fn finalize_bundle(&self, _: &str) -> Result<()> {
        panic!("finalize callback failed")
    }
}

#[tokio::test]
async fn panicking_bundle_returns_an_instruction_error() {
    let response = process_bundle_response(
        "bundle-panic".into(),
        bundle::BundlePlan {
            steps: Vec::new(),
            pbd_id: "panic-pbd".into(),
            state_endpoint: None,
            side_inputs: Default::default(),
        },
        Arc::new(PanickingRunner),
        Arc::new(Dispatcher::new(PbdCache::new(), 0)),
    )
    .await;
    assert_eq!(response.instruction_id, "bundle-panic");
    assert!(response.error.contains("executor panicked"));
    assert!(response
        .error
        .contains("validation failed in user transform"));
    assert!(response.response.is_none());
}

#[tokio::test]
async fn panicking_split_and_finalize_return_instruction_errors() {
    let runner = PanickingRunner;
    let split = split_response(
        "split-panic".into(),
        ProcessBundleSplitRequest {
            instruction_id: "bundle-panic".into(),
            ..Default::default()
        },
        &runner,
    )
    .await;
    assert_eq!(split.instruction_id, "split-panic");
    assert!(split.error.contains("split callback failed"));
    assert!(split.response.is_none());

    let finalize = finalize_response("finalize-panic".into(), "bundle-panic", &runner).await;
    assert_eq!(finalize.instruction_id, "finalize-panic");
    assert!(finalize.error.contains("finalize callback failed"));
    assert!(finalize.response.is_none());
}
