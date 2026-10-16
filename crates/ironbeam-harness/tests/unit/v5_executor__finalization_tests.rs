// Unit tests for src/v5_executor.rs.
use super::{BundleFinalizations, SinkGuard, V5BundleExecutor};
use crate::executor::BundleRunner;
use crate::finalization_journal::{
    FileFinalizationJournal, FinalizationJournal, ObjectFinalizationJournal,
};
use ironbeam_cloud_objects::{LocalObjectIO, ObjectStores};
use ironbeam_core::{Envelope, Error, ExecutionIdentity, Pipeline, Registry, SinkSession, Window};
use ironbeam_io_protobuf::{
    protobuf_fragment_sink, protobuf_fragment_store, publish_protobuf_fragments,
};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

struct Finalizable {
    calls: Arc<AtomicUsize>,
    fail_first: bool,
}

impl SinkSession for Finalizable {
    fn write(&mut self, _: Envelope) -> Result<(), Error> {
        Ok(())
    }
    fn commit(&mut self) -> Result<(), Error> {
        Ok(())
    }
    fn abort(&mut self) -> Result<(), Error> {
        Ok(())
    }
    fn requires_bundle_finalization(&self) -> bool {
        true
    }
    fn finalize_bundle(&mut self) -> Result<(), Error> {
        let previous = self.calls.fetch_add(1, Ordering::SeqCst);
        if self.fail_first && previous == 0 {
            Err(Error::io("sink", "test", "retry finalization"))
        } else {
            Ok(())
        }
    }
}

#[test]
fn registered_bundle_finalization_retries_and_deduplicates() {
    let calls = Arc::new(AtomicUsize::new(0));
    let registry = BundleFinalizations::default();
    let sink = SinkGuard {
        session: Box::new(Finalizable {
            calls: Arc::clone(&calls),
            fail_first: true,
        }),
        committed: true,
    };
    let empty = Registry::default();
    assert!(sink.requires_finalization().unwrap());
    registry
        .register("bundle-1", vec![("sink".into(), sink)])
        .unwrap();
    assert!(registry.register("bundle-1", vec![]).is_err());
    assert!(registry.finalize("unknown", &empty).is_err());
    assert!(registry.finalize("bundle-1", &empty).is_err());
    registry.finalize("bundle-1", &empty).unwrap();
    registry.finalize("bundle-1", &empty).unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 2);
}

#[test]
fn a_failed_finalizer_retries_only_itself_and_keeps_bundles_isolated() {
    let completed_calls = Arc::new(AtomicUsize::new(0));
    let retry_calls = Arc::new(AtomicUsize::new(0));
    let second_bundle_calls = Arc::new(AtomicUsize::new(0));
    let guard = |calls: &Arc<AtomicUsize>, fail_first| SinkGuard {
        session: Box::new(Finalizable {
            calls: Arc::clone(calls),
            fail_first,
        }),
        committed: true,
    };
    let registry = BundleFinalizations::default();
    let empty = Registry::default();
    registry
        .register(
            "bundle-1",
            vec![
                ("a-completed".into(), guard(&completed_calls, false)),
                ("b-retry".into(), guard(&retry_calls, true)),
            ],
        )
        .unwrap();
    registry
        .register(
            "bundle-2",
            vec![("sink".into(), guard(&second_bundle_calls, false))],
        )
        .unwrap();

    assert!(registry.finalize("bundle-1", &empty).is_err());
    assert_eq!(completed_calls.load(Ordering::SeqCst), 1);
    assert_eq!(retry_calls.load(Ordering::SeqCst), 1);
    registry.finalize("bundle-2", &empty).unwrap();
    registry.finalize("bundle-1", &empty).unwrap();
    registry.finalize("bundle-1", &empty).unwrap();
    registry.finalize("bundle-2", &empty).unwrap();
    assert_eq!(completed_calls.load(Ordering::SeqCst), 1);
    assert_eq!(retry_calls.load(Ordering::SeqCst), 2);
    assert_eq!(second_bundle_calls.load(Ordering::SeqCst), 1);
}

#[test]
fn restarted_registry_recovers_exact_journaled_protobuf_sinks() {
    let dir = tempfile::tempdir().unwrap();
    let staging = dir.path().join("staging");
    let pipeline = Pipeline::default();
    let source = pipeline.from_vec("source", vec![7_i32]).unwrap();
    let sink =
        protobuf_fragment_sink(&source, "final", staging.join("final"), "job", "graph").unwrap();
    protobuf_fragment_sink(&source, "second", staging.join("second"), "job", "graph").unwrap();
    let (_, registry) = sink.snapshot().unwrap();
    let identity = ExecutionIdentity {
        graph_fingerprint: "graph".into(),
        binary_build_id: "build".into(),
        registry_manifest: "registry".into(),
        coder_manifest: "coder".into(),
        source_config_digest: "source".into(),
        sink_config_digest: "sink".into(),
    };
    let mut stores = ObjectStores::new();
    stores
        .register(
            "file",
            LocalObjectIO::new(dir.path().join("objects")).unwrap(),
        )
        .unwrap();
    let journal: Arc<dyn FinalizationJournal> = Arc::new(
        ObjectFinalizationJournal::new(Arc::new(stores), "file://bucket/finalization/", identity)
            .unwrap(),
    );
    let first = BundleFinalizations::with_journal(Arc::clone(&journal));
    let mut guard = SinkGuard {
        session: registry.open_sink("final").unwrap(),
        committed: false,
    };
    guard.begin_bundle("bundle-1").unwrap();
    guard
        .write(Envelope {
            pane: ironbeam_core::PaneInfo::NO_FIRING,
            value: Box::new(7_i32),
            timestamp_ns: 0,
            window: Window::Global,
            assigned_window: None,
        })
        .unwrap();
    guard.commit().unwrap();
    let mut second = SinkGuard {
        session: registry.open_sink("second").unwrap(),
        committed: false,
    };
    second.begin_bundle("bundle-1").unwrap();
    second
        .write(Envelope {
            pane: ironbeam_core::PaneInfo::NO_FIRING,
            value: Box::new(8_i32),
            timestamp_ns: 0,
            window: Window::Global,
            assigned_window: None,
        })
        .unwrap();
    second.commit().unwrap();
    first
        .register(
            "bundle-1",
            vec![("second".into(), second), ("final".into(), guard)],
        )
        .unwrap();
    drop(first);

    let reopened = BundleFinalizations::with_journal(Arc::clone(&journal));
    assert!(reopened.finalize("unknown", &registry).is_err());
    journal.record("missing", &["final".into()]).unwrap();
    assert!(reopened.finalize("missing", &registry).is_err());
    assert!(!journal.load("missing").unwrap().unwrap().completed);
    reopened.finalize("bundle-1", &registry).unwrap();
    assert!(journal.load("bundle-1").unwrap().unwrap().completed);
    assert!(!reopened.entries.lock().unwrap().contains_key("bundle-1"));
    let another_process = BundleFinalizations::with_journal(journal);
    another_process.finalize("bundle-1", &registry).unwrap();
    assert!(another_process.entries.lock().unwrap().is_empty());
    let store = protobuf_fragment_store(staging.join("final"), "job", "graph", "final")
        .unwrap()
        .require_bundle_attempts(["bundle-1"])
        .unwrap();
    assert_eq!(store.accepted_fragments().unwrap().len(), 1);
    assert_eq!(
        publish_protobuf_fragments::<i32>(&store, 1, dir.path().join("result.pb"),)
            .unwrap()
            .rows,
        1
    );
    let second_store = protobuf_fragment_store(staging.join("second"), "job", "graph", "second")
        .unwrap()
        .require_bundle_attempts(["bundle-1"])
        .unwrap();
    assert_eq!(second_store.accepted_fragments().unwrap().len(), 1);
    assert_eq!(
        publish_protobuf_fragments::<i32>(&second_store, 1, dir.path().join("second.pb"),)
            .unwrap()
            .rows,
        1
    );
}

#[tokio::test]
async fn worker_constructor_recovers_an_object_journaled_bundle() {
    let dir = tempfile::tempdir().unwrap();
    let staging = dir.path().join("staging");
    let pipeline = Pipeline::default();
    let source = pipeline.from_vec("source", vec![7_i32]).unwrap();
    let sink = protobuf_fragment_sink(&source, "final", &staging, "job", "graph").unwrap();
    let (_, registry) = sink.snapshot().unwrap();
    let mut stores = ObjectStores::new();
    stores
        .register(
            "file",
            LocalObjectIO::new(dir.path().join("objects")).unwrap(),
        )
        .unwrap();
    let stores = Arc::new(stores);
    let journal_uri = "file://bucket/finalization/";
    let first = V5BundleExecutor::new_for_worker_with_object_journal(
        &pipeline,
        "build",
        Some("worker-a"),
        Arc::clone(&stores),
        journal_uri,
    )
    .unwrap();
    let mut guard = SinkGuard {
        session: registry.open_sink("final").unwrap(),
        committed: false,
    };
    guard.begin_bundle("bundle-1").unwrap();
    guard
        .write(Envelope {
            pane: ironbeam_core::PaneInfo::NO_FIRING,
            value: Box::new(7_i32),
            timestamp_ns: 0,
            window: Window::Global,
            assigned_window: None,
        })
        .unwrap();
    guard.commit().unwrap();
    first
        .finalizations
        .register("bundle-1", vec![("final".into(), guard)])
        .unwrap();
    drop(first);

    let replacement = V5BundleExecutor::new_for_worker_with_object_journal(
        &pipeline,
        "build",
        Some("worker-b"),
        Arc::clone(&stores),
        journal_uri,
    )
    .unwrap();
    replacement.finalize_bundle("bundle-1").await.unwrap();
    replacement.finalize_bundle("bundle-1").await.unwrap();
    let journal = ObjectFinalizationJournal::new(stores, journal_uri, replacement.identity).unwrap();
    let manifest = journal.completed_manifest(["final"]).unwrap();
    assert_eq!(manifest.attempts_for_sink("final").unwrap(), ["bundle-1"]);
    let store = protobuf_fragment_store(&staging, "job", "graph", "final")
        .unwrap()
        .require_bundle_attempts(manifest.attempts_for_sink("final").unwrap())
        .unwrap();
    assert_eq!(store.accepted_fragments().unwrap().len(), 1);
}

#[test]
fn finalized_fragment_survives_worker_process_exit() {
    const MODE: &str = "IRONBEAM_FINALIZATION_PROCESS_TEST_MODE";
    const ROOT: &str = "IRONBEAM_FINALIZATION_PROCESS_TEST_ROOT";
    const JOURNAL: &str = "IRONBEAM_V5_FINALIZATION_JOURNAL_DIR";
    if let Ok(mode) = std::env::var(MODE) {
        let root = std::path::PathBuf::from(std::env::var(ROOT).unwrap());
        let staging = root.join("staging");
        let pipeline = Pipeline::default();
        let source = pipeline.from_vec("source", vec![7_i32]).unwrap();
        let sink = protobuf_fragment_sink(&source, "final", &staging, "job", "graph").unwrap();
        let (_, registry) = sink.snapshot().unwrap();
        let executor = V5BundleExecutor::new(
            &pipeline,
            if mode == "wrong-build" {
                "different-build"
            } else {
                "build"
            },
        )
        .unwrap();
        if mode == "stage" {
            let mut guard = SinkGuard {
                session: registry.open_sink("final").unwrap(),
                committed: false,
            };
            guard.begin_bundle("bundle-1").unwrap();
            guard
                .write(Envelope {
                    pane: ironbeam_core::PaneInfo::NO_FIRING,
                    value: Box::new(7_i32),
                    timestamp_ns: 0,
                    window: Window::Global,
                    assigned_window: None,
                })
                .unwrap();
            guard.commit().unwrap();
            executor
                .finalizations
                .register("bundle-1", vec![("final".into(), guard)])
                .unwrap();
            // Model process loss immediately after the durable response state
            // is written, without running destructors for the sink session.
            std::process::exit(0);
        }
        let result = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(executor.finalize_bundle("bundle-1"));
        if mode == "wrong-build" {
            assert!(
                result.unwrap_err().to_string().contains("journal identity"),
                "changed worker build did not fail journal identity validation"
            );
        } else {
            assert!(matches!(mode.as_str(), "recover" | "duplicate"));
            result.unwrap();
        }
        return;
    }

    let dir = tempfile::tempdir().unwrap();
    let exe = std::env::current_exe().unwrap();
    let pipeline = Pipeline::default();
    let source = pipeline.from_vec("source", vec![7_i32]).unwrap();
    protobuf_fragment_sink(&source, "final", dir.path().join("staging"), "job", "graph").unwrap();
    let executor = V5BundleExecutor::new(&pipeline, "build").unwrap();
    let journal =
        FileFinalizationJournal::new(dir.path().join("journal"), executor.identity).unwrap();
    for mode in ["stage", "wrong-build", "recover", "duplicate"] {
        let status = std::process::Command::new(&exe)
            .arg("--exact")
            .arg("v5_executor::finalization_tests::finalized_fragment_survives_worker_process_exit")
            .env(MODE, mode)
            .env(ROOT, dir.path())
            .env(JOURNAL, dir.path().join("journal"))
            .status()
            .unwrap();
        assert!(status.success(), "{mode} process failed: {status}");
        if matches!(mode, "stage" | "wrong-build") {
            assert!(journal.completed_manifest(["final"]).is_err());
        }
    }
    let manifest = journal.completed_manifest(["final"]).unwrap();
    let store = protobuf_fragment_store(dir.path().join("staging"), "job", "graph", "final")
        .unwrap()
        .require_bundle_attempts(manifest.attempts_for_sink("final").unwrap())
        .unwrap();
    assert_eq!(store.accepted_fragments().unwrap().len(), 1);
    assert_eq!(
        publish_protobuf_fragments::<i32>(&store, 1, dir.path().join("result.pb"),)
            .unwrap()
            .rows,
        1
    );
}
