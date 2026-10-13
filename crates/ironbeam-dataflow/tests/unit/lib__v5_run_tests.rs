// Unit tests for src/lib.rs.
use super::{require_v5_success, run_v5, validate_v5_worker_binary, DataflowOptions, JobOutcome};

#[test]
fn v5_reports_failed_terminal_jobs_as_errors() {
    let failed = JobOutcome {
        job_id: "job-123".into(),
        final_state: "JOB_STATE_FAILED".into(),
    };
    let error = require_v5_success(failed).unwrap_err();
    assert!(error.to_string().contains("job-123"));
    assert!(error.to_string().contains("JOB_STATE_FAILED"));
    let done = JobOutcome {
        job_id: "job-456".into(),
        final_state: "JOB_STATE_DONE".into(),
    };
    assert_eq!(require_v5_success(done).unwrap().job_id, "job-456");
}

#[test]
fn accepts_only_x86_64_elf_before_remote_staging() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("worker");
    let mut header = [0_u8; 20];
    header[..4].copy_from_slice(b"\x7fELF");
    header[4] = 2;
    header[5] = 1;
    header[18..20].copy_from_slice(&62_u16.to_le_bytes());
    std::fs::write(&path, header).unwrap();
    validate_v5_worker_binary(&path).unwrap();

    header[18..20].copy_from_slice(&183_u16.to_le_bytes());
    std::fs::write(&path, header).unwrap();
    assert!(validate_v5_worker_binary(&path)
        .unwrap_err()
        .to_string()
        .contains("x86-64"));
    std::fs::write(&path, b"Mach-O").unwrap();
    assert!(validate_v5_worker_binary(&path).is_err());
}

#[tokio::test]
async fn v5_driver_rejects_incompatible_worker_before_authentication() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("worker");
    std::fs::write(&path, b"Mach-O worker binary").unwrap();
    let mut options = DataflowOptions::new(
        "test-project",
        "us-central1",
        "test-job",
        "gs://test/staging",
        "gs://test/temp",
    );
    options.worker_binary_path = Some(path);
    let error = run_v5(
        |pipeline| {
            pipeline.from_vec("source", vec![1_i32])?.output("out")?;
            Ok(())
        },
        options,
        "test-build",
    )
    .await
    .unwrap_err();
    assert!(error.to_string().contains("worker binary"), "{error}");
}
