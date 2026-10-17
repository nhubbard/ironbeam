// Unit tests for src/submit.rs.
use super::*;

fn opts() -> DataflowOptions {
    let mut o = DataflowOptions::new(
        "my-project",
        "us-central1",
        "wordcount-20260421",
        "gs://my-bucket/staging",
        "gs://my-bucket/tmp",
    );
    o.num_workers = Some(3);
    o.service_account_email = Some("svc@proj.iam.gserviceaccount.com".into());
    o.experiments = vec!["enable_streaming_engine".into()];
    o
}

#[test]
fn job_request_carries_runner_v2_experiments() {
    let v = build_job_request(
        &opts(),
        "gs://my-bucket/staging/wordcount/pipeline.pb",
        None,
        &[("ironbeam_env".into(), "gcr.io/ibeam:latest".into())],
    );
    let exps = v["environment"]["experiments"].as_array().unwrap();
    let strs: Vec<&str> = exps.iter().map(|e| e.as_str().unwrap()).collect();
    for e in REQUIRED_EXPERIMENTS {
        assert!(strs.contains(e), "missing required experiment: {e}");
    }
    // User-supplied experiment appended, not duplicated.
    assert!(strs.contains(&"enable_streaming_engine"));
    assert_eq!(
        strs.iter().filter(|s| **s == "beam_fn_api").count(),
        1,
        "no duplicates"
    );
}

#[test]
fn job_request_top_level_shape() {
    let v = build_job_request(
        &opts(),
        "gs://my-bucket/staging/wordcount/pipeline.pb",
        None,
        &[("ironbeam_env".into(), "gcr.io/ibeam:latest".into())],
    );
    assert_eq!(v["name"], "wordcount-20260421");
    assert_eq!(v["type"], "JOB_TYPE_BATCH");
    let options = &v["environment"]["sdkPipelineOptions"]["options"];
    assert_eq!(
        options["pipelineUrl"],
        "gs://my-bucket/staging/wordcount/pipeline.pb"
    );
    assert_eq!(options["project"], "my-project");
    assert_eq!(options["region"], "us-central1");
    assert_eq!(options["jobName"], "wordcount-20260421");
    assert_eq!(options["stagingLocation"], "gs://my-bucket/staging");
    assert_eq!(options["tempLocation"], "gs://my-bucket/tmp");
}

#[test]
fn job_request_worker_pool_contains_sdk_images() {
    let v = build_job_request(
        &opts(),
        "gs://p.pb",
        None,
        &[
            ("env_go".into(), "gcr.io/go:latest".into()),
            ("env_py".into(), "gcr.io/py:latest".into()),
        ],
    );
    let pool = &v["environment"]["workerPools"][0];
    assert_eq!(pool["kind"], "harness");
    assert_eq!(pool["numWorkers"], 3);
    let images = pool["sdkHarnessContainerImages"].as_array().unwrap();
    assert_eq!(images.len(), 2);
    assert_eq!(images[0]["environmentId"], "env_go");
    assert_eq!(images[0]["containerImage"], "gcr.io/go:latest");
    // serviceAccountEmail is an Environment-level field, NOT a WorkerPool
    // field — Dataflow rejects the job 400 INVALID_ARGUMENT otherwise.
    assert!(pool.get("serviceAccountEmail").is_none());
    assert_eq!(
        v["environment"]["serviceAccountEmail"],
        "svc@proj.iam.gserviceaccount.com"
    );
    // First image is mirrored into the legacy field.
    assert_eq!(pool["workerHarnessContainerImage"], "gcr.io/go:latest");
}

#[test]
fn job_request_defaults_when_no_environments() {
    let v = build_job_request(&opts(), "gs://p.pb", None, &[]);
    let pool = &v["environment"]["workerPools"][0];
    // Falls back to the default container image configured in opts.
    assert_eq!(
        pool["workerHarnessContainerImage"],
        crate::options::DEFAULT_SDK_CONTAINER_IMAGE
    );
    assert!(pool["sdkHarnessContainerImages"]
        .as_array()
        .unwrap()
        .is_empty());
}

#[test]
fn job_request_omits_network_fields_by_default() {
    let v = build_job_request(&opts(), "gs://p.pb", None, &[]);
    let pool = &v["environment"]["workerPools"][0];
    assert!(pool.get("network").is_none());
    assert!(pool.get("subnetwork").is_none());
    assert!(pool.get("ipConfiguration").is_none());
}

#[test]
fn job_request_emits_worker_binary_package() {
    let v = build_job_request(
        &opts(),
        "gs://p.pb",
        Some("gs://my-bucket/staging/job/ironbeam_worker"),
        &[],
    );
    let pool = &v["environment"]["workerPools"][0];
    let pkgs = pool["packages"].as_array().expect("packages emitted");
    assert_eq!(pkgs.len(), 1);
    assert_eq!(pkgs[0]["name"], "ironbeam_worker");
    assert_eq!(
        pkgs[0]["location"],
        "gs://my-bucket/staging/job/ironbeam_worker"
    );
}

#[test]
fn job_request_omits_packages_when_no_worker_url() {
    let v = build_job_request(&opts(), "gs://p.pb", None, &[]);
    let pool = &v["environment"]["workerPools"][0];
    assert!(pool.get("packages").is_none());
}

#[test]
fn job_request_emits_subnetwork_and_private_ip() {
    let mut o = opts();
    o.network = Some("vpc-0".into());
    o.subnetwork = Some(
        "https://www.googleapis.com/compute/v1/projects/p/regions/europe-west4/subnetworks/sn"
            .into(),
    );
    o.use_public_ips = false;
    let v = build_job_request(&o, "gs://p.pb", None, &[]);
    let pool = &v["environment"]["workerPools"][0];
    assert_eq!(pool["network"], "vpc-0");
    assert_eq!(
        pool["subnetwork"],
        "https://www.googleapis.com/compute/v1/projects/p/regions/europe-west4/subnetworks/sn"
    );
    assert_eq!(pool["ipConfiguration"], "WORKER_IP_PRIVATE");
}
