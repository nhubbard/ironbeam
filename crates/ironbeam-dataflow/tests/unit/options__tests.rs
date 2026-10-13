// Unit tests for src/options.rs.
use super::*;
use std::sync::Mutex;

// `std::env` is process-global; serialize tests that touch it.
static ENV_LOCK: Mutex<()> = Mutex::new(());

fn clear() {
    for k in [
        "GOOGLE_CLOUD_PROJECT",
        "IRONBEAM_STAGING_LOCATION",
        "IRONBEAM_TEMP_LOCATION",
        "DATAFLOW_REGION",
        "IRONBEAM_JOB_NAME",
        "IRONBEAM_SDK_CONTAINER_IMAGE",
        "IRONBEAM_SERVICE_ACCOUNT_EMAIL",
        "IRONBEAM_NUM_WORKERS",
        "IRONBEAM_MACHINE_TYPE",
        "IRONBEAM_EXPERIMENTS",
        "IRONBEAM_NETWORK",
        "IRONBEAM_SUBNETWORK",
        "IRONBEAM_USE_PUBLIC_IPS",
        "IRONBEAM_WORKER_BINARY",
    ] {
        std::env::remove_var(k);
    }
}

#[test]
fn from_env_happy_path() {
    let _g = ENV_LOCK.lock().unwrap();
    clear();
    std::env::set_var("GOOGLE_CLOUD_PROJECT", "my-proj");
    std::env::set_var("IRONBEAM_STAGING_LOCATION", "gs://b/stage");
    std::env::set_var("IRONBEAM_TEMP_LOCATION", "gs://b/tmp");
    std::env::set_var("DATAFLOW_REGION", "us-west1");
    std::env::set_var("IRONBEAM_JOB_NAME", "wc-42");
    std::env::set_var("IRONBEAM_NUM_WORKERS", "4");
    std::env::set_var("IRONBEAM_EXPERIMENTS", "enable_x, enable_y ,");

    let opts = DataflowOptions::from_env().unwrap();
    assert_eq!(opts.project, "my-proj");
    assert_eq!(opts.region, "us-west1");
    assert_eq!(opts.job_name, "wc-42");
    assert_eq!(opts.num_workers, Some(4));
    assert_eq!(opts.experiments, vec!["enable_x", "enable_y"]);
    clear();
}

#[test]
fn from_env_missing_project_errors() {
    let _g = ENV_LOCK.lock().unwrap();
    clear();
    std::env::set_var("IRONBEAM_STAGING_LOCATION", "gs://b/s");
    std::env::set_var("IRONBEAM_TEMP_LOCATION", "gs://b/t");
    let err = DataflowOptions::from_env().unwrap_err();
    assert!(format!("{err}").contains("GOOGLE_CLOUD_PROJECT"));
    clear();
}

#[test]
fn from_env_defaults_region_and_job_name() {
    let _g = ENV_LOCK.lock().unwrap();
    clear();
    std::env::set_var("GOOGLE_CLOUD_PROJECT", "p");
    std::env::set_var("IRONBEAM_STAGING_LOCATION", "gs://b/s");
    std::env::set_var("IRONBEAM_TEMP_LOCATION", "gs://b/t");
    let opts = DataflowOptions::from_env().unwrap();
    assert_eq!(opts.region, "us-central1");
    assert!(opts.job_name.starts_with("ironbeam-"));
    assert_eq!(opts.network, None);
    assert_eq!(opts.subnetwork, None);
    assert!(opts.use_public_ips);
    clear();
}

#[test]
fn from_env_reads_network_and_public_ips() {
    let _g = ENV_LOCK.lock().unwrap();
    clear();
    std::env::set_var("GOOGLE_CLOUD_PROJECT", "p");
    std::env::set_var("IRONBEAM_STAGING_LOCATION", "gs://b/s");
    std::env::set_var("IRONBEAM_TEMP_LOCATION", "gs://b/t");
    std::env::set_var("IRONBEAM_NETWORK", "my-vpc");
    std::env::set_var(
        "IRONBEAM_SUBNETWORK",
        "https://www.googleapis.com/compute/v1/projects/p/regions/europe-west4/subnetworks/s1",
    );
    std::env::set_var("IRONBEAM_USE_PUBLIC_IPS", "false");
    let opts = DataflowOptions::from_env().unwrap();
    assert_eq!(opts.network.as_deref(), Some("my-vpc"));
    assert!(opts
        .subnetwork
        .as_deref()
        .unwrap()
        .ends_with("/subnetworks/s1"));
    assert!(!opts.use_public_ips);
    clear();
}
