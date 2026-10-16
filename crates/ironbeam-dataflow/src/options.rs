//! Dataflow job options. Fields are modeled on the subset of
//! `google.dataflow.v1b3.Environment` and `PipelineOptions` the Dataflow
//! `projects.jobs.create` endpoint requires for a runner-v2 batch job.

pub use ironbeam_graphx::DEFAULT_SDK_CONTAINER_IMAGE;

#[derive(Debug, Clone)]
/// Project, staging, worker, and network settings for a Dataflow job.
pub struct DataflowOptions {
    pub project: String,
    pub region: String,
    pub job_name: String,
    /// `gs://<bucket>/<prefix>` — staging for pipeline.pb and worker binary.
    pub staging_location: String,
    /// `gs://<bucket>/<prefix>` — Dataflow temp storage.
    pub temp_location: String,
    pub sdk_container_image: String,
    pub service_account_email: Option<String>,
    pub num_workers: Option<u32>,
    pub worker_machine_type: Option<String>,
    /// Extra Dataflow experiments appended to the runner-v2 defaults.
    pub experiments: Vec<String>,
    /// Short name or full URL of the VPC network; leave unset to use `default`.
    /// Ignored by Dataflow when [`subnetwork`](Self::subnetwork) is set in a
    /// custom-mode VPC.
    pub network: Option<String>,
    /// Subnetwork URI —
    /// `https://www.googleapis.com/compute/v1/projects/<p>/regions/<r>/subnetworks/<name>`
    /// or the short `regions/<r>/subnetworks/<name>` form. Required when
    /// running against a Shared VPC or a custom-mode network.
    pub subnetwork: Option<String>,
    /// When `false`, workers launch with only private IPs (equivalent to the
    /// Go/Python SDKs' `--no_use_public_ips`). Requires Private Google Access
    /// on the subnetwork or a VPC NAT. Default: `true`.
    pub use_public_ips: bool,
    /// Explicit path to the worker binary that should be staged to GCS and
    /// execed inside the Dataflow container. Required when the driver runs on
    /// macOS/Windows, because Dataflow workers are linux-amd64 and
    /// `std::env::current_exe()` would otherwise stage an incompatible binary.
    /// When unset, the driver falls back to `std::env::current_exe()`.
    pub worker_binary_path: Option<std::path::PathBuf>,
}

impl DataflowOptions {
    /// Set the required project, region, job name, and GCS staging locations.
    ///
    /// Optional worker and networking settings retain their defaults.
    pub fn new(
        project: impl Into<String>,
        region: impl Into<String>,
        job_name: impl Into<String>,
        staging_location: impl Into<String>,
        temp_location: impl Into<String>,
    ) -> Self {
        Self {
            project: project.into(),
            region: region.into(),
            job_name: job_name.into(),
            staging_location: staging_location.into(),
            temp_location: temp_location.into(),
            sdk_container_image: DEFAULT_SDK_CONTAINER_IMAGE.to_string(),
            service_account_email: None,
            num_workers: None,
            worker_machine_type: None,
            experiments: Vec::new(),
            network: None,
            subnetwork: None,
            use_public_ips: true,
            worker_binary_path: None,
        }
    }

    /// Read options from environment variables. Mandatory: `GOOGLE_CLOUD_PROJECT`,
    /// `IRONBEAM_STAGING_LOCATION`, `IRONBEAM_TEMP_LOCATION`. Optional, with
    /// defaults:
    ///
    /// | Variable | Default |
    /// |---|---|
    /// | `DATAFLOW_REGION` | `us-central1` |
    /// | `IRONBEAM_JOB_NAME` | `ironbeam-<unix_seconds>` |
    /// | `IRONBEAM_SDK_CONTAINER_IMAGE` | stock Go SDK image |
    /// | `IRONBEAM_SERVICE_ACCOUNT_EMAIL` | unset |
    /// | `IRONBEAM_NUM_WORKERS` | unset (Dataflow default) |
    /// | `IRONBEAM_MACHINE_TYPE` | unset |
    /// | `IRONBEAM_EXPERIMENTS` | unset (comma-separated list when set) |
    /// | `IRONBEAM_NETWORK` | unset |
    /// | `IRONBEAM_SUBNETWORK` | unset |
    /// | `IRONBEAM_USE_PUBLIC_IPS` | `true` (set to `false`/`0`/`no` for private-IP workers) |
    /// | `IRONBEAM_WORKER_BINARY` | unset — fall back to `std::env::current_exe()`. **Required on macOS/Windows**, where `current_exe()` would stage an incompatible binary to a linux-amd64 Dataflow worker. |
    pub fn from_env() -> anyhow::Result<Self> {
        use std::env::{var, VarError};

        fn required(name: &str) -> anyhow::Result<String> {
            match std::env::var(name) {
                Ok(v) if !v.is_empty() => Ok(v),
                _ => Err(anyhow::anyhow!("required env var {name} is unset")),
            }
        }

        let project = required("GOOGLE_CLOUD_PROJECT")?;
        let staging_location = required("IRONBEAM_STAGING_LOCATION")?;
        let temp_location = required("IRONBEAM_TEMP_LOCATION")?;
        let region = var("DATAFLOW_REGION").unwrap_or_else(|_| "us-central1".to_string());
        let job_name = var("IRONBEAM_JOB_NAME").unwrap_or_else(|_| {
            format!(
                "ironbeam-{}",
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_secs())
                    .unwrap_or(0)
            )
        });
        let sdk_container_image = var("IRONBEAM_SDK_CONTAINER_IMAGE")
            .unwrap_or_else(|_| DEFAULT_SDK_CONTAINER_IMAGE.to_string());
        let service_account_email = match var("IRONBEAM_SERVICE_ACCOUNT_EMAIL") {
            Ok(v) if !v.is_empty() => Some(v),
            _ => None,
        };
        let num_workers = match var("IRONBEAM_NUM_WORKERS") {
            Ok(v) => Some(
                v.parse()
                    .map_err(|e| anyhow::anyhow!("IRONBEAM_NUM_WORKERS must be a u32: {e}"))?,
            ),
            Err(VarError::NotPresent) => None,
            Err(e) => return Err(e.into()),
        };
        let worker_machine_type = var("IRONBEAM_MACHINE_TYPE").ok();
        let experiments = var("IRONBEAM_EXPERIMENTS")
            .map(|s| {
                s.split(',')
                    .map(|t| t.trim().to_string())
                    .filter(|t| !t.is_empty())
                    .collect()
            })
            .unwrap_or_default();
        let network = match var("IRONBEAM_NETWORK") {
            Ok(v) if !v.is_empty() => Some(v),
            _ => None,
        };
        let subnetwork = match var("IRONBEAM_SUBNETWORK") {
            Ok(v) if !v.is_empty() => Some(v),
            _ => None,
        };
        let use_public_ips = match var("IRONBEAM_USE_PUBLIC_IPS") {
            Ok(v) => !matches!(
                v.trim().to_ascii_lowercase().as_str(),
                "false" | "0" | "no" | "off"
            ),
            Err(VarError::NotPresent) => true,
            Err(e) => return Err(e.into()),
        };
        let worker_binary_path = match var("IRONBEAM_WORKER_BINARY") {
            Ok(v) if !v.is_empty() => Some(std::path::PathBuf::from(v)),
            _ => None,
        };

        Ok(Self {
            project,
            region,
            job_name,
            staging_location,
            temp_location,
            sdk_container_image,
            service_account_email,
            num_workers,
            worker_machine_type,
            experiments,
            network,
            subnetwork,
            use_public_ips,
            worker_binary_path,
        })
    }
}

#[cfg(test)]
#[path = "../tests/unit/options__tests.rs"]
mod tests;
