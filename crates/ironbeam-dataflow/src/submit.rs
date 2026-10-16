//! Build Dataflow v1b3 `Job` JSON and POST it to
//! `dataflow.googleapis.com/v1b3/projects/.../jobs`.
//!
//! The shape mirrors the Go SDK's `sdks/go/pkg/beam/runners/dataflow/dataflowlib/job.go`.
//! Dataflow does not expose the portable `JobService` publicly — this REST
//! path is the only way in.

use serde_json::{json, Value};

use crate::auth::TokenSource;
use crate::options::DataflowOptions;

/// Required experiments for Runner v2 / FnAPI batch. Mirrors the Go SDK's
/// `enableRunnerV2Experiments`. User-supplied experiments are appended.
pub const REQUIRED_EXPERIMENTS: &[&str] = &[
    "beam_fn_api",
    "use_runner_v2",
    "use_unified_worker",
    "use_portable_job_submission",
];

/// Build the request body for `projects.locations.jobs.create`.
///
/// `pipeline_url` is the `gs://…/pipeline.pb` from the staged, artifact-rewritten
/// proto. `environment_images` lists one `(environment_id, container_image)` pair
/// per entry in `Components.environments` with URN `beam:env:docker:v1`.
pub fn build_job_request(
    opts: &DataflowOptions,
    pipeline_url: &str,
    worker_binary_url: Option<&str>,
    environment_images: &[(String, String)],
) -> Value {
    let mut experiments: Vec<String> = REQUIRED_EXPERIMENTS.iter().map(|s| s.to_string()).collect();
    for e in &opts.experiments {
        if !experiments.contains(e) {
            experiments.push(e.clone());
        }
    }

    let sdk_images: Vec<Value> = environment_images
        .iter()
        .map(|(env_id, image)| {
            json!({
                "containerImage": image,
                "environmentId": env_id,
            })
        })
        .collect();

    let primary_image = environment_images
        .first()
        .map(|(_, image)| image.clone())
        .unwrap_or_else(|| opts.sdk_container_image.clone());

    let mut worker_pool = json!({
        "kind": "harness",
        "sdkHarnessContainerImages": sdk_images,
        "numWorkers": opts.num_workers.unwrap_or(1),
        "machineType": opts.worker_machine_type
            .clone()
            .unwrap_or_else(|| "n1-standard-1".to_string()),
        // Retained for older Dataflow services that still read these.
        "workerHarnessContainerImage": primary_image,
    });
    if let Some(network) = &opts.network {
        worker_pool["network"] = json!(network);
    }
    if let Some(subnetwork) = &opts.subnetwork {
        worker_pool["subnetwork"] = json!(subnetwork);
    }
    if !opts.use_public_ips {
        // Dataflow v1b3 enum `WorkerIPAddressConfiguration`.
        worker_pool["ipConfiguration"] = json!("WORKER_IP_PRIVATE");
    }
    // Dataflow's classic v1b3 path populates `ProvisionInfo.dependencies`
    // from `WorkerPool.packages[]`, NOT from the pipeline proto's
    // `Environment.dependencies[]`. Without a package entry, the container
    // boot fails with "Failed to get Go Worker Artifact Name: no artifacts
    // staged". The `name` ends up in the `ArtifactInformation` the boot
    // iterates; basename of the staged path is conventional.
    if let Some(url) = worker_binary_url {
        let name = url
            .rsplit('/')
            .next()
            .filter(|s| !s.is_empty())
            .unwrap_or("ironbeam_worker")
            .to_string();
        worker_pool["packages"] = json!([{ "name": name, "location": url }]);
    }

    let mut options = json!({
        "pipelineUrl": pipeline_url,
        "project": opts.project,
        "region": opts.region,
        "jobName": opts.job_name,
        "experiments": experiments,
        "stagingLocation": opts.staging_location,
        "tempLocation": opts.temp_location,
    });
    if let Some(sa) = &opts.service_account_email {
        options["serviceAccountEmail"] = json!(sa);
    }

    // Dataflow parses `userAgent.version` to gate on "SDK ≥ 2.21.0" for
    // Runner v2. Ironbeam version numbers do not describe Beam SDK's version, so we pose as
    // "Apache Beam SDK for Go" at `BEAM_SDK_VERSION` — matching the capability
    // URN we advertise on the Environment. The real ironbeam-dataflow version
    // still surfaces in sdkPipelineOptions.display_data for debugging.
    let mut environment = json!({
        "userAgent": {
            "name": "Apache Beam SDK for Go",
            "version": ironbeam_graphx::environment::BEAM_SDK_VERSION,
        },
        "version": { "job_type": "FNAPI_BATCH", "major": "8" },
        "experiments": experiments,
        "sdkPipelineOptions": {
            "options": options,
            "display_data": [
                {
                    "key": "ironbeam_dataflow_version",
                    "namespace": "ironbeam-dataflow",
                    "type": "STRING",
                    "value": env!("CARGO_PKG_VERSION"),
                },
            ],
        },
        "workerPools": [worker_pool],
        "tempStoragePrefix": opts.temp_location,
    });
    // `serviceAccountEmail` is an `Environment`-level field in Dataflow v1b3 —
    // NOT a `WorkerPool` field (Dataflow rejects the job with 400 otherwise).
    if let Some(sa) = &opts.service_account_email {
        environment["serviceAccountEmail"] = json!(sa);
    }

    json!({
        "name": opts.job_name,
        "type": "JOB_TYPE_BATCH",
        "environment": environment,
    })
}

#[derive(Debug, Clone)]
pub struct SubmittedJob {
    pub id: String,
    pub current_state: String,
    pub raw: Value,
}

/// Submit a prepared Dataflow job request with the supplied token source.
pub async fn submit_job<T: TokenSource + ?Sized>(
    base_url: &str,
    token_source: &T,
    opts: &DataflowOptions,
    body: &Value,
) -> anyhow::Result<SubmittedJob> {
    let url = format!(
        "{}/v1b3/projects/{}/locations/{}/jobs",
        base_url, opts.project, opts.region
    );
    let token = token_source.token().await?;
    let client = reqwest::Client::new();
    let resp = client
        .post(&url)
        .bearer_auth(token)
        .json(body)
        .send()
        .await?;
    let status = resp.status();
    let text = resp.text().await.unwrap_or_default();
    if !status.is_success() {
        return Err(anyhow::anyhow!(
            "Dataflow jobs.create {} failed: {} — {}",
            url,
            status,
            text
        ));
    }
    let v: Value = serde_json::from_str(&text).map_err(|e| {
        anyhow::anyhow!("jobs.create returned non-JSON response: {e} — body: {text}")
    })?;
    let id = v
        .get("id")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow::anyhow!("jobs.create response missing `id`: {}", v))?
        .to_string();
    let current_state = v
        .get("currentState")
        .and_then(|v| v.as_str())
        .unwrap_or("JOB_STATE_UNKNOWN")
        .to_string();
    Ok(SubmittedJob {
        id,
        current_state,
        raw: v,
    })
}

#[cfg(test)]
#[path = "../tests/unit/submit__tests.rs"]
mod tests;
