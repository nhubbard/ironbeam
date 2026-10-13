//! Driver-mode orchestration: stage → rewrite artifacts → submit → poll.

use beam_proto::pipeline_v1::{DockerPayload, Pipeline};
use prost::Message;
use tracing::{info, warn};

use crate::artifact::{attach_worker_binary, StagedArtifact};
use crate::auth::TokenSource;
use crate::options::DataflowOptions;
use crate::poll::{self, PollConfig};
use crate::stage::Stager;
use crate::submit::{self, SubmittedJob};

/// Terminal outcome of a Dataflow submission.
#[derive(Debug, Clone)]
pub struct JobOutcome {
    pub job_id: String,
    pub final_state: String,
}

/// Top-level driver. Composes a [`Stager`], a [`TokenSource`], and the
/// Dataflow base URL into a reusable unit — tests pass a memory stager and a
/// wiremock URL; production uses [`crate::stage::GcsStager`] and
/// `https://dataflow.googleapis.com`.
pub struct Driver<S, T> {
    pub stager: S,
    pub token_source: T,
    pub dataflow_base_url: String,
    pub poll_config: PollConfig,
}

impl<S: Stager, T: TokenSource> Driver<S, T> {
    pub fn new(stager: S, token_source: T) -> Self {
        Self {
            stager,
            token_source,
            dataflow_base_url: "https://dataflow.googleapis.com".to_string(),
            poll_config: PollConfig::default(),
        }
    }

    pub fn with_dataflow_base_url(mut self, url: impl Into<String>) -> Self {
        self.dataflow_base_url = url.into();
        self
    }

    pub fn with_poll_config(mut self, cfg: PollConfig) -> Self {
        self.poll_config = cfg;
        self
    }

    /// Runs the full driver flow against a translated Beam `Pipeline` proto.
    pub async fn submit_pipeline(
        &self,
        mut pipeline: Pipeline,
        opts: &DataflowOptions,
    ) -> anyhow::Result<JobOutcome> {
        // 1. Stage the worker binary. Prefer the explicit override so users
        //    can cross-compile to linux-amd64 and point at the result.
        let exe = match opts.worker_binary_path.clone() {
            Some(p) => p,
            None => std::env::current_exe()
                .map_err(|e| anyhow::anyhow!("current_exe() for worker staging: {e}"))?,
        };
        warn_if_not_elf(&exe);
        info!(path = %exe.display(), "staging worker binary");
        let worker = self
            .stager
            .put_file(&format!("{}/ironbeam_worker", opts.job_name), &exe)
            .await?;

        // 2. Rewrite Environment.dependencies to point at the staged binary.
        attach_worker_binary(&mut pipeline, &worker);

        // 3. Stage the final pipeline.pb (after artifact rewrite).
        let bytes = pipeline.encode_to_vec();
        info!(size = bytes.len(), "staging pipeline.pb");
        let pipeline_artifact = self
            .stager
            .put(&format!("{}/pipeline.pb", opts.job_name), bytes)
            .await?;

        // 4. Collect per-environment SDK container images.
        let sdk_images = collect_sdk_images(&pipeline);
        if sdk_images.is_empty() {
            warn!("no beam:env:docker:v1 environments found — Dataflow will reject this job");
        }

        // 5. Build Job JSON and POST.
        let body = submit::build_job_request(
            opts,
            &pipeline_artifact.url,
            Some(worker.url.as_str()),
            &sdk_images,
        );
        let SubmittedJob {
            id, current_state, ..
        } = submit::submit_job(&self.dataflow_base_url, &self.token_source, opts, &body).await?;
        info!(%id, %current_state, "job submitted");

        // 6. Poll to terminal.
        let final_state = poll::poll_until_terminal(
            &self.dataflow_base_url,
            &self.token_source,
            opts,
            &id,
            &self.poll_config,
            |state, _| info!(%id, state, "job state update"),
        )
        .await?;
        info!(%id, %final_state, "job reached terminal state");

        Ok(JobOutcome {
            job_id: id,
            final_state,
        })
    }

    /// Variant that does not poll — submits and returns the initial
    /// [`SubmittedJob`]. Useful for fire-and-forget submissions and in tests.
    pub async fn submit_only(
        &self,
        mut pipeline: Pipeline,
        opts: &DataflowOptions,
        worker: &StagedArtifact,
    ) -> anyhow::Result<SubmittedJob> {
        attach_worker_binary(&mut pipeline, worker);
        let bytes = pipeline.encode_to_vec();
        let pipeline_artifact = self
            .stager
            .put(&format!("{}/pipeline.pb", opts.job_name), bytes)
            .await?;
        let sdk_images = collect_sdk_images(&pipeline);
        let body = submit::build_job_request(
            opts,
            &pipeline_artifact.url,
            Some(worker.url.as_str()),
            &sdk_images,
        );
        submit::submit_job(&self.dataflow_base_url, &self.token_source, opts, &body).await
    }
}

/// Peek at the first 4 bytes of the would-be worker binary. Dataflow workers
/// are linux-amd64, so anything that isn't an ELF almost certainly won't run.
/// Log-only — production surface the warning via `tracing` rather than aborting,
/// so esoteric setups (e.g. a shim script) still work.
fn warn_if_not_elf(path: &std::path::Path) {
    use std::io::Read;
    match std::fs::File::open(path) {
        Ok(mut f) => {
            let mut magic = [0u8; 4];
            match f.read_exact(&mut magic) {
                Ok(()) if magic == [0x7F, b'E', b'L', b'F'] => {}
                Ok(()) => warn!(
                    path = %path.display(),
                    magic = format!("{:02x} {:02x} {:02x} {:02x}",
                        magic[0], magic[1], magic[2], magic[3]),
                    "worker binary is not a Linux ELF — Dataflow workers will fail to exec it. \
                     Set IRONBEAM_WORKER_BINARY to a cross-compiled linux-amd64 build."
                ),
                Err(e) => warn!(path = %path.display(), error = %e, "worker binary unreadable"),
            }
        }
        Err(e) => warn!(path = %path.display(), error = %e, "worker binary unreadable"),
    }
}

fn collect_sdk_images(pipeline: &Pipeline) -> Vec<(String, String)> {
    let Some(comps) = pipeline.components.as_ref() else {
        return Vec::new();
    };
    let mut out: Vec<(String, String)> = comps
        .environments
        .iter()
        .filter(|(_, env)| env.urn == ironbeam_graphx::urns::ENV_DOCKER)
        .filter_map(|(id, env)| {
            DockerPayload::decode(env.payload.as_slice())
                .ok()
                .map(|p| (id.clone(), p.container_image))
        })
        .collect();
    out.sort();
    out
}
