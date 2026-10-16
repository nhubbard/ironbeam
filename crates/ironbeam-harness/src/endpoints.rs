//! Parses the argv flags that Dataflow's container boot passes to the SDK
//! worker binary. Matches the contract documented at
//! <https://beam.apache.org/documentation/runtime/environments/> and
//! implemented by `sdks/go/container/boot.go`.

use std::collections::HashMap;

use anyhow::{bail, Context, Result};

#[derive(Debug, Clone)]
pub struct Endpoints {
    /// Worker id assigned by the runner (`--id`). Becomes `WorkerId` in
    /// gRPC request metadata.
    pub id: String,
    /// Required. The runner's Control endpoint (host:port).
    pub control_endpoint: String,
    /// Required. The runner's Logging endpoint.
    pub logging_endpoint: String,
    /// Optional. Provision info source; absent when the container skips provisioning.
    pub provision_endpoint: Option<String>,
    /// Optional. Artifact retrieval service.
    pub artifact_endpoint: Option<String>,
    /// Optional. Scratch dir the runner gives us for persisted state.
    pub semi_persist_dir: Option<String>,
}

impl Endpoints {
    /// Resolve endpoints passed to a Beam process environment. Prism launches
    /// the process with only an id and Provision endpoint; the latter carries
    /// the Control and Logging endpoints for this worker.
    pub async fn from_process_argv<I, S>(args: I) -> Result<Self>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let flags = collect_flags(args.into_iter().map(Into::into));
        let id = flags
            .get("id")
            .cloned()
            .context("missing worker --id")?
            .trim_matches('\'')
            .to_string();
        let provision_endpoint = flags
            .get("provision_endpoint")
            .cloned()
            .context("missing --provision_endpoint")?;
        let info =
            crate::provision::get_provision_info_for_worker(&provision_endpoint, &id).await?;
        let control_endpoint = info
            .control_endpoint
            .context("ProvisionInfo has no control endpoint")?
            .url;
        let logging_endpoint = info
            .logging_endpoint
            .context("ProvisionInfo has no logging endpoint")?
            .url;
        Ok(Self {
            id,
            control_endpoint,
            logging_endpoint,
            provision_endpoint: Some(provision_endpoint),
            artifact_endpoint: info.artifact_endpoint.map(|endpoint| endpoint.url),
            semi_persist_dir: flags.get("semi_persist_dir").cloned(),
        })
    }

    /// Read launch arguments and resolve worker endpoints through Provision.
    pub async fn from_process_env() -> Result<Self> {
        Self::from_process_argv(std::env::args().skip(1)).await
    }

    /// Parse from an iterator of argv tokens (skipping `argv[0]` is the caller's
    /// responsibility). Accepts both `--flag value` and `--flag=value` forms.
    pub fn from_argv<I, S>(args: I) -> Result<Self>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let flags = collect_flags(args.into_iter().map(Into::into));
        let get_required = |name: &str| -> Result<String> {
            flags
                .get(name)
                .cloned()
                .with_context(|| format!("missing required flag --{name}"))
        };

        Ok(Self {
            id: get_required("id")?,
            control_endpoint: get_required("control_endpoint")?,
            logging_endpoint: get_required("logging_endpoint")?,
            provision_endpoint: flags.get("provision_endpoint").cloned(),
            artifact_endpoint: flags.get("artifact_endpoint").cloned(),
            semi_persist_dir: flags.get("semi_persist_dir").cloned(),
        })
    }

    /// Parse worker endpoints from the current process arguments.
    pub fn from_env() -> Result<Self> {
        Self::from_argv(std::env::args().skip(1))
    }
}

pub(crate) fn worker_request<T>(payload: T, worker_id: Option<&str>) -> Result<tonic::Request<T>> {
    let mut request = tonic::Request::new(payload);
    if let Some(id) = worker_id {
        request.metadata_mut().insert(
            "worker_id",
            id.parse().context("invalid worker id metadata")?,
        );
    }
    Ok(request)
}

fn collect_flags<I: Iterator<Item = String>>(mut args: I) -> HashMap<String, String> {
    let mut out = HashMap::new();
    while let Some(tok) = args.next() {
        let Some(rest) = tok.strip_prefix("--") else {
            continue;
        };
        if let Some((name, value)) = rest.split_once('=') {
            out.insert(name.to_string(), value.to_string());
        } else if let Some(next) = args.next() {
            out.insert(rest.to_string(), next);
        }
    }
    out
}

/// If an endpoint is given as a bare `host:port` (Beam/Dataflow convention),
/// tonic wants it as a URL. Prepend `http://` when no scheme is present.
pub fn normalize_endpoint(endpoint: &str) -> Result<String> {
    if endpoint.contains("://") {
        Ok(endpoint.to_string())
    } else if endpoint.contains(':') {
        Ok(format!("http://{endpoint}"))
    } else {
        bail!("endpoint {endpoint:?} is not host:port")
    }
}

#[cfg(test)]
#[path = "../tests/unit/endpoints__tests.rs"]
mod tests;
