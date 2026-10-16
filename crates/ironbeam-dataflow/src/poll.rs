//! Poll Dataflow `jobs.get` until the job reaches a terminal state and
//! optionally tail `jobs/{id}/messages` for worker-side logs.
//!
//! Terminal states (per the `JobState` enum in `dataflow_v1beta3`):
//! `JOB_STATE_DONE`, `JOB_STATE_FAILED`, `JOB_STATE_CANCELLED`,
//! `JOB_STATE_UPDATED`, `JOB_STATE_DRAINED`.
//!
//! `JOB_STATE_UPDATED` is terminal from the polling perspective because the
//! current job has been replaced by a newer one; the caller should reissue
//! the request against the new job id if they care.

use std::time::{Duration, Instant};

use serde_json::Value;
use tracing::{debug, info, warn};

use crate::auth::TokenSource;
use crate::options::DataflowOptions;

pub const TERMINAL_STATES: &[&str] = &[
    "JOB_STATE_DONE",
    "JOB_STATE_FAILED",
    "JOB_STATE_CANCELLED",
    "JOB_STATE_UPDATED",
    "JOB_STATE_DRAINED",
];

/// Return whether a Dataflow job state is terminal.
pub fn is_terminal(state: &str) -> bool {
    TERMINAL_STATES.contains(&state)
}

#[derive(Debug, Clone)]
/// Polling interval and timeout for a submitted Dataflow job.
pub struct PollConfig {
    pub interval: Duration,
    /// Hard timeout. `None` means no bound — poll until terminal.
    pub timeout: Option<Duration>,
    /// If true, fetch `jobs/{id}/messages` each cycle and emit them as
    /// `tracing` events keyed by importance. Newer-than filtering uses the
    /// latest RFC3339 `time` field already seen, so tests can assert at most
    /// one emission per message across cycles.
    pub tail_messages: bool,
}

impl Default for PollConfig {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(5),
            timeout: Some(Duration::from_secs(3600)),
            tail_messages: false,
        }
    }
}

/// Poll one Dataflow job until it reaches a terminal state or times out.
///
/// `on_state` receives each observed state before the next poll.
pub async fn poll_until_terminal<T, F>(
    base_url: &str,
    token_source: &T,
    opts: &DataflowOptions,
    job_id: &str,
    config: &PollConfig,
    mut on_state: F,
) -> anyhow::Result<String>
where
    T: TokenSource + ?Sized,
    F: FnMut(&str, &Value),
{
    let url = format!(
        "{}/v1b3/projects/{}/locations/{}/jobs/{}",
        base_url, opts.project, opts.region, job_id
    );
    let client = reqwest::Client::new();
    let start = Instant::now();
    let mut last_message_time: Option<String> = None;
    loop {
        let token = token_source.token().await?;
        let resp = client.get(&url).bearer_auth(token).send().await?;
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        if !status.is_success() {
            return Err(anyhow::anyhow!(
                "Dataflow jobs.get {} failed: {} — {}",
                url,
                status,
                text
            ));
        }
        let v: Value = serde_json::from_str(&text)?;
        let state = v
            .get("currentState")
            .and_then(|v| v.as_str())
            .unwrap_or("JOB_STATE_UNKNOWN");
        on_state(state, &v);

        if config.tail_messages {
            match list_messages(
                base_url,
                token_source,
                opts,
                job_id,
                last_message_time.as_deref(),
            )
            .await
            {
                Ok((msgs, latest)) => {
                    for m in &msgs {
                        log_message(m);
                    }
                    if latest.is_some() {
                        last_message_time = latest;
                    }
                }
                Err(e) => warn!(error = %e, "list_messages failed"),
            }
        }

        if is_terminal(state) {
            return Ok(state.to_string());
        }
        if let Some(t) = config.timeout {
            if start.elapsed() > t {
                return Err(anyhow::anyhow!(
                    "poll timed out after {:?}; last state = {state}",
                    t
                ));
            }
        }
        tokio::time::sleep(config.interval).await;
    }
}

/// One page of `jobs.messages`. Paginates server-side by time; `startTime`
/// is exclusive of messages whose `time` equals it, so feeding back the
/// previous call's max timestamp avoids re-emitting the same message.
///
/// Returns `(messages, latest_time)` where `latest_time` is the maximum
/// RFC3339 `time` seen (or `None` if the page is empty).
pub async fn list_messages<T: TokenSource + ?Sized>(
    base_url: &str,
    token_source: &T,
    opts: &DataflowOptions,
    job_id: &str,
    start_time: Option<&str>,
) -> anyhow::Result<(Vec<Value>, Option<String>)> {
    let mut url = reqwest::Url::parse(&format!(
        "{}/v1b3/projects/{}/locations/{}/jobs/{}/messages",
        base_url, opts.project, opts.region, job_id
    ))?;
    {
        let mut q = url.query_pairs_mut();
        q.append_pair("pageSize", "100");
        if let Some(s) = start_time {
            q.append_pair("startTime", s);
        }
    }
    let token = token_source.token().await?;
    let client = reqwest::Client::new();
    let resp = client.get(url).bearer_auth(token).send().await?;
    let status = resp.status();
    let text = resp.text().await.unwrap_or_default();
    if !status.is_success() {
        return Err(anyhow::anyhow!(
            "Dataflow jobs.messages failed: {} — {}",
            status,
            text
        ));
    }
    let v: Value = serde_json::from_str(&text)?;
    let msgs: Vec<Value> = v
        .get("jobMessages")
        .and_then(|x| x.as_array())
        .cloned()
        .unwrap_or_default();
    let latest = msgs
        .iter()
        .filter_map(|m| m.get("time").and_then(|t| t.as_str()))
        .max()
        .map(String::from);
    Ok((msgs, latest))
}

fn log_message(m: &Value) {
    let time = m.get("time").and_then(|v| v.as_str()).unwrap_or("?");
    let importance = m
        .get("messageImportance")
        .and_then(|v| v.as_str())
        .unwrap_or("JOB_MESSAGE_BASIC");
    let text = m.get("messageText").and_then(|v| v.as_str()).unwrap_or("");
    match importance {
        "JOB_MESSAGE_ERROR" => warn!(%time, kind = importance, "{text}"),
        "JOB_MESSAGE_WARNING" => warn!(%time, kind = importance, "{text}"),
        "JOB_MESSAGE_DEBUG" | "JOB_MESSAGE_DETAILED" => {
            debug!(%time, kind = importance, "{text}")
        }
        _ => info!(%time, kind = importance, "{text}"),
    }
}

#[cfg(test)]
#[path = "../tests/unit/poll__tests.rs"]
mod tests;
