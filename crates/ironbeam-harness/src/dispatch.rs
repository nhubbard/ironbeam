//! Turns `InstructionRequest` into `InstructionResponse`.
//!
//! ## Scope
//! - Progress, split, monitoring, sample, and register return valid empty/OK
//!   responses. Finalization needs an executor callback and is never a no-op.
//! - `ProcessBundle` without an executor: analyze a cached PBD and report its
//!   plan as an error. The control loop runs bundles through `BundleRunner`
//!   when one is configured. An uncached PBD asks the loop to fetch it first.
//!
//! The outer Control loop (`control.rs`) is responsible for threading these
//! responses back through the bidi stream AND for populating the PBD cache
//! via `GetProcessBundleDescriptor`. This module is deliberately gRPC-free
//! so we can unit-test it with plain `InstructionRequest` values.

use std::collections::HashMap;
use std::fmt::Write;
use std::sync::{Arc, Mutex};

use beam_proto::fn_execution_v1::{
    instruction_request::Request as InstrReq, instruction_response::Response as InstrResp,
    HarnessMonitoringInfosResponse, InstructionRequest, InstructionResponse,
    MonitoringInfosMetadataResponse, ProcessBundleDescriptor, ProcessBundleProgressResponse,
    ProcessBundleResponse, ProcessBundleSplitResponse, RegisterResponse, SampleDataResponse,
};
use beam_proto::pipeline_v1::MonitoringInfo;
use tracing::debug;

use crate::bundle::{self, Step};

/// A registry of `ProcessBundleDescriptor`s indexed by id. The control loop
/// populates this on-demand by calling
/// `BeamFnControl.GetProcessBundleDescriptor` when it sees an unfamiliar id.
#[derive(Default, Clone)]
pub struct PbdCache {
    inner: Arc<Mutex<HashMap<String, ProcessBundleDescriptor>>>,
}

impl PbdCache {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&self, pbd: ProcessBundleDescriptor) {
        self.inner.lock().unwrap().insert(pbd.id.clone(), pbd);
    }

    pub fn get(&self, id: &str) -> Option<ProcessBundleDescriptor> {
        self.inner.lock().unwrap().get(id).cloned()
    }

    pub fn len(&self) -> usize {
        self.inner.lock().unwrap().len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.lock().unwrap().is_empty()
    }
}

/// gRPC-free dispatcher. Holds just the state needed to produce
/// `InstructionResponse` values for a given `InstructionRequest`.
pub struct Dispatcher {
    pub pbd_cache: PbdCache,
    /// Structural fingerprint of the pipeline rebuilt locally. The ProcessBundle
    /// handler (M2b) will cross-check this against the one the driver embedded
    /// in pipeline display data — if they disagree, fail fast.
    pub local_fingerprint: u64,
    metric_registration: Mutex<HashMap<String, bool>>,
}

// Prism buffers unknown metric payloads in a map indexed by short ID. A
// transform-wide ID can therefore overwrite an earlier bundle's count while
// Prism is fetching its metadata. Give concurrent bundles distinct IDs until
// the stable ID's metadata response leaves Fn Control. Thereafter reuse it.
fn stable_metric_id(prefix: &str, label: &str) -> String {
    format!("{prefix}:{}:{label}", label.len())
}

fn metric_label<'a>(id: &'a str, prefix: &str) -> Option<&'a str> {
    let rest = id.strip_prefix(prefix)?.strip_prefix(':')?;
    let (length, rest) = rest.split_once(':')?;
    let length = length.parse::<usize>().ok()?;
    let label = rest.get(..length)?;
    let suffix = rest.get(length..)?;
    let valid_suffix = suffix.is_empty()
        || suffix
            .strip_prefix(":bundle:")
            .is_some_and(|instruction| !instruction.is_empty());
    (!label.is_empty() && valid_suffix).then_some(label)
}

fn metric_metadata(id: &str) -> Option<MonitoringInfo> {
    let (urn, labels) = if let Some(pc) = metric_label(id, "ironbeam-element-count") {
        (
            "beam:metric:element_count:v1",
            [("PCOLLECTION".into(), pc.into())].into(),
        )
    } else {
        let transform = metric_label(id, "ironbeam-user-sum")?;
        (
            "beam:metric:user:sum_int64:v1",
            [
                ("PTRANSFORM".into(), transform.into()),
                ("NAMESPACE".into(), "ironbeam.sink".into()),
                ("NAME".into(), "input_elements".into()),
            ]
            .into(),
        )
    };
    Some(MonitoringInfo {
        urn: urn.into(),
        r#type: "beam:metrics:sum_int64:v1".into(),
        labels,
        ..Default::default()
    })
}

impl Dispatcher {
    pub fn new(pbd_cache: PbdCache, local_fingerprint: u64) -> Self {
        Self {
            pbd_cache,
            local_fingerprint,
            metric_registration: Mutex::new(HashMap::new()),
        }
    }

    /// Publish distinct pending IDs, then stable IDs after runner registration.
    pub fn register_bundle_metrics(
        &self,
        instruction_id: &str,
        response: &mut ProcessBundleResponse,
    ) -> Result<(), String> {
        if instruction_id.is_empty() {
            return Err("bundle metric instruction ID is empty".into());
        }
        let mut registration = self
            .metric_registration
            .lock()
            .map_err(|_| "metric registration state poisoned".to_owned())?;
        let mut bundle_ids = HashMap::new();
        for info in &response.monitoring_infos {
            if info.r#type != "beam:metrics:sum_int64:v1" {
                return Err(format!("unsupported v5 monitoring info {}", info.urn));
            }
            let base_id = match info.urn.as_str() {
                "beam:metric:element_count:v1" => {
                    let pc = info
                        .labels
                        .get("PCOLLECTION")
                        .ok_or_else(|| "element count lacks PCOLLECTION label".to_owned())?;
                    stable_metric_id("ironbeam-element-count", pc)
                }
                "beam:metric:user:sum_int64:v1" => {
                    let transform = info
                        .labels
                        .get("PTRANSFORM")
                        .ok_or_else(|| "user count lacks PTRANSFORM label".to_owned())?;
                    let namespace = info
                        .labels
                        .get("NAMESPACE")
                        .ok_or_else(|| "user count lacks NAMESPACE label".to_owned())?;
                    let name = info
                        .labels
                        .get("NAME")
                        .ok_or_else(|| "user count lacks NAME label".to_owned())?;
                    if namespace != "ironbeam.sink" || name != "input_elements" {
                        return Err("unsupported v5 user count labels".into());
                    }
                    stable_metric_id("ironbeam-user-sum", transform)
                }
                _ => return Err(format!("unsupported v5 monitoring info {}", info.urn)),
            };
            let mut descriptor = info.clone();
            descriptor.payload.clear();
            if metric_metadata(&base_id).as_ref() != Some(&descriptor) {
                return Err(format!("monitoring metadata changed for {base_id}"));
            }
            let id = bundle_ids
                .entry(base_id.clone())
                .or_insert_with(|| match registration.get(&base_id) {
                    Some(true) | None => base_id.clone(),
                    Some(false) => format!("{base_id}:bundle:{instruction_id}"),
                })
                .clone();
            registration.entry(base_id).or_insert(false);
            if response
                .monitoring_data
                .insert(id.clone(), info.payload.clone())
                .is_some_and(|old| old != info.payload)
            {
                return Err(format!("duplicate monitoring data disagrees for {id}"));
            }
        }
        Ok(())
    }

    /// Called when the Fn Control transport takes a metadata response. Later
    /// bundle responses on that same stream may reuse its stable short IDs.
    pub fn mark_metric_metadata_sent(&self, response: &InstructionResponse) {
        let Some(InstrResp::MonitoringInfos(metadata)) = &response.response else {
            return;
        };
        if let Ok(mut registration) = self.metric_registration.lock() {
            for id in metadata.monitoring_info.keys() {
                if let Some(confirmed) = registration.get_mut(id) {
                    *confirmed = true;
                }
            }
        }
    }

    /// Convert a request into a response. Never panics — errors are surfaced
    /// as `InstructionResponse.error`.
    pub fn handle(&self, req: InstructionRequest) -> InstructionResponse {
        let id = req.instruction_id.clone();
        match req.request {
            None => error(id, "instruction had no request oneof set"),
            Some(InstrReq::ProcessBundle(pbr)) => {
                let pbd_id = &pbr.process_bundle_descriptor_id;
                debug!(
                    instruction_id = %id,
                    %pbd_id,
                    "ProcessBundle"
                );
                match self.pbd_cache.get(pbd_id) {
                    None => error(
                        id,
                        format!(
                            "ProcessBundle for unknown pbd_id={pbd_id}; \
                             control loop must fetch it via \
                             GetProcessBundleDescriptor first"
                        ),
                    ),
                    Some(pbd) => match bundle::plan(&pbd) {
                        Err(e) => error(id, format!("bundle plan failed for pbd_id={pbd_id}: {e}")),
                        Ok(plan) => error(
                            id,
                            format!(
                                "ProcessBundle execution not yet wired (M2c). \
                                 Bundle plan ({} steps) for pbd_id={pbd_id}:\n{}",
                                plan.steps.len(),
                                describe_plan(&plan.steps)
                            ),
                        ),
                    },
                }
            }
            Some(InstrReq::ProcessBundleProgress(_)) => ok(
                id,
                InstrResp::ProcessBundleProgress(ProcessBundleProgressResponse::default()),
            ),
            Some(InstrReq::ProcessBundleSplit(_)) => ok(
                id,
                InstrResp::ProcessBundleSplit(ProcessBundleSplitResponse::default()),
            ),
            Some(InstrReq::FinalizeBundle(request)) => error(
                id,
                format!(
                    "FinalizeBundle for {} requires an executor callback",
                    request.instruction_id
                ),
            ),
            Some(InstrReq::MonitoringInfos(request)) => {
                let mut monitoring_info = HashMap::new();
                for metric_id in request.monitoring_info_id {
                    let Some(info) = metric_metadata(&metric_id) else {
                        return error(id, format!("unknown monitoring info id {metric_id}"));
                    };
                    monitoring_info.insert(metric_id, info);
                }
                ok(
                    id,
                    InstrResp::MonitoringInfos(MonitoringInfosMetadataResponse { monitoring_info }),
                )
            }
            Some(InstrReq::HarnessMonitoringInfos(_)) => ok(
                id,
                InstrResp::HarnessMonitoringInfos(HarnessMonitoringInfosResponse::default()),
            ),
            Some(InstrReq::SampleData(_)) => {
                ok(id, InstrResp::SampleData(SampleDataResponse::default()))
            }
            Some(InstrReq::Register(_)) => {
                // Deprecated but still arrives from some runners.
                ok(id, InstrResp::Register(RegisterResponse::default()))
            }
        }
    }
}

fn describe_plan(steps: &[Step]) -> String {
    let mut out = String::new();
    for (i, s) in steps.iter().enumerate() {
        let _ = write!(&mut out, "  [{i}] ");
        match s {
            Step::Source {
                transform_id,
                output_pc,
                ..
            } => {
                let _ = writeln!(&mut out, "Source {transform_id} -> {output_pc}");
            }
            Step::V5ParDo {
                transform_id,
                node_ref,
                input_pc,
                output_pc,
            } => {
                let _ = writeln!(
                    &mut out,
                    "V5 ParDo {transform_id} (key={}): {input_pc} -> {output_pc}",
                    node_ref.key
                );
            }
            Step::Sdf {
                stage,
                transform_id,
                node_ref,
                input_pc,
                output_pc,
            } => {
                let _ = writeln!(
                    &mut out,
                    "V5 SDF {stage:?} {transform_id} (key={}): {input_pc} -> {output_pc}",
                    node_ref.key
                );
            }
            Step::WindowInto {
                transform_id,
                input_pc,
                output_pc,
                ..
            } => {
                let _ = writeln!(
                    &mut out,
                    "WindowInto {transform_id}: {input_pc} -> {output_pc}"
                );
            }
            Step::Sink {
                transform_id,
                input_pc,
                ..
            } => {
                let _ = writeln!(&mut out, "Sink {transform_id} <- {input_pc}");
            }
        }
    }
    out
}

fn ok(id: String, resp: InstrResp) -> InstructionResponse {
    InstructionResponse {
        instruction_id: id,
        error: String::new(),
        response: Some(resp),
    }
}

fn error(id: String, msg: impl Into<String>) -> InstructionResponse {
    InstructionResponse {
        instruction_id: id,
        error: msg.into(),
        response: None,
    }
}

#[cfg(test)]
#[path = "../tests/unit/dispatch__tests.rs"]
mod tests;
