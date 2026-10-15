use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};

use beam_proto::pipeline_v1::{
    is_bounded, output_time, test_stream_payload, Coder, Components, DisplayData, FunctionSpec, PCollection,
    PTransform, ParDoPayload, Pipeline as BeamPipeline, SideInput, TestStreamPayload,
    WindowIntoPayload,
};
use ironbeam_core::{
    Boundedness, Capabilities, ElementDescriptor, EncodedTestStreamEvent, ExecutionIdentity, Graph,
    Node, Operation, Pipeline, Registry, Replay, Time, Window, WindowOutputTime, WindowTrigger, CODER_VERSION,
};
use prost::Message;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{
    environment::docker_environment,
    ids, urns,
    windowing::{apply_trigger, fixed_window_fn, fixed_windowing_strategy, global_windowing_strategy, session_window_fn, session_windowing_strategy, sliding_window_fn, sliding_windowing_strategy},
};

pub const IDENTITY_URN: &str = "ironbeam:execution-identity:v5";
const CODER_V5_LP_BYTES: &str = "v5_lp_bytes";
const CODER_V5_KV: &str = "v5_kv_bytes";
const CODER_V5_ITER: &str = "v5_iter_lp_bytes";
const CODER_V5_KV_ITER: &str = "v5_kv_iter_bytes";
const SIDE_INPUT_LOCAL_ID: &str = "side";
const SIDE_INPUT_ITERABLE_URN: &str = "beam:side_input:iterable:v1";
const SIDE_INPUT_VIEW_URN: &str = "ironbeam:view:materialized:v5";
const SIDE_INPUT_GLOBAL_MAPPING_URN: &str = "ironbeam:windowmapping:global:v5";

pub fn side_input_spec() -> SideInput {
    SideInput {
        access_pattern: Some(FunctionSpec {
            urn: SIDE_INPUT_ITERABLE_URN.into(),
            payload: Vec::new(),
        }),
        view_fn: Some(FunctionSpec {
            urn: SIDE_INPUT_VIEW_URN.into(),
            payload: Vec::new(),
        }),
        window_mapping_fn: Some(FunctionSpec {
            urn: SIDE_INPUT_GLOBAL_MAPPING_URN.into(),
            payload: Vec::new(),
        }),
    }
}

fn windowing_id(time: &Time) -> String {
    let base = match &time.window {
        Window::Global => ids::WINDOWING_ID.into(),
        Window::Fixed { size_ns } => id("v5_ws", &format!("fixed:{size_ns}:0")),
        Window::FixedOffset { size_ns, offset_ns } => {
            id("v5_ws", &format!("fixed:{size_ns}:{offset_ns}"))
        }
        Window::Sliding { size_ns, period_ns, offset_ns } => {
            id("v5_ws", &format!("sliding:{size_ns}:{period_ns}:{offset_ns}"))
        }
        Window::Session { gap_ns } => id("v5_ws", &format!("session:{gap_ns}")),
    };
    let triggered = match time.trigger {
        WindowTrigger::Default => base,
        WindowTrigger::EarlyElementCount { count } => {
            id("v5_ws", &format!("{base}:early-count:{count}"))
        }
        WindowTrigger::EarlyElementCountAccumulating { count } => {
            id("v5_ws", &format!("{base}:early-count:{count}:accumulating"))
        }
        WindowTrigger::EarlyProcessingTime { delay_ns } => {
            id("v5_ws", &format!("{base}:early-processing-time:{delay_ns}"))
        }
        WindowTrigger::EarlyProcessingTimeAccumulating { delay_ns } => {
            id("v5_ws", &format!("{base}:early-processing-time:{delay_ns}:accumulating"))
        }
        WindowTrigger::EarlyCountOrProcessingTime { count, delay_ns } => {
            id("v5_ws", &format!("{base}:early-count-or-processing-time:{count}:{delay_ns}"))
        }
        WindowTrigger::LateElementCount { count } => {
            id("v5_ws", &format!("{base}:late-count:{count}"))
        }
        WindowTrigger::LateElementCountAccumulating { count } => {
            id("v5_ws", &format!("{base}:late-count:{count}:accumulating"))
        }
        WindowTrigger::EarlyAndLateElementCount { early_count, late_count } => {
            id("v5_ws", &format!("{base}:early-count:{early_count}:late-count:{late_count}"))
        }
        WindowTrigger::EarlyAndLateElementCountAccumulating { early_count, late_count } => {
            id("v5_ws", &format!("{base}:early-count:{early_count}:late-count:{late_count}:accumulating"))
        }
    };
    let output_timed = match time.output_time {
        WindowOutputTime::EarliestInPane => triggered,
        WindowOutputTime::EndOfWindow => id("v5_ws", &format!("{triggered}:end-of-window")),
        WindowOutputTime::LatestInPane => id("v5_ws", &format!("{triggered}:latest-in-pane")),
    };
    if time.allowed_lateness_ns == 0 {
        output_timed
    } else {
        id("v5_ws", &format!("{output_timed}:lateness:{}", time.allowed_lateness_ns))
    }
}

fn window_fn(window: &Window) -> Result<FunctionSpec, V5Error> {
    match window {
        Window::Global => Ok(FunctionSpec {
            urn: urns::WINDOW_FN_GLOBAL.into(),
            payload: Vec::new(),
        }),
        Window::Fixed { size_ns } => fixed_window_fn(*size_ns, 0).map_err(V5Error::Wire),
        Window::FixedOffset { size_ns, offset_ns } => {
            fixed_window_fn(*size_ns, *offset_ns).map_err(V5Error::Wire)
        }
        Window::Sliding { size_ns, period_ns, offset_ns } => {
            sliding_window_fn(*size_ns, *period_ns, *offset_ns).map_err(V5Error::Wire)
        }
        Window::Session { gap_ns } => session_window_fn(*gap_ns).map_err(V5Error::Wire),
    }
}

fn keyed_inputs(graph: &Graph) -> HashSet<&str> {
    let mut keyed = HashSet::new();
    // Native Flatten forwards encoded bytes unchanged, so each of its inputs
    // needs the same KV coder when its output feeds native GroupByKey.
    for group in graph
        .nodes
        .iter()
        .filter(|node| matches!(node.operation, Operation::GroupByKey))
    {
        let mut pending = group.input.iter().map(String::as_str).collect::<Vec<_>>();
        while let Some(key) = pending.pop() {
            if !keyed.insert(key) {
                continue;
            }
            if let Some(flatten) = graph
                .node(key)
                .filter(|node| matches!(node.operation, Operation::Flatten))
            {
                pending.extend(flatten.input.iter().map(String::as_str));
                pending.extend(
                    flatten
                        .additional_inputs
                        .iter()
                        .map(|edge| edge.key.as_str()),
                );
            }
        }
    }
    keyed
}

fn cogroup_inputs(node: &Node) -> Vec<&str> {
    node.input
        .iter()
        .map(String::as_str)
        .chain(node.additional_inputs.iter().map(|edge| edge.key.as_str()))
        .collect()
}

#[derive(Debug, thiserror::Error)]
pub enum V5Error {
    #[error(transparent)]
    Core(#[from] ironbeam_core::Error),
    #[error("unsupported v5 node {key}: {reason}")]
    Unsupported { key: String, reason: String },
    #[error("invalid v5 graph: {0}")]
    Graph(String),
    #[error("invalid v5 wire contract: {0}")]
    Wire(String),
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct V5NodeRef {
    pub key: String,
    pub identity: ExecutionIdentity,
    /// Internal shuffle tag stage. Co-group uses the input index; folds use
    /// zero for data and global folds use one for the empty-input seed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub co_group_input: Option<usize>,
}
impl V5NodeRef {
    pub fn encode(&self) -> Result<Vec<u8>, V5Error> {
        serde_json::to_vec(self).map_err(|error| V5Error::Wire(error.to_string()))
    }
    pub fn decode(bytes: &[u8]) -> Result<Self, V5Error> {
        serde_json::from_slice(bytes).map_err(|error| V5Error::Wire(error.to_string()))
    }
}

pub struct TranslatedV5 {
    pub pipeline: BeamPipeline,
    pub identity: ExecutionIdentity,
    /// Stable node keys in parent-before-child order, including Outputs.
    pub chain: Vec<String>,
    /// Sink ParDos that require the runner's postcommit bundle callback.
    pub finalization_sinks: HashSet<String>,
    /// Expected primitive payloads, retained for contract validation.
    pub test_stream_payloads: HashMap<String, TestStreamPayload>,
}

fn digest<T: Serialize>(value: &T) -> Result<String, V5Error> {
    let bytes = serde_json::to_vec(value).map_err(|error| V5Error::Wire(error.to_string()))?;
    Ok(format!("sha256-v1:{}", hex::encode(Sha256::digest(bytes))))
}

fn test_stream_millis(key: &str, nanos: i64) -> Result<i64, V5Error> {
    if nanos % 1_000_000 != 0 {
        return Err(V5Error::Unsupported {
            key: key.into(),
            reason: "Beam TestStream requires millisecond-aligned time".into(),
        });
    }
    Ok(nanos / 1_000_000)
}

fn length_prefixed(bytes: &[u8]) -> Vec<u8> {
    let mut size = bytes.len() as u64;
    let mut encoded = Vec::with_capacity(bytes.len() + 10);
    while size >= 0x80 {
        encoded.push((size as u8) | 0x80);
        size >>= 7;
    }
    encoded.push(size as u8);
    encoded.extend_from_slice(bytes);
    encoded
}

fn build_test_stream_payload(
    key: &str,
    coder_id: String,
    events: &[EncodedTestStreamEvent],
) -> Result<TestStreamPayload, V5Error> {
    use test_stream_payload::event::{
        AddElements, AdvanceProcessingTime, AdvanceWatermark, Event as EventKind,
    };
    let events = events
        .iter()
        .map(|event| {
            let event = match event {
                EncodedTestStreamEvent::Elements(values) => EventKind::ElementEvent(AddElements {
                    elements: values
                        .iter()
                        .map(|(bytes, timestamp)| {
                            Ok(test_stream_payload::TimestampedElement {
                                encoded_element: length_prefixed(bytes),
                                timestamp: test_stream_millis(key, *timestamp)?,
                            })
                        })
                        .collect::<Result<Vec<_>, V5Error>>()?,
                    tag: String::new(),
                }),
                EncodedTestStreamEvent::AdvanceWatermark(timestamp) => {
                    EventKind::WatermarkEvent(AdvanceWatermark {
                        new_watermark: test_stream_millis(key, *timestamp)?,
                        tag: String::new(),
                    })
                }
                EncodedTestStreamEvent::AdvanceProcessingTime(duration) => {
                    EventKind::ProcessingTimeEvent(AdvanceProcessingTime {
                        advance_duration: test_stream_millis(key, *duration)?,
                    })
                }
            };
            Ok(test_stream_payload::Event { event: Some(event) })
        })
        .collect::<Result<Vec<_>, V5Error>>()?;
    Ok(TestStreamPayload {
        coder_id,
        events,
        endpoint: None,
    })
}

fn combine_test_stream_payloads(
    chain: &[String],
    transforms: &mut HashMap<String, PTransform>,
    root_ids: &mut Vec<String>,
    payloads: &mut HashMap<String, TestStreamPayload>,
) -> Result<(), V5Error> {
    use test_stream_payload::event::Event;
    let keys = chain
        .iter()
        .filter(|key| payloads.contains_key(*key))
        .collect::<Vec<_>>();
    if keys.len() < 2 {
        return Ok(());
    }
    let first = keys[0];
    let coder_id = payloads[first].coder_id.clone();
    if keys.iter().any(|key| payloads[*key].coder_id != coder_id) {
        return Err(V5Error::Unsupported {
            key: "test-stream".into(),
            reason: "tagged local TestStream sources require the same element coder".into(),
        });
    }
    let mut events = Vec::new();
    // Beam has one processing clock for every tag in a TestStream. Direct
    // observes the greatest clock reached by an independent source reader.
    let mut source_time_ms = vec![0_i64; keys.len()];
    let mut shared_time_ms = 0_i64;
    let max_events = keys
        .iter()
        .map(|key| payloads[*key].events.len())
        .max()
        .unwrap_or(0);
    for index in 0..max_events {
        for (source_index, key) in keys.iter().enumerate() {
            let Some(mut event) = payloads[*key].events.get(index).cloned() else {
                continue;
            };
            match event.event.as_mut() {
                Some(Event::ElementEvent(element)) => element.tag.clone_from(key),
                Some(Event::WatermarkEvent(watermark)) => watermark.tag.clone_from(key),
                Some(Event::ProcessingTimeEvent(processing)) => {
                    let next = source_time_ms[source_index]
                        .checked_add(processing.advance_duration)
                        .ok_or_else(|| V5Error::Wire("tagged TestStream clock overflow".into()))?;
                    if processing.advance_duration <= 0 {
                        return Err(V5Error::Wire(
                            "tagged TestStream processing time must advance".into(),
                        ));
                    }
                    source_time_ms[source_index] = next;
                    if next <= shared_time_ms {
                        continue;
                    }
                    processing.advance_duration = next - shared_time_ms;
                    shared_time_ms = next;
                }
                _ => return Err(V5Error::Wire("unexpected tagged TestStream event".into())),
            }
            events.push(event);
        }
    }
    let outputs = keys
        .iter()
        .map(|key| ((*key).clone(), id("v5_pc", key)))
        .collect();
    let primary = transforms
        .get_mut(&id("v5_t", first))
        .ok_or_else(|| V5Error::Wire("missing primary TestStream transform".into()))?;
    primary.outputs = outputs;
    let combined = TestStreamPayload {
        coder_id,
        events,
        endpoint: None,
    };
    primary
        .spec
        .as_mut()
        .ok_or_else(|| V5Error::Wire("missing TestStream spec".into()))?
        .payload = combined.encode_to_vec();
    for key in keys.iter().skip(1) {
        let transform_id = id("v5_t", key);
        transforms.remove(&transform_id);
        root_ids.retain(|id| id != &transform_id);
        payloads.remove(*key);
    }
    payloads.insert(first.clone(), combined);
    Ok(())
}
