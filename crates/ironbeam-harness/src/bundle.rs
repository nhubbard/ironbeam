//! Turns a `ProcessBundleDescriptor` into a structured `BundlePlan` the
//! executor can walk.
//!
//! ### Scope (M2b)
//! Purely structural analysis — no gRPC, no execution. The executor in M2c
//! consumes this plan to open the right `BeamFnData` streams and dispatch
//! elements through the harness.
//!
//! ### Plan shape
//! A Dataflow-v2 PBD arriving at our harness generally contains:
//! - zero or one `beam:runner:source:v1` DataSource primitives (one
//!   `RemoteGrpcPort` to read input elements from)
//! - one or more `beam:transform:pardo:v1` ParDos with our
//!   `beam:dofn:ironbeam:v5` DoFn URN
//! - zero or more `beam:runner:sink:v1` DataSink primitives (one
//!   `RemoteGrpcPort` to write output elements to)
//!
//! GroupByKey is never in a PBD — the runner handles shuffles outside SDK
//! bundles — so we don't plan for it here.

use std::collections::{HashMap, HashSet};

use beam_proto::fn_execution_v1::{ProcessBundleDescriptor, RemoteGrpcPort};
use beam_proto::pipeline_v1::{
    state_spec, time_domain, Coder, FunctionSpec, PTransform, ParDoPayload, WindowIntoPayload,
};
use ironbeam_graphx::{urns, v5::V5NodeRef};
use prost::Message;

#[path = "bundle_map_windows.rs"]
mod map_windows;
use map_windows::{coders_equivalent, plan_map_windows};

pub const URN_RUNNER_SOURCE: &str = "beam:runner:source:v1";
pub const URN_RUNNER_SINK: &str = "beam:runner:sink:v1";

#[derive(Debug, thiserror::Error)]
pub enum PlanError {
    #[error("PTransform {0}: missing FunctionSpec")]
    MissingSpec(String),
    #[error("PTransform {id}: spec.payload failed to decode ({err})")]
    BadPayload { id: String, err: String },
    #[error("PTransform {id}: ParDo spec missing do_fn")]
    MissingDoFn { id: String },
    #[error("PTransform {id}: unsupported URN {urn}")]
    UnsupportedUrn { id: String, urn: String },
    #[error("PBD has {0} DataSource transforms; MVP supports exactly 0 or 1")]
    TooManySources(usize),
    #[error("transform {id} references unknown input/output PCollection {pc}")]
    UnknownPCollection { id: String, pc: String },
    #[error("bundle graph is not a connected linear chain rooted at the DataSource")]
    NotLinear,
}

fn reject_unsupported_state_and_timers(id: &str, payload: &ParDoPayload) -> Result<(), PlanError> {
    if !payload.state_specs.is_empty()
        || !payload.timer_family_specs.is_empty()
        || !payload.on_window_expiration_timer_family_spec.is_empty()
    {
        return Err(PlanError::BadPayload {
            id: id.into(),
            err: "v5 user state and timers require a stateful DoFn contract".into(),
        });
    }
    Ok(())
}

fn value_state_payload(
    id: &str,
    payload: &ParDoPayload,
    pbd: &ProcessBundleDescriptor,
) -> Result<bool, PlanError> {
    if payload.state_specs.is_empty() {
        reject_unsupported_state_and_timers(id, payload)?;
        return Ok(false);
    }
    let timers_valid = payload
        .timer_family_specs
        .iter()
        .all(|(family_id, family)| {
            !family_id.is_empty()
                && matches!(
                    time_domain::Enum::try_from(family.time_domain),
                    Ok(time_domain::Enum::EventTime | time_domain::Enum::ProcessingTime)
                )
                && pbd
                    .coders
                    .get(&family.timer_family_coder_id)
                    .is_some_and(|coder| {
                        coder.spec.as_ref().is_some_and(|spec| {
                            spec.urn == urns::CODER_TIMER && spec.payload.is_empty()
                        }) && coder.component_coder_ids.len() == 2
                            && pbd
                                .coders
                                .get(&coder.component_coder_ids[0])
                                .is_some_and(|key| {
                                    key.spec.as_ref().is_some_and(|spec| {
                                        spec.urn == urns::CODER_LENGTH_PREFIX
                                            && spec.payload.is_empty()
                                    }) && key.component_coder_ids.len() == 1
                                        && pbd.coders.get(&key.component_coder_ids[0]).is_some_and(
                                            |bytes| {
                                                bytes.spec.as_ref().is_some_and(|spec| {
                                                    spec.urn == urns::CODER_BYTES
                                                        && spec.payload.is_empty()
                                                }) && bytes.component_coder_ids.is_empty()
                                            },
                                        )
                                })
                            && pbd
                                .coders
                                .get(&coder.component_coder_ids[1])
                                .is_some_and(|window| {
                                    window.spec.as_ref().is_some_and(|spec| {
                                        matches!(
                                            spec.urn.as_str(),
                                            urns::CODER_GLOBAL_WINDOW | urns::CODER_INTERVAL_WINDOW
                                        ) && spec.payload.is_empty()
                                    }) && window.component_coder_ids.is_empty()
                                })
                    })
        });
    let valid = payload.state_specs.len() == 1
        && timers_valid
        && payload.on_window_expiration_timer_family_spec.is_empty()
        && payload
            .state_specs
            .iter()
            .next()
            .is_some_and(|(name, spec)| {
                let (coder_ids, protocol_urn) = match (name.as_str(), &spec.spec) {
                    ("value", Some(state_spec::Spec::ReadModifyWriteSpec(cell))) => {
                        (vec![&cell.coder_id], "beam:user_state:bag:v1")
                    }
                    ("bag", Some(state_spec::Spec::BagSpec(cell))) => {
                        (vec![&cell.element_coder_id], "beam:user_state:bag:v1")
                    }
                    ("combining", Some(state_spec::Spec::CombiningSpec(cell)))
                        if cell.combine_fn.as_ref().is_some_and(|combine| {
                            combine.urn.contains('/')
                                && serde_json::from_slice::<ironbeam_core::Version>(
                                    &combine.payload,
                                )
                                .is_ok_and(|version| version.major > 0)
                        }) =>
                    {
                        (vec![&cell.accumulator_coder_id], "beam:user_state:bag:v1")
                    }
                    ("set", Some(state_spec::Spec::SetSpec(cell))) => {
                        (vec![&cell.element_coder_id], "beam:user_state:multimap:v1")
                    }
                    ("map", Some(state_spec::Spec::MapSpec(cell))) => (
                        vec![&cell.key_coder_id, &cell.value_coder_id],
                        "beam:user_state:multimap:v1",
                    ),
                    ("multimap", Some(state_spec::Spec::MultimapSpec(cell))) => (
                        vec![&cell.key_coder_id, &cell.value_coder_id],
                        "beam:user_state:multimap:v1",
                    ),
                    ("ordered_list", Some(state_spec::Spec::OrderedListSpec(cell))) => (
                        vec![&cell.element_coder_id],
                        "beam:user_state:ordered_list:v1",
                    ),
                    _ => return false,
                };
                if !payload.timer_family_specs.is_empty() && name != "value" {
                    return false;
                }
                coder_ids.into_iter().all(|coder_id| {
                    pbd.coders.get(coder_id).is_some_and(|coder| {
                        coder.spec.as_ref().is_some_and(|spec| {
                            spec.urn == urns::CODER_LENGTH_PREFIX && spec.payload.is_empty()
                        }) && coder.component_coder_ids.len() == 1
                            && pbd
                                .coders
                                .get(&coder.component_coder_ids[0])
                                .is_some_and(|child| {
                                    child.spec.as_ref().is_some_and(|spec| {
                                        spec.urn == urns::CODER_BYTES && spec.payload.is_empty()
                                    }) && child.component_coder_ids.is_empty()
                                })
                    })
                }) && spec.protocol.as_ref().is_some_and(|protocol| {
                    protocol.urn == protocol_urn && protocol.payload.is_empty()
                })
            });
    if !valid {
        return Err(PlanError::BadPayload {
            id: id.into(),
            err: "v5 ParDo requires one supported state cell and valid timer-family coders (timers require the value cell)"
                .into(),
        });
    }
    Ok(true)
}

/// Classification of the per-element wire format the runner expects on a
/// bundle sink. Derived at plan time by walking the sink port's coder chain
/// in the PBD's `coders` map. The executor reads this to emit bytes the
/// runner's length-prefix decoder can actually parse.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SinkWire {
    /// `windowed_value || length_prefix(body)`. The entire element body is
    /// one opaque length-prefixed blob. Default for ironbeam's opaque
    /// `length_prefix<c_ib>` PCollections and for any coder shape we don't
    /// recognize — if we guess wrong and the runner actually expected KV,
    /// the runner will surface a coder error and we'll notice.
    Opaque,
    /// `windowed_value || length_prefix(key) || length_prefix(value)`. Used
    /// when the sink PCollection's coder is
    /// `kv<length_prefix<_>, length_prefix<_>>`, which Dataflow emits for
    /// pre-GBK edges after substituting our custom coder with `byte_array`.
    KvLp,
}

/// Classification of the per-element wire format arriving on a bundle
/// source. Post-GBK stages read from shuffle using a
/// `kv<lp<K>, iter<lp<V>>>` or `kv<lp<K>, state_backed_iterable<V>>` shape;
/// pre-shuffle sources (Impulse, in-memory) are plain `lp<_>`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceWire {
    /// `windowed_value || length_prefix(body)`. Default; matches Impulse
    /// and any `wv<lp<_>, gw>` PCollection coder.
    Opaque,
    /// `windowed_value || lp(k) || lp(v)` for a keyed collection crossing
    /// a runner boundary before GroupByKey.
    KvLp,
    /// `windowed_value || lp(k) || int32_be(count) || [lp(v)] × count`.
    /// Inline iterable — matches `wv<kv<lp<_>, iterable<lp<_>>>, gw>`. The
    /// executor reassembles each element via the predecessor's
    /// `PostcardGroupedKvCoder` before the first post-GBK ParDo runs.
    KvIterLp,
    /// `wv<kv<lp<_>, state_backed_iterable<_>>, gw>`. Runners may substitute
    /// this for GBK outputs. The executor obtains the iterable bytes through
    /// the Beam Fn State API before reconstructing the grouped element.
    KvStateBackedIter,
}

/// A single step in the bundle plan.
#[derive(Debug, Clone)]
pub enum Step {
    /// Read bytes from `port`, decode with `coder_id`, emit to `output_pc`.
    Source {
        transform_id: String,
        port: RemoteGrpcPort,
        output_pc: String,
        wire: SourceWire,
    },
    V5ParDo {
        transform_id: String,
        node_ref: V5NodeRef,
        input_pc: String,
        output_pc: String,
        stateful_value: bool,
    },
    Sdf {
        stage: SdfStage,
        transform_id: String,
        node_ref: V5NodeRef,
        input_pc: String,
        output_pc: String,
    },
    WindowInto {
        transform_id: String,
        input_pc: String,
        output_pc: String,
        window_fn: FunctionSpec,
    },
    MapWindows {
        transform_id: String,
        input_pc: String,
        output_pc: String,
        mapping: WindowMapping,
        outer_window: WindowCoder,
        input_window: WindowCoder,
    },
    /// Encode elements from `input_pc` with `coder_id`, write to `port`.
    Sink {
        transform_id: String,
        port: RemoteGrpcPort,
        input_pc: String,
        wire: SinkWire,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WindowCoder {
    Global,
    Interval,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WindowMapping {
    Global,
    Identity,
    Fixed {
        size_millis: i64,
        offset_millis: i64,
    },
    Sliding {
        size_millis: i64,
        period_millis: i64,
        offset_millis: i64,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SdfStage {
    PairWithRestriction,
    SplitAndSizeRestrictions,
    TruncateSizedRestrictions,
    ProcessSizedElements,
}

#[derive(Debug, Clone)]
pub struct BundlePlan {
    /// Steps in the order the executor should run them.
    pub steps: Vec<Step>,
    /// PBD id the plan was derived from — used in log messages.
    pub pbd_id: String,
    /// Runner state service used by side inputs and state-backed iterables.
    pub state_endpoint: Option<String>,
    /// Side PCollection for each portable transform key in this bundle.
    pub side_inputs: HashMap<String, String>,
}

pub fn plan(pbd: &ProcessBundleDescriptor) -> Result<BundlePlan, PlanError> {
    // 1. Categorize every transform.
    let mut kinds: HashMap<&str, TransformKind> = HashMap::new();
    for (id, t) in &pbd.transforms {
        let urn = t
            .spec
            .as_ref()
            .map(|s| s.urn.as_str())
            .ok_or_else(|| PlanError::MissingSpec(id.clone()))?;

        let kind = match urn {
            URN_RUNNER_SOURCE => TransformKind::Source,
            URN_RUNNER_SINK => TransformKind::Sink,
            u if u == urns::TRANSFORM_PARDO => TransformKind::Pardo,
            u if u == urns::TRANSFORM_SDF_PAIR_WITH_RESTRICTION => {
                TransformKind::Sdf(SdfStage::PairWithRestriction)
            }
            u if u == urns::TRANSFORM_SDF_SPLIT_AND_SIZE => {
                TransformKind::Sdf(SdfStage::SplitAndSizeRestrictions)
            }
            u if u == urns::TRANSFORM_SDF_PROCESS_SIZED => {
                TransformKind::Sdf(SdfStage::ProcessSizedElements)
            }
            u if u == urns::TRANSFORM_SDF_TRUNCATE_SIZED => {
                TransformKind::Sdf(SdfStage::TruncateSizedRestrictions)
            }
            u if u == urns::TRANSFORM_WINDOW_INTO => TransformKind::WindowInto,
            u if u == urns::TRANSFORM_MAP_WINDOWS => TransformKind::MapWindows,
            // Dataflow may wrap impulse/gbk inside a PBD boundary; skip them.
            u if u == urns::TRANSFORM_IMPULSE || u == urns::TRANSFORM_GROUP_BY_KEY => {
                TransformKind::Skip
            }
            u => {
                return Err(PlanError::UnsupportedUrn {
                    id: id.clone(),
                    urn: u.to_string(),
                });
            }
        };
        kinds.insert(id.as_str(), kind);
    }

    // 2. Identify the single source (if any) and order the rest by PCollection
    //    flow.
    let source_ids: Vec<&str> = kinds
        .iter()
        .filter_map(|(id, k)| (matches!(k, TransformKind::Source)).then_some(*id))
        .collect();
    if source_ids.len() > 1 {
        return Err(PlanError::TooManySources(source_ids.len()));
    }

    let mut ordered: Vec<&str> = Vec::with_capacity(pbd.transforms.len());
    if let Some(&src_id) = source_ids.first() {
        ordered.push(src_id);
    }
    // BFS by output→consumer.
    let pc_producers = build_pc_producer_index(pbd);
    let pc_consumers = build_pc_consumer_index(pbd);
    let mut visited: HashSet<String> = ordered.iter().map(|s| s.to_string()).collect();

    let mut frontier = ordered.clone();
    while let Some(id) = frontier.pop() {
        let t = pbd.transforms.get(id).expect("id in transforms");
        for out_pc in t.outputs.values() {
            if let Some(consumer_ids) = pc_consumers.get(out_pc) {
                for consumer in consumer_ids {
                    if visited.insert(consumer.clone()) {
                        ordered.push(consumer);
                        frontier.push(consumer);
                    }
                }
            }
        }
    }
    // Also include transforms not reachable from the source (common case:
    // there's no explicit source, just a Sink-only PBD the runner uses to
    // consume outputs from another stage).
    for id in pbd.transforms.keys() {
        if !visited.contains(id) && !matches!(kinds[id.as_str()], TransformKind::Skip) {
            ordered.push(id);
            visited.insert(id.clone());
        }
    }

    // 3. Emit steps.
    let mut steps = Vec::new();
    let mut side_inputs = HashMap::new();
    for id in ordered {
        let t = pbd.transforms.get(id).expect("id in transforms");
        match kinds[id] {
            TransformKind::Skip => continue,
            TransformKind::Source => {
                let port = decode_port(id, t)?;
                let output_pc = single_output(id, t, &pc_producers)?;
                let wire = classify_source_wire(pbd, &port.coder_id);
                steps.push(Step::Source {
                    transform_id: id.to_string(),
                    port,
                    output_pc,
                    wire,
                });
            }
            TransformKind::Sink => {
                let port = decode_port(id, t)?;
                let input_pc = single_input(id, t)?;
                let wire = classify_sink_wire(pbd, &port.coder_id);
                steps.push(Step::Sink {
                    transform_id: id.to_string(),
                    port,
                    input_pc,
                    wire,
                });
            }
            TransformKind::Pardo => {
                let node_ref = decode_ironbeam_node_ref(id, t)?;
                let pardo = ParDoPayload::decode(
                    t.spec
                        .as_ref()
                        .expect("categorized spec")
                        .payload
                        .as_slice(),
                )
                .map_err(|error| PlanError::BadPayload {
                    id: id.to_string(),
                    err: error.to_string(),
                })?;
                let stateful_value = value_state_payload(id, &pardo, pbd)?;
                let input_pc = if t.inputs.len() == 2 {
                    if pardo.side_inputs.len() != 1
                        || !matches!(
                            pardo.side_inputs.get("side"),
                            Some(spec)
                                if spec == &ironbeam_graphx::v5::side_input_spec()
                                    || spec == &ironbeam_graphx::v5::windowed_wait_side_input_spec()
                        )
                    {
                        return Err(PlanError::BadPayload {
                            id: id.to_string(),
                            err: "v5 side input contract differs".into(),
                        });
                    }
                    let side = t.inputs.get("side").ok_or(PlanError::NotLinear)?;
                    side_inputs.insert(node_ref.key.clone(), side.clone());
                    t.inputs.get("in").ok_or(PlanError::NotLinear)?.clone()
                } else {
                    single_input(id, t)?
                };
                let output_pc = if t.outputs.is_empty() {
                    String::new()
                } else {
                    single_output(id, t, &pc_producers)?
                };
                steps.push(Step::V5ParDo {
                    transform_id: id.to_string(),
                    node_ref,
                    input_pc,
                    output_pc,
                    stateful_value,
                });
            }
            TransformKind::Sdf(stage) => {
                let payload = ParDoPayload::decode(
                    t.spec
                        .as_ref()
                        .expect("categorized spec")
                        .payload
                        .as_slice(),
                )
                .map_err(|error| PlanError::BadPayload {
                    id: id.to_string(),
                    err: error.to_string(),
                })?;
                reject_unsupported_state_and_timers(id, &payload)?;
                if payload.restriction_coder_id.is_empty() || !payload.side_inputs.is_empty() {
                    return Err(PlanError::BadPayload {
                        id: id.to_string(),
                        err: "v5 SDF requires a restriction coder and no side inputs".into(),
                    });
                }
                steps.push(Step::Sdf {
                    stage,
                    transform_id: id.to_string(),
                    node_ref: decode_ironbeam_node_ref(id, t)?,
                    input_pc: single_input(id, t)?,
                    output_pc: single_output(id, t, &pc_producers)?,
                });
            }
            TransformKind::WindowInto => {
                let payload = t
                    .spec
                    .as_ref()
                    .expect("categorized spec")
                    .payload
                    .as_slice();
                let payload =
                    WindowIntoPayload::decode(payload).map_err(|error| PlanError::BadPayload {
                        id: id.to_string(),
                        err: error.to_string(),
                    })?;
                let window_fn = payload.window_fn.ok_or_else(|| PlanError::BadPayload {
                    id: id.to_string(),
                    err: "WindowInto function is missing".into(),
                })?;
                steps.push(Step::WindowInto {
                    transform_id: id.to_string(),
                    input_pc: single_input(id, t)?,
                    output_pc: single_output(id, t, &pc_producers)?,
                    window_fn,
                });
            }
            TransformKind::MapWindows => {
                steps.push(plan_map_windows(pbd, id, t, &pc_producers)?);
            }
        }
    }

    if steps
        .iter()
        .any(|step| matches!(step, Step::MapWindows { .. }))
    {
        let [Step::Source {
            port: source_port,
            output_pc: source_pc,
            ..
        }, Step::MapWindows {
            input_pc,
            output_pc,
            ..
        }, Step::Sink {
            port: sink_port,
            input_pc: sink_pc,
            ..
        }] = steps.as_slice()
        else {
            return Err(PlanError::NotLinear);
        };
        let source_coder = pbd
            .pcollections
            .get(source_pc)
            .ok_or(PlanError::NotLinear)?;
        let sink_coder = pbd.pcollections.get(sink_pc).ok_or(PlanError::NotLinear)?;
        if source_pc != input_pc
            || output_pc != sink_pc
            || !coders_equivalent(pbd, &source_port.coder_id, &source_coder.coder_id)
            || !coders_equivalent(pbd, &sink_port.coder_id, &sink_coder.coder_id)
        {
            return Err(PlanError::NotLinear);
        }
    }

    Ok(BundlePlan {
        steps,
        pbd_id: pbd.id.clone(),
        state_endpoint: pbd
            .state_api_service_descriptor
            .as_ref()
            .map(|port| port.url.clone()),
        side_inputs,
    })
}

#[derive(Debug, Clone, Copy)]
enum TransformKind {
    Source,
    Sink,
    Pardo,
    Sdf(SdfStage),
    WindowInto,
    MapWindows,
    /// Runner-native primitives that may appear in the PBD envelope but
    /// aren't the SDK's responsibility to execute.
    Skip,
}

fn decode_port(id: &str, t: &PTransform) -> Result<RemoteGrpcPort, PlanError> {
    let payload = t
        .spec
        .as_ref()
        .map(|s| s.payload.as_slice())
        .ok_or_else(|| PlanError::MissingSpec(id.to_string()))?;
    RemoteGrpcPort::decode(payload).map_err(|e| PlanError::BadPayload {
        id: id.to_string(),
        err: e.to_string(),
    })
}

fn decode_ironbeam_node_ref(id: &str, t: &PTransform) -> Result<V5NodeRef, PlanError> {
    let payload = t
        .spec
        .as_ref()
        .map(|s| s.payload.as_slice())
        .ok_or_else(|| PlanError::MissingSpec(id.to_string()))?;
    let pardo = beam_proto::pipeline_v1::ParDoPayload::decode(payload).map_err(|e| {
        PlanError::BadPayload {
            id: id.to_string(),
            err: e.to_string(),
        }
    })?;
    let do_fn = pardo
        .do_fn
        .ok_or_else(|| PlanError::MissingDoFn { id: id.to_string() })?;
    match do_fn.urn.as_str() {
        urns::DOFN_IRONBEAM_V5 => {
            V5NodeRef::decode(&do_fn.payload).map_err(|e| PlanError::BadPayload {
                id: id.to_string(),
                err: e.to_string(),
            })
        }
        _ => Err(PlanError::UnsupportedUrn {
            id: id.to_string(),
            urn: do_fn.urn,
        }),
    }
}

fn single_input(id: &str, t: &PTransform) -> Result<String, PlanError> {
    let mut it = t.inputs.values();
    let first = it
        .next()
        .ok_or_else(|| PlanError::UnknownPCollection {
            id: id.to_string(),
            pc: "<none>".to_string(),
        })?
        .clone();
    if it.next().is_some() {
        return Err(PlanError::NotLinear);
    }
    Ok(first)
}

fn single_output(
    id: &str,
    t: &PTransform,
    _pc_producers: &HashMap<String, String>,
) -> Result<String, PlanError> {
    let mut it = t.outputs.values();
    let first = it
        .next()
        .ok_or_else(|| PlanError::UnknownPCollection {
            id: id.to_string(),
            pc: "<none>".to_string(),
        })?
        .clone();
    if it.next().is_some() {
        return Err(PlanError::NotLinear);
    }
    Ok(first)
}

/// Walk the PBD's coder graph starting at the sink port's `coder_id` and
/// classify the per-element wire format. Unknown / unexpected shapes fall
/// back to `Opaque`, which matches the harness' historical behavior.
fn classify_sink_wire(pbd: &ProcessBundleDescriptor, port_coder_id: &str) -> SinkWire {
    let coders = &pbd.coders;
    let Some(top) = coders.get(port_coder_id) else {
        return SinkWire::Opaque;
    };
    // Unwrap the windowed_value wrapper if present — sinks almost always have
    // `windowed_value<element_coder, window>` as their port coder.
    let element_coder = match coder_urn(top) {
        urn if urn == urns::CODER_WINDOWED_VALUE => top
            .component_coder_ids
            .first()
            .and_then(|id| coders.get(id)),
        _ => Some(top),
    };
    let Some(element) = element_coder else {
        return SinkWire::Opaque;
    };
    if coder_urn(element) != urns::CODER_KV {
        return SinkWire::Opaque;
    }
    let halves = &element.component_coder_ids;
    if halves.len() != 2 {
        return SinkWire::Opaque;
    }
    let both_lp = halves
        .iter()
        .all(|h| coders.get(h).map(coder_urn).unwrap_or_default() == urns::CODER_LENGTH_PREFIX);
    if both_lp {
        SinkWire::KvLp
    } else {
        SinkWire::Opaque
    }
}

fn coder_urn(c: &Coder) -> &str {
    c.spec.as_ref().map(|s| s.urn.as_str()).unwrap_or_default()
}

/// Walk the PBD's coder graph starting at the source port's `coder_id` and
/// classify the per-element wire format. Unknown shapes fall back to
/// `Opaque`, which matches how the harness worked before source-shape
/// awareness was added.
fn classify_source_wire(pbd: &ProcessBundleDescriptor, port_coder_id: &str) -> SourceWire {
    let coders = &pbd.coders;
    let Some(top) = coders.get(port_coder_id) else {
        return SourceWire::Opaque;
    };
    let element_coder = match coder_urn(top) {
        urn if urn == urns::CODER_WINDOWED_VALUE => top
            .component_coder_ids
            .first()
            .and_then(|id| coders.get(id)),
        _ => Some(top),
    };
    let Some(element) = element_coder else {
        return SourceWire::Opaque;
    };
    if coder_urn(element) != urns::CODER_KV {
        return SourceWire::Opaque;
    }
    let halves = &element.component_coder_ids;
    if halves.len() != 2 {
        return SourceWire::Opaque;
    }
    let key_is_lp =
        coders.get(&halves[0]).map(coder_urn).unwrap_or_default() == urns::CODER_LENGTH_PREFIX;
    if !key_is_lp {
        return SourceWire::Opaque;
    }
    let value_urn = coders
        .get(&halves[1])
        .map(coder_urn)
        .unwrap_or_default()
        .to_string();
    match value_urn.as_str() {
        urn if urn == urns::CODER_ITERABLE => {
            // Must be iter<lp<_>> for our byte-reassembly trick to work.
            let iter_component = coders
                .get(&halves[1])
                .and_then(|c| c.component_coder_ids.first())
                .and_then(|id| coders.get(id));
            let iter_inner_is_lp = iter_component
                .map(coder_urn)
                .map(|u| u == urns::CODER_LENGTH_PREFIX)
                .unwrap_or(false);
            if iter_inner_is_lp {
                SourceWire::KvIterLp
            } else {
                SourceWire::Opaque
            }
        }
        urn if urn == urns::CODER_STATE_BACKED_ITERABLE => {
            let inner_is_lp = coders
                .get(&halves[1])
                .and_then(|coder| coder.component_coder_ids.first())
                .and_then(|id| coders.get(id))
                .is_some_and(|coder| coder_urn(coder) == urns::CODER_LENGTH_PREFIX);
            if inner_is_lp {
                SourceWire::KvStateBackedIter
            } else {
                SourceWire::Opaque
            }
        }
        urn if urn == urns::CODER_LENGTH_PREFIX => SourceWire::KvLp,
        _ => SourceWire::Opaque,
    }
}

fn build_pc_producer_index(pbd: &ProcessBundleDescriptor) -> HashMap<String, String> {
    let mut m = HashMap::new();
    for (id, t) in &pbd.transforms {
        for out_pc in t.outputs.values() {
            m.insert(out_pc.clone(), id.clone());
        }
    }
    m
}

fn build_pc_consumer_index(pbd: &ProcessBundleDescriptor) -> HashMap<String, Vec<String>> {
    let mut m: HashMap<String, Vec<String>> = HashMap::new();
    for (id, t) in &pbd.transforms {
        for in_pc in t.inputs.values() {
            m.entry(in_pc.clone()).or_default().push(id.clone());
        }
    }
    m
}

#[cfg(test)]
#[path = "../tests/unit/bundle__tests.rs"]
mod tests;
