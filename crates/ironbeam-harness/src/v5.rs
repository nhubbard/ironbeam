//! Local v5 worker contract check and bounded mock bundle execution.
//! This exercises the driver/worker agreement before any source side effect;
//! it does not implement the remote Beam Fn API data streams.

use ironbeam_core::{Envelope, Operation, Pipeline, Window};
use ironbeam_graphx::v5::{execution_identity, validate_beam_contract, TranslatedV5, V5Error};

struct WireValue {
    bytes: Vec<u8>,
    timestamp_ns: i64,
}

/// Run the selected v5 operation chain through encoded element boundaries.
/// All identity, graph, and coder checks complete before opening the source.
pub fn run_mock_bundle(
    local: &Pipeline,
    translated: &TranslatedV5,
    binary_build_id: &str,
) -> Result<Vec<Vec<u8>>, V5Error> {
    let (graph, registry) = local.snapshot()?;
    let worker_identity = execution_identity(&graph, &registry, binary_build_id)?;
    translated.identity.verify(&worker_identity)?;
    validate_beam_contract(&graph, translated)?;
    for pair in translated.chain.windows(2) {
        let node = graph
            .node(&pair[1])
            .ok_or_else(|| V5Error::Graph(format!("missing node {}", pair[1])))?;
        if node.input.as_deref() != Some(pair[0].as_str()) {
            return Err(V5Error::Unsupported {
                key: node.key.clone(),
                reason: "mock bundle only supports one linear output chain".into(),
            });
        }
    }
    if translated
        .chain
        .last()
        .and_then(|key| graph.node(key))
        .is_none_or(|node| !matches!(node.operation, Operation::Output))
    {
        return Err(V5Error::Graph(
            "mock bundle requires one terminal Output".into(),
        ));
    }
    for key in translated
        .chain
        .iter()
        .take(translated.chain.len().saturating_sub(1))
    {
        if !registry.has_coder(key) {
            return Err(V5Error::Unsupported {
                key: key.clone(),
                reason: "worker has no coder registered for this element type".into(),
            });
        }
    }

    let source_key = &translated.chain[0];
    graph
        .node(source_key)
        .ok_or_else(|| V5Error::Graph(format!("missing source {source_key}")))?;
    let mut wire = Vec::new();
    for item in registry.open_source(source_key)? {
        let value = item?;
        if value.window != Window::Global || value.assigned_window.is_some() {
            return Err(V5Error::Wire(
                "source emitted unsupported window metadata".into(),
            ));
        }
        wire.push(WireValue {
            bytes: registry.encode_element(source_key, value.value.as_ref())?,
            timestamp_ns: value.timestamp_ns,
        });
    }
    for key in translated.chain.iter().skip(1) {
        let node = graph
            .node(key)
            .ok_or_else(|| V5Error::Graph(format!("missing node {key}")))?;
        if matches!(node.operation, Operation::Output) {
            break;
        }
        let input_key = node
            .input
            .as_ref()
            .ok_or_else(|| V5Error::Graph(format!("missing input for {key}")))?;
        graph
            .node(input_key)
            .ok_or_else(|| V5Error::Graph(format!("missing input node {input_key}")))?;
        let mut next = Vec::new();
        for item in wire {
            let envelope = Envelope {
                pane: ironbeam_core::PaneInfo::NO_FIRING,
                value: registry.decode_element(input_key, &item.bytes)?,
                timestamp_ns: item.timestamp_ns,
                window: Window::Global,
                assigned_window: None,
            };
            for value in registry.apply_many(key, envelope)? {
                if value.window != Window::Global || value.assigned_window.is_some() {
                    return Err(V5Error::Wire(format!(
                        "transform {key} emitted unsupported window metadata"
                    )));
                }
                next.push(WireValue {
                    bytes: registry.encode_element(key, value.value.as_ref())?,
                    timestamp_ns: value.timestamp_ns,
                });
            }
        }
        wire = next;
    }
    Ok(wire.into_iter().map(|item| item.bytes).collect())
}
