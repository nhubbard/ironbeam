use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};
use std::time::Duration;
use tokio::sync::Mutex as AsyncMutex;

use anyhow::{anyhow, bail, Context, Result};
use async_trait::async_trait;
use beam_proto::fn_execution_v1::{
    state_key, BundleApplication, DelayedBundleApplication, ProcessBundleResponse,
    ProcessBundleSplitRequest, ProcessBundleSplitResponse, RemoteGrpcPort, StateKey,
};
use beam_proto::pipeline_v1::{is_bounded, FunctionSpec, MonitoringInfo};
use ironbeam_core::{
    Boundedness, Envelope, Error as CoreError, Graph, Node, Operation, Pipeline, Registry, Replay,
    SinkMode, SinkSession, TimerFiring, TimerMutation, UserStateKind, Window, WindowInterval,
};
use ironbeam_graphx::v5::{
    cogroup_flatten_collection_id, cogroup_tag_collection_id, collection_id, execution_identity,
    fold_flatten_collection_id, fold_group_collection_id, fold_seed_collection_id,
    fold_tag_collection_id, side_global_collection_id, expected_side_input_specs,
};
use ironbeam_graphx::windowing::{fixed_window_fn, session_window_fn, sliding_window_fn};

use crate::bundle::{BundlePlan, SdfStage, SinkWire, SourceWire, Step, WindowCoder, WindowMapping};
use crate::coders::{
    decode_interval_window, decode_length_prefix, decode_windowed_value, encode_interval_window,
    encode_interval_windowed_value_with_metadata,
    encode_length_prefix, encode_varint, encode_windowed_value, encode_windowed_value_with_metadata,
    try_decode_windowed_value, PaneInfo,
};
use crate::data::DataClient;
use crate::endpoints::normalize_endpoint;
use crate::executor::{element_count_info, BundleRunner};
use crate::finalization_journal::{FileFinalizationJournal, FinalizationJournal};
use crate::sdf_wire::{
    decode_sized, encode_sized, pair_with_restriction, split_and_size, try_bytes_body_len,
    try_pair_body_len, try_sized_body_len, truncate_sized, SizedSourcePair,
};
use crate::state::{StateClient, UserStateAccess, ValueStateMutation};
use crate::timer_coder::TimerRecord;
use crate::data::TimerFamilyScope;
use crate::unbounded::{SourcePoll, SourcePoller};

#[derive(Clone)]
struct WireValue {
    bytes: Vec<u8>,
    timestamp_ns: i64,
    window: Option<WindowInterval>,
    pane: PaneInfo,
    element_metadata: Option<Vec<u8>>,
}

fn user_panic_error(key: &str, operation: &str, panic: Box<dyn Any + Send>) -> CoreError {
    let detail = panic
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| panic.downcast_ref::<&str>().copied())
        .unwrap_or("unknown panic");
    CoreError::execution(
        key,
        "runner",
        format!("user {operation} panicked: {detail}"),
    )
}

fn has_group_application(registry: &Registry, node: &Node) -> bool {
    if matches!(node.operation, Operation::GlobalCombine) {
        registry.has_aggregate(&node.key)
    } else {
        registry.has_fold(&node.key)
    }
}

/// Resolve a runner collection that aliases a graph parent after Prism
/// coalesces adjacent native Reshuffle transforms. Never skip a user node.
fn matching_reshuffle_ancestor<'a>(
    graph: &'a Graph,
    expected_parent: &'a str,
    input_pc: &str,
) -> Option<(&'a str, bool)> {
    let mut ancestor = expected_parent;
    let mut elided = false;
    loop {
        if input_pc == collection_id(ancestor) {
            return Some((ancestor, elided));
        }
        let node = graph.node(ancestor)?;
        if !matches!(node.operation, Operation::Reshuffle) {
            return None;
        }
        ancestor = node.input.as_deref()?;
        elided = true;
    }
}

// Beam's bounded global-window GBK uses the end-of-window timestamp, which
// lies outside Ironbeam's signed-nanosecond range. Reserve i64::MAX in the
// portable worker for that exact Beam sentinel and restore it on output.
const BEAM_GLOBAL_WINDOW_END_MS: i64 = i64::MAX / 1_000 - 86_400_000;

fn sink_input_count_info(transform_id: &str, count: u64) -> Result<MonitoringInfo> {
    let mut payload = Vec::new();
    encode_varint(count, &mut payload)?;
    Ok(MonitoringInfo {
        urn: "beam:metric:user:sum_int64:v1".into(),
        r#type: "beam:metrics:sum_int64:v1".into(),
        payload,
        labels: [
            ("PTRANSFORM".into(), transform_id.into()),
            ("NAMESPACE".into(), "ironbeam.sink".into()),
            ("NAME".into(), "input_elements".into()),
        ]
        .into(),
        ..Default::default()
    })
}

fn decode_timestamp_ns(millis: i64) -> Result<i64> {
    if millis == BEAM_GLOBAL_WINDOW_END_MS {
        Ok(i64::MAX)
    } else {
        millis
            .checked_mul(1_000_000)
            .ok_or_else(|| anyhow!("v5 runner timestamp {millis} ms exceeds nanosecond range"))
    }
}

fn encode_timestamp_millis(nanos: i64) -> Result<i64> {
    if nanos == i64::MAX {
        Ok(BEAM_GLOBAL_WINDOW_END_MS)
    } else if nanos % 1_000_000 == 0 {
        Ok(nanos / 1_000_000)
    } else {
        bail!("v5 Fn API timestamp has submillisecond precision")
    }
}

fn protobuf_timestamp(nanos: i64) -> prost_types::Timestamp {
    prost_types::Timestamp {
        seconds: nanos.div_euclid(1_000_000_000),
        nanos: nanos.rem_euclid(1_000_000_000) as i32,
    }
}

/// Decode one complete windowed length-prefixed record from a possibly
/// fragmented Fn API chunk. `None` means more bytes are needed.
fn decode_interval_ns(window: Option<(i64, i64)>) -> Result<Option<WindowInterval>> {
    window
        .map(|(start_ms, end_ms)| {
            Ok(WindowInterval {
                start_ns: start_ms
                    .checked_mul(1_000_000)
                    .ok_or_else(|| anyhow!("v5 window start exceeds nanosecond range"))?,
                end_ns: end_ms
                    .checked_mul(1_000_000)
                    .ok_or_else(|| anyhow!("v5 window end exceeds nanosecond range"))?,
            })
        })
        .transpose()
}

fn decode_boundary_record(
    bytes: &[u8],
    interval_window: bool,
) -> Result<Option<(WireValue, usize)>> {
    let Some((windowed, header_len)) = try_decode_windowed_value(bytes, interval_window)? else {
        return Ok(None);
    };
    let mut length = 0_u64;
    for index in 0..10 {
        let Some(&byte) = windowed.element.get(index) else {
            return Ok(None);
        };
        if index == 9 && byte > 1 {
            bail!("v5 runner record length overflows varint");
        }
        length |= u64::from(byte & 0x7f) << (index * 7);
        if byte & 0x80 == 0 {
            let body_len = usize::try_from(length)
                .map_err(|_| anyhow!("v5 runner record length exceeds platform range"))?;
            let body_start = header_len + index + 1;
            let end = body_start
                .checked_add(body_len)
                .ok_or_else(|| anyhow!("v5 runner record length overflows"))?;
            if bytes.len() < end {
                return Ok(None);
            }
            let timestamp_ns = decode_timestamp_ns(windowed.timestamp_millis)?;
            return Ok(Some((
                WireValue {
                    bytes: bytes[body_start..end].to_vec(),
                    timestamp_ns,
                    window: decode_interval_ns(windowed.interval_millis)?,
                    pane: windowed.pane,
                    element_metadata: windowed.element_metadata.map(ToOwned::to_owned),
                },
                end,
            )));
        }
    }
    bail!("v5 runner record length varint is too long")
}

fn decode_impulse_boundary_record(bytes: &[u8]) -> Result<Option<(WireValue, usize)>> {
    let Some((windowed, header_len)) = try_decode_windowed_value(bytes, false)? else {
        return Ok(None);
    };
    let Some((body, used)) = decode_length_prefix_partial(windowed.element)? else {
        return Ok(None);
    };
    if !body.is_empty() || windowed.interval_millis.is_some() {
        bail!("v5 global fold seed expects an empty global Impulse");
    }
    // Beam's Impulse uses its minimum millisecond timestamp, which cannot be
    // represented by Ironbeam's signed nanosecond clock. The seed carries no
    // user event time; the fold identity has timestamp zero.
    Ok(Some((
        WireValue {
            bytes: Vec::new(),
            timestamp_ns: 0,
            window: None,
            pane: windowed.pane,
            element_metadata: windowed.element_metadata.map(ToOwned::to_owned),
        },
        header_len + used,
    )))
}

fn decode_length_prefix_partial(bytes: &[u8]) -> Result<Option<(&[u8], usize)>> {
    let mut length = 0_u64;
    for index in 0..10 {
        let Some(&byte) = bytes.get(index) else {
            return Ok(None);
        };
        if index == 9 && byte > 1 {
            bail!("v5 keyed record length overflows varint");
        }
        length |= u64::from(byte & 0x7f) << (index * 7);
        if byte & 0x80 == 0 {
            let length = usize::try_from(length)
                .map_err(|_| anyhow!("v5 keyed record length exceeds platform range"))?;
            let end = (index + 1)
                .checked_add(length)
                .ok_or_else(|| anyhow!("v5 keyed record length overflows"))?;
            return Ok(bytes.get(index + 1..end).map(|body| (body, end)));
        }
    }
    bail!("v5 keyed record length varint is too long")
}

fn decode_bag_elements(mut bytes: &[u8]) -> Result<Vec<Vec<u8>>> {
    let mut elements = Vec::new();
    while !bytes.is_empty() {
        let (element, used) = decode_length_prefix(bytes)?;
        elements.push(element.to_vec());
        bytes = &bytes[used..];
    }
    Ok(elements)
}

fn encode_bag_elements(elements: &[Vec<u8>]) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    for element in elements {
        encode_length_prefix(element, &mut bytes)?;
    }
    Ok(bytes)
}
