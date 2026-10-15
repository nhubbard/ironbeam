//! Direct execution for resumable unbounded sources.

use crate::multi_output::{self, PairPlan};
use crate::planner::{self, PhysicalPlan, Route};
use crate::stream_checkpoint::{StreamCheckpoint, hex_bytes};
use crate::{
    DirectRunner, JobConfig, JobHandle, JobStatus, cancelled, open_source_safe, read_source_batch,
    validate_config,
};
use ironbeam_core::{
    Element, Envelope, Error, ErrorKind, FoldSession, Graph, Operation, ProgressSignal, Registry,
    Replay, Sink, SinkMode, SinkSession, UnboundedPoll, UnboundedSource, Window, WindowInterval,
    WindowTrigger,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

mod multi;
use multi::{start_dual_side_stream, start_multi_grouped_stream_sink, start_multi_stream_sink};

/// A running unbounded Direct job. Dropping it requests cancellation and joins
/// its worker; `wait` preserves the execution error for the caller.
pub struct StreamJob {
    handle: JobHandle,
    progress: Arc<Mutex<Option<ProgressSignal>>>,
    worker: Option<JoinHandle<Result<(), Error>>>,
}

struct GroupedStreamRoute {
    source_key: String,
    before_group: Vec<String>,
    group_key: String,
    window: Window,
    trigger: WindowTrigger,
    allowed_lateness_ns: i64,
    after_group: Vec<String>,
}

struct BoundedSideStreamRoute {
    wait_on_signal: bool,
    window: Window,
    source_key: String,
    main_stages: Vec<String>,
    main_input_key: String,
    side_source_key: String,
    side_stages: Vec<String>,
    side_input_key: String,
    aggregate_key: String,
    after_aggregate: Vec<String>,
    other_aggregates: Vec<(String, Vec<String>)>,
    after_fanout: Vec<String>,
}

struct BoundedWaitStreamRoute {
    source_key: String,
    operations: Vec<String>,
    signals: Vec<BoundedWaitSignal>,
}

struct BoundedWaitSignal {
    key: String,
    sources: Vec<String>,
    linear: Option<(String, Vec<String>)>,
}

struct MultiGroupedStreamRoute {
    sources: Vec<MultiGroupedSource>,
    group_input_key: String,
    group_key: String,
    window: Window,
    after_group: Vec<String>,
}

struct MultiGroupedSource {
    key: String,
    branches: Vec<Vec<String>>,
    bounded: bool,
}

type WindowFolds = BTreeMap<(i64, i64), (Box<dyn FoldSession>, usize)>;
type WindowRows = BTreeMap<(i64, i64), Vec<Envelope>>;
const GROUPED_STATE_MAGIC: &[u8; 5] = b"IBGS1";

#[derive(Serialize, Deserialize)]
struct CodedWindowRows {
    version: u8,
    windows: Vec<CodedWindow>,
}

#[derive(Serialize, Deserialize)]
struct CodedWindow {
    start_ns: i64,
    end_ns: i64,
    events: Vec<CodedEvent>,
}

#[derive(Serialize, Deserialize)]
struct CodedEvent {
    value: Vec<u8>,
    timestamp_ns: i64,
    window: Window,
    assigned_window: Option<WindowInterval>,
    #[serde(default = "no_firing_pane")]
    pane: ironbeam_core::PaneInfo,
}

const fn no_firing_pane() -> ironbeam_core::PaneInfo {
    ironbeam_core::PaneInfo::NO_FIRING
}

fn encode_window_rows(
    registry: &Registry,
    input_key: &str,
    rows: &WindowRows,
    max_bytes: usize,
) -> Result<Vec<u8>, Error> {
    let mut windows = Vec::with_capacity(rows.len());
    for (&(start_ns, end_ns), events) in rows {
        let mut coded = Vec::with_capacity(events.len());
        for event in events {
            coded.push(CodedEvent {
                value: registry.encode_element(input_key, event.value.as_ref())?,
                timestamp_ns: event.timestamp_ns,
                window: event.window.clone(),
                assigned_window: event.assigned_window,
                pane: event.pane,
            });
        }
        windows.push(CodedWindow {
            start_ns,
            end_ns,
            events: coded,
        });
    }
    let bytes = serde_json::to_vec(&CodedWindowRows {
        version: 1,
        windows,
    })
    .map_err(|error| Error::recovery("checkpoint", error.to_string()))?;
    if bytes.len() > max_bytes {
        return Err(Error::resource(
            "checkpoint",
            "stream window state exceeds memory budget",
        ));
    }
    Ok(bytes)
}

fn restore_window_rows(
    registry: &Registry,
    input_key: &str,
    bytes: &[u8],
    budget_elements: usize,
) -> Result<(WindowRows, usize), Error> {
    let coded: CodedWindowRows = serde_json::from_slice(bytes)
        .map_err(|error| Error::recovery("checkpoint", error.to_string()))?;
    if coded.version != 1 {
        return Err(Error::incompatible("stream window state version mismatch"));
    }
    let mut rows = BTreeMap::new();
    let mut buffered = 0_usize;
    for window in coded.windows {
        let interval = (window.start_ns, window.end_ns);
        if interval.1 <= interval.0
            || window.events.is_empty()
            || window.events.len() > budget_elements.saturating_sub(buffered)
            || rows.contains_key(&interval)
        {
            return Err(Error::recovery(
                "checkpoint",
                "invalid or duplicate stream window state",
            ));
        }
        let mut events = Vec::with_capacity(window.events.len());
        for event in window.events {
            if event.assigned_window
                != Some(WindowInterval {
                    start_ns: interval.0,
                    end_ns: interval.1,
                })
                || !event
                    .window
                    .assign_all(event.timestamp_ns)?
                    .contains(&event.assigned_window)
            {
                return Err(Error::recovery("checkpoint", "invalid stream event window"));
            }
            events.push(Envelope {
                pane: event.pane,
                value: registry.decode_element(input_key, &event.value)?,
                timestamp_ns: event.timestamp_ns,
                window: event.window,
                assigned_window: event.assigned_window,
            });
        }
        buffered += events.len();
        rows.insert(interval, events);
    }
    Ok((rows, buffered))
}

fn encode_grouped_folds(
    folds: &WindowFolds,
    max_bytes: usize,
    group_key: &str,
) -> Result<Vec<u8>, Error> {
    let count = u32::try_from(folds.len())
        .map_err(|_| Error::resource(group_key, "too many grouped stream windows"))?;
    let mut bytes = Vec::new();
    bytes.extend_from_slice(GROUPED_STATE_MAGIC);
    bytes.extend_from_slice(&count.to_le_bytes());
    for (&(start, end), (fold, rows)) in folds {
        let state = catch_unwind(AssertUnwindSafe(|| fold.checkpoint_state()))
            .unwrap_or_else(|_| {
                Err(Error::execution(
                    group_key,
                    "direct",
                    "group fold checkpoint panicked",
                ))
            })?
            .ok_or_else(|| Error::unsupported(group_key, "direct", "fold-checkpoint-state"))?;
        bytes.extend_from_slice(&start.to_le_bytes());
        bytes.extend_from_slice(&end.to_le_bytes());
        bytes.extend_from_slice(&u64::try_from(*rows).unwrap_or(u64::MAX).to_le_bytes());
        bytes.extend_from_slice(&u64::try_from(state.len()).unwrap_or(u64::MAX).to_le_bytes());
        bytes.extend_from_slice(&state);
        if bytes.len() > max_bytes {
            return Err(Error::resource(
                "checkpoint",
                "grouped stream fold state exceeds memory budget",
            ));
        }
    }
    Ok(bytes)
}

fn take_state_bytes<'a>(remaining: &mut &'a [u8], len: usize) -> Result<&'a [u8], Error> {
    let Some((taken, rest)) = remaining.split_at_checked(len) else {
        return Err(Error::recovery(
            "checkpoint",
            "truncated grouped stream fold state",
        ));
    };
    *remaining = rest;
    Ok(taken)
}

fn restore_grouped_folds(
    registry: &Registry,
    group_key: &str,
    bytes: &[u8],
    budget_elements: usize,
    allow_empty: bool,
) -> Result<(WindowFolds, usize), Error> {
    let mut remaining = bytes;
    if take_state_bytes(&mut remaining, GROUPED_STATE_MAGIC.len())? != GROUPED_STATE_MAGIC {
        return Err(Error::incompatible(
            "grouped stream fold state version mismatch",
        ));
    }
    let count = u32::from_le_bytes(
        take_state_bytes(&mut remaining, 4)?
            .try_into()
            .expect("four bytes"),
    );
    let mut folds = BTreeMap::new();
    let mut buffered = 0_usize;
    for _ in 0..count {
        let start = i64::from_le_bytes(
            take_state_bytes(&mut remaining, 8)?
                .try_into()
                .expect("eight bytes"),
        );
        let end = i64::from_le_bytes(
            take_state_bytes(&mut remaining, 8)?
                .try_into()
                .expect("eight bytes"),
        );
        let rows = usize::try_from(u64::from_le_bytes(
            take_state_bytes(&mut remaining, 8)?
                .try_into()
                .expect("eight bytes"),
        ))
        .map_err(|_| Error::recovery("checkpoint", "invalid grouped stream row count"))?;
        let len = usize::try_from(u64::from_le_bytes(
            take_state_bytes(&mut remaining, 8)?
                .try_into()
                .expect("eight bytes"),
        ))
        .map_err(|_| Error::recovery("checkpoint", "invalid grouped stream fold size"))?;
        if end <= start
            || (rows == 0 && !allow_empty)
            || rows > budget_elements.saturating_sub(buffered)
        {
            return Err(Error::recovery(
                "checkpoint",
                "invalid grouped stream window or row count",
            ));
        }
        let state = take_state_bytes(&mut remaining, len)?;
        let mut fold = catch_unwind(AssertUnwindSafe(|| registry.open_fold(group_key)))
            .unwrap_or_else(|_| {
                Err(Error::execution(
                    group_key,
                    "direct",
                    "group fold factory panicked",
                ))
            })?;
        catch_unwind(AssertUnwindSafe(|| fold.restore_checkpoint_state(state))).unwrap_or_else(
            |_| {
                Err(Error::execution(
                    group_key,
                    "direct",
                    "group fold recovery panicked",
                ))
            },
        )?;
        if folds.insert((start, end), (fold, rows)).is_some() {
            return Err(Error::recovery(
                "checkpoint",
                "duplicate grouped stream window",
            ));
        }
        buffered += rows;
    }
    if !remaining.is_empty() {
        return Err(Error::recovery(
            "checkpoint",
            "trailing grouped stream fold state",
        ));
    }
    Ok((folds, buffered))
}

fn grouped_stream_route(
    graph: &Graph,
    sink_key: &str,
) -> Result<Option<GroupedStreamRoute>, Error> {
    let mut key = sink_key;
    let mut downstream = Vec::new();
    let mut group_key: Option<String> = None;
    let mut upstream = Vec::new();
    loop {
        let node = graph
            .node(key)
            .ok_or_else(|| Error::graph(key, "stream route node missing"))?;
        if !node.additional_inputs.is_empty() {
            return Ok(None);
        }
        match &node.operation {
            Operation::Source { .. } => {
                let Some(group_key) = group_key else {
                    return Ok(None);
                };
                upstream.reverse();
                downstream.reverse();
                return Ok(Some(GroupedStreamRoute {
                    source_key: key.to_owned(),
                    before_group: upstream,
                    window: graph
                        .node(group_key.as_str())
                        .expect("group node exists")
                        .time
                        .window
                        .clone(),
                    trigger: graph
                        .node(group_key.as_str())
                        .expect("group node exists")
                        .time
                        .trigger,
                    allowed_lateness_ns: graph
                        .node(group_key.as_str())
                        .expect("group node exists")
                        .time
                        .allowed_lateness_ns,
                    group_key,
                    after_group: downstream,
                }));
            }
            Operation::GroupByKey if group_key.is_none() => group_key = Some(key.to_owned()),
            Operation::Output
            | Operation::Sink { .. }
            | Operation::Map
            | Operation::Filter
            | Operation::FlatMap
            | Operation::AssignTimestamp
            | Operation::FixedWindow
            | Operation::SlidingWindow
            | Operation::SessionWindow => {
                if !matches!(node.operation, Operation::Output | Operation::Sink { .. }) {
                    if group_key.is_some() {
                        upstream.push(key.to_owned());
                    } else {
                        downstream.push(key.to_owned());
                    }
                }
            }
            _ => return Ok(None),
        }
        key = node
            .input
            .as_deref()
            .ok_or_else(|| Error::graph(key, "stream route has no parent"))?;
    }
}

fn linear_stream_input(graph: &Graph, input_key: &str) -> Option<(String, Vec<String>)> {
    let mut key = input_key;
    let mut stages = Vec::new();
    loop {
        let node = graph.node(key)?;
        if !node.additional_inputs.is_empty() {
            return None;
        }
        match node.operation {
            Operation::Source { .. } => {
                stages.reverse();
                return Some((key.to_owned(), stages));
            }
            Operation::Map
            | Operation::Filter
            | Operation::FlatMap
            | Operation::AssignTimestamp
            | Operation::FixedWindow
            | Operation::SlidingWindow
            | Operation::SessionWindow => stages.push(key.to_owned()),
            _ => return None,
        }
        key = node.input.as_deref()?;
    }
}

fn bounded_signal_sources(graph: &Graph, key: &str) -> Option<Vec<String>> {
    let mut pending = vec![key];
    let mut visited = BTreeSet::new();
    let mut sources = BTreeSet::new();
    while let Some(key) = pending.pop() {
        if !visited.insert(key) {
            continue;
        }
        let node = graph.node(key)?;
        if matches!(node.operation, Operation::Source { .. }) {
            sources.insert(key.to_owned());
        } else {
            pending.extend(node.input.iter().map(String::as_str));
            pending.extend(node.additional_inputs.iter().map(|edge| edge.key.as_str()));
        }
    }
    Some(sources.into_iter().collect())
}

fn bounded_wait_stream_route(graph: &Graph, sink_key: &str) -> Option<BoundedWaitStreamRoute> {
    // start_stream_sink validates the graph before matching this physical route.
    let mut key = sink_key;
    let mut operations = Vec::new();
    let mut signals = Vec::new();
    loop {
        let node = graph.node(key)?;
        match node.operation {
            Operation::Source { .. } => {
                if signals.is_empty() {
                    return None;
                }
                operations.reverse();
                signals.reverse();
                return Some(BoundedWaitStreamRoute {
                    source_key: key.to_owned(),
                    operations,
                    signals,
                });
            }
            Operation::WaitOn => {
                let signal = &node.additional_inputs[0];
                signals.push(BoundedWaitSignal {
                    key: signal.key.clone(),
                    sources: bounded_signal_sources(graph, &signal.key)?,
                    linear: linear_stream_input(graph, &signal.key),
                });
            }
            Operation::Sink { .. } | Operation::Output => {}
            Operation::Map
            | Operation::Filter
            | Operation::FlatMap
            | Operation::AssignTimestamp
            | Operation::FixedWindow
            | Operation::SlidingWindow
            | Operation::SessionWindow => {
                operations.push(key.to_owned());
            }
            _ => return None,
        }
        key = node.input.as_deref()?;
    }
}

fn side_stream_route(
    graph: &Graph,
    sink_key: &str,
    side_boundedness: ironbeam_core::Boundedness,
) -> Option<BoundedSideStreamRoute> {
    let mut key = sink_key;
    let mut downstream = Vec::new();
    loop {
        let node = graph.node(key)?;
        if matches!(node.operation, Operation::CoGroupByKey)
            || matches!(
                node.operation,
                Operation::JoinInner
                    | Operation::JoinLeft
                    | Operation::JoinRight
                    | Operation::JoinFull
                    | Operation::WaitOnWindowed
            )
        {
            let [side] = node.additional_inputs.as_slice() else {
                return None;
            };
            let main_input_key = node.input.as_deref()?;
            let (source_key, main_stages) = linear_stream_input(graph, main_input_key)?;
            let (side_source_key, side_stages) = linear_stream_input(graph, &side.key)?;
            if graph.node(&source_key)?.boundedness != ironbeam_core::Boundedness::Unbounded
                || graph.node(&side_source_key)?.boundedness != side_boundedness
                || !(matches!(
                    node.time.window,
                    ironbeam_core::Window::Fixed { .. }
                        | ironbeam_core::Window::FixedOffset { .. }
                        | ironbeam_core::Window::Sliding { .. }
                ) || (matches!(node.time.window, ironbeam_core::Window::Session { .. })
                    && side_boundedness == ironbeam_core::Boundedness::Unbounded))
            {
                return None;
            }
            downstream.reverse();
            return Some(BoundedSideStreamRoute {
                wait_on_signal: matches!(node.operation, Operation::WaitOnWindowed),
                window: node.time.window.clone(),
                source_key,
                main_stages,
                main_input_key: main_input_key.to_owned(),
                side_source_key,
                side_stages,
                side_input_key: side.key.clone(),
                aggregate_key: key.to_owned(),
                after_aggregate: downstream,
                other_aggregates: Vec::new(),
                after_fanout: Vec::new(),
            });
        }
        if !node.additional_inputs.is_empty() {
            return None;
        }
        match node.operation {
            Operation::Sink { .. } | Operation::Output => {}
            Operation::Map
            | Operation::Filter
            | Operation::FlatMap
            | Operation::AssignTimestamp
            | Operation::FixedWindow
            | Operation::SlidingWindow
            | Operation::SessionWindow => downstream.push(key.to_owned()),
            _ => return None,
        }
        key = node.input.as_deref()?;
    }
}

fn stateful_fanout_route(graph: &Graph, sink_key: &str) -> Option<BoundedSideStreamRoute> {
    let mut key = sink_key;
    let mut after_fanout = Vec::new();
    let flatten = loop {
        let node = graph.node(key)?;
        if matches!(node.operation, Operation::Flatten) {
            break node;
        }
        if !node.additional_inputs.is_empty() {
            return None;
        }
        match node.operation {
            Operation::Output | Operation::Sink { .. } => {}
            Operation::Map
            | Operation::Filter
            | Operation::FlatMap
            | Operation::AssignTimestamp
            | Operation::FixedWindow
            | Operation::SlidingWindow
            | Operation::SessionWindow => after_fanout.push(key.to_owned()),
            _ => return None,
        }
        key = node.input.as_deref()?;
    };
    after_fanout.reverse();
    let inputs = std::iter::once(flatten.input.as_deref()?).chain(
        flatten
            .additional_inputs
            .iter()
            .map(|input| input.key.as_str()),
    );
    let mut routes = inputs
        .map(|key| side_stream_route(graph, key, ironbeam_core::Boundedness::Unbounded))
        .collect::<Option<Vec<_>>>()?;
    if routes.len() < 2 {
        return None;
    }
    let mut primary = routes.remove(0);
    let window = &graph.node(&primary.aggregate_key)?.time.window;
    if routes.iter().any(|route| {
        route.source_key != primary.source_key
            || route.side_source_key != primary.side_source_key
            || route.main_input_key != primary.main_input_key
            || route.side_input_key != primary.side_input_key
            || route.main_stages != primary.main_stages
            || route.side_stages != primary.side_stages
            || graph
                .node(&route.aggregate_key)
                .is_none_or(|node| &node.time.window != window)
    }) {
        return None;
    }
    primary.other_aggregates = routes
        .into_iter()
        .map(|route| (route.aggregate_key, route.after_aggregate))
        .collect();
    primary.after_fanout = after_fanout;
    Some(primary)
}

fn multi_grouped_stream_route(graph: &Graph, sink_key: &str) -> Option<MultiGroupedStreamRoute> {
    let mut key = sink_key;
    let mut after_group = Vec::new();
    let group = loop {
        let node = graph.node(key)?;
        if matches!(node.operation, Operation::GroupByKey) {
            break node;
        }
        if !node.additional_inputs.is_empty() {
            return None;
        }
        match node.operation {
            Operation::Output | Operation::Sink { .. } => {}
            Operation::Map
            | Operation::Filter
            | Operation::FlatMap
            | Operation::AssignTimestamp
            | Operation::FixedWindow
            | Operation::SlidingWindow => after_group.push(key.to_owned()),
            _ => return None,
        }
        key = node.input.as_deref()?;
    };
    let flatten = graph.node(group.input.as_deref()?)?;
    if !matches!(flatten.operation, Operation::Flatten)
        || !matches!(
            group.time.window,
            Window::Fixed { .. }
                | Window::FixedOffset { .. }
                | Window::Sliding { .. }
                | Window::Session { .. }
                | Window::Global
        )
    {
        return None;
    }
    let input_keys = std::iter::once(flatten.input.as_deref()?).chain(
        flatten
            .additional_inputs
            .iter()
            .map(|edge| edge.key.as_str()),
    );
    let input_routes = input_keys
        .map(|input_key| {
            if graph.node(input_key)?.time != group.time {
                return None;
            }
            linear_stream_input(graph, input_key)
        })
        .collect::<Option<Vec<_>>>()?;
    if input_routes.is_empty() {
        return None;
    }
    let mut sources: Vec<MultiGroupedSource> = Vec::new();
    for (source_key, stages) in input_routes {
        if let Some(source) = sources.iter_mut().find(|source| source.key == source_key) {
            source.branches.push(stages);
        } else {
            let bounded =
                graph.node(&source_key)?.boundedness == ironbeam_core::Boundedness::Bounded;
            sources.push(MultiGroupedSource {
                key: source_key,
                branches: vec![stages],
                bounded,
            });
        }
    }
    after_group.reverse();
    Some(MultiGroupedStreamRoute {
        sources,
        group_input_key: flatten.key.clone(),
        group_key: group.key.clone(),
        window: group.time.window.clone(),
        after_group,
    })
}

impl StreamJob {
    #[must_use]
    pub const fn handle(&self) -> &JobHandle {
        &self.handle
    }

    #[must_use]
    pub fn progress(&self) -> Option<ProgressSignal> {
        self.progress
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    pub fn cancel(&self) {
        self.handle.cancel();
    }

    pub fn wait(mut self) -> Result<JobHandle, Error> {
        if let Some(worker) = self.worker.take() {
            worker
                .join()
                .map_err(|_| Error::execution("job", "direct", "stream worker panicked"))??;
        }
        Ok(self.handle.clone())
    }
}

impl Drop for StreamJob {
    fn drop(&mut self) {
        if let Some(worker) = self.worker.take() {
            self.handle.cancel();
            let _ = worker.join();
        }
    }
}

impl DirectRunner {
    /// Start resumable unbounded sources feeding a linear route, stateless
    /// `Flatten`, fixed-window `GroupByKey`, or fixed-window co-group/join.
    /// Stateful routes close on the applicable source watermarks or completion
    /// and checkpoint coded open-window state.
    /// Validation and checkpoint recovery finish before opening endpoints.
    #[allow(clippy::too_many_lines)] // Worker setup validates recovery before opening endpoints.
    pub fn start_stream_sink<T: Element>(
        &self,
        sink: &Sink<T>,
        config: &JobConfig,
    ) -> Result<StreamJob, Error> {
        let (graph, registry) = sink.snapshot()?;
        Self::validate_with_unbounded(&graph, true)?;
        validate_config(config)?;
        let sink_key = sink.key().to_owned();
        let sink_node = graph
            .node(&sink_key)
            .ok_or_else(|| Error::graph(&sink_key, "sink node missing"))?;
        if !matches!(&sink_node.operation, Operation::Sink { spec }
            if spec.mode == SinkMode::Append && spec.replay == Replay::Idempotent && spec.commit)
        {
            return Err(Error::unsupported(
                &sink_key,
                "direct",
                "idempotent-stream-sink",
            ));
        }
        if !registry.has_sink(&sink_key) {
            return Err(Error::unsupported(&sink_key, "direct", "sink-factory"));
        }
        if config.spill.is_some() {
            return Err(Error::unsupported(&sink_key, "direct", "stream-spill"));
        }
        let mut plan = planner::lower(&graph, &registry, &sink_key, config.buffer_budget_elements)?;
        if graph.nodes.iter().any(|node| {
            node.boundedness == ironbeam_core::Boundedness::Unbounded
                && matches!(node.operation, Operation::StatefulParDo { .. })
        }) && (plan.materialized || plan.routes.len() != 1)
        {
            return Err(Error::unsupported(
                &sink_key,
                "direct",
                "unbounded-value-state-route",
            ));
        }
        if !plan.materialized && plan.routes.len() > 1 {
            return start_multi_stream_sink::<T>(&graph, registry, plan, sink_key, config);
        }
        let grouped = if plan.materialized {
            grouped_stream_route(&graph, &sink_key)?
        } else {
            None
        };
        let bounded_side = if plan.materialized && grouped.is_none() {
            side_stream_route(&graph, &sink_key, ironbeam_core::Boundedness::Bounded)
        } else {
            None
        };
        let bounded_wait = if plan.materialized && grouped.is_none() && bounded_side.is_none() {
            bounded_wait_stream_route(&graph, &sink_key)
        } else {
            None
        };
        if let Some(wait) = &bounded_wait {
            plan.summary.source_key.clone_from(&wait.source_key);
            plan.routes = vec![Route {
                source_key: wait.source_key.clone(),
                operations: wait.operations.clone(),
            }];
        }
        if plan.materialized
            && grouped.is_none()
            && bounded_side.is_none()
            && bounded_wait.is_none()
        {
            if let Some(grouped_fan_in) = multi_grouped_stream_route(&graph, &sink_key) {
                return start_multi_grouped_stream_sink::<T>(
                    &graph,
                    registry,
                    plan,
                    sink_key,
                    config,
                    grouped_fan_in,
                );
            }
            if let Some(fanout) = stateful_fanout_route(&graph, &sink_key) {
                return start_dual_side_stream::<T>(
                    &graph, registry, plan, sink_key, config, fanout,
                );
            }
            if let Some(dual) =
                side_stream_route(&graph, &sink_key, ironbeam_core::Boundedness::Unbounded)
            {
                return start_dual_side_stream::<T>(&graph, registry, plan, sink_key, config, dual);
            }
            return Err(Error::unsupported(
                &sink_key,
                "direct",
                "stateful-stream-route",
            ));
        }
        if let Some(side) = &bounded_side
            && config.checkpoint.is_some()
            && !registry.has_coder(&side.main_input_key)
        {
            return Err(Error::unsupported(
                &side.main_input_key,
                "direct",
                "stream-window-state-coder",
            ));
        }
        if let Some(grouped) = &grouped
            && !registry.has_fold(&grouped.group_key)
        {
            return Err(Error::unsupported(
                &grouped.group_key,
                "direct",
                "fold-function",
            ));
        }
        let source_key = if let Some(grouped) = &grouped {
            grouped.source_key.as_str()
        } else if let Some(side) = &bounded_side {
            side.source_key.as_str()
        } else if let Some(wait) = &bounded_wait {
            wait.source_key.as_str()
        } else {
            let [route] = plan.routes.as_slice() else {
                return Err(Error::unsupported(
                    &sink_key,
                    "direct",
                    "multi-source-stream",
                ));
            };
            &route.source_key
        };
        let source_node = graph
            .node(source_key)
            .ok_or_else(|| Error::graph(source_key, "stream source node missing"))?;
        let Operation::Source {
            progress: source_progress,
        } = &source_node.operation
        else {
            return Err(Error::graph(source_key, "stream route has no source"));
        };
        if source_node.boundedness != ironbeam_core::Boundedness::Unbounded
            || !source_progress.stable_split_id
            || source_progress.replay == Replay::None
            || !registry.has_unbounded_source(source_key)
        {
            return Err(Error::unsupported(
                source_key,
                "direct",
                "resumable-stream-source",
            ));
        }
        if (grouped.is_some() || bounded_side.is_some()) && !source_progress.reports_event_time {
            return Err(Error::unsupported(source_key, "direct", "stream-watermark"));
        }
        if let Some(grouped) = &grouped
            && config.checkpoint.is_some()
        {
            let fold = catch_unwind(AssertUnwindSafe(|| registry.open_fold(&grouped.group_key)))
                .unwrap_or_else(|_| {
                    Err(Error::execution(
                        &grouped.group_key,
                        "direct",
                        "group fold factory panicked",
                    ))
                })?;
            let state = catch_unwind(AssertUnwindSafe(|| fold.checkpoint_state()))
                .unwrap_or_else(|_| {
                    Err(Error::execution(
                        &grouped.group_key,
                        "direct",
                        "group fold checkpoint panicked",
                    ))
                })?
                .ok_or_else(|| {
                    Error::unsupported(&grouped.group_key, "direct", "fold-checkpoint-state")
                })?;
            let mut restored =
                catch_unwind(AssertUnwindSafe(|| registry.open_fold(&grouped.group_key)))
                    .unwrap_or_else(|_| {
                        Err(Error::execution(
                            &grouped.group_key,
                            "direct",
                            "group fold factory panicked",
                        ))
                    })?;
            catch_unwind(AssertUnwindSafe(|| {
                restored.restore_checkpoint_state(&state)
            }))
            .unwrap_or_else(|_| {
                Err(Error::execution(
                    &grouped.group_key,
                    "direct",
                    "group fold recovery panicked",
                ))
            })?;
        }
        let checkpoint = config
            .checkpoint
            .as_ref()
            .map(|checkpoint| {
                let main_source_digest =
                    registry.source_config_digest(source_key).ok_or_else(|| {
                        Error::unsupported(source_key, "direct", "source-config-digest")
                    })?;
                let source_digest = if let Some(wait) = &bounded_wait {
                    use std::fmt::Write as _;
                    let mut digest = format!("{}:{main_source_digest}", main_source_digest.len());
                    for signal in &wait.signals {
                        for source in &signal.sources {
                            let signal_digest =
                                registry.source_config_digest(source).ok_or_else(|| {
                                    Error::unsupported(source, "direct", "source-config-digest")
                                })?;
                            write!(&mut digest, "{}:{signal_digest}", signal_digest.len())
                                .expect("writing to String cannot fail");
                        }
                    }
                    digest
                } else if let Some(side) = &bounded_side {
                    let side_digest = registry
                        .source_config_digest(&side.side_source_key)
                        .ok_or_else(|| {
                            Error::unsupported(
                                &side.side_source_key,
                                "direct",
                                "source-config-digest",
                            )
                        })?;
                    format!(
                        "{}:{main_source_digest}{}:{side_digest}",
                        main_source_digest.len(),
                        side_digest.len()
                    )
                } else {
                    main_source_digest.to_owned()
                };
                StreamCheckpoint::new::<T>(
                    checkpoint,
                    &plan.summary.graph_fingerprint,
                    &source_digest,
                    registry.sink_config_digest(&sink_key).ok_or_else(|| {
                        Error::unsupported(&sink_key, "direct", "sink-config-digest")
                    })?,
                    source_key,
                    &sink_key,
                )
            })
            .transpose()?;
        let (recovered, grouped_folds, grouped_buffered) = if let Some(grouped) = &grouped {
            let saved = checkpoint
                .as_ref()
                .map(|checkpoint| checkpoint.load_state(config.memory_budget_bytes))
                .transpose()?
                .flatten();
            if let Some((progress, state)) = saved {
                let (folds, buffered) = restore_grouped_folds(
                    &registry,
                    &grouped.group_key,
                    &state,
                    config.buffer_budget_elements,
                    grouped.trigger != WindowTrigger::Default || grouped.allowed_lateness_ns > 0,
                )?;
                if progress.completed && !folds.is_empty() {
                    return Err(Error::recovery(
                        "checkpoint",
                        "completed grouped stream retains open windows",
                    ));
                }
                (Some(progress), folds, buffered)
            } else {
                (None, BTreeMap::new(), 0)
            }
        } else {
            (
                checkpoint
                    .as_ref()
                    .map(StreamCheckpoint::load)
                    .transpose()?
                    .flatten(),
                BTreeMap::new(),
                0,
            )
        };
        let (recovered, side_windows, side_buffered) = if let Some(side) = &bounded_side {
            let saved = checkpoint
                .as_ref()
                .map(|checkpoint| checkpoint.load_state(config.memory_budget_bytes))
                .transpose()?
                .flatten();
            if let Some((progress, state)) = saved {
                let (windows, buffered) = restore_window_rows(
                    &registry,
                    &side.main_input_key,
                    &state,
                    config.buffer_budget_elements,
                )?;
                if progress.completed && !windows.is_empty() {
                    return Err(Error::recovery(
                        "checkpoint",
                        "completed stream retains open windows",
                    ));
                }
                (Some(progress), windows, buffered)
            } else {
                (None, BTreeMap::new(), 0)
            }
        } else {
            (recovered, BTreeMap::new(), 0)
        };
        let stateful_keys = if plan.materialized {
            Vec::new()
        } else {
            plan.routes
                .iter()
                .flat_map(|route| route.operations.iter())
                .filter(|key| registry.has_stateful_value(key))
                .collect::<Vec<_>>()
        };
        if stateful_keys.len() > 1 {
            return Err(Error::unsupported(
                &sink_key,
                "direct",
                "multiple-stream-value-state",
            ));
        }
        let recovered =
            if let (Some(key), Some(checkpoint)) = (stateful_keys.first(), checkpoint.as_ref()) {
                let saved = checkpoint.load_state(config.memory_budget_bytes)?;
                if let Some((progress, state)) = saved {
                    registry.restore_stateful_value(key, &state)?;
                    Some(progress)
                } else {
                    None
                }
            } else {
                recovered
            };
        #[allow(unused_mut)] // Metrics feature installs a collector before the worker starts.
        let mut handle = JobHandle::new(config.cancellation.clone(), JobStatus::Submitted);
        #[cfg(feature = "metrics")]
        {
            handle
                .metrics_collector
                .clone_from(&config.metrics_collector);
        }
        if config.cancellation.load(Ordering::Acquire) {
            return Err(cancelled());
        }
        if recovered
            .as_ref()
            .is_some_and(|progress| progress.completed)
        {
            handle.set_status(JobStatus::Succeeded);
            return Ok(StreamJob {
                handle,
                progress: Arc::new(Mutex::new(recovered)),
                worker: None,
            });
        }
        let progress = Arc::new(Mutex::new(recovered.clone()));
        let worker_progress = Arc::clone(&progress);
        let worker_handle = handle.clone();
        let worker_config = config.clone();
        handle.set_status(JobStatus::Running);
        let worker = std::thread::Builder::new()
            .name(format!("ironbeam-stream-{}", handle.id()))
            .spawn(move || {
                let result = catch_unwind(AssertUnwindSafe(|| {
                    if let Some(grouped) = &grouped {
                        run_grouped_stream(
                            &registry,
                            grouped,
                            &plan.summary.graph_fingerprint,
                            &sink_key,
                            &worker_config,
                            &worker_handle,
                            &worker_progress,
                            checkpoint.as_ref(),
                            recovered.clone(),
                            grouped_folds,
                            grouped_buffered,
                        )
                    } else if let Some(side) = &bounded_side {
                        run_bounded_side_stream(
                            &registry,
                            side,
                            &plan.summary.graph_fingerprint,
                            &sink_key,
                            &worker_config,
                            &worker_handle,
                            &worker_progress,
                            checkpoint.as_ref(),
                            recovered.clone(),
                            side_windows,
                            side_buffered,
                        )
                    } else {
                        if let Some(wait) = &bounded_wait {
                            let pool = wait
                                .signals
                                .iter()
                                .any(|signal| signal.linear.is_none())
                                .then(|| {
                                    rayon::ThreadPoolBuilder::new()
                                        .num_threads(worker_config.parallelism)
                                        .build()
                                        .map_err(|error| {
                                            Error::execution("job", "direct", error.to_string())
                                        })
                                })
                                .transpose()?;
                            for signal in &wait.signals {
                                if let Some((source, stages)) = &signal.linear {
                                    crate::materialized::drain_linear_signal(
                                        &registry,
                                        source,
                                        stages,
                                        &worker_config,
                                        &worker_handle.metrics,
                                    )?;
                                } else {
                                    let pool = pool.as_ref().ok_or_else(|| {
                                        Error::execution(
                                            "job",
                                            "direct",
                                            "materialized signal pool was not initialized",
                                        )
                                    })?;
                                    crate::materialized::eval(
                                        pool,
                                        &graph,
                                        &registry,
                                        &signal.key,
                                        &worker_config,
                                        &worker_handle.metrics,
                                    )?;
                                }
                            }
                        }
                        run_stream(
                            &registry,
                            &plan,
                            &sink_key,
                            &worker_config,
                            &worker_handle,
                            &worker_progress,
                            checkpoint.as_ref(),
                            recovered,
                        )
                    }
                }))
                .unwrap_or_else(|_| {
                    Err(Error::execution("job", "direct", "stream worker panicked"))
                });
                worker_handle.set_status(match &result {
                    Ok(()) => JobStatus::Succeeded,
                    Err(error) if error.kind == ErrorKind::Cancelled => JobStatus::Cancelled,
                    Err(_) => JobStatus::Failed,
                });
                result
            })
            .map_err(|error| Error::execution("job", "direct", error.to_string()))?;
        Ok(StreamJob {
            handle,
            progress,
            worker: Some(worker),
        })
    }

    /// Start two idempotent append-sink branches from one resumable source.
    /// Both sinks commit before a batch cursor is checkpointed. A retry can
    /// replay a committed first sink if the second commit failed.
    #[allow(clippy::too_many_lines)] // Both sink branches share validation and worker startup.
    pub fn start_stream_sink_pair<T: Element, U: Element>(
        &self,
        left: &Sink<T>,
        right: &Sink<U>,
        config: &JobConfig,
    ) -> Result<StreamJob, Error> {
        if !left.shares_pipeline_with(right) || left.key() == right.key() {
            return Err(Error::graph(
                "job",
                "stream sinks must be distinct in one pipeline",
            ));
        }
        let (graph, registry) = left.snapshot()?;
        Self::validate_with_unbounded(&graph, true)?;
        validate_config(config)?;
        if config.spill.is_some() {
            return Err(Error::unsupported("job", "direct", "stream-spill"));
        }
        for key in [left.key(), right.key()] {
            let Some(Operation::Sink { spec }) = graph.node(key).map(|node| &node.operation) else {
                return Err(Error::graph(key, "requested terminal is not a sink"));
            };
            if spec.mode != SinkMode::Append
                || spec.replay != Replay::Idempotent
                || !spec.commit
                || !registry.has_sink(key)
            {
                return Err(Error::unsupported(key, "direct", "idempotent-stream-sink"));
            }
        }
        let plan = multi_output::prepare_sinks(&graph, &registry, left.key(), right.key())?;
        let source_node = graph
            .node(&plan.source_key)
            .ok_or_else(|| Error::graph(&plan.source_key, "stream source node missing"))?;
        let Operation::Source {
            progress: source_progress,
        } = &source_node.operation
        else {
            return Err(Error::graph(&plan.source_key, "stream route has no source"));
        };
        if source_node.boundedness != ironbeam_core::Boundedness::Unbounded
            || !source_progress.stable_split_id
            || source_progress.replay == Replay::None
            || !registry.has_unbounded_source(&plan.source_key)
        {
            return Err(Error::unsupported(
                &plan.source_key,
                "direct",
                "resumable-stream-source",
            ));
        }
        let graph_fingerprint = graph.fingerprint()?;
        let checkpoint = config
            .checkpoint
            .as_ref()
            .map(|checkpoint| {
                StreamCheckpoint::new_pair::<T, U>(
                    checkpoint,
                    &graph_fingerprint,
                    registry
                        .source_config_digest(&plan.source_key)
                        .ok_or_else(|| {
                            Error::unsupported(&plan.source_key, "direct", "source-config-digest")
                        })?,
                    &plan.source_key,
                    (
                        left.key(),
                        registry.sink_config_digest(left.key()).ok_or_else(|| {
                            Error::unsupported(left.key(), "direct", "sink-config-digest")
                        })?,
                    ),
                    (
                        right.key(),
                        registry.sink_config_digest(right.key()).ok_or_else(|| {
                            Error::unsupported(right.key(), "direct", "sink-config-digest")
                        })?,
                    ),
                )
            })
            .transpose()?;
        let recovered = checkpoint
            .as_ref()
            .map(StreamCheckpoint::load)
            .transpose()?
            .flatten();
        #[allow(unused_mut)]
        let mut handle = JobHandle::new(config.cancellation.clone(), JobStatus::Submitted);
        #[cfg(feature = "metrics")]
        {
            handle
                .metrics_collector
                .clone_from(&config.metrics_collector);
        }
        if config.cancellation.load(Ordering::Acquire) {
            return Err(cancelled());
        }
        if recovered
            .as_ref()
            .is_some_and(|progress| progress.completed)
        {
            handle.set_status(JobStatus::Succeeded);
            return Ok(StreamJob {
                handle,
                progress: Arc::new(Mutex::new(recovered)),
                worker: None,
            });
        }
        let progress = Arc::new(Mutex::new(recovered.clone()));
        let worker_progress = Arc::clone(&progress);
        let worker_handle = handle.clone();
        let worker_config = config.clone();
        let left_key = left.key().to_owned();
        let right_key = right.key().to_owned();
        handle.set_status(JobStatus::Running);
        let worker = std::thread::Builder::new()
            .name(format!("ironbeam-stream-{}", handle.id()))
            .spawn(move || {
                let result = catch_unwind(AssertUnwindSafe(|| {
                    run_stream_pair(
                        &registry,
                        &plan,
                        &graph_fingerprint,
                        &left_key,
                        &right_key,
                        &worker_config,
                        &worker_handle,
                        &worker_progress,
                        checkpoint.as_ref(),
                        recovered,
                    )
                }))
                .unwrap_or_else(|_| {
                    Err(Error::execution("job", "direct", "stream worker panicked"))
                });
                worker_handle.set_status(match &result {
                    Ok(()) => JobStatus::Succeeded,
                    Err(error) if error.kind == ErrorKind::Cancelled => JobStatus::Cancelled,
                    Err(_) => JobStatus::Failed,
                });
                result
            })
            .map_err(|error| Error::execution("job", "direct", error.to_string()))?;
        Ok(StreamJob {
            handle,
            progress,
            worker: Some(worker),
        })
    }
}

fn merge_progress(previous: Option<&ProgressSignal>, mut next: ProgressSignal) -> ProgressSignal {
    if let Some(previous) = previous {
        if next.checkpoint_position.is_none() {
            next.checkpoint_position
                .clone_from(&previous.checkpoint_position);
        }
        if next.event_time_ns.is_none() {
            next.event_time_ns = previous.event_time_ns;
        }
    }
    next
}

fn open_sink(registry: &Registry, key: &str) -> Result<Box<dyn SinkSession>, Error> {
    let mut session = catch_unwind(AssertUnwindSafe(|| registry.open_sink(key)))
        .unwrap_or_else(|_| Err(Error::execution(key, "direct", "sink factory panicked")))?;
    let Ok(requires_finalization) =
        catch_unwind(AssertUnwindSafe(|| session.requires_bundle_finalization()))
    else {
        let _ = catch_unwind(AssertUnwindSafe(|| session.abort()));
        return Err(Error::execution(
            key,
            "direct",
            "sink finalization probe panicked",
        ));
    };
    if requires_finalization {
        let _ = catch_unwind(AssertUnwindSafe(|| session.abort()));
        return Err(Error::unsupported(
            key,
            "direct",
            "stream-bundle-finalization",
        ));
    }
    Ok(session)
}

fn begin_sink_bundle(
    registry: &Registry,
    key: &str,
    instruction_id: &str,
) -> Result<Box<dyn SinkSession>, Error> {
    let mut session = open_sink(registry, key)?;
    let started = catch_unwind(AssertUnwindSafe(|| session.begin_bundle(instruction_id)))
        .unwrap_or_else(|_| {
            Err(Error::execution(
                key,
                "direct",
                "sink bundle begin panicked",
            ))
        });
    if let Err(error) = started {
        let _ = catch_unwind(AssertUnwindSafe(|| session.abort()));
        return Err(error);
    }
    Ok(session)
}

fn bundle_id(graph_fingerprint: &str, sink_keys: &[&str], progress: &ProgressSignal) -> String {
    let mut hasher = Sha256::new();
    for bytes in std::iter::once(graph_fingerprint.as_bytes())
        .chain(sink_keys.iter().map(|key| key.as_bytes()))
        .chain([
            progress.source_key.as_bytes(),
            progress.split_id.as_bytes(),
            progress.checkpoint_position.as_deref().unwrap_or_default(),
        ])
    {
        hasher.update((bytes.len() as u64).to_le_bytes());
        hasher.update(bytes);
    }
    match progress.event_time_ns {
        Some(watermark_ns) => {
            hasher.update([1]);
            hasher.update(watermark_ns.to_le_bytes());
        }
        None => hasher.update([0]),
    }
    hasher.update([u8::from(progress.completed)]);
    hex_bytes(&hasher.finalize())
}

fn processing_time_now_ns(source: &dyn UnboundedSource<Envelope>) -> Result<i64, Error> {
    if let Some(now_ns) = source.processing_time_ns() {
        return Ok(now_ns);
    }
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| Error::resource("processing-clock", error.to_string()))?;
    i64::try_from(elapsed.as_nanos())
        .map_err(|_| Error::resource("processing-clock", "wall clock exceeds i64 nanoseconds"))
}

fn timer_bundle_id(
    graph_fingerprint: &str,
    sink_key: &str,
    progress: &ProgressSignal,
    prior_state: &[u8],
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"ironbeam-direct-processing-timer-v1");
    hasher.update(bundle_id(graph_fingerprint, &[sink_key], progress).as_bytes());
    hasher.update((prior_state.len() as u64).to_le_bytes());
    hasher.update(prior_state);
    hex_bytes(&hasher.finalize())
}

fn repeated_cursor_bundle_id(
    graph_fingerprint: &str,
    sink_key: &str,
    progress: &ProgressSignal,
    prior_state: &[u8],
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"ironbeam-direct-repeated-cursor-v1");
    hasher.update(bundle_id(graph_fingerprint, &[sink_key], progress).as_bytes());
    hasher.update(progress.event_time_ns.unwrap_or(i64::MIN).to_le_bytes());
    hasher.update([u8::from(progress.completed)]);
    hasher.update((prior_state.len() as u64).to_le_bytes());
    hasher.update(prior_state);
    hex_bytes(&hasher.finalize())
}

fn apply_grouped_stages(
    registry: &Registry,
    stages: &[String],
    input: Envelope,
    config: &JobConfig,
    handle: &JobHandle,
) -> Result<Vec<Envelope>, Error> {
    let mut events = vec![input];
    for key in stages {
        let mut next = Vec::new();
        for event in events {
            if config.cancellation.load(Ordering::Acquire) {
                return Err(cancelled());
            }
            let produced = catch_unwind(AssertUnwindSafe(|| registry.apply_many(key, event)))
                .unwrap_or_else(|_| {
                    Err(Error::execution(key, "direct", "user transform panicked"))
                })?;
            if produced.is_empty() {
                handle
                    .metrics
                    .filtered_elements
                    .fetch_add(1, Ordering::Relaxed);
            }
            if next.len().saturating_add(produced.len()) > config.buffer_budget_elements {
                return Err(Error::resource(
                    key,
                    "stream transform output exceeds buffer budget",
                ));
            }
            next.extend(produced);
        }
        events = next;
    }
    Ok(events)
}

fn stream_window(event: &Envelope, key: &str) -> Result<(i64, i64), Error> {
    if event.window == Window::Global && event.assigned_window.is_none() {
        return Ok((i64::MIN, i64::MAX));
    }
    let interval = event.assigned_window.ok_or_else(|| {
        Error::execution(key, "direct", "stateful stream input needs a fixed window")
    })?;
    Ok((interval.start_ns, interval.end_ns))
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
fn run_bounded_side_stream(
    registry: &Registry,
    route: &BoundedSideStreamRoute,
    graph_fingerprint: &str,
    sink_key: &str,
    config: &JobConfig,
    handle: &JobHandle,
    shared_progress: &Arc<Mutex<Option<ProgressSignal>>>,
    checkpoint: Option<&StreamCheckpoint>,
    mut progress: Option<ProgressSignal>,
    mut main_windows: WindowRows,
    mut buffered: usize,
) -> Result<(), Error> {
    let mut side_source = open_source_safe(registry, &route.side_source_key)?;
    let mut side_windows: WindowRows = BTreeMap::new();
    loop {
        if config.cancellation.load(Ordering::Acquire) {
            return Err(cancelled());
        }
        let batch = read_source_batch(
            &mut side_source,
            config.buffer_budget_elements,
            &route.side_source_key,
        )?;
        if batch.is_empty() {
            break;
        }
        let count = u64::try_from(batch.len()).unwrap_or(u64::MAX);
        handle
            .metrics
            .source_elements
            .fetch_add(count, Ordering::Relaxed);
        handle
            .metrics
            .peak_batch_elements
            .fetch_max(count, Ordering::Relaxed);
        handle.metrics.batches.fetch_add(1, Ordering::Relaxed);
        for input in batch {
            for event in apply_grouped_stages(registry, &route.side_stages, input, config, handle)?
            {
                let window = stream_window(&event, &route.aggregate_key)?;
                if progress
                    .as_ref()
                    .and_then(|saved| saved.event_time_ns)
                    .is_some_and(|watermark| window.1 <= watermark)
                {
                    continue;
                }
                if buffered >= config.buffer_budget_elements {
                    return Err(Error::resource(
                        &route.aggregate_key,
                        "stateful stream input exceeds buffer budget",
                    ));
                }
                side_windows.entry(window).or_default().push(event);
                buffered += 1;
            }
        }
    }
    let mut source = catch_unwind(AssertUnwindSafe(|| {
        registry.open_unbounded_source(&route.source_key, progress.clone())
    }))
    .unwrap_or_else(|_| {
        Err(Error::execution(
            &route.source_key,
            "direct",
            "source factory panicked",
        ))
    })?;
    loop {
        if config.cancellation.load(Ordering::Acquire) {
            return Err(cancelled());
        }
        let polled = catch_unwind(AssertUnwindSafe(|| {
            source.poll(config.buffer_budget_elements)
        }))
        .unwrap_or_else(|_| {
            Err(Error::execution(
                &route.source_key,
                "direct",
                "source poll panicked",
            ))
        })?;
        let UnboundedPoll::Ready(batch) = polled else {
            std::thread::sleep(Duration::from_millis(10));
            continue;
        };
        if !batch.elements.is_empty()
            && progress.as_ref().is_some_and(|previous| {
                previous.checkpoint_position == batch.progress.checkpoint_position
            })
        {
            return Err(Error::execution(
                &route.source_key,
                "source",
                "data batch did not advance its cursor",
            ));
        }
        let count = u64::try_from(batch.elements.len()).unwrap_or(u64::MAX);
        handle
            .metrics
            .source_elements
            .fetch_add(count, Ordering::Relaxed);
        handle
            .metrics
            .peak_batch_elements
            .fetch_max(count, Ordering::Relaxed);
        handle.metrics.batches.fetch_add(1, Ordering::Relaxed);
        for input in batch.elements {
            for event in apply_grouped_stages(registry, &route.main_stages, input, config, handle)?
            {
                let window = stream_window(&event, &route.aggregate_key)?;
                if progress
                    .as_ref()
                    .and_then(|previous| previous.event_time_ns)
                    .is_some_and(|watermark| window.1 <= watermark)
                {
                    continue;
                }
                if buffered >= config.buffer_budget_elements {
                    return Err(Error::resource(
                        &route.aggregate_key,
                        "stateful stream input exceeds buffer budget",
                    ));
                }
                main_windows.entry(window).or_default().push(event);
                buffered += 1;
            }
        }
        let next_progress = merge_progress(progress.as_ref(), batch.progress);
        let closing = main_windows
            .keys()
            .chain(side_windows.keys())
            .copied()
            .filter(|(_, end)| {
                next_progress.completed
                    || next_progress
                        .event_time_ns
                        .is_some_and(|watermark| *end <= watermark)
            })
            .collect::<BTreeSet<_>>();
        let instruction_id = bundle_id(graph_fingerprint, &[sink_key], &next_progress);
        let mut session = None;
        let mut written = 0_u64;
        let result = (|| {
            for window in closing {
                let main = main_windows.remove(&window).unwrap_or_default();
                let side = side_windows.remove(&window).unwrap_or_default();
                buffered -= main.len() + side.len();
                let grouped = catch_unwind(AssertUnwindSafe(|| {
                    registry.apply_aggregate(&route.aggregate_key, vec![main, side])
                }))
                .unwrap_or_else(|_| {
                    Err(Error::execution(
                        &route.aggregate_key,
                        "direct",
                        "user aggregate panicked",
                    ))
                })?;
                for event in grouped {
                    for output in apply_grouped_stages(
                        registry,
                        &route.after_aggregate,
                        event,
                        config,
                        handle,
                    )? {
                        if session.is_none() {
                            session = Some(begin_sink_bundle(registry, sink_key, &instruction_id)?);
                        }
                        catch_unwind(AssertUnwindSafe(|| {
                            session.as_mut().expect("opened sink").write(output)
                        }))
                        .unwrap_or_else(|_| {
                            Err(Error::execution(sink_key, "direct", "sink writer panicked"))
                        })?;
                        written = written.saturating_add(1);
                    }
                }
            }
            if config.cancellation.load(Ordering::Acquire) {
                return Err(cancelled());
            }
            if let Some(session) = &mut session {
                catch_unwind(AssertUnwindSafe(|| session.commit())).unwrap_or_else(|_| {
                    Err(Error::execution(sink_key, "direct", "sink commit panicked"))
                })?;
            }
            Ok(())
        })();
        if let Err(error) = result {
            if let Some(session) = &mut session {
                let _ = catch_unwind(AssertUnwindSafe(|| session.abort()));
            }
            return Err(error);
        }
        if let Some(checkpoint) = checkpoint
            && (next_progress.checkpoint_position.is_some() || next_progress.completed)
        {
            let state = encode_window_rows(
                registry,
                &route.main_input_key,
                &main_windows,
                config.memory_budget_bytes,
            )?;
            checkpoint.save_state(&next_progress, &state, config.memory_budget_bytes)?;
        }
        let total = handle
            .metrics
            .output_elements
            .fetch_add(written, Ordering::Relaxed)
            .saturating_add(written);
        handle.processed.store(total, Ordering::Relaxed);
        *shared_progress
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(next_progress.clone());
        if next_progress.completed {
            return Ok(());
        }
        progress = Some(next_progress);
    }
}

#[allow(clippy::too_many_arguments)]
fn write_grouped_panes(
    registry: &Registry,
    route: &GroupedStreamRoute,
    sink_key: &str,
    instruction_id: &str,
    config: &JobConfig,
    handle: &JobHandle,
    panes: Vec<Envelope>,
    session: &mut Option<Box<dyn SinkSession>>,
    written: &mut u64,
) -> Result<(), Error> {
    for event in panes {
        for output in apply_grouped_stages(registry, &route.after_group, event, config, handle)? {
            if session.is_none() {
                *session = Some(begin_sink_bundle(registry, sink_key, instruction_id)?);
            }
            catch_unwind(AssertUnwindSafe(|| {
                session.as_mut().expect("opened sink").write(output)
            }))
            .unwrap_or_else(|_| {
                Err(Error::execution(sink_key, "direct", "sink writer panicked"))
            })?;
            *written = written.saturating_add(1);
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
#[allow(clippy::too_many_lines)] // Window state, sink commit, and progress form one execution cycle.
fn run_grouped_stream(
    registry: &Registry,
    route: &GroupedStreamRoute,
    graph_fingerprint: &str,
    sink_key: &str,
    config: &JobConfig,
    handle: &JobHandle,
    shared_progress: &Arc<Mutex<Option<ProgressSignal>>>,
    checkpoint: Option<&StreamCheckpoint>,
    mut progress: Option<ProgressSignal>,
    mut folds: WindowFolds,
    mut buffered: usize,
) -> Result<(), Error> {
    let mut source = catch_unwind(AssertUnwindSafe(|| {
        registry.open_unbounded_source(&route.source_key, progress.clone())
    }))
    .unwrap_or_else(|_| {
        Err(Error::execution(
            &route.source_key,
            "direct",
            "source factory panicked",
        ))
    })?;
    loop {
        if config.cancellation.load(Ordering::Acquire) {
            return Err(cancelled());
        }
        let polled = catch_unwind(AssertUnwindSafe(|| {
            source.poll(config.buffer_budget_elements)
        }))
        .unwrap_or_else(|_| {
            Err(Error::execution(
                &route.source_key,
                "direct",
                "source poll panicked",
            ))
        })?;
        let UnboundedPoll::Ready(batch) = polled else {
            if route.trigger.early_processing_time().is_some()
                && let Some(current_progress) = progress.as_ref()
            {
                let now_ns = processing_time_now_ns(source.as_ref())?;
                if let Some(deadline_ns) = folds
                    .values()
                    .filter_map(|(fold, _)| fold.next_processing_time_deadline())
                    .min()
                    .filter(|deadline| *deadline <= now_ns)
                {
                    let prior_state =
                        encode_grouped_folds(&folds, config.memory_budget_bytes, &route.group_key)?;
                    let instruction_id = timer_bundle_id(
                        graph_fingerprint,
                        sink_key,
                        current_progress,
                        &prior_state,
                    );
                    let mut panes = Vec::new();
                    for (fold, rows) in folds.values_mut() {
                        let (mut fired, released) =
                            fold.drain_ready_processing_time(deadline_ns)?;
                        *rows = rows.checked_sub(released).ok_or_else(|| {
                            Error::recovery(
                                &route.group_key,
                                "processing trigger released too many rows",
                            )
                        })?;
                        buffered = buffered.checked_sub(released).ok_or_else(|| {
                            Error::recovery(&route.group_key, "group buffer count underflow")
                        })?;
                        panes.append(&mut fired);
                    }
                    if panes.is_empty() && !route.trigger.is_count_and_processing_time() {
                        return Err(Error::recovery(
                            &route.group_key,
                            "armed processing timer produced no pane",
                        ));
                    }
                    let mut session = None;
                    let mut written = 0_u64;
                    let result = (|| {
                        write_grouped_panes(
                            registry,
                            route,
                            sink_key,
                            &instruction_id,
                            config,
                            handle,
                            panes,
                            &mut session,
                            &mut written,
                        )?;
                        if config.cancellation.load(Ordering::Acquire) {
                            return Err(cancelled());
                        }
                        if let Some(session) = &mut session {
                            catch_unwind(AssertUnwindSafe(|| session.commit())).unwrap_or_else(
                                |_| {
                                    Err(Error::execution(
                                        sink_key,
                                        "direct",
                                        "sink commit panicked",
                                    ))
                                },
                            )?;
                        }
                        Ok(())
                    })();
                    if let Err(error) = result {
                        if let Some(session) = &mut session {
                            let _ = catch_unwind(AssertUnwindSafe(|| session.abort()));
                        }
                        return Err(error);
                    }
                    if let Some(checkpoint) = checkpoint {
                        let state = encode_grouped_folds(
                            &folds,
                            config.memory_budget_bytes,
                            &route.group_key,
                        )?;
                        checkpoint.save_state(
                            current_progress,
                            &state,
                            config.memory_budget_bytes,
                        )?;
                    }
                    let total = handle
                        .metrics
                        .output_elements
                        .fetch_add(written, Ordering::Relaxed)
                        .saturating_add(written);
                    handle.processed.store(total, Ordering::Relaxed);
                    continue;
                }
            }
            std::thread::sleep(Duration::from_millis(10));
            continue;
        };
        let repeated_cursor_state = if batch.elements.is_empty()
            && progress.as_ref().is_some_and(|previous| {
                previous.checkpoint_position == batch.progress.checkpoint_position
            }) {
            Some(encode_grouped_folds(
                &folds,
                config.memory_budget_bytes,
                &route.group_key,
            )?)
        } else {
            None
        };
        if !batch.elements.is_empty()
            && progress.as_ref().is_some_and(|previous| {
                previous.checkpoint_position == batch.progress.checkpoint_position
            })
        {
            return Err(Error::execution(
                &route.source_key,
                "source",
                "data batch did not advance its cursor",
            ));
        }
        let input_count = u64::try_from(batch.elements.len()).unwrap_or(u64::MAX);
        handle
            .metrics
            .source_elements
            .fetch_add(input_count, Ordering::Relaxed);
        handle
            .metrics
            .peak_batch_elements
            .fetch_max(input_count, Ordering::Relaxed);
        handle.metrics.batches.fetch_add(1, Ordering::Relaxed);
        let mut late_windows = BTreeSet::new();
        for input in batch.elements {
            for event in apply_grouped_stages(registry, &route.before_group, input, config, handle)?
            {
                let interval = match (route.window == Window::Global, event.assigned_window) {
                    (true, None) => WindowInterval {
                        start_ns: i64::MIN,
                        end_ns: i64::MAX,
                    },
                    (false, Some(interval)) => interval,
                    _ => {
                        return Err(Error::execution(
                            &route.group_key,
                            "direct",
                            "grouped stream window metadata does not match its declaration",
                        ));
                    }
                };
                let prior_watermark = progress
                    .as_ref()
                    .and_then(|previous| previous.event_time_ns);
                let expired_at = interval.end_ns.saturating_add(route.allowed_lateness_ns);
                if prior_watermark.is_some_and(|watermark| expired_at <= watermark) {
                    continue;
                }
                if buffered >= config.buffer_budget_elements {
                    return Err(Error::resource(
                        &route.group_key,
                        "grouped stream state exceeds buffer budget",
                    ));
                }
                // Session intervals merge by key as bridge events arrive. A
                // single fold owns every open session so it can see both sides
                // of a bridge, even when their original intervals differ.
                let scope = if matches!(route.window, Window::Session { .. }) {
                    (i64::MIN, i64::MAX)
                } else {
                    (interval.start_ns, interval.end_ns)
                };
                if prior_watermark.is_some_and(|watermark| interval.end_ns <= watermark) {
                    late_windows.insert(scope);
                }
                let entry = folds.entry(scope);
                let (fold, count) = match entry {
                    std::collections::btree_map::Entry::Occupied(entry) => entry.into_mut(),
                    std::collections::btree_map::Entry::Vacant(entry) => {
                        let fold =
                            catch_unwind(AssertUnwindSafe(|| registry.open_fold(&route.group_key)))
                                .unwrap_or_else(|_| {
                                    Err(Error::execution(
                                        &route.group_key,
                                        "direct",
                                        "group fold factory panicked",
                                    ))
                                })?;
                        entry.insert((fold, 0))
                    }
                };
                catch_unwind(AssertUnwindSafe(|| fold.add(event))).unwrap_or_else(|_| {
                    Err(Error::execution(
                        &route.group_key,
                        "direct",
                        "group fold panicked",
                    ))
                })?;
                *count += 1;
                buffered += 1;
            }
        }
        let next_progress = merge_progress(progress.as_ref(), batch.progress);
        let mut early_panes = Vec::new();
        if let Some(count) = route.trigger.early_count() {
            for ((_, end), (fold, rows)) in &mut folds {
                if progress
                    .as_ref()
                    .and_then(|prior| prior.event_time_ns)
                    .is_some_and(|watermark| *end <= watermark)
                {
                    continue;
                }
                let (mut fired, released) =
                    catch_unwind(AssertUnwindSafe(|| fold.drain_ready_count(count)))
                        .unwrap_or_else(|_| {
                            Err(Error::execution(
                                &route.group_key,
                                "direct",
                                "group count trigger panicked",
                            ))
                        })?;
                *rows = rows.checked_sub(released).ok_or_else(|| {
                    Error::recovery(&route.group_key, "count trigger released too many rows")
                })?;
                buffered = buffered.checked_sub(released).ok_or_else(|| {
                    Error::recovery(&route.group_key, "group buffer count underflow")
                })?;
                early_panes.append(&mut fired);
            }
        }
        if let Some(delay_ns) = route.trigger.early_processing_time() {
            let now_ns = processing_time_now_ns(source.as_ref())?;
            for (fold, rows) in folds.values_mut() {
                fold.arm_processing_time(now_ns, delay_ns)?;
                let (mut fired, released) = fold.drain_ready_processing_time(now_ns)?;
                *rows = rows.checked_sub(released).ok_or_else(|| {
                    Error::recovery(
                        &route.group_key,
                        "processing trigger released too many rows",
                    )
                })?;
                buffered = buffered.checked_sub(released).ok_or_else(|| {
                    Error::recovery(&route.group_key, "group buffer count underflow")
                })?;
                early_panes.append(&mut fired);
            }
        }
        let closing = if matches!(route.window, Window::Session { .. }) {
            if next_progress.completed || next_progress.event_time_ns.is_some() {
                folds.keys().copied().collect::<Vec<_>>()
            } else {
                Vec::new()
            }
        } else {
            folds
                .keys()
                .copied()
                .filter(|(_, end)| {
                    let not_previously_closed = route.allowed_lateness_ns == 0
                        || progress
                            .as_ref()
                            .and_then(|previous| previous.event_time_ns)
                            .is_none_or(|watermark| watermark < *end);
                    not_previously_closed
                        && (next_progress.completed
                            || next_progress
                                .event_time_ns
                                .is_some_and(|watermark| *end <= watermark))
                })
                .collect::<Vec<_>>()
        };
        let instruction_id = repeated_cursor_state.as_ref().map_or_else(
            || bundle_id(graph_fingerprint, &[sink_key], &next_progress),
            |state| repeated_cursor_bundle_id(graph_fingerprint, sink_key, &next_progress, state),
        );
        let mut session = None;
        let mut written = 0_u64;
        let result = (|| {
            write_grouped_panes(
                registry,
                route,
                sink_key,
                &instruction_id,
                config,
                handle,
                early_panes,
                &mut session,
                &mut written,
            )?;
            for window in closing {
                let grouped = if route.allowed_lateness_ns > 0 && next_progress.completed {
                    let (mut fold, count) =
                        folds.remove(&window).expect("completed window fold exists");
                    let grouped = catch_unwind(AssertUnwindSafe(|| {
                        let (mut on_time, _) = fold.drain_closed(i64::MAX)?;
                        let (late, _) = fold.drain_late(true)?;
                        on_time.extend(late);
                        Ok::<_, Error>(on_time)
                    }))
                    .unwrap_or_else(|_| {
                        Err(Error::execution(
                            &route.group_key,
                            "direct",
                            "group fold panicked",
                        ))
                    })?;
                    buffered = buffered.checked_sub(count).ok_or_else(|| {
                        Error::recovery(&route.group_key, "group buffer count underflow")
                    })?;
                    grouped
                } else if route.allowed_lateness_ns > 0 {
                    let (fold, count) =
                        folds.get_mut(&window).expect("retained window fold exists");
                    let watermark = next_progress.event_time_ns.unwrap_or(i64::MAX);
                    let (grouped, released) =
                        catch_unwind(AssertUnwindSafe(|| fold.drain_closed(watermark)))
                            .unwrap_or_else(|_| {
                                Err(Error::execution(
                                    &route.group_key,
                                    "direct",
                                    "group fold panicked",
                                ))
                            })?;
                    *count = count.checked_sub(released).ok_or_else(|| {
                        Error::recovery(&route.group_key, "on-time fold released too many rows")
                    })?;
                    buffered = buffered.checked_sub(released).ok_or_else(|| {
                        Error::recovery(&route.group_key, "group buffer count underflow")
                    })?;
                    grouped
                } else if matches!(route.window, Window::Session { .. }) && !next_progress.completed
                {
                    let (fold, count) = folds.get_mut(&window).expect("session fold exists");
                    let watermark = next_progress.event_time_ns.expect("watermark checked");
                    let (grouped, released) =
                        catch_unwind(AssertUnwindSafe(|| fold.drain_closed(watermark)))
                            .unwrap_or_else(|_| {
                                Err(Error::execution(
                                    &route.group_key,
                                    "direct",
                                    "group fold panicked",
                                ))
                            })?;
                    *count = count.checked_sub(released).ok_or_else(|| {
                        Error::recovery(&route.group_key, "session fold released too many rows")
                    })?;
                    buffered = buffered.checked_sub(released).ok_or_else(|| {
                        Error::recovery(&route.group_key, "session buffer count underflow")
                    })?;
                    if *count == 0 {
                        folds.remove(&window);
                    }
                    grouped
                } else {
                    let (fold, count) = folds.remove(&window).expect("closing fold exists");
                    let grouped =
                        catch_unwind(AssertUnwindSafe(|| fold.finish())).unwrap_or_else(|_| {
                            Err(Error::execution(
                                &route.group_key,
                                "direct",
                                "group fold panicked",
                            ))
                        })?;
                    buffered -= count;
                    grouped
                };
                write_grouped_panes(
                    registry,
                    route,
                    sink_key,
                    &instruction_id,
                    config,
                    handle,
                    grouped,
                    &mut session,
                    &mut written,
                )?;
            }
            for window in late_windows {
                if next_progress.completed
                    || next_progress.event_time_ns.is_some_and(|watermark| {
                        window.1.saturating_add(route.allowed_lateness_ns) <= watermark
                    })
                {
                    continue;
                }
                let (fold, count) = folds.get_mut(&window).expect("late window fold exists");
                let (late, released) = catch_unwind(AssertUnwindSafe(|| fold.drain_late(false)))
                    .unwrap_or_else(|_| {
                        Err(Error::execution(
                            &route.group_key,
                            "direct",
                            "late group fold panicked",
                        ))
                    })?;
                *count = count.checked_sub(released).ok_or_else(|| {
                    Error::recovery(&route.group_key, "late fold released too many rows")
                })?;
                buffered = buffered.checked_sub(released).ok_or_else(|| {
                    Error::recovery(&route.group_key, "group buffer count underflow")
                })?;
                write_grouped_panes(
                    registry,
                    route,
                    sink_key,
                    &instruction_id,
                    config,
                    handle,
                    late,
                    &mut session,
                    &mut written,
                )?;
            }
            if route.allowed_lateness_ns > 0 {
                let expiring = folds
                    .keys()
                    .copied()
                    .filter(|(_, end)| {
                        next_progress.event_time_ns.is_some_and(|watermark| {
                            end.saturating_add(route.allowed_lateness_ns) <= watermark
                        })
                    })
                    .collect::<Vec<_>>();
                for window in expiring {
                    let (fold, count) = folds.get_mut(&window).expect("expiring fold exists");
                    let (late, released) = catch_unwind(AssertUnwindSafe(|| fold.drain_late(true)))
                        .unwrap_or_else(|_| {
                            Err(Error::execution(
                                &route.group_key,
                                "direct",
                                "late group fold panicked",
                            ))
                        })?;
                    *count = count.checked_sub(released).ok_or_else(|| {
                        Error::recovery(&route.group_key, "late fold released too many rows")
                    })?;
                    buffered = buffered.checked_sub(released).ok_or_else(|| {
                        Error::recovery(&route.group_key, "group buffer count underflow")
                    })?;
                    write_grouped_panes(
                        registry,
                        route,
                        sink_key,
                        &instruction_id,
                        config,
                        handle,
                        late,
                        &mut session,
                        &mut written,
                    )?;
                }
            }
            if config.cancellation.load(Ordering::Acquire) {
                return Err(cancelled());
            }
            if let Some(session) = &mut session {
                catch_unwind(AssertUnwindSafe(|| session.commit())).unwrap_or_else(|_| {
                    Err(Error::execution(sink_key, "direct", "sink commit panicked"))
                })?;
            }
            Ok(())
        })();
        if let Err(error) = result {
            if let Some(session) = &mut session {
                let _ = catch_unwind(AssertUnwindSafe(|| session.abort()));
            }
            return Err(error);
        }
        if route.allowed_lateness_ns > 0 {
            folds.retain(|(_, end), _| {
                !next_progress.completed
                    && next_progress.event_time_ns.is_none_or(|watermark| {
                        end.saturating_add(route.allowed_lateness_ns) > watermark
                    })
            });
        }
        if let Some(checkpoint) = checkpoint
            && (next_progress.checkpoint_position.is_some() || next_progress.completed)
        {
            let state = encode_grouped_folds(&folds, config.memory_budget_bytes, &route.group_key)?;
            checkpoint.save_state(&next_progress, &state, config.memory_budget_bytes)?;
        }
        let total = handle
            .metrics
            .output_elements
            .fetch_add(written, Ordering::Relaxed)
            .saturating_add(written);
        handle.processed.store(total, Ordering::Relaxed);
        *shared_progress
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(next_progress.clone());
        if next_progress.completed {
            return Ok(());
        }
        progress = Some(next_progress);
    }
}

#[allow(clippy::too_many_arguments)]
#[allow(clippy::too_many_lines)] // Poll, sink commit, and cursor save form one replay state machine.
fn run_stream(
    registry: &Registry,
    plan: &PhysicalPlan,
    sink_key: &str,
    config: &JobConfig,
    handle: &JobHandle,
    shared_progress: &Arc<Mutex<Option<ProgressSignal>>>,
    checkpoint: Option<&StreamCheckpoint>,
    mut progress: Option<ProgressSignal>,
) -> Result<(), Error> {
    let route = &plan.routes[0];
    let stateful_key = route
        .operations
        .iter()
        .find(|key| registry.has_stateful_value(key));
    let mut source = catch_unwind(AssertUnwindSafe(|| {
        registry.open_unbounded_source(&route.source_key, progress.clone())
    }))
    .unwrap_or_else(|_| {
        Err(Error::execution(
            &route.source_key,
            "direct",
            "source factory panicked",
        ))
    })?;
    loop {
        if config.cancellation.load(Ordering::Acquire) {
            return Err(cancelled());
        }
        let polled = catch_unwind(AssertUnwindSafe(|| source.poll(plan.summary.batch_limit)))
            .unwrap_or_else(|_| {
                Err(Error::execution(
                    &route.source_key,
                    "direct",
                    "source poll panicked",
                ))
            })?;
        let UnboundedPoll::Ready(batch) = polled else {
            std::thread::sleep(Duration::from_millis(10));
            continue;
        };
        if !batch.elements.is_empty()
            && progress.as_ref().is_some_and(|previous| {
                previous.checkpoint_position == batch.progress.checkpoint_position
            })
        {
            return Err(Error::execution(
                &route.source_key,
                "source",
                "data batch did not advance its cursor",
            ));
        }
        let instruction_id = bundle_id(
            &plan.summary.graph_fingerprint,
            &[sink_key],
            &batch.progress,
        );
        let input_count = u64::try_from(batch.elements.len()).unwrap_or(u64::MAX);
        handle
            .metrics
            .source_elements
            .fetch_add(input_count, Ordering::Relaxed);
        handle
            .metrics
            .peak_batch_elements
            .fetch_max(input_count, Ordering::Relaxed);
        handle.metrics.batches.fetch_add(1, Ordering::Relaxed);
        let mut session = None;
        let mut written = 0_u64;
        let result = (|| {
            for input in batch.elements {
                let mut events = vec![input];
                for key in &route.operations {
                    let mut next = Vec::new();
                    for event in events {
                        if config.cancellation.load(Ordering::Acquire) {
                            return Err(cancelled());
                        }
                        let produced =
                            catch_unwind(AssertUnwindSafe(|| registry.apply_many(key, event)))
                                .unwrap_or_else(|_| {
                                    Err(Error::execution(key, "direct", "user transform panicked"))
                                })?;
                        if produced.is_empty() {
                            handle
                                .metrics
                                .filtered_elements
                                .fetch_add(1, Ordering::Relaxed);
                        }
                        if next.len().saturating_add(produced.len()) > config.buffer_budget_elements
                        {
                            return Err(Error::resource(
                                key,
                                "stream transform output exceeds buffer budget",
                            ));
                        }
                        next.extend(produced);
                    }
                    events = next;
                }
                for event in events {
                    if config.cancellation.load(Ordering::Acquire) {
                        return Err(cancelled());
                    }
                    if session.is_none() {
                        session = Some(begin_sink_bundle(registry, sink_key, &instruction_id)?);
                    }
                    catch_unwind(AssertUnwindSafe(|| {
                        session.as_mut().expect("opened sink").write(event)
                    }))
                    .unwrap_or_else(|_| {
                        Err(Error::execution(sink_key, "direct", "sink writer panicked"))
                    })?;
                    written = written.saturating_add(1);
                }
            }
            if config.cancellation.load(Ordering::Acquire) {
                return Err(cancelled());
            }
            let state = stateful_key
                .map(|key| registry.checkpoint_stateful_value(key))
                .transpose()?;
            if state
                .as_ref()
                .is_some_and(|bytes| bytes.len() > config.memory_budget_bytes)
            {
                return Err(Error::resource(
                    "stateful-value",
                    "stream value state exceeds memory budget",
                ));
            }
            if let Some(session) = &mut session {
                catch_unwind(AssertUnwindSafe(|| session.commit())).unwrap_or_else(|_| {
                    Err(Error::execution(sink_key, "direct", "sink commit panicked"))
                })?;
            }
            Ok(state)
        })();
        let state = match result {
            Ok(state) => state,
            Err(error) => {
                if let Some(session) = &mut session {
                    let _ = catch_unwind(AssertUnwindSafe(|| session.abort()));
                }
                return Err(error);
            }
        };
        let next_progress = merge_progress(progress.as_ref(), batch.progress);
        if let Some(checkpoint) = &checkpoint
            && (next_progress.checkpoint_position.is_some() || next_progress.completed)
        {
            if let Some(state) = state {
                checkpoint.save_state(&next_progress, &state, config.memory_budget_bytes)?;
            } else {
                checkpoint.save(&next_progress)?;
            }
        }
        let total = handle
            .metrics
            .output_elements
            .fetch_add(written, Ordering::Relaxed)
            .saturating_add(written);
        handle.processed.store(total, Ordering::Relaxed);
        *shared_progress
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(next_progress.clone());
        if next_progress.completed {
            return Ok(());
        }
        progress = Some(next_progress);
    }
}

fn write_stream_branch(
    registry: &Registry,
    key: &str,
    instruction_id: &str,
    events: Vec<ironbeam_core::Envelope>,
    session: &mut Option<Box<dyn SinkSession>>,
    config: &JobConfig,
) -> Result<u64, Error> {
    let mut written = 0_u64;
    for event in events {
        if config.cancellation.load(Ordering::Acquire) {
            return Err(cancelled());
        }
        if session.is_none() {
            *session = Some(begin_sink_bundle(registry, key, instruction_id)?);
        }
        catch_unwind(AssertUnwindSafe(|| {
            session.as_mut().expect("opened sink").write(event)
        }))
        .unwrap_or_else(|_| Err(Error::execution(key, "direct", "sink writer panicked")))?;
        written = written.saturating_add(1);
    }
    Ok(written)
}

fn commit_stream_branch(
    key: &str,
    session: &mut Option<Box<dyn SinkSession>>,
) -> Result<(), Error> {
    if let Some(session) = session {
        catch_unwind(AssertUnwindSafe(|| session.commit()))
            .unwrap_or_else(|_| Err(Error::execution(key, "direct", "sink commit panicked")))?;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
#[allow(clippy::too_many_lines)] // Paired commit and replay need one ordered lifecycle.
fn run_stream_pair(
    registry: &Registry,
    plan: &PairPlan,
    graph_fingerprint: &str,
    left_key: &str,
    right_key: &str,
    config: &JobConfig,
    handle: &JobHandle,
    shared_progress: &Arc<Mutex<Option<ProgressSignal>>>,
    checkpoint: Option<&StreamCheckpoint>,
    mut progress: Option<ProgressSignal>,
) -> Result<(), Error> {
    let mut source = catch_unwind(AssertUnwindSafe(|| {
        registry.open_unbounded_source(&plan.source_key, progress.clone())
    }))
    .unwrap_or_else(|_| {
        Err(Error::execution(
            &plan.source_key,
            "direct",
            "source factory panicked",
        ))
    })?;
    loop {
        if config.cancellation.load(Ordering::Acquire) {
            return Err(cancelled());
        }
        let polled = catch_unwind(AssertUnwindSafe(|| {
            source.poll(config.buffer_budget_elements)
        }))
        .unwrap_or_else(|_| {
            Err(Error::execution(
                &plan.source_key,
                "direct",
                "source poll panicked",
            ))
        })?;
        let UnboundedPoll::Ready(batch) = polled else {
            std::thread::sleep(Duration::from_millis(10));
            continue;
        };
        if !batch.elements.is_empty()
            && progress.as_ref().is_some_and(|previous| {
                previous.checkpoint_position == batch.progress.checkpoint_position
            })
        {
            return Err(Error::execution(
                &plan.source_key,
                "source",
                "data batch did not advance its cursor",
            ));
        }
        let instruction_id = bundle_id(graph_fingerprint, &[left_key, right_key], &batch.progress);
        let input_count = u64::try_from(batch.elements.len()).unwrap_or(u64::MAX);
        handle
            .metrics
            .source_elements
            .fetch_add(input_count, Ordering::Relaxed);
        handle
            .metrics
            .peak_batch_elements
            .fetch_max(input_count, Ordering::Relaxed);
        handle.metrics.batches.fetch_add(1, Ordering::Relaxed);
        let mut left_session = None;
        let mut right_session = None;
        let mut written = 0_u64;
        let result = (|| {
            for input in batch.elements {
                let (left_events, right_events) =
                    multi_output::process_event(input, plan, registry, config, &handle.metrics)?;
                if left_events.len().saturating_add(right_events.len())
                    > config.buffer_budget_elements
                {
                    return Err(Error::resource(
                        "job",
                        "stream branch output exceeds buffer budget",
                    ));
                }
                written = written.saturating_add(write_stream_branch(
                    registry,
                    left_key,
                    &instruction_id,
                    left_events,
                    &mut left_session,
                    config,
                )?);
                written = written.saturating_add(write_stream_branch(
                    registry,
                    right_key,
                    &instruction_id,
                    right_events,
                    &mut right_session,
                    config,
                )?);
            }
            if config.cancellation.load(Ordering::Acquire) {
                return Err(cancelled());
            }
            commit_stream_branch(left_key, &mut left_session)?;
            commit_stream_branch(right_key, &mut right_session)?;
            Ok(())
        })();
        if let Err(error) = result {
            if let Some(session) = &mut left_session {
                let _ = catch_unwind(AssertUnwindSafe(|| session.abort()));
            }
            if let Some(session) = &mut right_session {
                let _ = catch_unwind(AssertUnwindSafe(|| session.abort()));
            }
            return Err(error);
        }
        let next_progress = merge_progress(progress.as_ref(), batch.progress);
        if let Some(checkpoint) = &checkpoint
            && (next_progress.checkpoint_position.is_some() || next_progress.completed)
        {
            checkpoint.save(&next_progress)?;
        }
        let total = handle
            .metrics
            .output_elements
            .fetch_add(written, Ordering::Relaxed)
            .saturating_add(written);
        handle.processed.store(total, Ordering::Relaxed);
        *shared_progress
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(next_progress.clone());
        if next_progress.completed {
            return Ok(());
        }
        progress = Some(next_progress);
    }
}

#[cfg(test)]
#[path = "../tests/unit/stream__state_codec_tests.rs"]
mod state_codec_tests;

#[cfg(test)]
#[path = "../tests/unit/stream__sink_open_tests.rs"]
mod sink_open_tests;
