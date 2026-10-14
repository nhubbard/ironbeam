//! Per-key pane state for two typed, independently watermarked inputs.

use super::*;
use ironbeam_core::{PaneInfo, PaneTiming};

pub(super) type PaneKey = (i64, i64, Vec<u8>);
pub(super) type ClosedPanes = BTreeMap<PaneKey, i64>;

#[derive(Serialize, Deserialize)]
pub(super) struct CodedClosedPane {
    pub start_ns: i64,
    pub end_ns: i64,
    pub key: Vec<u8>,
    pub next_index: i64,
}

pub(super) struct DualFiring {
    pub main: Vec<Envelope>,
    pub side: Vec<Envelope>,
    pub pane: Option<PaneInfo>,
}

pub(super) struct FiringClock {
    pub prior_watermark: Option<i64>,
    pub watermark: Option<i64>,
    pub lateness_ns: i64,
    pub budget_elements: usize,
}

pub(super) fn restore_closed(
    coded: Vec<CodedClosedPane>,
    watermark: Option<i64>,
    lateness_ns: i64,
    budget_elements: usize,
) -> Result<ClosedPanes, Error> {
    if coded.len() > budget_elements {
        return Err(Error::resource(
            "checkpoint",
            "retained dual-source panes exceed buffer budget",
        ));
    }
    let mut closed = ClosedPanes::new();
    for row in coded {
        if row.start_ns >= row.end_ns
            || row.next_index <= 0
            || watermark.is_some_and(|current| row.end_ns.saturating_add(lateness_ns) <= current)
            || closed
                .insert((row.start_ns, row.end_ns, row.key), row.next_index)
                .is_some()
        {
            return Err(Error::recovery(
                "checkpoint",
                "invalid retained dual-source pane",
            ));
        }
    }
    Ok(closed)
}

pub(super) fn encode_closed(closed: &ClosedPanes) -> Vec<CodedClosedPane> {
    closed
        .iter()
        .map(|((start_ns, end_ns, key), next_index)| CodedClosedPane {
            start_ns: *start_ns,
            end_ns: *end_ns,
            key: key.clone(),
            next_index: *next_index,
        })
        .collect()
}

fn encoded_key(
    registry: &Registry,
    route: &BoundedSideStreamRoute,
    input: usize,
    event: &Envelope,
) -> Result<Vec<u8>, Error> {
    let input_key = if input == 0 {
        &route.main_input_key
    } else {
        &route.side_input_key
    };
    let encoded = registry.encode_element(input_key, event.value.as_ref())?;
    if registry.has_join_coder(&route.aggregate_key) {
        registry
            .split_join_input(&route.aggregate_key, input, &encoded, event.timestamp_ns)
            .map(|(key, _)| key)
    } else {
        registry
            .split_cogroup_input(&route.aggregate_key, input, &encoded)
            .map(|(key, _)| key)
    }
}

/// Drain every ready key/window. A watermark closes the on-time pane, while
/// later input starts a new discarding pane until allowed lateness expires.
pub(super) fn drain_ready(
    registry: &Registry,
    route: &BoundedSideStreamRoute,
    main_windows: &mut WindowRows,
    side_windows: &mut WindowRows,
    closed: &mut ClosedPanes,
    clock: FiringClock,
) -> Result<Vec<DualFiring>, Error> {
    let Some(watermark) = clock.watermark else {
        return Ok(Vec::new());
    };
    let ready = main_windows
        .keys()
        .chain(side_windows.keys())
        .copied()
        .filter(|(_, end)| *end <= watermark)
        .collect::<BTreeSet<_>>();
    closed.retain(|(start, end, _), _| {
        end.saturating_add(clock.lateness_ns) > watermark || ready.contains(&(*start, *end))
    });
    let mut firings = Vec::new();
    for (start, end) in ready {
        let mut groups: BTreeMap<Vec<u8>, (Vec<Envelope>, Vec<Envelope>)> = BTreeMap::new();
        for (input, events) in [
            main_windows.remove(&(start, end)).unwrap_or_default(),
            side_windows.remove(&(start, end)).unwrap_or_default(),
        ]
        .into_iter()
        .enumerate()
        {
            for event in events {
                let key = encoded_key(registry, route, input, &event)?;
                let group = groups.entry(key).or_default();
                if input == 0 {
                    group.0.push(event);
                } else {
                    group.1.push(event);
                }
            }
        }
        for (key, (main, side)) in groups {
            let scope = (start, end, key);
            let index = closed.get(&scope).copied().unwrap_or(0);
            let timing = if index > 0 || clock.prior_watermark.is_some_and(|prior| end <= prior) {
                PaneTiming::Late
            } else {
                PaneTiming::OnTime
            };
            let final_firing = end.saturating_add(clock.lateness_ns) <= watermark;
            let pane = PaneInfo {
                is_first: index == 0,
                is_last: final_firing,
                timing,
                index,
                nonspeculative_index: index,
                contains_element_metadata: false,
            };
            if !final_firing {
                if !closed.contains_key(&scope) && closed.len() >= clock.budget_elements {
                    return Err(Error::resource(
                        &route.aggregate_key,
                        "retained dual-source panes exceed buffer budget",
                    ));
                }
                closed.insert(
                    scope,
                    index.checked_add(1).ok_or_else(|| {
                        Error::resource(&route.aggregate_key, "dual-source pane index exhausted")
                    })?,
                );
            }
            firings.push(DualFiring {
                main,
                side,
                pane: Some(pane),
            });
        }
    }
    closed.retain(|(_, end, _), _| end.saturating_add(clock.lateness_ns) > watermark);
    Ok(firings)
}
