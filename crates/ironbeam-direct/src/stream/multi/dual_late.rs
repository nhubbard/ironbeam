//! Per-key pane state for two typed, independently watermarked inputs.

use super::*;
use ironbeam_core::{PaneInfo, PaneTiming};

pub(super) type PaneKey = (i64, i64, Vec<u8>);
pub(super) type ClosedPanes = BTreeMap<PaneKey, ClosedPane>;

#[derive(Clone, Copy)]
pub(super) struct ClosedPane {
    pub next_index: i64,
    pub fired_value_count: usize,
}

#[derive(Serialize, Deserialize)]
pub(super) struct CodedClosedPane {
    pub start_ns: i64,
    pub end_ns: i64,
    pub key: Vec<u8>,
    pub next_index: i64,
    #[serde(default)]
    pub fired_value_count: usize,
}

pub(super) struct DualFiring {
    pub main: Vec<Envelope>,
    pub side: Vec<Envelope>,
    pub pane: Option<PaneInfo>,
    pub retained: bool,
}

pub(super) struct DualDrain {
    pub firings: Vec<DualFiring>,
    pub dropped: usize,
}

#[derive(Clone, Copy)]
pub(super) struct FiringClock {
    pub prior_watermark: Option<i64>,
    pub watermark: Option<i64>,
    pub lateness_ns: i64,
    pub budget_elements: usize,
    pub late_count: Option<u64>,
    pub accumulating: bool,
}

pub(super) fn restore_closed(
    coded: Vec<CodedClosedPane>,
    watermark: Option<i64>,
    lateness_ns: i64,
    budget_elements: usize,
    accumulating: bool,
) -> Result<ClosedPanes, Error> {
    if coded.len() > budget_elements {
        return Err(Error::resource(
            "checkpoint",
            "retained dual-source panes exceed buffer budget",
        ));
    }
    let mut closed = ClosedPanes::new();
    for row in coded {
        let invalid_count = if accumulating {
            row.fired_value_count == 0
        } else {
            row.fired_value_count != 0
        };
        if row.start_ns >= row.end_ns
            || row.next_index <= 0
            || invalid_count
            || watermark.is_some_and(|current| row.end_ns.saturating_add(lateness_ns) <= current)
            || closed
                .insert(
                    (row.start_ns, row.end_ns, row.key),
                    ClosedPane {
                        next_index: row.next_index,
                        fired_value_count: row.fired_value_count,
                    },
                )
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
        .map(|((start_ns, end_ns, key), state)| CodedClosedPane {
            start_ns: *start_ns,
            end_ns: *end_ns,
            key: key.clone(),
            next_index: state.next_index,
            fired_value_count: state.fired_value_count,
        })
        .collect()
}

pub(super) fn validate_retained_counts(
    registry: &Registry,
    route: &BoundedSideStreamRoute,
    main_windows: &WindowRows,
    side_windows: &WindowRows,
    closed: &ClosedPanes,
) -> Result<(), Error> {
    let mut counts = BTreeMap::<PaneKey, usize>::new();
    for (input, windows) in [main_windows, side_windows].into_iter().enumerate() {
        for (&(start, end), events) in windows {
            for event in events {
                let key = encoded_key(registry, route, input, event)?;
                *counts.entry((start, end, key)).or_default() += 1;
            }
        }
    }
    for (scope, state) in closed {
        if state.fired_value_count == 0
            || state.fired_value_count > counts.get(scope).copied().unwrap_or(0)
        {
            return Err(Error::recovery(
                &route.aggregate_key,
                "invalid retained dual-source pane count",
            ));
        }
    }
    Ok(())
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

/// Drain every ready key/window. A watermark closes the on-time pane; later
/// input can fire either a discarding or accumulating pane before expiry.
#[expect(
    clippy::too_many_lines,
    reason = "pane retention, firing, and final release share one watermark transition"
)]
pub(super) fn drain_ready(
    registry: &Registry,
    route: &BoundedSideStreamRoute,
    main_windows: &mut WindowRows,
    side_windows: &mut WindowRows,
    closed: &mut ClosedPanes,
    clock: FiringClock,
) -> Result<DualDrain, Error> {
    let Some(watermark) = clock.watermark else {
        return Ok(DualDrain {
            firings: Vec::new(),
            dropped: 0,
        });
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
    let mut dropped = 0_usize;
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
            let prior = closed.get(&scope).copied();
            let index = prior.map_or(0, |state| state.next_index);
            let timing = if index > 0 || clock.prior_watermark.is_some_and(|prior| end <= prior) {
                PaneTiming::Late
            } else {
                PaneTiming::OnTime
            };
            let final_firing = end.saturating_add(clock.lateness_ns) <= watermark;
            let total = main.len().saturating_add(side.len());
            let fired_count = prior.map_or(0, |state| state.fired_value_count);
            let new_count = total.checked_sub(fired_count).ok_or_else(|| {
                Error::recovery(
                    &route.aggregate_key,
                    format!(
                        "retained dual-source pane count {fired_count} exceeds its {total} rows"
                    ),
                )
            })?;
            if timing == PaneTiming::Late && new_count == 0 {
                if final_firing {
                    dropped = dropped.saturating_add(total);
                } else {
                    if !main.is_empty() {
                        main_windows.entry((start, end)).or_default().extend(main);
                    }
                    if !side.is_empty() {
                        side_windows.entry((start, end)).or_default().extend(side);
                    }
                }
                continue;
            }
            if timing == PaneTiming::Late
                && !final_firing
                && clock
                    .late_count
                    .is_some_and(|count| u64::try_from(new_count).unwrap_or(u64::MAX) < count)
            {
                if !main.is_empty() {
                    main_windows.entry((start, end)).or_default().extend(main);
                }
                if !side.is_empty() {
                    side_windows.entry((start, end)).or_default().extend(side);
                }
                continue;
            }
            let pane = PaneInfo {
                is_first: index == 0,
                is_last: final_firing,
                timing,
                index,
                nonspeculative_index: index,
                contains_element_metadata: false,
            };
            let retained = clock.accumulating && !final_firing;
            let (output_main, output_side) = if retained {
                let output_main = copy_window_events(registry, &route.main_input_key, &main)?;
                let output_side = copy_window_events(registry, &route.side_input_key, &side)?;
                if !main.is_empty() {
                    main_windows.entry((start, end)).or_default().extend(main);
                }
                if !side.is_empty() {
                    side_windows.entry((start, end)).or_default().extend(side);
                }
                (output_main, output_side)
            } else {
                (main, side)
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
                    ClosedPane {
                        next_index: index.checked_add(1).ok_or_else(|| {
                            Error::resource(
                                &route.aggregate_key,
                                "dual-source pane index exhausted",
                            )
                        })?,
                        fired_value_count: if retained { total } else { 0 },
                    },
                );
            }
            firings.push(DualFiring {
                main: output_main,
                side: output_side,
                pane: Some(pane),
                retained,
            });
        }
    }
    closed.retain(|(_, end, _), _| end.saturating_add(clock.lateness_ns) > watermark);
    Ok(DualDrain { firings, dropped })
}
