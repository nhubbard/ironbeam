//! Cross-input keyed session closure for two resumable streams.

use super::{BoundedSideStreamRoute, Envelope, Error, Registry, WindowRows};
use std::collections::BTreeMap;

/// Return the end of the active keyed session formed by an incoming row.
/// A row's assigned interval may be past the watermark while its merged
/// interval is still open. Only rows with the same encoded key can extend it.
pub(super) fn resolved_group_session_end(
    registry: &Registry,
    input_key: &str,
    group_key: &str,
    windows: &WindowRows,
    incoming: &Envelope,
) -> Result<i64, Error> {
    let candidate = incoming
        .assigned_window
        .ok_or_else(|| Error::incompatible("session input lacks an interval"))?;
    let encoded = registry.encode_element(input_key, incoming.value.as_ref())?;
    let key = registry.split_group_input(group_key, &encoded)?.0;
    let mut intervals = vec![((candidate.start_ns, candidate.end_ns), true)];
    for (&interval, events) in windows {
        for event in events {
            let encoded = registry.encode_element(input_key, event.value.as_ref())?;
            if registry.split_group_input(group_key, &encoded)?.0 == key {
                intervals.push((interval, false));
                break;
            }
        }
    }
    Ok(session_component_end(intervals))
}

pub(super) fn resolved_dual_session_end(
    registry: &Registry,
    route: &BoundedSideStreamRoute,
    main: &WindowRows,
    side: &WindowRows,
    incoming_input: usize,
    incoming: &Envelope,
) -> Result<i64, Error> {
    let candidate = incoming
        .assigned_window
        .ok_or_else(|| Error::incompatible("session input lacks an interval"))?;
    let key = dual_session_key(registry, route, incoming_input, incoming)?;
    let mut intervals = vec![((candidate.start_ns, candidate.end_ns), true)];
    for (input, windows) in [(0, main), (1, side)] {
        for (&interval, events) in windows {
            for event in events {
                if dual_session_key(registry, route, input, event)? == key {
                    intervals.push((interval, false));
                    break;
                }
            }
        }
    }
    Ok(session_component_end(intervals))
}

fn dual_session_key(
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
        Ok(registry
            .split_join_input(&route.aggregate_key, input, &encoded, event.timestamp_ns)?
            .0)
    } else {
        Ok(registry
            .split_cogroup_input(&route.aggregate_key, input, &encoded)?
            .0)
    }
}

fn session_component_end(mut intervals: Vec<((i64, i64), bool)>) -> i64 {
    intervals.sort_unstable_by_key(|&(interval, _)| interval);
    let mut component = intervals[0].0;
    let mut contains_candidate = intervals[0].1;
    for (interval, is_candidate) in intervals.into_iter().skip(1) {
        if interval.0 < component.1 {
            component.1 = component.1.max(interval.1);
            contains_candidate |= is_candidate;
        } else {
            if contains_candidate {
                return component.1;
            }
            component = interval;
            contains_candidate = is_candidate;
        }
    }
    component.1
}

struct SessionRow {
    interval: (i64, i64),
    input: usize,
    event: Envelope,
}

pub(super) type ClosedSession = (Vec<Envelope>, Vec<Envelope>);

/// Keep original element windows in the checkpoint. The aggregate merges the
/// closed rows by typed key and interval; this pass only decides when it is safe
/// to close each connected component.
pub(super) fn close_ready_sessions(
    registry: &Registry,
    route: &BoundedSideStreamRoute,
    main: &mut WindowRows,
    side: &mut WindowRows,
    watermark: i64,
) -> Result<Vec<ClosedSession>, Error> {
    let mut keyed: BTreeMap<Vec<u8>, Vec<SessionRow>> = BTreeMap::new();
    for (input, windows) in [(0, std::mem::take(main)), (1, std::mem::take(side))] {
        for (interval, events) in windows {
            for event in events {
                let key = dual_session_key(registry, route, input, &event)?;
                keyed.entry(key).or_default().push(SessionRow {
                    interval,
                    input,
                    event,
                });
            }
        }
    }
    let mut open_main = BTreeMap::new();
    let mut open_side = BTreeMap::new();
    let mut closed = Vec::new();
    for rows in keyed.values_mut() {
        rows.sort_by_key(|row| row.interval);
        let mut component = Vec::new();
        let mut end = i64::MIN;
        for row in rows.drain(..) {
            if !component.is_empty() && row.interval.0 >= end {
                settle(
                    component,
                    end,
                    watermark,
                    &mut open_main,
                    &mut open_side,
                    &mut closed,
                );
                component = Vec::new();
            }
            end = end.max(row.interval.1);
            component.push(row);
        }
        if !component.is_empty() {
            settle(
                component,
                end,
                watermark,
                &mut open_main,
                &mut open_side,
                &mut closed,
            );
        }
    }
    *main = open_main;
    *side = open_side;
    Ok(closed)
}

fn settle(
    rows: Vec<SessionRow>,
    end: i64,
    watermark: i64,
    open_main: &mut WindowRows,
    open_side: &mut WindowRows,
    closed: &mut Vec<ClosedSession>,
) {
    if end <= watermark {
        let mut main = Vec::new();
        let mut side = Vec::new();
        for row in rows {
            if row.input == 0 {
                main.push(row.event);
            } else {
                side.push(row.event);
            }
        }
        closed.push((main, side));
    } else {
        for row in rows {
            let windows = if row.input == 0 {
                &mut *open_main
            } else {
                &mut *open_side
            };
            windows.entry(row.interval).or_default().push(row.event);
        }
    }
}

struct GroupSessionRow {
    interval: (i64, i64),
    event: Envelope,
}

/// Close connected sessions from one flattened keyed input only after the
/// minimum active source watermark has passed each merged interval.
pub(super) fn close_ready_group_sessions(
    registry: &Registry,
    input_key: &str,
    group_key: &str,
    windows: &mut WindowRows,
    watermark: i64,
) -> Result<Vec<Vec<Envelope>>, Error> {
    let mut keyed: BTreeMap<Vec<u8>, Vec<GroupSessionRow>> = BTreeMap::new();
    for (interval, events) in std::mem::take(windows) {
        for event in events {
            let encoded = registry.encode_element(input_key, event.value.as_ref())?;
            let key = registry.split_group_input(group_key, &encoded)?.0;
            keyed
                .entry(key)
                .or_default()
                .push(GroupSessionRow { interval, event });
        }
    }
    let mut open = BTreeMap::new();
    let mut closed = Vec::new();
    for rows in keyed.values_mut() {
        rows.sort_by_key(|row| row.interval);
        let mut component = Vec::new();
        let mut end = i64::MIN;
        for row in rows.drain(..) {
            if !component.is_empty() && row.interval.0 >= end {
                settle_group(component, end, watermark, &mut open, &mut closed);
                component = Vec::new();
            }
            end = end.max(row.interval.1);
            component.push(row);
        }
        if !component.is_empty() {
            settle_group(component, end, watermark, &mut open, &mut closed);
        }
    }
    *windows = open;
    Ok(closed)
}

fn settle_group(
    rows: Vec<GroupSessionRow>,
    end: i64,
    watermark: i64,
    open: &mut WindowRows,
    closed: &mut Vec<Vec<Envelope>>,
) {
    if end <= watermark {
        closed.push(rows.into_iter().map(|row| row.event).collect());
    } else {
        for row in rows {
            open.entry(row.interval).or_default().push(row.event);
        }
    }
}
