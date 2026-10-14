//! Cross-input keyed session closure for two resumable streams.

use super::{BoundedSideStreamRoute, Envelope, Error, Registry, WindowRows};
use std::collections::BTreeMap;

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
    for (input, windows, input_key) in [
        (0, std::mem::take(main), route.main_input_key.as_str()),
        (1, std::mem::take(side), route.side_input_key.as_str()),
    ] {
        for (interval, events) in windows {
            for event in events {
                let encoded = registry.encode_element(input_key, event.value.as_ref())?;
                let key = if registry.has_join_coder(&route.aggregate_key) {
                    registry
                        .split_join_input(
                            &route.aggregate_key,
                            input,
                            &encoded,
                            event.timestamp_ns,
                        )?
                        .0
                } else {
                    registry
                        .split_cogroup_input(&route.aggregate_key, input, &encoded)?
                        .0
                };
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
            if !component.is_empty() && row.interval.0 > end {
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
