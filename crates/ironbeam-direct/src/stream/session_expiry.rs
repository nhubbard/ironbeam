//! Resolve a keyed session before the Direct reader checks expiry.

use super::{GroupedStreamRoute, WindowFolds};
use ironbeam_core::{Envelope, Error, FoldSession, Window, WindowInterval};
use std::panic::{AssertUnwindSafe, catch_unwind};

pub(super) fn resolved_group_interval(
    route: &GroupedStreamRoute,
    folds: &WindowFolds,
    event: &Envelope,
    assigned: WindowInterval,
) -> Result<WindowInterval, Error> {
    if !matches!(route.window, Window::Session { .. }) {
        return Ok(assigned);
    }
    let Some((fold, _)) = folds.get(&(i64::MIN, i64::MAX)) else {
        return Ok(assigned);
    };
    // Beam merges an incoming window with active keyed sessions first, then
    // checks expiry against the resolved interval.
    catch_unwind(AssertUnwindSafe(|| fold.resolved_interval(event)))
        .map_err(|_| Error::execution(&route.group_key, "direct", "window resolution panicked"))??
        .ok_or_else(|| Error::execution(&route.group_key, "direct", "session interval missing"))
}

pub(super) fn drain_grouped_session(
    fold: &mut dyn FoldSession,
    watermark_ns: i64,
    completed: bool,
) -> Result<(Vec<Envelope>, usize), Error> {
    // Final late panes must be emitted before expiry removes retained rows.
    let (mut panes, final_released) = fold.drain_late_at(Some(watermark_ns), true)?;
    let (mut on_time, closed_released) = fold.drain_closed(watermark_ns)?;
    panes.append(&mut on_time);
    let mut released = final_released.saturating_add(closed_released);
    if !completed {
        let (mut late, late_released) = fold.drain_late_at(Some(watermark_ns), false)?;
        panes.append(&mut late);
        released = released.saturating_add(late_released);
    }
    Ok((panes, released))
}

#[cfg(test)]
#[path = "../../tests/unit/stream__session_expiry_tests.rs"]
mod tests;
