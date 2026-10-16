//! Resolve a keyed session before the Direct reader checks expiry.

use super::{GroupedStreamRoute, WindowFolds};
use ironbeam_core::{Envelope, Error, Window, WindowInterval};
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
