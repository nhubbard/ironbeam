//! Window rows for default grouping and retained folds for late keyed panes.

use super::*;

fn catch_group_user<T>(
    key: &str,
    message: &str,
    action: impl FnOnce() -> Result<T, Error>,
) -> Result<T, Error> {
    catch_unwind(AssertUnwindSafe(action))
        .unwrap_or_else(|_| Err(Error::execution(key, "direct", message)))
}

pub(super) enum MultiWindowState {
    Rows {
        windows: WindowRows,
        buffered: usize,
    },
    Late {
        folds: WindowFolds,
        buffered: usize,
        late_windows: BTreeSet<(i64, i64)>,
        allowed_lateness_ns: i64,
    },
    Early {
        folds: WindowFolds,
        buffered: usize,
        policy: EarlyPolicy,
    },
    EarlyLate {
        folds: WindowFolds,
        buffered: usize,
        late_windows: BTreeSet<(i64, i64)>,
        allowed_lateness_ns: i64,
        policy: EarlyPolicy,
    },
}

#[derive(Clone, Copy)]
pub(super) enum EarlyPolicy {
    Count(u64),
    ProcessingTime(i64),
    CountOrProcessingTime(u64, i64),
    CountAndProcessingTime(u64, i64),
    CountThenProcessingTime(u64, i64),
}

impl MultiWindowState {
    pub(super) const fn is_processing_time(&self) -> bool {
        matches!(
            self,
            Self::Early {
                policy: EarlyPolicy::ProcessingTime(_)
                    | EarlyPolicy::CountOrProcessingTime(_, _)
                    | EarlyPolicy::CountAndProcessingTime(_, _)
                    | EarlyPolicy::CountThenProcessingTime(_, _),
                ..
            } | Self::EarlyLate {
                policy: EarlyPolicy::ProcessingTime(_)
                    | EarlyPolicy::CountOrProcessingTime(_, _)
                    | EarlyPolicy::CountAndProcessingTime(_, _)
                    | EarlyPolicy::CountThenProcessingTime(_, _),
                ..
            }
        )
    }

    pub(super) fn early(policy: EarlyPolicy) -> Self {
        Self::Early {
            folds: BTreeMap::new(),
            buffered: 0,
            policy,
        }
    }

    pub(super) fn early_late(policy: EarlyPolicy, allowed_lateness_ns: i64) -> Self {
        Self::EarlyLate {
            folds: BTreeMap::new(),
            buffered: 0,
            late_windows: BTreeSet::new(),
            allowed_lateness_ns,
            policy,
        }
    }

    pub(super) fn restore_early_late(
        registry: &Registry,
        route: &MultiGroupedStreamRoute,
        bytes: &[u8],
        version: u8,
        policy: EarlyPolicy,
        allowed_lateness_ns: i64,
        budget_elements: usize,
    ) -> Result<Self, Error> {
        if version != 4 || allowed_lateness_ns <= 0 {
            return Err(Error::incompatible(
                "multi-group early/late state version mismatch",
            ));
        }
        let (folds, buffered) =
            restore_grouped_folds(registry, &route.group_key, bytes, budget_elements, true)?;
        Ok(Self::EarlyLate {
            folds,
            buffered,
            late_windows: BTreeSet::new(),
            allowed_lateness_ns,
            policy,
        })
    }

    pub(super) fn restore_early(
        registry: &Registry,
        route: &MultiGroupedStreamRoute,
        bytes: &[u8],
        version: u8,
        policy: EarlyPolicy,
        budget_elements: usize,
    ) -> Result<Self, Error> {
        if version != 3 {
            return Err(Error::incompatible(
                "multi-group early state version mismatch",
            ));
        }
        let (folds, buffered) =
            restore_grouped_folds(registry, &route.group_key, bytes, budget_elements, true)?;
        Ok(Self::Early {
            folds,
            buffered,
            policy,
        })
    }

    pub(super) fn arm_processing_time(&mut self, now_ns: i64) -> Result<(), Error> {
        if let Self::Early { folds, policy, .. } | Self::EarlyLate { folds, policy, .. } = self {
            let delay_ns = match policy {
                EarlyPolicy::ProcessingTime(delay_ns)
                | EarlyPolicy::CountOrProcessingTime(_, delay_ns)
                | EarlyPolicy::CountAndProcessingTime(_, delay_ns)
                | EarlyPolicy::CountThenProcessingTime(_, delay_ns) => *delay_ns,
                EarlyPolicy::Count(_) => return Ok(()),
            };
            for (fold, _) in folds.values_mut() {
                fold.arm_processing_time(now_ns, delay_ns)?;
            }
        }
        Ok(())
    }

    pub(super) fn next_processing_time_deadline(&self) -> Option<i64> {
        match self {
            Self::Early { folds, policy, .. } | Self::EarlyLate { folds, policy, .. }
                if !matches!(policy, EarlyPolicy::Count(_)) =>
            {
                folds
                    .values()
                    .filter_map(|(fold, _)| fold.next_processing_time_deadline())
                    .min()
            }
            _ => None,
        }
    }

    pub(super) fn drain_ready_processing_time(
        &mut self,
        route: &MultiGroupedStreamRoute,
        deadline_ns: i64,
    ) -> Result<Vec<Envelope>, Error> {
        let (folds, buffered, policy) = match self {
            Self::Early {
                folds,
                buffered,
                policy,
            }
            | Self::EarlyLate {
                folds,
                buffered,
                policy,
                ..
            } => (folds, buffered, policy),
            _ => return Ok(Vec::new()),
        };
        let mut panes = Vec::new();
        for (fold, rows) in folds.values_mut() {
            let (mut fired, released) = catch_group_user(
                &route.group_key,
                "group processing trigger panicked",
                || fold.drain_ready_processing_time(deadline_ns),
            )?;
            *rows = rows.checked_sub(released).ok_or_else(|| {
                Error::recovery(
                    &route.group_key,
                    "processing trigger released too many rows",
                )
            })?;
            *buffered = buffered
                .checked_sub(released)
                .ok_or_else(|| Error::recovery(&route.group_key, "group buffer count underflow"))?;
            panes.append(&mut fired);
        }
        if panes.is_empty() && !matches!(policy, EarlyPolicy::CountAndProcessingTime(_, _)) {
            return Err(Error::recovery(
                &route.group_key,
                "armed processing timer produced no pane",
            ));
        }
        Ok(panes)
    }

    pub(super) fn empty(allowed_lateness_ns: i64) -> Self {
        if allowed_lateness_ns > 0 {
            Self::Late {
                folds: BTreeMap::new(),
                buffered: 0,
                late_windows: BTreeSet::new(),
                allowed_lateness_ns,
            }
        } else {
            Self::Rows {
                windows: BTreeMap::new(),
                buffered: 0,
            }
        }
    }

    pub(super) fn restore(
        registry: &Registry,
        route: &MultiGroupedStreamRoute,
        bytes: &[u8],
        version: u8,
        allowed_lateness_ns: i64,
        budget_elements: usize,
    ) -> Result<Self, Error> {
        match (allowed_lateness_ns > 0, version) {
            (false, 1) => {
                let (windows, buffered) =
                    restore_window_rows(registry, &route.group_input_key, bytes, budget_elements)?;
                Ok(Self::Rows { windows, buffered })
            }
            (true, 2) => {
                let (folds, buffered) = restore_grouped_folds(
                    registry,
                    &route.group_key,
                    bytes,
                    budget_elements,
                    true,
                )?;
                Ok(Self::Late {
                    folds,
                    buffered,
                    late_windows: BTreeSet::new(),
                    allowed_lateness_ns,
                })
            }
            _ => Err(Error::incompatible(
                "multi-group window state version mismatch",
            )),
        }
    }

    pub(super) const fn version(&self) -> u8 {
        match self {
            Self::Rows { .. } => 1,
            Self::Late { .. } => 2,
            Self::Early { .. } => 3,
            Self::EarlyLate { .. } => 4,
        }
    }

    pub(super) fn is_empty(&self) -> bool {
        match self {
            Self::Rows { windows, .. } => windows.is_empty(),
            Self::Late { folds, .. }
            | Self::Early { folds, .. }
            | Self::EarlyLate { folds, .. } => folds.is_empty(),
        }
    }

    pub(super) fn encode(
        &self,
        registry: &Registry,
        route: &MultiGroupedStreamRoute,
        max_bytes: usize,
    ) -> Result<Vec<u8>, Error> {
        match self {
            Self::Rows { windows, .. } => {
                encode_window_rows(registry, &route.group_input_key, windows, max_bytes)
            }
            Self::Late { folds, .. }
            | Self::Early { folds, .. }
            | Self::EarlyLate { folds, .. } => {
                encode_grouped_folds(folds, max_bytes, &route.group_key)
            }
        }
    }

    pub(super) fn add(
        &mut self,
        registry: &Registry,
        route: &MultiGroupedStreamRoute,
        event: Envelope,
        prior_watermark: Option<i64>,
        budget_elements: usize,
    ) -> Result<Vec<Envelope>, Error> {
        let window = stream_window(&event, &route.group_key)?;
        let early_count = match self {
            Self::Early { policy, .. } | Self::EarlyLate { policy, .. } => match policy {
                EarlyPolicy::Count(count) | EarlyPolicy::CountOrProcessingTime(count, _) => {
                    Some(*count)
                }
                EarlyPolicy::ProcessingTime(_)
                | EarlyPolicy::CountAndProcessingTime(_, _)
                | EarlyPolicy::CountThenProcessingTime(_, _) => None,
            },
            Self::Rows { .. } | Self::Late { .. } => None,
        };
        let mut panes = Vec::new();
        match self {
            Self::Rows { windows, buffered } => {
                let original_expired =
                    prior_watermark.is_some_and(|watermark| window.1 <= watermark);
                let resolved_end =
                    if original_expired && matches!(route.window, Window::Session { .. }) {
                        session::resolved_group_session_end(
                            registry,
                            &route.group_input_key,
                            &route.group_key,
                            windows,
                            &event,
                        )?
                    } else {
                        window.1
                    };
                if prior_watermark.is_some_and(|watermark| resolved_end <= watermark) {
                    return Ok(panes);
                }
                if *buffered >= budget_elements {
                    return Err(Error::resource(
                        &route.group_key,
                        "stateful stream input exceeds buffer budget",
                    ));
                }
                windows.entry(window).or_default().push(event);
                *buffered += 1;
            }
            Self::Late {
                folds,
                buffered,
                late_windows,
                allowed_lateness_ns,
            }
            | Self::EarlyLate {
                folds,
                buffered,
                late_windows,
                allowed_lateness_ns,
                ..
            } => {
                let session = matches!(route.window, Window::Session { .. });
                let scope = if session {
                    super::super::session_expiry::SESSION_SCOPE
                } else {
                    window
                };
                let resolved_end = if session {
                    folds.get(&scope).map_or(Ok(window.1), |(fold, _)| {
                        catch_group_user(&route.group_key, "session resolution panicked", || {
                            fold.resolved_interval(&event)?
                                .map(|interval| interval.end_ns)
                                .ok_or_else(|| {
                                    Error::execution(
                                        &route.group_key,
                                        "direct",
                                        "session interval missing",
                                    )
                                })
                        })
                    })?
                } else {
                    window.1
                };
                if prior_watermark.is_some_and(|watermark| {
                    resolved_end.saturating_add(*allowed_lateness_ns) <= watermark
                }) {
                    return Ok(panes);
                }
                if *buffered >= budget_elements {
                    return Err(Error::resource(
                        &route.group_key,
                        "stateful stream input exceeds buffer budget",
                    ));
                }
                if !session && prior_watermark.is_some_and(|watermark| window.1 <= watermark) {
                    late_windows.insert(window);
                }
                let (fold, count) = match folds.entry(scope) {
                    std::collections::btree_map::Entry::Occupied(entry) => entry.into_mut(),
                    std::collections::btree_map::Entry::Vacant(entry) => {
                        let fold = catch_group_user(
                            &route.group_key,
                            "group fold factory panicked",
                            || registry.open_fold(&route.group_key),
                        )?;
                        entry.insert((fold, 0))
                    }
                };
                catch_group_user(&route.group_key, "group fold panicked", || fold.add(event))?;
                *count += 1;
                *buffered += 1;
                if let Some(threshold) = early_count
                    && !prior_watermark.is_some_and(|watermark| resolved_end <= watermark)
                {
                    let (mut fired, released) = catch_group_user(
                        &route.group_key,
                        "group early count trigger panicked",
                        || fold.drain_ready_count(threshold),
                    )?;
                    *count = count.checked_sub(released).ok_or_else(|| {
                        Error::recovery(&route.group_key, "early fold released too many rows")
                    })?;
                    *buffered = buffered.checked_sub(released).ok_or_else(|| {
                        Error::recovery(&route.group_key, "group buffer count underflow")
                    })?;
                    panes.append(&mut fired);
                }
            }
            Self::Early {
                folds, buffered, ..
            } => {
                if prior_watermark.is_some_and(|watermark| window.1 <= watermark) {
                    return Ok(panes);
                }
                if *buffered >= budget_elements {
                    return Err(Error::resource(
                        &route.group_key,
                        "stateful stream input exceeds buffer budget",
                    ));
                }
                let (fold, rows) = match folds.entry(window) {
                    std::collections::btree_map::Entry::Occupied(entry) => entry.into_mut(),
                    std::collections::btree_map::Entry::Vacant(entry) => {
                        let fold = catch_group_user(
                            &route.group_key,
                            "group fold factory panicked",
                            || registry.open_fold(&route.group_key),
                        )?;
                        entry.insert((fold, 0))
                    }
                };
                catch_group_user(&route.group_key, "group fold panicked", || fold.add(event))?;
                *rows += 1;
                *buffered += 1;
                if let Some(threshold) = early_count {
                    let (mut fired, released) =
                        catch_group_user(&route.group_key, "group count trigger panicked", || {
                            fold.drain_ready_count(threshold)
                        })?;
                    *rows = rows.checked_sub(released).ok_or_else(|| {
                        Error::recovery(&route.group_key, "count trigger released too many rows")
                    })?;
                    *buffered = buffered.checked_sub(released).ok_or_else(|| {
                        Error::recovery(&route.group_key, "group buffer count underflow")
                    })?;
                    panes.append(&mut fired);
                }
            }
        }
        Ok(panes)
    }

    #[allow(clippy::too_many_lines)] // Pane emission and state release share one watermark transition.
    pub(super) fn drain(
        &mut self,
        registry: &Registry,
        route: &MultiGroupedStreamRoute,
        prior_watermark: Option<i64>,
        progress: &ProgressSignal,
    ) -> Result<Vec<Envelope>, Error> {
        let early_late_policy = match &*self {
            Self::EarlyLate { policy, .. } => Some(*policy),
            _ => None,
        };
        match self {
            Self::Rows { windows, buffered } => {
                let Some(watermark) = (if progress.completed {
                    Some(i64::MAX)
                } else {
                    progress.event_time_ns
                }) else {
                    return Ok(Vec::new());
                };
                let closing = if matches!(route.window, Window::Session { .. }) {
                    session::close_ready_group_sessions(
                        registry,
                        &route.group_input_key,
                        &route.group_key,
                        windows,
                        watermark,
                    )?
                } else {
                    windows
                        .keys()
                        .copied()
                        .filter(|(_, end)| *end <= watermark)
                        .collect::<Vec<_>>()
                        .into_iter()
                        .map(|window| windows.remove(&window).expect("selected open window"))
                        .collect()
                };
                let mut panes = Vec::new();
                for events in closing {
                    *buffered = buffered.checked_sub(events.len()).ok_or_else(|| {
                        Error::recovery(&route.group_key, "group buffer count underflow")
                    })?;
                    let mut grouped =
                        catch_group_user(&route.group_key, "user aggregate panicked", || {
                            registry.apply_aggregate(&route.group_key, vec![events])
                        })?;
                    panes.append(&mut grouped);
                }
                Ok(panes)
            }
            Self::Late {
                folds,
                buffered,
                late_windows,
                allowed_lateness_ns,
            }
            | Self::EarlyLate {
                folds,
                buffered,
                late_windows,
                allowed_lateness_ns,
                ..
            } => {
                let mut panes = Vec::new();
                if let Some(
                    EarlyPolicy::Count(count)
                    | EarlyPolicy::CountOrProcessingTime(count, _)
                    | EarlyPolicy::CountAndProcessingTime(count, _)
                    | EarlyPolicy::CountThenProcessingTime(count, _),
                ) = early_late_policy
                {
                    for (&(_, end), (fold, rows)) in folds.iter_mut() {
                        if prior_watermark.is_some_and(|prior| end <= prior) {
                            continue;
                        }
                        let (mut fired, released) = catch_group_user(
                            &route.group_key,
                            "group early count trigger panicked",
                            || fold.drain_ready_count(count),
                        )?;
                        *rows = rows.checked_sub(released).ok_or_else(|| {
                            Error::recovery(&route.group_key, "early fold released too many rows")
                        })?;
                        *buffered = buffered.checked_sub(released).ok_or_else(|| {
                            Error::recovery(&route.group_key, "group buffer count underflow")
                        })?;
                        panes.append(&mut fired);
                    }
                }
                let watermark = if progress.completed {
                    Some(i64::MAX)
                } else {
                    progress.event_time_ns
                };
                if matches!(route.window, Window::Session { .. }) {
                    let Some(watermark) = watermark else {
                        return Ok(Vec::new());
                    };
                    let scope = super::super::session_expiry::SESSION_SCOPE;
                    if progress.completed {
                        let Some((mut fold, count)) = folds.remove(&scope) else {
                            return Ok(Vec::new());
                        };
                        let (panes, released) =
                            catch_group_user(&route.group_key, "session fold panicked", || {
                                super::super::session_expiry::drain_grouped_session(
                                    fold.as_mut(),
                                    watermark,
                                    true,
                                )
                            })?;
                        if released > count {
                            return Err(Error::recovery(
                                &route.group_key,
                                "session fold released too many rows",
                            ));
                        }
                        *buffered = buffered.checked_sub(count).ok_or_else(|| {
                            Error::recovery(&route.group_key, "group buffer count underflow")
                        })?;
                        return Ok(panes);
                    }
                    let Some((fold, count)) = folds.get_mut(&scope) else {
                        return Ok(Vec::new());
                    };
                    let (panes, released) =
                        catch_group_user(&route.group_key, "session fold panicked", || {
                            super::super::session_expiry::drain_grouped_session(
                                fold.as_mut(),
                                watermark,
                                false,
                            )
                        })?;
                    *count = count.checked_sub(released).ok_or_else(|| {
                        Error::recovery(&route.group_key, "session fold released too many rows")
                    })?;
                    *buffered = buffered.checked_sub(released).ok_or_else(|| {
                        Error::recovery(&route.group_key, "group buffer count underflow")
                    })?;
                    return Ok(panes);
                }
                let closing = folds
                    .keys()
                    .copied()
                    .filter(|(_, end)| {
                        prior_watermark.is_none_or(|prior| prior < *end)
                            && watermark.is_some_and(|current| *end <= current)
                    })
                    .collect::<Vec<_>>();
                for window in closing {
                    let (fold, count) = folds.get_mut(&window).expect("closing fold exists");
                    let (mut closed, released) = catch_unwind(AssertUnwindSafe(|| {
                        fold.drain_closed(watermark.expect("closing window has watermark"))
                    }))
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
                    *buffered = buffered.checked_sub(released).ok_or_else(|| {
                        Error::recovery(&route.group_key, "group buffer count underflow")
                    })?;
                    panes.append(&mut closed);
                }
                for window in std::mem::take(late_windows) {
                    if progress.completed
                        || watermark.is_some_and(|current| {
                            window.1.saturating_add(*allowed_lateness_ns) <= current
                        })
                    {
                        continue;
                    }
                    let (fold, count) = folds.get_mut(&window).expect("late fold exists");
                    let (mut late, released) =
                        catch_unwind(AssertUnwindSafe(|| fold.drain_late(false))).unwrap_or_else(
                            |_| {
                                Err(Error::execution(
                                    &route.group_key,
                                    "direct",
                                    "late group fold panicked",
                                ))
                            },
                        )?;
                    *count = count.checked_sub(released).ok_or_else(|| {
                        Error::recovery(&route.group_key, "late fold released too many rows")
                    })?;
                    *buffered = buffered.checked_sub(released).ok_or_else(|| {
                        Error::recovery(&route.group_key, "group buffer count underflow")
                    })?;
                    panes.append(&mut late);
                }
                let expiring = folds
                    .keys()
                    .copied()
                    .filter(|(_, end)| {
                        progress.completed
                            || watermark.is_some_and(|current| {
                                end.saturating_add(*allowed_lateness_ns) <= current
                            })
                    })
                    .collect::<Vec<_>>();
                for window in expiring {
                    let (mut fold, count) = folds.remove(&window).expect("expiring fold exists");
                    let (mut late, released) =
                        catch_unwind(AssertUnwindSafe(|| fold.drain_late(true))).unwrap_or_else(
                            |_| {
                                Err(Error::execution(
                                    &route.group_key,
                                    "direct",
                                    "late group fold panicked",
                                ))
                            },
                        )?;
                    if released > count {
                        return Err(Error::recovery(
                            &route.group_key,
                            "late fold released too many rows",
                        ));
                    }
                    *buffered = buffered.checked_sub(count).ok_or_else(|| {
                        Error::recovery(&route.group_key, "group buffer count underflow")
                    })?;
                    panes.append(&mut late);
                }
                Ok(panes)
            }
            Self::Early {
                folds,
                buffered,
                policy,
            } => {
                let mut panes = Vec::new();
                if let EarlyPolicy::Count(count)
                | EarlyPolicy::CountOrProcessingTime(count, _)
                | EarlyPolicy::CountAndProcessingTime(count, _)
                | EarlyPolicy::CountThenProcessingTime(count, _) = policy
                {
                    for (&(_, end), (fold, rows)) in folds.iter_mut() {
                        if prior_watermark.is_some_and(|prior| end <= prior) {
                            continue;
                        }
                        let (mut fired, released) = catch_group_user(
                            &route.group_key,
                            "group count trigger panicked",
                            || fold.drain_ready_count(*count),
                        )?;
                        *rows = rows.checked_sub(released).ok_or_else(|| {
                            Error::recovery(
                                &route.group_key,
                                "count trigger released too many rows",
                            )
                        })?;
                        *buffered = buffered.checked_sub(released).ok_or_else(|| {
                            Error::recovery(&route.group_key, "group buffer count underflow")
                        })?;
                        panes.append(&mut fired);
                    }
                }
                let watermark = if progress.completed {
                    Some(i64::MAX)
                } else {
                    progress.event_time_ns
                };
                let closing = folds
                    .keys()
                    .copied()
                    .filter(|(_, end)| watermark.is_some_and(|current| *end <= current))
                    .collect::<Vec<_>>();
                for window in closing {
                    let (fold, rows) = folds.remove(&window).expect("closing fold exists");
                    let mut closed =
                        catch_group_user(&route.group_key, "group fold panicked", || {
                            fold.finish()
                        })?;
                    *buffered = buffered.checked_sub(rows).ok_or_else(|| {
                        Error::recovery(&route.group_key, "group buffer count underflow")
                    })?;
                    panes.append(&mut closed);
                }
                Ok(panes)
            }
        }
    }
}
