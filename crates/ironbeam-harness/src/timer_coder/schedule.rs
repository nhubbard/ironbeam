use std::collections::HashSet;

use anyhow::{anyhow, bail, Result};
use ironbeam_core::{TimerFiring, TimerIdentity, TimerSchedule, UserTimerDomain, WindowInterval};

use crate::coders::{decode_interval_window, encode_interval_window};

use super::TimerRecord;

fn encoded_window(window: Option<WindowInterval>) -> Result<Vec<u8>> {
    let Some(window) = window else {
        return Ok(Vec::new());
    };
    if window.end_ns <= window.start_ns
        || window.start_ns % 1_000_000 != 0
        || window.end_ns % 1_000_000 != 0
    {
        bail!("Beam timer window requires a valid millisecond interval");
    }
    let mut encoded = Vec::new();
    encode_interval_window(
        window.start_ns / 1_000_000,
        window.end_ns / 1_000_000,
        &mut encoded,
    )?;
    Ok(encoded)
}

impl TimerRecord {
    /// Build an outbound Beam TimerCoder record from one runner timer firing.
    pub fn from_firing(firing: &TimerFiring) -> Result<Self> {
        let schedule = firing.schedule;
        if schedule.fire_timestamp_ns % 1_000_000 != 0
            || schedule.hold_timestamp_ns % 1_000_000 != 0
        {
            bail!("Beam timer timestamps require whole milliseconds");
        }
        Ok(Self {
            encoded_key: firing.identity.encoded_key.clone(),
            dynamic_tag: firing.identity.dynamic_tag.clone(),
            encoded_windows: vec![encoded_window(firing.identity.window)?],
            clear: false,
            fire_timestamp_millis: Some(schedule.fire_timestamp_ns / 1_000_000),
            hold_timestamp_millis: Some(schedule.hold_timestamp_ns / 1_000_000),
            pane: Some(schedule.pane),
        })
    }

    /// Build a clear operation for one keyed, windowed timer slot.
    pub fn from_clear(identity: &TimerIdentity) -> Result<Self> {
        Ok(Self {
            encoded_key: identity.encoded_key.clone(),
            dynamic_tag: identity.dynamic_tag.clone(),
            encoded_windows: vec![encoded_window(identity.window)?],
            clear: true,
            fire_timestamp_millis: None,
            hold_timestamp_millis: None,
            pane: None,
        })
    }

    /// Decode an inbound timer firing, expanding a record that groups multiple
    /// windows into one callback per keyed window.
    pub fn to_firings(
        &self,
        family_id: &str,
        domain: UserTimerDomain,
        global_window: bool,
    ) -> Result<Vec<TimerFiring>> {
        if self.clear {
            bail!("runner timer input cannot be a clear operation");
        }
        if family_id.is_empty() || self.encoded_windows.is_empty() {
            bail!("runner timer input requires a family and at least one window");
        }
        let fire_timestamp_ns = self
            .fire_timestamp_millis
            .ok_or_else(|| anyhow!("runner timer input has no fire timestamp"))?
            .checked_mul(1_000_000)
            .ok_or_else(|| anyhow!("runner timer fire timestamp overflows nanoseconds"))?;
        let hold_timestamp_ns = self
            .hold_timestamp_millis
            .ok_or_else(|| anyhow!("runner timer input has no hold timestamp"))?
            .checked_mul(1_000_000)
            .ok_or_else(|| anyhow!("runner timer hold timestamp overflows nanoseconds"))?;
        let pane = self
            .pane
            .ok_or_else(|| anyhow!("runner timer input has no pane"))?;
        let firings = self
            .encoded_windows
            .iter()
            .map(|encoded| {
                let window = if global_window {
                    if !encoded.is_empty() {
                        bail!("global timer has interval window bytes");
                    }
                    None
                } else {
                    let ((start, end), used) = decode_interval_window(encoded)?;
                    if used != encoded.len() {
                        bail!("timer window coder has trailing bytes");
                    }
                    let start_ns = start
                        .checked_mul(1_000_000)
                        .ok_or_else(|| anyhow!("timer window start overflows nanoseconds"))?;
                    let end_ns = end
                        .checked_mul(1_000_000)
                        .ok_or_else(|| anyhow!("timer window end overflows nanoseconds"))?;
                    if end_ns <= start_ns {
                        bail!("timer interval window is empty or reversed");
                    }
                    Some(WindowInterval { start_ns, end_ns })
                };
                Ok(TimerFiring {
                    identity: TimerIdentity {
                        family_id: family_id.into(),
                        dynamic_tag: self.dynamic_tag.clone(),
                        encoded_key: self.encoded_key.clone(),
                        window,
                    },
                    schedule: TimerSchedule {
                        fire_timestamp_ns,
                        hold_timestamp_ns,
                        pane,
                    },
                    domain,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let mut windows = HashSet::new();
        if firings
            .iter()
            .any(|firing| !windows.insert(firing.identity.window))
        {
            bail!("runner timer input repeats a window");
        }
        Ok(firings)
    }
}
