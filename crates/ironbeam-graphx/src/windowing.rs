//! Beam windowing strategies for Ironbeam collections.

use beam_proto::pipeline_v1::{
    accumulation_mode, closing_behavior, merge_status, on_time_behavior, output_time,
    trigger::{self, Default as DefaultTrigger},
    FixedWindowsPayload, FunctionSpec, SessionWindowsPayload, SlidingWindowsPayload, Trigger,
    WindowingStrategy,
};
use prost::Message;

use crate::ids;
use crate::urns;
use ironbeam_core::WindowTrigger;

pub fn global_windowing_strategy() -> WindowingStrategy {
    WindowingStrategy {
        window_fn: Some(FunctionSpec {
            urn: urns::WINDOW_FN_GLOBAL.to_string(),
            payload: Vec::new(),
        }),
        merge_status: merge_status::Enum::NonMerging as i32,
        window_coder_id: ids::CODER_GLOBAL_WINDOW.to_string(),
        trigger: Some(Trigger {
            trigger: Some(trigger::Trigger::Default(DefaultTrigger {})),
        }),
        accumulation_mode: accumulation_mode::Enum::Discarding as i32,
        output_time: output_time::Enum::EarliestInPane as i32,
        closing_behavior: closing_behavior::Enum::EmitIfNonempty as i32,
        allowed_lateness: 0,
        on_time_behavior: on_time_behavior::Enum::FireIfNonempty as i32,
        assigns_to_one_window: true,
        environment_id: ids::ENV_ID.to_string(),
    }
}

pub(crate) fn apply_trigger(strategy: &mut WindowingStrategy, policy: WindowTrigger) {
    if policy.early_count().is_some() || policy.late_count().is_some() {
        if policy.is_accumulating() {
            strategy.accumulation_mode = accumulation_mode::Enum::Accumulating as i32;
        }
        strategy.trigger = Some(Trigger {
            trigger: Some(trigger::Trigger::AfterEndOfWindow(Box::new(
                trigger::AfterEndOfWindow {
                    early_firings: policy.early_count().map(repeated_count_trigger),
                    late_firings: policy.late_count().map(repeated_count_trigger),
                },
            ))),
        });
    }
}

fn repeated_count_trigger(count: u64) -> Box<Trigger> {
    Box::new(Trigger {
        trigger: Some(trigger::Trigger::Repeat(Box::new(trigger::Repeat {
            subtrigger: Some(Box::new(Trigger {
                trigger: Some(trigger::Trigger::ElementCount(trigger::ElementCount {
                    element_count: count as i32,
                })),
            })),
        }))),
    })
}

pub fn fixed_window_fn(size_ns: i64, offset_ns: i64) -> Result<FunctionSpec, String> {
    const NANOS_PER_MILLI: i64 = 1_000_000;
    const NANOS_PER_SECOND: i64 = 1_000_000_000;
    if size_ns <= 0 || size_ns % NANOS_PER_MILLI != 0 {
        return Err("fixed window size must be a positive whole millisecond".into());
    }
    if offset_ns % NANOS_PER_MILLI != 0 {
        return Err("fixed window offset must be a whole millisecond".into());
    }
    let offset_ns = offset_ns.rem_euclid(size_ns);
    let payload = FixedWindowsPayload {
        size: Some(prost_types::Duration {
            seconds: size_ns / NANOS_PER_SECOND,
            nanos: (size_ns % NANOS_PER_SECOND) as i32,
        }),
        offset: Some(prost_types::Timestamp {
            seconds: offset_ns / NANOS_PER_SECOND,
            nanos: (offset_ns % NANOS_PER_SECOND) as i32,
        }),
    };
    Ok(FunctionSpec {
        urn: urns::WINDOW_FN_FIXED.into(),
        payload: payload.encode_to_vec(),
    })
}

/// Encode a Beam sliding-window function. The period controls how often a
/// window starts; one element may belong to several windows.
pub fn sliding_window_fn(
    size_ns: i64,
    period_ns: i64,
    offset_ns: i64,
) -> Result<FunctionSpec, String> {
    const NANOS_PER_MILLI: i64 = 1_000_000;
    const NANOS_PER_SECOND: i64 = 1_000_000_000;
    if size_ns <= 0 || size_ns % NANOS_PER_MILLI != 0 {
        return Err("sliding window size must be a positive whole millisecond".into());
    }
    if period_ns <= 0 || period_ns % NANOS_PER_MILLI != 0 {
        return Err("sliding window period must be a positive whole millisecond".into());
    }
    if offset_ns % NANOS_PER_MILLI != 0 {
        return Err("sliding window offset must be a whole millisecond".into());
    }
    let offset_ns = offset_ns.rem_euclid(period_ns);
    let payload = SlidingWindowsPayload {
        size: Some(prost_types::Duration {
            seconds: size_ns / NANOS_PER_SECOND,
            nanos: (size_ns % NANOS_PER_SECOND) as i32,
        }),
        offset: Some(prost_types::Timestamp {
            seconds: offset_ns / NANOS_PER_SECOND,
            nanos: (offset_ns % NANOS_PER_SECOND) as i32,
        }),
        period: Some(prost_types::Duration {
            seconds: period_ns / NANOS_PER_SECOND,
            nanos: (period_ns % NANOS_PER_SECOND) as i32,
        }),
    };
    Ok(FunctionSpec {
        urn: urns::WINDOW_FN_SLIDING.into(),
        payload: payload.encode_to_vec(),
    })
}

/// Encode a Beam session-window function. Native keyed grouping performs the
/// merge; encoding the function alone does not merge intervals.
pub fn session_window_fn(gap_ns: i64) -> Result<FunctionSpec, String> {
    const NANOS_PER_MILLI: i64 = 1_000_000;
    const NANOS_PER_SECOND: i64 = 1_000_000_000;
    if gap_ns <= 0 || gap_ns % NANOS_PER_MILLI != 0 {
        return Err("session gap must be a positive whole millisecond".into());
    }
    Ok(FunctionSpec {
        urn: urns::WINDOW_FN_SESSIONS.into(),
        payload: SessionWindowsPayload {
            gap_size: Some(prost_types::Duration {
                seconds: gap_ns / NANOS_PER_SECOND,
                nanos: (gap_ns % NANOS_PER_SECOND) as i32,
            }),
        }
        .encode_to_vec(),
    })
}

pub(crate) fn fixed_windowing_strategy(
    size_ns: i64,
    offset_ns: i64,
) -> Result<WindowingStrategy, String> {
    let mut strategy = global_windowing_strategy();
    strategy.window_fn = Some(fixed_window_fn(size_ns, offset_ns)?);
    strategy.window_coder_id = ids::CODER_INTERVAL_WINDOW.into();
    Ok(strategy)
}

pub(crate) fn sliding_windowing_strategy(
    size_ns: i64,
    period_ns: i64,
    offset_ns: i64,
) -> Result<WindowingStrategy, String> {
    let mut strategy = global_windowing_strategy();
    strategy.window_fn = Some(sliding_window_fn(size_ns, period_ns, offset_ns)?);
    strategy.window_coder_id = ids::CODER_INTERVAL_WINDOW.into();
    strategy.assigns_to_one_window = size_ns <= period_ns;
    Ok(strategy)
}

pub(crate) fn session_windowing_strategy(gap_ns: i64) -> Result<WindowingStrategy, String> {
    let mut strategy = global_windowing_strategy();
    strategy.window_fn = Some(session_window_fn(gap_ns)?);
    strategy.window_coder_id = ids::CODER_INTERVAL_WINDOW.into();
    strategy.merge_status = merge_status::Enum::NeedsMerge as i32;
    Ok(strategy)
}

#[cfg(test)]
#[path = "../tests/unit/windowing__tests.rs"]
mod tests;
