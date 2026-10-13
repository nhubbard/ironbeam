//! Beam windowing strategies for bounded Ironbeam collections.

use beam_proto::pipeline_v1::{
    accumulation_mode, closing_behavior, merge_status, on_time_behavior, output_time,
    trigger::{self, Default as DefaultTrigger},
    FixedWindowsPayload, FunctionSpec, Trigger, WindowingStrategy,
};
use prost::Message;

use crate::ids;
use crate::urns;

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

pub(crate) fn fixed_windowing_strategy(
    size_ns: i64,
    offset_ns: i64,
) -> Result<WindowingStrategy, String> {
    let mut strategy = global_windowing_strategy();
    strategy.window_fn = Some(fixed_window_fn(size_ns, offset_ns)?);
    strategy.window_coder_id = ids::CODER_INTERVAL_WINDOW.into();
    Ok(strategy)
}

#[cfg(test)]
#[path = "../tests/unit/windowing__tests.rs"]
mod tests;
