//! Validate a runner-issued Beam `MapWindows` bundle against its coder tree.

use super::*;
use beam_proto::pipeline_v1::{FixedWindowsPayload, SlidingWindowsPayload};

pub(super) fn plan_map_windows(
    pbd: &ProcessBundleDescriptor,
    id: &str,
    transform: &PTransform,
    pc_producers: &HashMap<String, String>,
) -> Result<Step, PlanError> {
    let mapping_spec = FunctionSpec::decode(
        transform
            .spec
            .as_ref()
            .expect("categorized spec")
            .payload
            .as_slice(),
    )
    .map_err(|error| PlanError::BadPayload {
        id: id.to_string(),
        err: error.to_string(),
    })?;
    let mapping = match mapping_spec.urn.as_str() {
        urns::WINDOW_MAPPING_GLOBAL => WindowMapping::Global,
        urns::WINDOW_MAPPING_IDENTITY => WindowMapping::Identity,
        urns::WINDOW_MAPPING_FIXED => {
            let payload =
                FixedWindowsPayload::decode(mapping_spec.payload.as_slice()).map_err(|error| {
                    PlanError::BadPayload {
                        id: id.to_string(),
                        err: error.to_string(),
                    }
                })?;
            let size_millis =
                duration_millis(payload.size.as_ref()).map_err(|err| PlanError::BadPayload {
                    id: id.to_string(),
                    err,
                })?;
            let offset_millis =
                timestamp_millis(payload.offset.as_ref()).map_err(|err| PlanError::BadPayload {
                    id: id.to_string(),
                    err,
                })?;
            if size_millis <= 0 {
                return Err(PlanError::BadPayload {
                    id: id.to_string(),
                    err: "fixed mapping size must be positive".into(),
                });
            }
            WindowMapping::Fixed {
                size_millis,
                offset_millis: offset_millis.rem_euclid(size_millis),
            }
        }
        urns::WINDOW_MAPPING_SLIDING => {
            let payload = SlidingWindowsPayload::decode(mapping_spec.payload.as_slice()).map_err(
                |error| PlanError::BadPayload {
                    id: id.to_string(),
                    err: error.to_string(),
                },
            )?;
            let size_millis =
                duration_millis(payload.size.as_ref()).map_err(|err| PlanError::BadPayload {
                    id: id.to_string(),
                    err,
                })?;
            let period_millis =
                duration_millis(payload.period.as_ref()).map_err(|err| PlanError::BadPayload {
                    id: id.to_string(),
                    err,
                })?;
            let offset_millis =
                timestamp_millis(payload.offset.as_ref()).map_err(|err| PlanError::BadPayload {
                    id: id.to_string(),
                    err,
                })?;
            if size_millis <= 0 || period_millis <= 0 {
                return Err(PlanError::BadPayload {
                    id: id.to_string(),
                    err: "sliding mapping size and period must be positive".into(),
                });
            }
            WindowMapping::Sliding {
                size_millis,
                period_millis,
                offset_millis: offset_millis.rem_euclid(period_millis),
            }
        }
        _ => {
            return Err(PlanError::BadPayload {
                id: id.to_string(),
                err: "unsupported MapWindows function".into(),
            });
        }
    };
    if matches!(mapping, WindowMapping::Global | WindowMapping::Identity)
        && !mapping_spec.payload.is_empty()
    {
        return Err(PlanError::BadPayload {
            id: id.to_string(),
            err: "MapWindows function has an unexpected payload".into(),
        });
    }
    let input_pc = single_input(id, transform)?;
    let output_pc = single_output(id, transform, pc_producers)?;
    let (outer_window, input_window, key_coder) = map_windows_coder(pbd, id, &input_pc)?;
    let (output_outer, output_window, output_key_coder) = map_windows_coder(pbd, id, &output_pc)?;
    if outer_window != output_outer
        || !coders_equivalent(pbd, &key_coder, &output_key_coder)
        || (mapping == WindowMapping::Identity && input_window != output_window)
        || (mapping == WindowMapping::Global && output_window != WindowCoder::Global)
        || (matches!(
            mapping,
            WindowMapping::Fixed { .. } | WindowMapping::Sliding { .. }
        ) && (input_window != WindowCoder::Interval || output_window != WindowCoder::Interval))
    {
        return Err(PlanError::BadPayload {
            id: id.to_string(),
            err: "MapWindows input and output coders differ from the mapping".into(),
        });
    }
    Ok(Step::MapWindows {
        transform_id: id.to_string(),
        input_pc,
        output_pc,
        mapping,
        outer_window,
        input_window,
    })
}

fn duration_millis(value: Option<&prost_types::Duration>) -> Result<i64, String> {
    let value = value.ok_or("window mapping is missing a duration")?;
    whole_millis(value.seconds, value.nanos)
}

fn timestamp_millis(value: Option<&prost_types::Timestamp>) -> Result<i64, String> {
    let value = value.ok_or("window mapping is missing an offset")?;
    whole_millis(value.seconds, value.nanos)
}

fn whole_millis(seconds: i64, nanos: i32) -> Result<i64, String> {
    if !(0..1_000_000_000).contains(&nanos) || nanos % 1_000_000 != 0 {
        return Err("window mapping requires whole-millisecond values".into());
    }
    let total = i128::from(seconds) * 1000 + i128::from(nanos / 1_000_000);
    i64::try_from(total).map_err(|_| "window mapping value exceeds i64 milliseconds".into())
}

fn map_windows_coder(
    pbd: &ProcessBundleDescriptor,
    id: &str,
    pc: &str,
) -> Result<(WindowCoder, WindowCoder, String), PlanError> {
    let bad = || PlanError::BadPayload {
        id: id.to_string(),
        err: format!("MapWindows collection {pc} needs windowed KV<bytes, window> coding"),
    };
    let coders = &pbd.coders;
    let collection = pbd.pcollections.get(pc).ok_or_else(bad)?;
    let top = coders.get(&collection.coder_id).ok_or_else(bad)?;
    if coder_urn(top) != urns::CODER_WINDOWED_VALUE || top.component_coder_ids.len() != 2 {
        return Err(bad());
    }
    let outer = coders.get(&top.component_coder_ids[1]).ok_or_else(bad)?;
    let outer = window_coder(outer).ok_or_else(bad)?;
    let element = coders.get(&top.component_coder_ids[0]).ok_or_else(bad)?;
    if coder_urn(element) != urns::CODER_KV || element.component_coder_ids.len() != 2 {
        return Err(bad());
    }
    let key_id = element.component_coder_ids[0].clone();
    let key = coders.get(&key_id).ok_or_else(bad)?;
    if !matches!(
        coder_urn(key),
        urns::CODER_BYTES | urns::CODER_STRING_UTF8 | urns::CODER_LENGTH_PREFIX
    ) {
        return Err(bad());
    }
    if (coder_urn(key) == urns::CODER_LENGTH_PREFIX && key.component_coder_ids.len() != 1)
        || (coder_urn(key) != urns::CODER_LENGTH_PREFIX && !key.component_coder_ids.is_empty())
    {
        return Err(bad());
    }
    let value = coders
        .get(&element.component_coder_ids[1])
        .ok_or_else(bad)?;
    let value = window_coder(value).ok_or_else(bad)?;
    Ok((outer, value, key_id))
}

fn window_coder(coder: &Coder) -> Option<WindowCoder> {
    if !coder.component_coder_ids.is_empty() {
        return None;
    }
    match coder_urn(coder) {
        urns::CODER_GLOBAL_WINDOW => Some(WindowCoder::Global),
        urns::CODER_INTERVAL_WINDOW => Some(WindowCoder::Interval),
        _ => None,
    }
}

/// Runner ports may use a fresh coder ID for the same wire shape. Compare the
/// referenced definitions, including payloads and child coders, rather than
/// treating local IDs as wire identities.
pub(super) fn coders_equivalent(pbd: &ProcessBundleDescriptor, left: &str, right: &str) -> bool {
    fn compare(
        coders: &HashMap<String, Coder>,
        left: &str,
        right: &str,
        visiting: &mut HashSet<(String, String)>,
        depth: usize,
    ) -> bool {
        if depth > 64 {
            return false;
        }
        let (Some(a), Some(b)) = (coders.get(left), coders.get(right)) else {
            return false;
        };
        let pair = (left.to_owned(), right.to_owned());
        if !visiting.insert(pair.clone()) {
            return false;
        }
        let same = a.spec == b.spec
            && a.component_coder_ids.len() == b.component_coder_ids.len()
            && a.component_coder_ids
                .iter()
                .zip(&b.component_coder_ids)
                .all(|(child_a, child_b)| compare(coders, child_a, child_b, visiting, depth + 1));
        visiting.remove(&pair);
        same
    }
    compare(&pbd.coders, left, right, &mut HashSet::new(), 0)
}
