//! Validate a runner-issued Beam `MapWindows` bundle against its coder tree.

use super::*;

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
        _ => {
            return Err(PlanError::BadPayload {
                id: id.to_string(),
                err: "unsupported MapWindows function".into(),
            });
        }
    };
    if !mapping_spec.payload.is_empty() {
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
        || key_coder != output_key_coder
        || (mapping == WindowMapping::Identity && input_window != output_window)
        || (mapping == WindowMapping::Global && output_window != WindowCoder::Global)
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
    let value = coders
        .get(&element.component_coder_ids[1])
        .ok_or_else(bad)?;
    let value = window_coder(value).ok_or_else(bad)?;
    Ok((outer, value, key_id))
}

fn window_coder(coder: &Coder) -> Option<WindowCoder> {
    match coder_urn(coder) {
        urns::CODER_GLOBAL_WINDOW => Some(WindowCoder::Global),
        urns::CODER_INTERVAL_WINDOW => Some(WindowCoder::Interval),
        _ => None,
    }
}
