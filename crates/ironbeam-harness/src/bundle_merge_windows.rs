//! Validate a runner-issued Beam `MergeWindows` bundle for session windows.

use super::*;
use beam_proto::pipeline_v1::SessionWindowsPayload;

pub(super) fn plan_merge_windows(
    pbd: &ProcessBundleDescriptor,
    id: &str,
    transform: &PTransform,
    pc_producers: &HashMap<String, String>,
) -> Result<Step, PlanError> {
    let spec = FunctionSpec::decode(
        transform
            .spec
            .as_ref()
            .expect("categorized spec")
            .payload
            .as_slice(),
    )
    .map_err(|error| bad(id, error.to_string()))?;
    if spec.urn != urns::WINDOW_FN_SESSIONS {
        return Err(bad(id, "unsupported MergeWindows function"));
    }
    let payload = SessionWindowsPayload::decode(spec.payload.as_slice())
        .map_err(|error| bad(id, error.to_string()))?;
    let gap = payload
        .gap_size
        .ok_or_else(|| bad(id, "session gap is missing"))?;
    let gap_millis = i128::from(gap.seconds) * 1_000 + i128::from(gap.nanos) / 1_000_000;
    if !(0..1_000_000_000).contains(&gap.nanos)
        || gap.nanos % 1_000_000 != 0
        || gap_millis <= 0
        || gap_millis > i128::from(i64::MAX)
    {
        return Err(bad(id, "session gap must be a positive whole millisecond"));
    }

    let input_pc = single_input(id, transform)?;
    let output_pc = single_output(id, transform, pc_producers)?;
    let (input_outer, input_nonce, input_interval) = merge_coder(pbd, id, &input_pc, false)?;
    let (output_outer, output_nonce, output_interval) = merge_coder(pbd, id, &output_pc, true)?;
    if input_outer != output_outer
        || !coders_equivalent(pbd, &input_nonce, &output_nonce)
        || !coders_equivalent(pbd, &input_interval, &output_interval)
    {
        return Err(bad(id, "MergeWindows input and output coders differ"));
    }
    Ok(Step::MergeWindows {
        transform_id: id.to_owned(),
        input_pc,
        output_pc,
        outer_window: input_outer,
    })
}

fn bad(id: &str, err: impl Into<String>) -> PlanError {
    PlanError::BadPayload {
        id: id.to_owned(),
        err: err.into(),
    }
}

fn merge_coder(
    pbd: &ProcessBundleDescriptor,
    id: &str,
    pc: &str,
    output: bool,
) -> Result<(WindowCoder, String, String), PlanError> {
    let malformed = || {
        bad(
            id,
            format!("MergeWindows collection {pc} has the wrong coder tree"),
        )
    };
    let collection = pbd.pcollections.get(pc).ok_or_else(malformed)?;
    let coders = &pbd.coders;
    let top = coders.get(&collection.coder_id).ok_or_else(malformed)?;
    if coder_urn(top) != urns::CODER_WINDOWED_VALUE || top.component_coder_ids.len() != 2 {
        return Err(malformed());
    }
    let outer = coders
        .get(&top.component_coder_ids[1])
        .ok_or_else(malformed)?;
    let outer = map_windows::window_coder(outer).ok_or_else(malformed)?;
    let element = pair(coders, &top.component_coder_ids[0]).ok_or_else(malformed)?;
    let nonce_id = element.component_coder_ids[0].clone();
    let nonce = coders.get(&nonce_id).ok_or_else(malformed)?;
    if !matches!(
        coder_urn(nonce),
        urns::CODER_BYTES | urns::CODER_STRING_UTF8
    ) || !nonce.component_coder_ids.is_empty()
    {
        return Err(malformed());
    }
    let value_id = &element.component_coder_ids[1];
    let interval_id = if output {
        let value = pair(coders, value_id).ok_or_else(malformed)?;
        let first = iterable(coders, &value.component_coder_ids[0]).ok_or_else(malformed)?;
        let merged = iterable(coders, &value.component_coder_ids[1]).ok_or_else(malformed)?;
        let merged_pair = pair(coders, merged).ok_or_else(malformed)?;
        let consumed =
            iterable(coders, &merged_pair.component_coder_ids[1]).ok_or_else(malformed)?;
        if !coders_equivalent(pbd, first, &merged_pair.component_coder_ids[0])
            || !coders_equivalent(pbd, first, consumed)
        {
            return Err(malformed());
        }
        first.clone()
    } else {
        iterable(coders, value_id).ok_or_else(malformed)?.clone()
    };
    let interval = coders.get(&interval_id).ok_or_else(malformed)?;
    if map_windows::window_coder(interval) != Some(WindowCoder::Interval) {
        return Err(malformed());
    }
    Ok((outer, nonce_id, interval_id))
}

fn pair<'a>(coders: &'a HashMap<String, Coder>, id: &str) -> Option<&'a Coder> {
    let coder = coders.get(id)?;
    (coder_urn(coder) == urns::CODER_KV && coder.component_coder_ids.len() == 2).then_some(coder)
}

fn iterable<'a>(coders: &'a HashMap<String, Coder>, id: &str) -> Option<&'a String> {
    let coder = coders.get(id)?;
    (coder_urn(coder) == urns::CODER_ITERABLE && coder.component_coder_ids.len() == 1)
        .then(|| &coder.component_coder_ids[0])
}
