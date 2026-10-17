/// Execute Beam's runner-requested session-window merge primitive. The nonce,
/// enclosing window, timestamp, and pane are carried through unchanged.
impl V5BundleExecutor {
    async fn run_merge_windows(
        &self,
        instruction_id: &str,
        plan: &BundlePlan,
    ) -> Result<ProcessBundleResponse> {
        let [
            Step::Source { transform_id: source_id, port: source_port, output_pc: source_pc, .. },
            Step::MergeWindows { input_pc, output_pc, outer_window, .. },
            Step::Sink { transform_id: sink_id, port: sink_port, input_pc: sink_pc, .. },
        ] = plan.steps.as_slice() else {
            bail!("MergeWindows requires one runner source, merge, and runner sink");
        };
        if source_pc != input_pc || output_pc != sink_pc {
            bail!("MergeWindows runner collections are disconnected");
        }
        let source_client = self.data_client(source_port).await?;
        let sink_client = self.data_client(sink_port).await?;
        let mut reader = source_client.open_read(instruction_id, source_id);
        let mut pending = Vec::new();
        let mut count = 0_u64;
        while let Some(chunk) = reader.recv().await {
            pending.extend_from_slice(&chunk);
            let mut consumed = 0;
            while let Some((windowed, header_len)) = try_decode_windowed_value(
                &pending[consumed..],
                *outer_window == WindowCoder::Interval,
            )? {
                let Some((body_len, nonce, windows)) =
                    try_merge_windows_body(windowed.element)?
                else {
                    break;
                };
                let body = merge_windows_body(nonce, windows)?;
                let mut encoded = Vec::new();
                match windowed.interval_millis {
                    Some((start, end)) => encode_interval_windowed_value_with_metadata(
                        windowed.timestamp_millis,
                        start,
                        end,
                        windowed.pane,
                        windowed.element_metadata,
                        &body,
                        &mut encoded,
                    )?,
                    None => encode_windowed_value_with_metadata(
                        windowed.timestamp_millis,
                        windowed.pane,
                        windowed.element_metadata,
                        &body,
                        &mut encoded,
                    )?,
                }
                sink_client.write_chunk(instruction_id, sink_id, encoded)?;
                consumed += header_len + body_len;
                count += 1;
                if consumed == pending.len() {
                    break;
                }
            }
            if consumed != 0 {
                pending.drain(..consumed);
            }
            if pending.len() > MAX_MERGE_WINDOWS_RECORD_BYTES {
                bail!("MergeWindows has an oversized incomplete runner record");
            }
        }
        if !pending.is_empty() {
            bail!("MergeWindows ended with a truncated runner record");
        }
        sink_client.close_write(instruction_id, sink_id)?;
        sink_client.flush_outbound().await?;
        Ok(ProcessBundleResponse {
            monitoring_infos: vec![
                element_count_info(source_pc, count)?,
                element_count_info(sink_pc, count)?,
            ],
            ..Default::default()
        })
    }
}

const MAX_MERGE_WINDOWS_RECORD_BYTES: usize = 2 * 1024 * 1024;
const MAX_MERGE_WINDOWS: usize = 100_000;
type MillisWindow = (i64, i64);
type MergeRequest<'a> = (usize, &'a [u8], Vec<MillisWindow>);
type MergedWindow = (MillisWindow, Vec<MillisWindow>);

/// Parse KV<nonce, iterable<IntervalWindow>> without consuming a partial
/// runner record. Beam permits both known-length and chunked iterable coders.
fn try_merge_windows_body(bytes: &[u8]) -> Result<Option<MergeRequest<'_>>> {
    let Some(nonce_len) = try_map_key_len(bytes)? else { return Ok(None); };
    let Some(count_bytes) = bytes.get(nonce_len..nonce_len + 4) else { return Ok(None); };
    let count = i32::from_be_bytes(count_bytes.try_into().expect("four bytes"));
    let mut offset = nonce_len + 4;
    let mut windows = Vec::new();
    if count >= 0 {
        let count = usize::try_from(count).expect("nonnegative i32 fits usize");
        if count > MAX_MERGE_WINDOWS {
            bail!("MergeWindows iterable contains too many windows");
        }
        for _ in 0..count {
            let Some((window, used)) = try_merge_interval(&bytes[offset..])? else {
                return Ok(None);
            };
            windows.push(window);
            offset += used;
        }
    } else if count == -1 {
        loop {
            let Some((chunk, used)) = try_merge_varint(&bytes[offset..])? else {
                return Ok(None);
            };
            offset += used;
            if chunk == 0 { break; }
            let chunk = usize::try_from(chunk).context("MergeWindows chunk count exceeds usize")?;
            if chunk > MAX_MERGE_WINDOWS - windows.len() {
                bail!("MergeWindows iterable contains too many windows");
            }
            for _ in 0..chunk {
                let Some((window, used)) = try_merge_interval(&bytes[offset..])? else {
                    return Ok(None);
                };
                windows.push(window);
                offset += used;
            }
        }
    } else {
        bail!("MergeWindows iterable has an invalid negative length");
    }
    Ok(Some((offset, &bytes[..nonce_len], windows)))
}

fn try_merge_varint(bytes: &[u8]) -> Result<Option<(u64, usize)>> {
    let mut value = 0_u64;
    for index in 0..10 {
        let Some(&byte) = bytes.get(index) else { return Ok(None); };
        if index == 9 && byte > 1 {
            bail!("MergeWindows chunk length exceeds 64 bits");
        }
        value |= u64::from(byte & 0x7f) << (7 * index);
        if byte & 0x80 == 0 { return Ok(Some((value, index + 1))); }
    }
    bail!("MergeWindows chunk length varint is too long")
}

fn try_merge_interval(bytes: &[u8]) -> Result<Option<((i64, i64), usize)>> {
    if bytes.len() < 8 { return Ok(None); }
    for index in 8..18 {
        let Some(&byte) = bytes.get(index) else { return Ok(None); };
        if index == 17 && byte > 1 {
            bail!("MergeWindows interval duration exceeds 64 bits");
        }
        if byte & 0x80 == 0 {
            let used = index + 1;
            let (window, read) = decode_interval_window(&bytes[..used])?;
            if read != used || window.0 >= window.1 {
                bail!("MergeWindows interval is invalid");
            }
            return Ok(Some((window, used)));
        }
    }
    bail!("MergeWindows interval duration varint is too long")
}

fn merge_windows_body(nonce: &[u8], mut windows: Vec<(i64, i64)>) -> Result<Vec<u8>> {
    windows.sort_unstable();
    windows.dedup();
    let mut unmerged = Vec::new();
    let mut merged = Vec::<MergedWindow>::new();
    let mut component = Vec::<(i64, i64)>::new();
    let mut union = (0, 0);
    for window in windows {
        if !component.is_empty() && window.0 >= union.1 {
            settle_merge_component(&mut component, union, &mut unmerged, &mut merged);
        }
        if component.is_empty() {
            union = window;
        } else {
            union.1 = union.1.max(window.1);
        }
        component.push(window);
    }
    if !component.is_empty() {
        settle_merge_component(&mut component, union, &mut unmerged, &mut merged);
    }
    let mut out = Vec::new();
    out.extend_from_slice(nonce);
    encode_merge_iterable(&unmerged, &mut out)?;
    out.extend_from_slice(&i32::try_from(merged.len()).context("too many merged windows")?.to_be_bytes());
    for (result, consumed) in merged {
        encode_interval_window(result.0, result.1, &mut out)?;
        encode_merge_iterable(&consumed, &mut out)?;
    }
    Ok(out)
}

fn settle_merge_component(
    component: &mut Vec<(i64, i64)>,
    union: (i64, i64),
    unmerged: &mut Vec<(i64, i64)>,
    merged: &mut Vec<MergedWindow>,
) {
    if component.len() == 1 {
        unmerged.push(component[0]);
        component.clear();
    } else {
        merged.push((union, std::mem::take(component)));
    }
}

fn encode_merge_iterable(windows: &[(i64, i64)], out: &mut Vec<u8>) -> Result<()> {
    out.extend_from_slice(&i32::try_from(windows.len()).context("too many windows")?.to_be_bytes());
    for &(start, end) in windows {
        encode_interval_window(start, end, out)?;
    }
    Ok(())
}

#[cfg(test)]
#[path = "../../tests/unit/v5_executor__merge_windows_tests.rs"]
mod merge_windows_tests;
