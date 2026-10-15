/// Execute Beam's runner-requested side-input window mapping primitive.
/// The key is an opaque nested bytes or UTF-8 coder value; only the encoded
/// window value changes. The enclosing element window and pane stay intact.
impl V5BundleExecutor {
    async fn run_map_windows(
        &self,
        instruction_id: &str,
        plan: &BundlePlan,
    ) -> Result<ProcessBundleResponse> {
        let [
            Step::Source {
                transform_id: source_id,
                port: source_port,
                output_pc: source_pc,
                ..
            },
            Step::MapWindows {
                input_pc,
                output_pc,
                mapping,
                outer_window,
                input_window,
                ..
            },
            Step::Sink {
                transform_id: sink_id,
                port: sink_port,
                input_pc: sink_pc,
                ..
            },
        ] = plan.steps.as_slice()
        else {
            bail!("MapWindows requires one runner source, mapping, and runner sink");
        };
        if source_pc != input_pc || output_pc != sink_pc {
            bail!("MapWindows runner collections are disconnected");
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
                let Some((key_len, body_len)) =
                    try_map_window_body_len(windowed.element, *input_window)?
                else {
                    break;
                };
                let body = &windowed.element[..body_len];
                let mapped = match mapping {
                    WindowMapping::Global => &body[..key_len],
                    WindowMapping::Identity => body,
                };
                let mut encoded = Vec::new();
                match windowed.interval_millis {
                    Some((start, end)) => encode_interval_windowed_value_with_metadata(
                        windowed.timestamp_millis,
                        start,
                        end,
                        windowed.pane,
                        windowed.element_metadata,
                        mapped,
                        &mut encoded,
                    )?,
                    None => encode_windowed_value_with_metadata(
                        windowed.timestamp_millis,
                        windowed.pane,
                        windowed.element_metadata,
                        mapped,
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
            if pending.len() > MAX_MAP_WINDOWS_RECORD_BYTES {
                bail!("MapWindows has an oversized incomplete runner record");
            }
        }
        if !pending.is_empty() {
            bail!("MapWindows ended with a truncated runner record");
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

fn try_map_window_body_len(
    bytes: &[u8],
    value_window: WindowCoder,
) -> Result<Option<(usize, usize)>> {
    let Some(key_len) = try_map_key_len(bytes)? else {
        return Ok(None);
    };
    if value_window == WindowCoder::Global {
        return Ok(Some((key_len, key_len)));
    }
    let window = &bytes[key_len..];
    if window.len() < 8 {
        return Ok(None);
    }
    for index in 8..18 {
        let Some(&byte) = window.get(index) else {
            return Ok(None);
        };
        if index == 17 && byte > 1 {
            bail!("MapWindows interval duration exceeds 64 bits");
        }
        if byte & 0x80 == 0 {
            let end = index + 1;
            let (_, used) = decode_interval_window(&window[..end])?;
            if used != end {
                bail!("MapWindows interval window length differs from its coder");
            }
            return Ok(Some((key_len, key_len + end)));
        }
    }
    bail!("MapWindows interval duration varint is too long")
}

const MAX_MAP_WINDOWS_RECORD_BYTES: usize = 2 * 1024 * 1024;

fn try_map_key_len(bytes: &[u8]) -> Result<Option<usize>> {
    let mut length = 0_u64;
    for index in 0..10 {
        let Some(&byte) = bytes.get(index) else {
            return Ok(None);
        };
        if index == 9 && byte > 1 {
            bail!("MapWindows key length exceeds 64 bits");
        }
        length |= u64::from(byte & 0x7f) << (7 * index);
        if byte & 0x80 == 0 {
            let length = usize::try_from(length).context("MapWindows key length exceeds usize")?;
            let end = (index + 1).checked_add(length).context("MapWindows key length overflow")?;
            if end > MAX_MAP_WINDOWS_RECORD_BYTES {
                bail!("MapWindows key exceeds the 2 MiB record limit");
            }
            return Ok((bytes.len() >= end).then_some(end));
        }
    }
    bail!("MapWindows key length varint is too long")
}

#[cfg(test)]
#[path = "../../tests/unit/v5_executor__map_windows_tests.rs"]
mod map_windows_tests;
