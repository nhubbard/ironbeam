fn decode_keyed_boundary_record(
    bytes: &[u8],
    registry: &Registry,
    group_key: &str,
    interval_window: bool,
) -> Result<Option<(WireValue, usize)>> {
    let Some((windowed, header_len)) = try_decode_windowed_value(bytes, interval_window)? else {
        return Ok(None);
    };
    let Some((key, key_len)) = decode_length_prefix_partial(windowed.element)? else {
        return Ok(None);
    };
    let Some((value, value_len)) = decode_length_prefix_partial(&windowed.element[key_len..])?
    else {
        return Ok(None);
    };
    let typed = registry
        .assemble_group_input(group_key, key, value)
        .with_context(|| {
            format!(
                "assembling grouped input {group_key} (key {} bytes, value {} bytes)",
                key.len(),
                value.len()
            )
        })?;
    Ok(Some((
        WireValue {
            bytes: typed,
            timestamp_ns: decode_timestamp_ns(windowed.timestamp_millis)?,
            window: decode_interval_ns(windowed.interval_millis)?,
        },
        header_len + key_len + value_len,
    )))
}

fn decode_tagged_boundary_record(
    bytes: &[u8],
    interval_window: bool,
) -> Result<Option<(WireValue, usize)>> {
    let Some((windowed, header_len)) = try_decode_windowed_value(bytes, interval_window)? else {
        return Ok(None);
    };
    let Some((key, key_len)) = decode_length_prefix_partial(windowed.element)? else {
        return Ok(None);
    };
    let Some((value, value_len)) = decode_length_prefix_partial(&windowed.element[key_len..])?
    else {
        return Ok(None);
    };
    let mut tagged = Vec::new();
    encode_length_prefix(key, &mut tagged)?;
    encode_length_prefix(value, &mut tagged)?;
    Ok(Some((
        WireValue {
            bytes: tagged,
            timestamp_ns: decode_timestamp_ns(windowed.timestamp_millis)?,
            window: decode_interval_ns(windowed.interval_millis)?,
        },
        header_len + key_len + value_len,
    )))
}

fn decode_varint_partial(bytes: &[u8]) -> Result<Option<(u64, usize)>> {
    let mut value = 0_u64;
    for index in 0..10 {
        let Some(&byte) = bytes.get(index) else {
            return Ok(None);
        };
        if index == 9 && byte > 1 {
            bail!("v5 iterable count overflows varint");
        }
        value |= u64::from(byte & 0x7f) << (index * 7);
        if byte & 0x80 == 0 {
            return Ok(Some((value, index + 1)));
        }
    }
    bail!("v5 iterable count varint is too long")
}

#[cfg(test)]
fn decode_group_boundary_record(
    bytes: &[u8],
    registry: &Registry,
    group_key: &str,
    interval_window: bool,
    co_group: bool,
) -> Result<Option<(WireValue, usize)>> {
    let Some((mut rows, consumed)) = decode_group_boundary_rows(
        bytes,
        registry,
        group_key,
        if interval_window {
            Window::Fixed { size_ns: 1 }
        } else {
            Window::Global
        },
        co_group,
    )?
    else {
        return Ok(None);
    };
    if rows.len() != 1 {
        bail!("grouped boundary did not contain exactly one element");
    }
    Ok(Some((rows.remove(0), consumed)))
}

fn decode_group_boundary_rows(
    bytes: &[u8],
    registry: &Registry,
    group_key: &str,
    window: Window,
    co_group: bool,
) -> Result<Option<(Vec<WireValue>, usize)>> {
    let Some((group, consumed)) = decode_group_boundary_parts(bytes, window.clone())? else {
        return Ok(None);
    };
    if group.state_token.is_some() {
        bail!("v5 grouped iterable requires the Fn State API");
    }
    Ok(Some((
        assemble_group_boundary_rows(group, registry, group_key, window, co_group)?,
        consumed,
    )))
}

struct GroupBoundary {
    key: Vec<u8>,
    values: Vec<Vec<u8>>,
    state_token: Option<Vec<u8>>,
    timestamp_ns: i64,
    assigned_window: Option<WindowInterval>,
}

fn decode_group_boundary_parts(
    bytes: &[u8],
    window: Window,
) -> Result<Option<(GroupBoundary, usize)>> {
    let interval_window = window != Window::Global;
    let Some((windowed, header_len)) = try_decode_windowed_value(bytes, interval_window)? else {
        return Ok(None);
    };
    let element = windowed.element;
    let Some((key, mut consumed)) = decode_length_prefix_partial(element)? else {
        return Ok(None);
    };
    let key = key.to_vec();
    let Some(count_bytes) = element.get(consumed..consumed + 4) else {
        return Ok(None);
    };
    let count = i32::from_be_bytes(count_bytes.try_into().expect("four bytes"));
    consumed += 4;
    if count < -1 {
        bail!("v5 grouped iterable has invalid count {count}");
    }
    let mut values = Vec::new();
    let mut block_count = if count == -1 {
        let Some((count, used)) = decode_varint_partial(&element[consumed..])? else {
            return Ok(None);
        };
        consumed += used;
        count
    } else {
        count as u64
    };
    let mut state_token = None;
    loop {
        if count == -1 && block_count == 0 {
            break;
        }
        if count == -1 && block_count == u64::MAX {
            let Some((token, used)) = decode_length_prefix_partial(&element[consumed..])? else {
                return Ok(None);
            };
            consumed += used;
            state_token = Some(token.to_vec());
            break;
        }
        if block_count > i64::MAX as u64 {
            bail!("v5 grouped iterable has invalid block count");
        }
        if block_count > (element.len() - consumed) as u64 {
            return Ok(None);
        }
        for _ in 0..block_count {
            let Some((value, used)) = decode_length_prefix_partial(&element[consumed..])? else {
                return Ok(None);
            };
            values.push(value.to_vec());
            consumed += used;
        }
        if count != -1 {
            break;
        }
        let Some((next, used)) = decode_varint_partial(&element[consumed..])? else {
            return Ok(None);
        };
        consumed += used;
        block_count = next;
    }
    Ok(Some((
        GroupBoundary {
            key,
            values,
            state_token,
            timestamp_ns: decode_timestamp_ns(windowed.timestamp_millis)?,
            assigned_window: decode_interval_ns(windowed.interval_millis)?,
        },
        header_len + consumed,
    )))
}

fn assemble_group_boundary_rows(
    group: GroupBoundary,
    registry: &Registry,
    group_key: &str,
    window: Window,
    co_group: bool,
) -> Result<Vec<WireValue>> {
    let GroupBoundary {
        key,
        values,
        timestamp_ns,
        assigned_window,
        ..
    } = group;
    let rows = if registry.has_join_coder(group_key) {
        registry
            .execute_join_group(group_key, &key, &values, &window, assigned_window)?
            .into_iter()
            .map(|(bytes, timestamp_ns)| WireValue {
                bytes,
                timestamp_ns,
                window: assigned_window,
            })
            .collect()
    } else {
        vec![WireValue {
            bytes: if co_group {
                registry.assemble_cogroup_output(group_key, &key, &values)?
            } else {
                registry
                    .assemble_group_output(group_key, &key, &values)
                    .with_context(|| {
                        format!(
                            "assembling grouped output {group_key} (key {} bytes, {} values)",
                            key.len(),
                            values.len()
                        )
                    })?
            },
            timestamp_ns,
            window: assigned_window,
        }]
    };
    Ok(rows)
}
