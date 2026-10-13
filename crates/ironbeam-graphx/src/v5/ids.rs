pub fn portable_order(graph: &Graph) -> Result<Vec<String>, V5Error> {
    graph.validate_for(
        "dataflow-v5-preview",
        &Capabilities {
            external_bounded_source: true,
            unbounded_source: true,
            flatten: true,
            group_by_key: true,
            incremental_combine: true,
            global_combine: true,
            co_group_by_key: true,
            side_input_map: true,
            wait_on: true,
            join_inner: true,
            join_outer: true,
            reshuffle: true,
            fixed_window: true,
            assign_timestamp: true,
            append_sink: true,
            // A FinalResult sink publishes at job commit, while this worker
            // currently commits each Fn API bundle independently.
            final_result_sink: false,
            ..Capabilities::default()
        },
    )?;
    let mut children: BTreeMap<&str, Vec<&Node>> = BTreeMap::new();
    let mut remaining_parents: BTreeMap<&str, usize> = BTreeMap::new();
    let mut sources = Vec::new();
    for node in &graph.nodes {
        if node.time.initial_timestamp_ns != Time::default().initial_timestamp_ns {
            return Err(V5Error::Unsupported {
                key: node.key.clone(),
                reason: "portable nodes require default initial time".into(),
            });
        }
        if let Err(reason) = match node.time.window {
            Window::Global => Ok(()),
            Window::Fixed { size_ns } => fixed_window_fn(size_ns, 0).map(|_| ()),
            Window::FixedOffset { size_ns, offset_ns } => {
                fixed_window_fn(size_ns, offset_ns).map(|_| ())
            }
        } {
            return Err(V5Error::Unsupported {
                key: node.key.clone(),
                reason,
            });
        }
        if node.coder_version != CODER_VERSION {
            return Err(V5Error::Unsupported {
                key: node.key.clone(),
                reason: "coder version is not supported by the v5 harness".into(),
            });
        }
        if !node.additional_inputs.is_empty()
            && !matches!(
                node.operation,
                Operation::Flatten
                    | Operation::CoGroupByKey
                    | Operation::JoinInner
                    | Operation::JoinLeft
                    | Operation::JoinRight
                    | Operation::JoinFull
                    | Operation::SideInputMap
                    | Operation::WaitOn
            )
        {
            return Err(V5Error::Unsupported {
                key: node.key.clone(),
                reason: "multiple inputs are not supported".into(),
            });
        }
        match node.operation {
            Operation::Source { .. } if node.input.is_none() => {
                sources.push(node);
            }
            Operation::Map
            | Operation::Filter
            | Operation::FlatMap
            | Operation::AssignTimestamp
            | Operation::Flatten
            | Operation::GroupByKey
            | Operation::FoldPerKey
            | Operation::FoldGlobal
            | Operation::GlobalCombine
            | Operation::CoGroupByKey
            | Operation::JoinInner
            | Operation::JoinLeft
            | Operation::JoinRight
            | Operation::JoinFull
            | Operation::FixedWindow
            | Operation::Reshuffle
            | Operation::SideInputMap
            | Operation::WaitOn
            | Operation::Output
            | Operation::Sink { .. } => {
                let parent = node
                    .input
                    .as_deref()
                    .ok_or_else(|| V5Error::Graph(format!("node {} has no input", node.key)))?;
                children.entry(parent).or_default().push(node);
                for edge in &node.additional_inputs {
                    children.entry(&edge.key).or_default().push(node);
                }
                remaining_parents.insert(&node.key, 1 + node.additional_inputs.len());
            }
            _ => {
                return Err(V5Error::Unsupported {
                    key: node.key.clone(),
                    reason: format!(
                        "operation {:?} is outside the first v5 slice",
                        node.operation
                    ),
                });
            }
        }
    }
    if sources.is_empty() {
        return Err(V5Error::Graph("at least one source is required".into()));
    }
    sources.sort_by(|left, right| left.key.cmp(&right.key));
    let mut queue = VecDeque::from(sources);
    let mut order = Vec::new();
    while let Some(node) = queue.pop_front() {
        order.push(node.key.clone());
        let mut next = children.remove(node.key.as_str()).unwrap_or_default();
        if matches!(node.operation, Operation::Output | Operation::Sink { .. }) {
            if !next.is_empty() {
                return Err(V5Error::Graph(format!(
                    "terminal node {} has children",
                    node.key
                )));
            }
        } else if next.is_empty() {
            return Err(V5Error::Graph(format!(
                "node {} has no Output or Sink path",
                node.key
            )));
        }
        next.sort_by(|a, b| a.key.cmp(&b.key));
        for child in next {
            let remaining = remaining_parents
                .get_mut(child.key.as_str())
                .ok_or_else(|| V5Error::Graph(format!("missing parent count for {}", child.key)))?;
            *remaining -= 1;
            if *remaining == 0 {
                queue.push_back(child);
            }
        }
    }
    if order.len() != graph.nodes.len() {
        return Err(V5Error::Graph(
            "disconnected nodes are not supported".into(),
        ));
    }
    Ok(order)
}

fn id(prefix: &str, key: &str) -> String {
    format!("{prefix}_{}", hex::encode(key.as_bytes()))
}

/// Stable Beam PCollection ID for a v5 graph node.
pub fn collection_id(key: &str) -> String {
    id("v5_pc", key)
}

fn is_production_source(node: &Node) -> bool {
    matches!(node.operation, Operation::Source { .. })
        && node.boundedness == Boundedness::Unbounded
        && node.semantic_id != "ironbeam/test-stream"
}

pub fn cogroup_tag_collection_id(key: &str, input_index: usize) -> String {
    id("v5_cogroup_tag_pc", &format!("{key}:{input_index}"))
}

pub fn cogroup_flatten_collection_id(key: &str) -> String {
    id("v5_cogroup_flat_pc", key)
}

pub fn fold_group_collection_id(key: &str) -> String {
    id("v5_fold_group_pc", key)
}

pub fn fold_tag_collection_id(key: &str) -> String {
    id("v5_fold_tag_pc", key)
}

pub fn fold_seed_collection_id(key: &str) -> String {
    id("v5_fold_seed_pc", key)
}

pub fn fold_flatten_collection_id(key: &str) -> String {
    id("v5_fold_flat_pc", key)
}

fn fold_tag_transform_id(key: &str) -> String {
    id("v5_fold_tag_t", key)
}

fn fold_seed_transform_id(key: &str) -> String {
    id("v5_fold_seed_t", key)
}

fn fold_flatten_transform_id(key: &str) -> String {
    id("v5_fold_flat_t", key)
}

fn fold_group_transform_id(key: &str) -> String {
    id("v5_fold_group_t", key)
}

pub fn side_global_collection_id(key: &str) -> String {
    id("v5_side_global_pc", key)
}

fn side_global_transform_id(key: &str) -> String {
    id("v5_side_global_t", key)
}

fn cogroup_tag_transform_id(key: &str, input_index: usize) -> String {
    id("v5_cogroup_tag_t", &format!("{key}:{input_index}"))
}

fn cogroup_flatten_transform_id(key: &str) -> String {
    id("v5_cogroup_flat_t", key)
}

fn descriptor(node: &Node) -> ElementDescriptor {
    ElementDescriptor {
        type_id: node.type_id.clone(),
        coder_id: node.coder_id.clone(),
        coder_version: node.coder_version,
    }
}

fn beam_boundedness(node: &Node) -> i32 {
    match node.boundedness {
        Boundedness::Bounded => is_bounded::Enum::Bounded as i32,
        Boundedness::Unbounded => is_bounded::Enum::Unbounded as i32,
    }
}
