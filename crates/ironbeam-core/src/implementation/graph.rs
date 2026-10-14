impl Graph {
    pub fn canonical_json(&self) -> Result<String, Error> {
        let mut copy = self.clone();
        copy.nodes.sort_by(|a, b| a.key.cmp(&b.key));
        serde_json::to_string(&copy).map_err(|e| Error::incompatible(e.to_string()))
    }
    pub fn fingerprint(&self) -> Result<String, Error> {
        let bytes = self.canonical_json()?;
        let digest = Sha256::digest(bytes.as_bytes());
        let mut result = String::from("sha256-v1:");
        for byte in digest {
            write!(result, "{byte:02x}").map_err(|e| Error::incompatible(e.to_string()))?;
        }
        Ok(result)
    }
    pub fn validate(&self) -> Result<(), Error> {
        if self.version.major != GRAPH_VERSION.major || self.version.minor > GRAPH_VERSION.minor {
            return Err(Error::incompatible("unsupported graph descriptor version"));
        }
        if self.checkpoint_version.major != CHECKPOINT_VERSION.major
            || self.checkpoint_version.minor > CHECKPOINT_VERSION.minor
        {
            return Err(Error::incompatible("unsupported checkpoint schema version"));
        }
        let mut by_key = BTreeMap::new();
        for node in &self.nodes {
            if node.key.is_empty() || by_key.insert(node.key.as_str(), node).is_some() {
                return Err(Error::graph(
                    &node.key,
                    "empty or duplicate stable node key",
                ));
            }
            if node.type_id.is_empty() || node.coder_id.is_empty() {
                return Err(Error::graph(&node.key, "type and coder IDs are required"));
            }
            if node.semantic_id.is_empty() || node.semantic_version.major == 0 {
                return Err(Error::graph(
                    &node.key,
                    "semantic operation ID and major version are required",
                ));
            }
            if matches!(node.operation, Operation::Source { .. }) && !node.semantic_id.contains('/')
            {
                return Err(Error::graph(
                    &node.key,
                    "source semantic ID must be namespaced",
                ));
            }
            if node.coder_version.major != CODER_VERSION.major
                || node.coder_version.minor > CODER_VERSION.minor
            {
                return Err(Error::incompatible(format!(
                    "unsupported coder version at {}",
                    node.key
                )));
            }
            if matches!(node.time.window, Window::Fixed { size_ns } | Window::FixedOffset { size_ns, .. } if size_ns <= 0)
            {
                return Err(Error::graph(
                    &node.key,
                    "fixed window size must be positive",
                ));
            }
            if matches!(node.time.window, Window::Sliding { size_ns, period_ns, .. } if size_ns <= 0 || period_ns <= 0) {
                return Err(Error::graph(&node.key, "sliding size and period must be positive"));
            }
            if matches!(node.time.window, Window::Session { gap_ns } if gap_ns <= 0) {
                return Err(Error::graph(&node.key, "session gap must be positive"));
            }
            if let Some(count) = node.time.trigger.early_count()
                && (count == 0
                    || count > i32::MAX as u64
                    || !matches!(node.time.window, Window::Fixed { .. } | Window::FixedOffset { .. }))
            {
                return Err(Error::graph(
                    &node.key,
                    "early count trigger requires fixed windows and a positive count",
                ));
            }
            if matches!(node.operation, Operation::FixedWindow)
                && !matches!(
                    node.time.window,
                    Window::Fixed { .. } | Window::FixedOffset { .. }
                )
            {
                return Err(Error::graph(
                    &node.key,
                    "fixed-window operation requires a fixed window descriptor",
                ));
            }
            if matches!(node.operation, Operation::SlidingWindow)
                && !matches!(node.time.window, Window::Sliding { .. })
            {
                return Err(Error::graph(&node.key, "sliding-window operation requires a sliding descriptor"));
            }
            if matches!(node.operation, Operation::SessionWindow)
                && !matches!(node.time.window, Window::Session { .. })
            {
                return Err(Error::graph(&node.key, "session-window operation requires a session descriptor"));
            }
            if matches!(node.operation, Operation::BatchMapValues { batch_size: 0 }) {
                return Err(Error::graph(&node.key, "batch size must be positive"));
            }
            if matches!(node.operation, Operation::BatchMapValues { .. })
                && node.boundedness == Boundedness::Unbounded
            {
                return Err(Error::graph(
                    &node.key,
                    "batch mapping requires a bounded input",
                ));
            }
            if let Operation::Custom {
                semantic_id,
                version,
            } = &node.operation
                && (!semantic_id.contains('/') || version.major == 0)
            {
                return Err(Error::graph(
                    &node.key,
                    "custom operation requires a namespaced ID and major version",
                ));
            }
            let requires_additional = matches!(
                node.operation,
                Operation::WaitOn
                    | Operation::CoGroupByKey
                    | Operation::JoinInner
                    | Operation::JoinLeft
                    | Operation::JoinRight
                    | Operation::JoinFull
                    | Operation::SideInputMap
            );
            let allows_additional =
                requires_additional || matches!(node.operation, Operation::Flatten);
            if (requires_additional && node.additional_inputs.is_empty())
                || (!allows_additional && !node.additional_inputs.is_empty())
            {
                return Err(Error::graph(
                    &node.key,
                    "multi-input operation requires additional inputs and other operations forbid them",
                ));
            }
            if matches!(
                node.operation,
                Operation::JoinInner
                    | Operation::JoinLeft
                    | Operation::JoinRight
                    | Operation::JoinFull
                    | Operation::WaitOn
                    | Operation::SideInputMap
            ) && node.additional_inputs.len() != 1
            {
                return Err(Error::graph(
                    &node.key,
                    "operation requires exactly two inputs",
                ));
            }
            if let Operation::Custom {
                semantic_id,
                version,
            } = &node.operation
                && (semantic_id != &node.semantic_id || version != &node.semantic_version)
            {
                return Err(Error::graph(
                    &node.key,
                    "custom operation identity differs from descriptor",
                ));
            }
        }
        for node in &self.nodes {
            match (&node.operation, &node.input, &node.input_element) {
                (Operation::Source { .. }, None, None) => {}
                (Operation::Source { .. }, _, _) | (_, None, _) | (_, _, None) => {
                    return Err(Error::graph(&node.key, "invalid source/input relationship"));
                }
                (_, Some(parent), Some(expected)) => {
                    let upstream = by_key
                        .get(parent.as_str())
                        .ok_or_else(|| Error::graph(&node.key, "missing input node"))?;
                    if expected.type_id != upstream.type_id
                        || expected.coder_id != upstream.coder_id
                        || expected.coder_version != upstream.coder_version
                    {
                        return Err(Error::graph(&node.key, "input edge type/coder mismatch"));
                    }
                    if matches!(
                        node.operation,
                        Operation::Filter
                            | Operation::AssignTimestamp
                            | Operation::Flatten
                            | Operation::Reshuffle
                            | Operation::WaitOn
                            | Operation::FixedWindow
                            | Operation::SlidingWindow
                            | Operation::SessionWindow
                            | Operation::Output
                            | Operation::Sink { .. }
                    ) && (node.type_id != upstream.type_id
                        || node.coder_id != upstream.coder_id
                        || node.coder_version != upstream.coder_version)
                    {
                        return Err(Error::graph(&node.key, "input type/coder mismatch"));
                    }
                    if !matches!(
                        node.operation,
                        Operation::Flatten
                            | Operation::CoGroupByKey
                            | Operation::JoinInner
                            | Operation::JoinLeft
                            | Operation::JoinRight
                            | Operation::JoinFull
                            | Operation::WaitOn
                            | Operation::SideInputMap
                    ) && node.boundedness != upstream.boundedness
                    {
                        return Err(Error::graph(&node.key, "boundedness must propagate"));
                    }
                    if matches!(
                        node.operation,
                        Operation::Map
                            | Operation::AssignTimestamp
                            | Operation::FlatMap
                            | Operation::Filter
                            | Operation::Flatten
                            | Operation::Reshuffle
                            | Operation::WaitOn
                            | Operation::GroupByKey
                            | Operation::CoGroupByKey
                            | Operation::JoinInner
                            | Operation::JoinLeft
                            | Operation::JoinRight
                            | Operation::JoinFull
                            | Operation::SideInputMap
                            | Operation::GlobalCombine
                            | Operation::FoldGlobal
                            | Operation::FoldPerKey
                            | Operation::FoldOrderedGlobal
                            | Operation::FoldOrderedPerKey
                            | Operation::BatchMapValues { .. }
                            | Operation::Output
                            | Operation::Sink { .. }
                    ) && node.time != upstream.time
                    {
                        return Err(Error::graph(&node.key, "event time/window must propagate"));
                    }
                    if matches!(
                        node.operation,
                        Operation::GlobalCombine
                            | Operation::FoldGlobal
                            | Operation::FoldPerKey
                            | Operation::FoldOrderedGlobal
                            | Operation::FoldOrderedPerKey
                            | Operation::GroupByKey
                            | Operation::CoGroupByKey
                    ) && node.boundedness == Boundedness::Unbounded
                        && node.time.window == Window::Global
                    {
                        return Err(Error::graph(
                            &node.key,
                            "unbounded global aggregation requires a finite window",
                        ));
                    }
                    if matches!(
                        node.operation,
                        Operation::Flatten
                            | Operation::WaitOn
                            | Operation::CoGroupByKey
                            | Operation::JoinInner
                            | Operation::JoinLeft
                            | Operation::JoinRight
                            | Operation::JoinFull
                            | Operation::SideInputMap
                    ) {
                        let mut unbounded = upstream.boundedness == Boundedness::Unbounded;
                        for edge in &node.additional_inputs {
                            let other = by_key.get(edge.key.as_str()).ok_or_else(|| {
                                Error::graph(&node.key, "missing additional input node")
                            })?;
                            if edge.element.type_id != other.type_id
                                || edge.element.coder_id != other.coder_id
                                || edge.element.coder_version != other.coder_version
                            {
                                return Err(Error::graph(
                                    &node.key,
                                    "additional input type/coder mismatch",
                                ));
                            }
                            if matches!(node.operation, Operation::Flatten)
                                && (node.type_id != other.type_id
                                    || node.coder_id != other.coder_id
                                    || node.coder_version != other.coder_version)
                            {
                                return Err(Error::graph(
                                    &node.key,
                                    "flatten input type/coder mismatch",
                                ));
                            }
                            if other.time != node.time
                                && !matches!(node.operation, Operation::WaitOn)
                            {
                                return Err(Error::graph(
                                    &node.key,
                                    "flatten time/window mismatch",
                                ));
                            }
                            unbounded |= other.boundedness == Boundedness::Unbounded;
                        }
                        let expected = if matches!(node.operation, Operation::WaitOn) {
                            upstream.boundedness
                        } else if unbounded {
                            Boundedness::Unbounded
                        } else {
                            Boundedness::Bounded
                        };
                        if node.boundedness != expected {
                            return Err(Error::graph(
                                &node.key,
                                "multi-input boundedness mismatch",
                            ));
                        }
                        if matches!(
                            node.operation,
                            Operation::JoinInner
                                | Operation::CoGroupByKey
                                | Operation::JoinLeft
                                | Operation::JoinRight
                                | Operation::JoinFull
                        ) && expected == Boundedness::Unbounded
                            && node.time.window == Window::Global
                        {
                            return Err(Error::graph(
                                &node.key,
                                "unbounded global join requires a finite window",
                            ));
                        }
                        if matches!(node.operation, Operation::SideInputMap)
                            && node.additional_inputs.iter().any(|edge| {
                                by_key[edge.key.as_str()].boundedness == Boundedness::Unbounded
                            })
                        {
                            return Err(Error::graph(&node.key, "side input must be bounded"));
                        }
                        if matches!(node.operation, Operation::WaitOn)
                            && node.additional_inputs.iter().any(|edge| {
                                by_key[edge.key.as_str()].boundedness == Boundedness::Unbounded
                            })
                        {
                            return Err(Error::graph(&node.key, "wait signal must be bounded"));
                        }
                    }
                }
            }
        }
        for node in &self.nodes {
            let mut active = BTreeSet::new();
            let mut complete = BTreeSet::new();
            let mut stack = vec![(node.key.as_str(), false)];
            while let Some((key, exiting)) = stack.pop() {
                if exiting {
                    active.remove(key);
                    complete.insert(key);
                    continue;
                }
                if complete.contains(key) {
                    continue;
                }
                if !active.insert(key) {
                    return Err(Error::graph(&node.key, "cycle in graph"));
                }
                stack.push((key, true));
                let current = by_key[key];
                for edge in &current.additional_inputs {
                    stack.push((edge.key.as_str(), false));
                }
                if let Some(parent) = &current.input {
                    stack.push((parent.as_str(), false));
                }
            }
        }
        Ok(())
    }
    pub fn validate_for(&self, backend: &str, capabilities: &Capabilities) -> Result<(), Error> {
        self.validate()?;
        for node in &self.nodes {
            let missing = match &node.operation {
                Operation::Source { .. }
                    if node.boundedness == Boundedness::Unbounded
                        && !capabilities.unbounded_source =>
                {
                    Some("unbounded-source".to_string())
                }
                Operation::Source { .. }
                    if node.semantic_id != "ironbeam/source"
                        && !capabilities.external_bounded_source =>
                {
                    Some(node.semantic_id.clone())
                }
                Operation::FixedWindow | Operation::SlidingWindow | Operation::SessionWindow if !capabilities.fixed_window => {
                    Some("fixed-window".to_string())
                }
                Operation::AssignTimestamp if !capabilities.assign_timestamp => {
                    Some("assign-timestamp".to_string())
                }
                Operation::Flatten if !capabilities.flatten => Some("flatten".to_string()),
                Operation::Reshuffle if !capabilities.reshuffle => Some("reshuffle".to_string()),
                Operation::WaitOn if !capabilities.wait_on => Some("wait-on".to_string()),
                Operation::GroupByKey if !capabilities.group_by_key => {
                    Some("group-by-key".to_string())
                }
                Operation::CoGroupByKey if !capabilities.co_group_by_key => {
                    Some("co-group-by-key".to_string())
                }
                Operation::JoinInner if !capabilities.join_inner => Some("join-inner".to_string()),
                Operation::JoinLeft | Operation::JoinRight | Operation::JoinFull
                    if !capabilities.join_outer =>
                {
                    Some("join-outer".to_string())
                }
                Operation::SideInputMap if !capabilities.side_input_map => {
                    Some("side-input-map".to_string())
                }
                Operation::GlobalCombine if !capabilities.global_combine => {
                    Some("global-combine".to_string())
                }
                Operation::FoldGlobal | Operation::FoldPerKey
                    if !capabilities.incremental_combine =>
                {
                    Some("incremental-combine".to_string())
                }
                Operation::FoldOrderedGlobal | Operation::FoldOrderedPerKey
                    if !capabilities.ordered_fold =>
                {
                    Some("ordered-fold".to_string())
                }
                Operation::BatchMapValues { .. } if !capabilities.batch_map_values => {
                    Some("batch-map-values".to_string())
                }
                Operation::Sink { spec }
                    if spec.mode == SinkMode::Append && !capabilities.append_sink =>
                {
                    Some("append-sink".to_string())
                }
                Operation::Sink { spec }
                    if spec.mode == SinkMode::Update && !capabilities.update_sink =>
                {
                    Some("update-sink".to_string())
                }
                Operation::Sink { spec }
                    if spec.mode == SinkMode::FinalResult && !capabilities.final_result_sink =>
                {
                    Some("final-result-sink".to_string())
                }
                Operation::Custom { semantic_id, .. }
                    if !capabilities.custom_operations.contains(semantic_id) =>
                {
                    Some(semantic_id.clone())
                }
                _ => None,
            };
            if let Some(missing) = missing {
                return Err(Error::unsupported(&node.key, backend, missing));
            }
        }
        Ok(())
    }
    #[must_use]
    pub fn node(&self, key: &str) -> Option<&Node> {
        self.nodes.iter().find(|n| n.key == key)
    }
}
