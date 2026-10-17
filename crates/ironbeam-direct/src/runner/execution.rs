fn ensure_aggregates(
    graph: &Graph,
    registry: &ironbeam_core::Registry,
    output_key: &str,
) -> Result<(), Error> {
    let mut stack = vec![output_key];
    let mut seen = std::collections::BTreeSet::new();
    while let Some(key) = stack.pop() {
        if !seen.insert(key) {
            continue;
        }
        let node = graph
            .node(key)
            .ok_or_else(|| Error::graph(key, "physical node missing"))?;
        if matches!(
            node.operation,
            Operation::GroupByKey
                | Operation::CoGroupByKey
                | Operation::JoinInner
                | Operation::JoinLeft
                | Operation::JoinRight
                | Operation::JoinFull
                | Operation::SideInputMap
                | Operation::WaitOnWindowed
                | Operation::GlobalCombine
                | Operation::BatchMapValues { .. }
        ) && !registry.has_aggregate(key)
        {
            return Err(Error::unsupported(key, "direct", "aggregate-function"));
        }
        if matches!(
            node.operation,
            Operation::FoldGlobal
                | Operation::FoldPerKey
                | Operation::FoldOrderedGlobal
                | Operation::FoldOrderedPerKey
        ) && !registry.has_fold(key)
        {
            return Err(Error::unsupported(key, "direct", "fold-function"));
        }
        if let Some(input) = node.input.as_deref() {
            stack.push(input);
        }
        stack.extend(node.additional_inputs.iter().map(|edge| edge.key.as_str()));
    }
    Ok(())
}
pub(crate) fn open_source_safe(registry: &Registry, key: &str) -> Result<SourceIterator, Error> {
    catch_unwind(AssertUnwindSafe(|| registry.open_source(key)))
        .unwrap_or_else(|_| Err(Error::execution(key, "direct", "source factory panicked")))
}
fn open_typed_source_safe<T: Element>(
    registry: &Registry,
    key: &str,
) -> Result<Option<TypedSourceIterator<T>>, Error> {
    catch_unwind(AssertUnwindSafe(|| registry.open_typed_source::<T>(key)))
        .unwrap_or_else(|_| Err(Error::execution(key, "direct", "source factory panicked")))
}
fn read_typed_source_batch<T: Element>(
    source: &mut TypedSourceIterator<T>,
    limit: usize,
    key: &str,
) -> Result<Vec<T>, Error> {
    catch_unwind(AssertUnwindSafe(|| {
        let mut batch = Vec::with_capacity(source.size_hint().0.min(limit));
        for next in source.by_ref().take(limit) {
            batch.push(next?);
        }
        Ok(batch)
    }))
    .unwrap_or_else(|_| Err(Error::execution(key, "direct", "source iterator panicked")))
}
pub(crate) fn read_source_batch(
    source: &mut SourceIterator,
    limit: usize,
    key: &str,
) -> Result<Vec<Envelope>, Error> {
    catch_unwind(AssertUnwindSafe(|| {
        let mut batch = Vec::with_capacity(source.size_hint().0.min(limit));
        for next in source.by_ref().take(limit) {
            batch.push(next?);
        }
        Ok(batch)
    }))
    .unwrap_or_else(|_| Err(Error::execution(key, "direct", "source iterator panicked")))
}
pub(crate) fn ensure_bounded_sink_session(key: &str, session: &dyn SinkSession) -> Result<(), Error> {
    match catch_unwind(AssertUnwindSafe(|| session.requires_bundle_finalization())) {
        Ok(false) => Ok(()),
        Ok(true) => Err(Error::unsupported(key, "direct", "bundle-finalization")),
        Err(_) => Err(Error::execution(
            key,
            "direct",
            "sink finalization probe panicked",
        )),
    }
}

pub(crate) fn validate_config(config: &JobConfig) -> Result<(), Error> {
    if config.parallelism == 0
        || config.memory_budget_bytes == 0
        || config.buffer_budget_elements == 0
    {
        return Err(Error::graph(
            "job",
            "resource budgets and parallelism must be positive",
        ));
    }
    if config
        .checkpoint
        .as_ref()
        .is_some_and(|c| c.interval_batches == 0)
    {
        return Err(Error::graph(
            "checkpoint",
            "checkpoint interval must be positive",
        ));
    }
    if config
        .checkpoint
        .as_ref()
        .is_some_and(|c| c.binary_build_id.is_empty())
    {
        return Err(Error::graph(
            "checkpoint",
            "binary build identity must be supplied",
        ));
    }
    if config
        .spill
        .as_ref()
        .is_some_and(|s| s.threshold_bytes == 0)
    {
        return Err(Error::graph("spill", "spill threshold must be positive"));
    }
    Ok(())
}

#[allow(clippy::too_many_lines)]
fn execute<T: Element>(
    pool: Option<&rayon::ThreadPool>,
    registry: &ironbeam_core::Registry,
    plan: &planner::PhysicalPlan,
    output_key: &str,
    config: &JobConfig,
    checkpoint_identity: Option<checkpoint::Identity>,
    handle: &JobHandle,
) -> Result<Vec<T>, Error> {
    let mut store = OutputStore::<T>::new(
        config.memory_budget_bytes,
        config.spill.as_ref(),
        handle.metrics.clone(),
    )?;
    let checkpoint = config
        .checkpoint
        .as_ref()
        .zip(checkpoint_identity)
        .map(|(cfg, identity)| {
            checkpoint::CheckpointManager::new(cfg, identity, handle.metrics.clone())
        })
        .transpose()?;
    let recovered_offset = if let Some(manager) = &checkpoint {
        manager.recover(&mut store)?
    } else {
        0
    };
    if plan
        .summary
        .source_len_hint
        .is_some_and(|len| recovered_offset > len)
    {
        return Err(Error::recovery(
            &plan.summary.source_key,
            "checkpoint offset exceeds source length",
        ));
    }
    handle.processed.store(
        handle.metrics.recovered_elements.load(Ordering::Relaxed),
        Ordering::Relaxed,
    );
    for route in &plan.routes {
        let typed_transforms: Option<Vec<_>> = route
            .operations
            .iter()
            .map(|key| registry.typed_value_transform::<T>(key))
            .collect();
        let single_transforms: Option<Vec<_>> = route
            .operations
            .iter()
            .map(|key| registry.single_transform(key))
            .collect();
        if route.operations.is_empty()
            && checkpoint.is_none()
            && let Some((source_len, read_batch)) =
                registry.vec_source_batches::<T>(&route.source_key)
        {
            if config.cancellation.load(Ordering::Acquire) {
                return Err(cancelled());
            }
            for start in (0..source_len).step_by(plan.summary.batch_limit) {
                if config.cancellation.load(Ordering::Acquire) {
                    return Err(cancelled());
                }
                let len = (source_len - start).min(plan.summary.batch_limit);
                let batch = catch_unwind(AssertUnwindSafe(|| read_batch(start, len)))
                    .unwrap_or_else(|_| {
                        Err(Error::execution(
                            &route.source_key,
                            "direct",
                            "source value clone panicked",
                        ))
                    })?;
                let count = u64::try_from(batch.len()).unwrap_or(u64::MAX);
                handle
                    .metrics
                    .source_elements
                    .fetch_add(count, Ordering::Relaxed);
                handle
                    .metrics
                    .peak_batch_elements
                    .fetch_max(count, Ordering::Relaxed);
                let collect = || catch_unwind(AssertUnwindSafe(|| store.extend_batch(batch)));
                let result = match pool {
                    Some(pool) => pool.install(collect),
                    None => collect(),
                };
                result.unwrap_or_else(|_| {
                    Err(Error::execution(
                        output_key,
                        "direct",
                        "output collection panicked",
                    ))
                })?;
                let total = handle
                    .metrics
                    .output_elements
                    .fetch_add(count, Ordering::Relaxed)
                    .saturating_add(count);
                handle.processed.store(total, Ordering::Relaxed);
                handle.metrics.batches.fetch_add(1, Ordering::Relaxed);
            }
            continue;
        }
        let mut source_offset = recovered_offset;
        let vector_batches = checkpoint
            .is_none()
            .then(|| registry.vec_source_batches::<T>(&route.source_key))
            .flatten();
        let typed_eligible =
            (config.parallelism == 1 || route.operations.is_empty()) && typed_transforms.is_some();
        let mut source = if typed_eligible && vector_batches.is_none() {
            open_typed_source_safe::<T>(registry, &route.source_key)?
        } else {
            None
        };
        if typed_eligible
            && let Some(typed_transforms) = typed_transforms
            && (vector_batches.is_some() || source.is_some())
        {
            if let Some(source) = source.as_mut() {
                for _ in 0..source_offset {
                    if read_typed_source_batch(source, 1, &route.source_key)?.is_empty() {
                        return Err(Error::recovery(
                            &route.source_key,
                            "source ended before checkpoint position",
                        ));
                    }
                }
            }
            let mut batches = 0_usize;
            loop {
                if config.cancellation.load(Ordering::Acquire) {
                    return Err(cancelled());
                }
                let batch = if let Some((source_len, read_batch)) = vector_batches {
                    if source_offset >= source_len {
                        Vec::new()
                    } else {
                        let len = (source_len - source_offset).min(plan.summary.batch_limit);
                        catch_unwind(AssertUnwindSafe(|| read_batch(source_offset, len)))
                            .unwrap_or_else(|_| {
                                Err(Error::execution(
                                    &route.source_key,
                                    "direct",
                                    "source value clone panicked",
                                ))
                            })?
                    }
                } else {
                    read_typed_source_batch(
                        source.as_mut().expect("typed source is available"),
                        plan.summary.batch_limit,
                        &route.source_key,
                    )?
                };
                if batch.is_empty() {
                    break;
                }
                let batch_len = batch.len();
                let count = u64::try_from(batch_len).unwrap_or(u64::MAX);
                handle
                    .metrics
                    .source_elements
                    .fetch_add(count, Ordering::Relaxed);
                handle
                    .metrics
                    .peak_batch_elements
                    .fetch_max(count, Ordering::Relaxed);
                let mut output_count = 0_u64;
                let mut filtered_count = 0_u64;
                let mut panic_node = route.source_key.as_str();
                let perform = || {
                    catch_unwind(AssertUnwindSafe(|| -> Result<(), Error> {
                        if route.operations.is_empty() {
                            output_count = u64::try_from(batch.len()).unwrap_or(u64::MAX);
                            panic_node = output_key;
                            store.extend_batch(batch)?;
                            return Ok(());
                        }
                        let mut produced = Vec::with_capacity(batch.len());
                        if let [first, second] = typed_transforms.as_slice() {
                            let first_key = route.operations[0].as_str();
                            let second_key = route.operations[1].as_str();
                            for value in batch {
                                if config.cancellation.load(Ordering::Acquire) {
                                    return Err(cancelled());
                                }
                                panic_node = first_key;
                                let Some(value) = first.apply(value) else {
                                    filtered_count = filtered_count.saturating_add(1);
                                    continue;
                                };
                                if config.cancellation.load(Ordering::Acquire) {
                                    return Err(cancelled());
                                }
                                panic_node = second_key;
                                if let Some(value) = second.apply(value) {
                                    produced.push(value);
                                } else {
                                    filtered_count = filtered_count.saturating_add(1);
                                }
                            }
                        } else {
                            for value in batch {
                                let mut current = Some(value);
                                for (key, transform) in
                                    route.operations.iter().zip(&typed_transforms)
                                {
                                    let Some(value) = current else {
                                        break;
                                    };
                                    if config.cancellation.load(Ordering::Acquire) {
                                        return Err(cancelled());
                                    }
                                    panic_node = key;
                                    current = transform.apply(value);
                                    if current.is_none() {
                                        filtered_count = filtered_count.saturating_add(1);
                                    }
                                }
                                if let Some(value) = current {
                                    produced.push(value);
                                }
                            }
                        }
                        output_count = u64::try_from(produced.len()).unwrap_or(u64::MAX);
                        panic_node = output_key;
                        store.extend_batch(produced)?;
                        Ok(())
                    }))
                };
                let result = match pool {
                    Some(pool) => pool.install(perform),
                    None => perform(),
                };
                match result {
                    Ok(result) => result?,
                    Err(_) => {
                        return Err(Error::execution(
                            panic_node,
                            "direct",
                            if panic_node == output_key {
                                "output collection panicked"
                            } else {
                                "user transform panicked"
                            },
                        ));
                    }
                }
                handle
                    .metrics
                    .filtered_elements
                    .fetch_add(filtered_count, Ordering::Relaxed);
                let total = handle
                    .metrics
                    .output_elements
                    .fetch_add(output_count, Ordering::Relaxed)
                    + output_count;
                handle.processed.store(total, Ordering::Relaxed);
                source_offset = source_offset
                    .checked_add(batch_len)
                    .ok_or_else(|| Error::resource("job", "source offset overflow"))?;
                batches += 1;
                handle.metrics.batches.fetch_add(1, Ordering::Relaxed);
                if let Some(manager) = &checkpoint
                    && manager.should_save(batches)
                {
                    manager.save(source_offset, &store)?;
                }
            }
            if let Some(manager) = &checkpoint
                && batches > 0
                && !manager.should_save(batches)
            {
                manager.save(source_offset, &store)?;
            }
            continue;
        }
        let mut source = open_source_safe(registry, &route.source_key)?;
        for _ in 0..source_offset {
            if read_source_batch(&mut source, 1, &route.source_key)?.is_empty() {
                return Err(Error::recovery(
                    &route.source_key,
                    "source ended before checkpoint position",
                ));
            }
        }
        let mut batches = 0_usize;
        loop {
            if config.cancellation.load(Ordering::Acquire) {
                return Err(cancelled());
            }
            let batch =
                read_source_batch(&mut source, plan.summary.batch_limit, &route.source_key)?;
            if batch.is_empty() {
                break;
            }
            let batch_len = batch.len();
            handle.metrics.source_elements.fetch_add(
                u64::try_from(batch_len).unwrap_or(u64::MAX),
                Ordering::Relaxed,
            );
            handle.metrics.peak_batch_elements.fetch_max(
                u64::try_from(batch_len).unwrap_or(u64::MAX),
                Ordering::Relaxed,
            );
            let mut output_count = 0_u64;
            if let Some(transforms) = &single_transforms {
                if config.parallelism == 1 {
                    let mut panic_node = route.source_key.as_str();
                    let result = pool.expect("transform route owns a pool").install(|| {
                        catch_unwind(AssertUnwindSafe(|| -> Result<(), Error> {
                            for event in batch {
                                let mut current = Some(event);
                                for (key, transform) in route.operations.iter().zip(transforms) {
                                    let Some(event) = current else {
                                        break;
                                    };
                                    if config.cancellation.load(Ordering::Acquire) {
                                        return Err(cancelled());
                                    }
                                    panic_node = key;
                                    current = transform(event)?;
                                    if current.is_none() {
                                        handle
                                            .metrics
                                            .filtered_elements
                                            .fetch_add(1, Ordering::Relaxed);
                                    }
                                }
                                if let Some(event) = current {
                                    panic_node = output_key;
                                    push_output(event, &mut store, output_key)?;
                                    output_count += 1;
                                }
                            }
                            Ok(())
                        }))
                    });
                    match result {
                        Ok(result) => result?,
                        Err(_) => {
                            return Err(Error::execution(
                                panic_node,
                                "direct",
                                if panic_node == output_key {
                                    "output collection panicked"
                                } else {
                                    "user transform panicked"
                                },
                            ));
                        }
                    }
                } else {
                    let apply_single = |event| {
                        let mut current = Some(event);
                        for (key, transform) in route.operations.iter().zip(transforms) {
                            let Some(event) = current else {
                                break;
                            };
                            if config.cancellation.load(Ordering::Acquire) {
                                return Err(cancelled());
                            }
                            current = match catch_unwind(AssertUnwindSafe(|| transform(event))) {
                                Ok(result) => result?,
                                Err(_) => {
                                    return Err(Error::execution(
                                        key,
                                        "direct",
                                        "user transform panicked",
                                    ));
                                }
                            };
                            if current.is_none() {
                                handle
                                    .metrics
                                    .filtered_elements
                                    .fetch_add(1, Ordering::Relaxed);
                            }
                        }
                        Ok(current)
                    };
                    let results: Vec<Result<Option<Envelope>, Error>> = pool
                        .expect("transform route owns a pool")
                        .install(|| batch.into_par_iter().map(apply_single).collect());
                    for result in results {
                        if let Some(event) = result? {
                            push_output(event, &mut store, output_key)?;
                            output_count += 1;
                        }
                    }
                }
            } else {
                let results: Vec<Result<Vec<Envelope>, Error>> =
                    pool.expect("transform route owns a pool").install(|| {
                        batch
                            .into_par_iter()
                            .map(|event| {
                                let mut events = vec![event];
                                for key in &route.operations {
                                    let mut next_events = Vec::new();
                                    for event in events {
                                        if config.cancellation.load(Ordering::Acquire) {
                                            return Err(cancelled());
                                        }
                                        match catch_unwind(AssertUnwindSafe(|| {
                                            registry.apply_many(key, event)
                                        })) {
                                            Ok(Ok(produced)) => {
                                                if produced.is_empty() {
                                                    handle
                                                        .metrics
                                                        .filtered_elements
                                                        .fetch_add(1, Ordering::Relaxed);
                                                }
                                                next_events.extend(produced);
                                            }
                                            Ok(Err(error)) => return Err(error),
                                            Err(_) => {
                                                return Err(Error::execution(
                                                    key,
                                                    "direct",
                                                    "user transform panicked",
                                                ));
                                            }
                                        }
                                    }
                                    events = next_events;
                                }
                                Ok(events)
                            })
                            .collect()
                    });
                for result in results {
                    for event in result? {
                        push_output(event, &mut store, output_key)?;
                        output_count += 1;
                    }
                }
            }
            let total = handle
                .metrics
                .output_elements
                .fetch_add(output_count, Ordering::Relaxed)
                + output_count;
            handle.processed.store(total, Ordering::Relaxed);
            source_offset = source_offset
                .checked_add(batch_len)
                .ok_or_else(|| Error::resource("job", "source offset overflow"))?;
            batches += 1;
            handle.metrics.batches.fetch_add(1, Ordering::Relaxed);
            if let Some(manager) = &checkpoint
                && manager.should_save(batches)
            {
                manager.save(source_offset, &store)?;
            }
        }
        if let Some(manager) = &checkpoint
            && batches > 0
            && !manager.should_save(batches)
        {
            manager.save(source_offset, &store)?;
        }
    }
    store.into_vec()
}
fn push_output<T: Element>(
    event: Envelope,
    store: &mut OutputStore<T>,
    output_key: &str,
) -> Result<(), Error> {
    let value = event
        .value
        .downcast::<T>()
        .map_err(|_| Error::execution(output_key, "direct", "output type mismatch"))?;
    store.push(*value)?;
    Ok(())
}
pub(crate) fn cancelled() -> Error {
    Error {
        kind: ErrorKind::Cancelled,
        node: None,
        backend: Some("direct".into()),
        retry: RetryClass::Permanent,
        message: "job cancelled".into(),
    }
}
