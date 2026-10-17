use ironbeam_cloud_ops::helpers::{
    BatchConfig, CloudIOExecutor, OperationBuilder, OperationContext, run_batch_operation,
    run_cloud_io_batch, run_cloud_io_paginated, run_parallel, run_parallel_with_limit,
    run_with_context,
};
use ironbeam_cloud_ops::utils::{
    ConnectionPool, PaginationConfig, RetryConfig, batch_in_chunks, block_on_async, paginate,
    parse_resource_uri, retry_with_backoff, validate_key_path, validate_resource_name,
    with_timeout,
};
use ironbeam_cloud_service_api::{CloudIOError, CloudResult, ErrorKind};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
    mpsc,
};
use std::time::Duration;

#[test]
fn retries_transient_failures_but_not_permanent_failures() {
    let config = RetryConfig {
        max_attempts: 3,
        initial_delay_ms: 0,
        max_delay_ms: 0,
        backoff_multiplier: 2.0,
    };
    let mut attempts = 0;
    let result = retry_with_backoff(&config, || {
        attempts += 1;
        if attempts < 3 {
            Err(CloudIOError::new(ErrorKind::Network, "retry"))
        } else {
            Ok(42)
        }
    });
    assert_eq!(result.unwrap(), 42);
    assert_eq!(attempts, 3);

    let mut permanent_attempts = 0;
    let error = OperationBuilder::new()
        .with_retry(config)
        .execute(|| -> CloudResult<()> {
            permanent_attempts += 1;
            Err(CloudIOError::new(ErrorKind::InvalidInput, "stop"))
        })
        .unwrap_err();
    assert_eq!(error.kind, ErrorKind::InvalidInput);
    assert_eq!(permanent_attempts, 1);
}

#[test]
fn pagination_and_context_keep_page_and_operation_metadata() {
    let mut pages = Vec::new();
    let values = paginate(
        &PaginationConfig {
            page_size: 2,
            max_pages: None,
        },
        |page, page_size| {
            pages.push((page, page_size));
            Ok((if page == 0 { vec![1, 2] } else { vec![3] }, page == 0))
        },
    )
    .unwrap();
    assert_eq!(values, [1, 2, 3]);
    assert_eq!(pages, [(0, 2), (1, 2)]);

    let (value, context) = run_with_context(OperationContext::new("list"), |context| {
        context.add_metadata("provider", "local");
        context.increment_retry();
        Ok::<_, CloudIOError>(values.len())
    })
    .unwrap();
    assert_eq!(value, 3);
    assert_eq!(context.operation_name, "list");
    assert_eq!(context.retry_count, 1);
    assert_eq!(context.metadata["provider"], "local");
}

#[test]
fn pagination_follows_empty_continuations_and_rejects_zero_limits() {
    let mut seen = Vec::new();
    let values = paginate(&PaginationConfig::default(), |page, _| {
        seen.push(page);
        Ok((if page == 0 { vec![] } else { vec![7] }, page == 0))
    })
    .unwrap();
    assert_eq!(seen, [0, 1]);
    assert_eq!(values, [7]);

    for config in [
        PaginationConfig {
            page_size: 0,
            max_pages: None,
        },
        PaginationConfig {
            page_size: 1,
            max_pages: Some(0),
        },
    ] {
        let error = paginate(&config, |_, _| -> CloudResult<(Vec<i32>, bool)> {
            panic!("invalid pagination must not call the provider")
        })
        .unwrap_err();
        assert_eq!(error.kind, ErrorKind::InvalidInput);
    }
}

#[test]
fn batch_and_cloud_wrappers_preserve_item_order() {
    let source = [1, 2, 3, 4, 5];
    let chunks = batch_in_chunks(&source, 2, Ok).unwrap();
    assert_eq!(chunks, source);
    let batch = run_batch_operation(
        &source,
        &BatchConfig {
            chunk_size: 2,
            parallel: false,
        },
        |chunk| Ok(chunk.into_iter().map(|value| value * 2).collect()),
    )
    .unwrap();
    assert_eq!(batch, [2, 4, 6, 8, 10]);

    let config = RetryConfig {
        max_attempts: 1,
        initial_delay_ms: 0,
        max_delay_ms: 0,
        backoff_multiplier: 2.0,
    };
    let values = run_cloud_io_batch(&config, &source, |value| Ok(value + 10)).unwrap();
    assert_eq!(values, [11, 12, 13, 14, 15]);
    let pages = run_cloud_io_paginated(&PaginationConfig::default(), |page, _| {
        Ok((vec![page], false))
    })
    .unwrap();
    assert_eq!(pages, [0]);
}

#[test]
fn batch_helpers_reject_zero_chunk_size_before_invoking_provider() {
    let error = batch_in_chunks(&[1], 0, |_| -> CloudResult<Vec<i32>> {
        panic!("invalid batch must not call the provider")
    })
    .unwrap_err();
    assert_eq!(error.kind, ErrorKind::InvalidInput);
    let error = run_batch_operation(
        &[1],
        &BatchConfig {
            chunk_size: 0,
            parallel: false,
        },
        |_| -> CloudResult<Vec<i32>> { panic!("invalid batch must not call the provider") },
    )
    .unwrap_err();
    assert_eq!(error.kind, ErrorKind::InvalidInput);
}

#[test]
fn operation_wrappers_preserve_errors_and_timeout_classification() {
    let config = RetryConfig {
        max_attempts: 2,
        initial_delay_ms: 0,
        max_delay_ms: 0,
        backoff_multiplier: 2.0,
    };
    let mut calls = 0;
    let actual = CloudIOExecutor::new()
        .with_retry(config)
        .with_timeout(Duration::from_secs(1))
        .execute(|| {
            calls += 1;
            if calls == 1 {
                Err(CloudIOError::new(ErrorKind::ServiceUnavailable, "retry"))
            } else {
                Ok(17)
            }
        })
        .unwrap();
    assert_eq!(actual, 17);
    assert_eq!(calls, 2);

    let error = with_timeout(Duration::ZERO, || {
        std::thread::sleep(Duration::from_millis(1));
        Ok(5)
    })
    .unwrap_err();
    assert_eq!(error.kind, ErrorKind::Timeout);
    let error = block_on_async(async {
        Err::<(), _>(CloudIOError::new(ErrorKind::Authorization, "forbidden"))
    })
    .unwrap_err();
    assert_eq!(error.kind, ErrorKind::Authorization);

    let operations: Vec<Box<dyn FnOnce() -> CloudResult<i32> + Send>> =
        vec![Box::new(|| Ok(1)), Box::new(|| Ok(2))];
    assert_eq!(run_parallel(operations).unwrap(), [1, 2]);
}

#[test]
fn parallel_operations_overlap_and_return_input_order() {
    let (left_sent, left_received) = mpsc::channel();
    let (right_sent, right_received) = mpsc::channel();
    let operations: Vec<Box<dyn FnOnce() -> CloudResult<i32> + Send>> = vec![
        Box::new(move || {
            left_sent.send(()).unwrap();
            right_received
                .recv_timeout(Duration::from_secs(5))
                .map_err(|error| CloudIOError::new(ErrorKind::Timeout, error.to_string()))?;
            Ok(1)
        }),
        Box::new(move || {
            right_sent.send(()).unwrap();
            left_received
                .recv_timeout(Duration::from_secs(5))
                .map_err(|error| CloudIOError::new(ErrorKind::Timeout, error.to_string()))?;
            Ok(2)
        }),
    ];
    assert_eq!(run_parallel(operations).unwrap(), [1, 2]);
}

#[test]
fn parallel_operations_join_all_and_classify_panics() {
    let completed = Arc::new(AtomicUsize::new(0));
    let later = Arc::clone(&completed);
    let operations: Vec<Box<dyn FnOnce() -> CloudResult<i32> + Send>> = vec![
        Box::new(|| Err(CloudIOError::new(ErrorKind::NotFound, "missing"))),
        Box::new(move || {
            later.fetch_add(1, Ordering::SeqCst);
            Ok(2)
        }),
    ];
    assert_eq!(
        run_parallel(operations).unwrap_err().kind,
        ErrorKind::NotFound
    );
    assert_eq!(completed.load(Ordering::SeqCst), 1);

    let operations: Vec<Box<dyn FnOnce() -> CloudResult<i32> + Send>> =
        vec![Box::new(|| panic!("broken provider"))];
    assert_eq!(
        run_parallel(operations).unwrap_err().kind,
        ErrorKind::InternalError
    );
}

#[test]
fn parallel_operations_respect_limit_and_complete_after_failures() {
    let active = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));
    let completed = Arc::new(AtomicUsize::new(0));
    let operations = (0_usize..12)
        .map(|index| {
            let active = Arc::clone(&active);
            let peak = Arc::clone(&peak);
            let completed = Arc::clone(&completed);
            Box::new(move || {
                let current = active.fetch_add(1, Ordering::SeqCst) + 1;
                peak.fetch_max(current, Ordering::SeqCst);
                std::thread::sleep(Duration::from_millis(10));
                active.fetch_sub(1, Ordering::SeqCst);
                completed.fetch_add(1, Ordering::SeqCst);
                if index == 0 {
                    Err(CloudIOError::new(ErrorKind::NotFound, "first failure"))
                } else {
                    Ok(index)
                }
            }) as Box<dyn FnOnce() -> CloudResult<usize> + Send>
        })
        .collect();
    let error = run_parallel_with_limit(operations, 3).unwrap_err();
    assert_eq!(error.kind, ErrorKind::NotFound);
    assert_eq!(completed.load(Ordering::SeqCst), 12);
    assert!(peak.load(Ordering::SeqCst) <= 3);
    assert_eq!(
        run_parallel_with_limit::<usize>(Vec::new(), 2).unwrap(),
        Vec::<usize>::new()
    );
    let ordered = (0_usize..8)
        .map(|index| Box::new(move || Ok(index)) as Box<dyn FnOnce() -> CloudResult<usize> + Send>)
        .collect();
    assert_eq!(
        run_parallel_with_limit(ordered, 2).unwrap(),
        (0..8).collect::<Vec<_>>()
    );
}

#[test]
fn parallel_operations_reject_zero_limit_without_running() {
    let called = Arc::new(AtomicUsize::new(0));
    let operation_called = Arc::clone(&called);
    let operations: Vec<Box<dyn FnOnce() -> CloudResult<()> + Send>> = vec![Box::new(move || {
        operation_called.fetch_add(1, Ordering::SeqCst);
        Ok(())
    })];
    assert_eq!(
        run_parallel_with_limit(operations, 0).unwrap_err().kind,
        ErrorKind::InvalidInput
    );
    assert_eq!(called.load(Ordering::SeqCst), 0);
}

#[test]
fn provider_neutral_resources_and_connections_validate_inputs() {
    assert_eq!(
        parse_resource_uri("vendor://account/queue").unwrap(),
        ("vendor".into(), vec!["account".into(), "queue".into()])
    );
    assert_eq!(
        parse_resource_uri("broken").unwrap_err().kind,
        ErrorKind::InvalidInput
    );
    assert!(validate_resource_name("bucket_1").is_ok());
    assert_eq!(
        validate_resource_name("bad/name").unwrap_err().kind,
        ErrorKind::InvalidInput
    );
    assert!(validate_key_path("prefix/item").is_ok());
    assert_eq!(
        validate_key_path("/absolute").unwrap_err().kind,
        ErrorKind::InvalidInput
    );

    let mut pool = ConnectionPool::new(1);
    let first = pool
        .acquire(|| Ok::<_, CloudIOError>("connection".to_string()))
        .unwrap();
    pool.release(first);
    assert_eq!(pool.size(), 1);
    let reused = pool
        .acquire(|| -> CloudResult<String> { panic!("must reuse") })
        .unwrap();
    assert_eq!(reused, "connection");
    assert_eq!(pool.size(), 0);
}
