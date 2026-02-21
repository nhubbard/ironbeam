// Unit tests for end-user cloud operation helpers
//
// These tests verify the high-level helpers that allow end-users to run
// custom cloud operations with built-in retry, timeout, and error handling.

use ironbeam::helpers::cloud::*;
use ironbeam::io::cloud::CloudResult;
use ironbeam::io::cloud::helpers::{PaginationConfig, RetryConfig};
use ironbeam::io::cloud::traits::{CloudIOError, ErrorKind};
use std::time::Duration;

// ============================================================================
// Retry Tests
// ============================================================================

#[test]
fn test_run_with_retry_success() {
    let config = RetryConfig::default();
    let mut attempts = 0;

    let result = run_with_retry(&config, || {
        attempts += 1;
        if attempts < 2 {
            Err(CloudIOError::new(ErrorKind::Network, "Temporary failure"))
        } else {
            Ok("success")
        }
    });

    assert!(result.is_ok());
    assert_eq!(result.unwrap(), "success");
    assert_eq!(attempts, 2);
}

#[test]
fn test_run_with_retry_exhausted() {
    let config = RetryConfig {
        max_attempts: 2,
        ..RetryConfig::default()
    };
    let mut attempts = 0;

    let result: CloudResult<i32> = run_with_retry(&config, || {
        attempts += 1;
        Err(CloudIOError::new(ErrorKind::Network, "Always fails"))
    });

    assert!(result.is_err());
    assert_eq!(attempts, 2);
}

#[test]
fn test_run_with_retry_non_retryable_error() {
    let config = RetryConfig::default();
    let mut attempts = 0;

    let result: CloudResult<i32> = run_with_retry(&config, || {
        attempts += 1;
        Err(CloudIOError::new(
            ErrorKind::InvalidInput,
            "Bad input (non-retryable)",
        ))
    });

    assert!(result.is_err());
    assert_eq!(attempts, 1); // Should not retry non-retryable errors
}

// ============================================================================
// Parallel Execution Tests
// ============================================================================

#[test]
fn test_run_parallel_success() {
    let operations: Vec<Box<dyn FnOnce() -> CloudResult<i32> + Send>> =
        vec![Box::new(|| Ok(1)), Box::new(|| Ok(2)), Box::new(|| Ok(3))];

    let results = run_parallel(operations);
    assert!(results.is_ok());
    let values = results.unwrap();
    assert_eq!(values, vec![1, 2, 3]);
}

#[test]
fn test_run_parallel_with_error() {
    let operations: Vec<Box<dyn FnOnce() -> CloudResult<i32> + Send>> = vec![
        Box::new(|| Ok(1)),
        Box::new(|| Err(CloudIOError::new(ErrorKind::Network, "Failed"))),
        Box::new(|| Ok(3)),
    ];

    let results = run_parallel(operations);
    assert!(results.is_err());
}

#[test]
fn test_run_parallel_empty() {
    let operations: Vec<Box<dyn FnOnce() -> CloudResult<i32> + Send>> = vec![];

    let results = run_parallel(operations);
    assert!(results.is_ok());
    assert_eq!(results.unwrap().len(), 0);
}

// ============================================================================
// Timeout and Retry Tests
// ============================================================================

#[test]
fn test_run_with_timeout_and_retry_success() {
    let retry_config = RetryConfig::default();
    let timeout = Duration::from_secs(5);
    let mut attempts = 0;

    let result = run_with_timeout_and_retry(&retry_config, timeout, || {
        attempts += 1;
        if attempts < 2 {
            Err(CloudIOError::new(ErrorKind::Network, "Retry me"))
        } else {
            Ok(42)
        }
    });

    assert!(result.is_ok());
    assert_eq!(result.unwrap(), 42);
}

#[test]
fn test_run_with_timeout_and_retry_no_retry_needed() {
    let retry_config = RetryConfig::default();
    let timeout = Duration::from_secs(5);

    let result = run_with_timeout_and_retry(&retry_config, timeout, || Ok(100));

    assert!(result.is_ok());
    assert_eq!(result.unwrap(), 100);
}

// ============================================================================
// Batch Operation Tests
// ============================================================================

#[test]
fn test_run_batch_operation() {
    let items = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
    let config = BatchConfig {
        chunk_size: 3,
        parallel: false,
    };

    let result = run_batch_operation(&items, &config, |chunk| {
        Ok(chunk.iter().map(|x| x * 2).collect())
    });

    assert!(result.is_ok());
    let results = result.unwrap();
    assert_eq!(results.len(), 10);
    assert_eq!(results[0], 2);
    assert_eq!(results[9], 20);
}

#[test]
fn test_run_batch_operation_with_error() {
    let items = vec![1, 2, 3, 4, 5];
    let config = BatchConfig::default();

    let result = run_batch_operation(&items, &config, |chunk| {
        if chunk.contains(&3) {
            Err(CloudIOError::new(
                ErrorKind::InvalidInput,
                "Cannot process 3",
            ))
        } else {
            Ok(chunk.iter().map(|x| x * 2).collect())
        }
    });

    assert!(result.is_err());
}

#[test]
fn test_run_batch_operation_empty() {
    let items: Vec<i32> = vec![];
    let config = BatchConfig::default();

    let result = run_batch_operation(&items, &config, |chunk| {
        Ok(chunk.iter().map(|x| x * 2).collect())
    });

    assert!(result.is_ok());
    assert_eq!(result.unwrap().len(), 0);
}

#[test]
fn test_batch_config_default() {
    let config = BatchConfig::default();
    assert_eq!(config.chunk_size, 100);
    assert!(!config.parallel);
}

// ============================================================================
// Pagination Tests
// ============================================================================

#[test]
fn test_run_paginated_operation() {
    let config = PaginationConfig {
        page_size: 5,
        max_pages: Some(3),
    };

    let result = run_paginated_operation(&config, |page, size| {
        let start = page * size;
        let items: Vec<i32> = (start..start + size).map(u32::cast_signed).collect();
        let has_more = page < 5;
        Ok((items, has_more))
    });

    assert!(result.is_ok());
    let items = result.unwrap();
    assert_eq!(items.len(), 15); // 3 pages * 5 items
}

#[test]
fn test_run_paginated_operation_no_pages() {
    let config = PaginationConfig {
        page_size: 10,
        max_pages: Some(5),
    };

    let result = run_paginated_operation(&config, |_page, _size| {
        let items: Vec<i32> = vec![];
        Ok((items, false))
    });

    assert!(result.is_ok());
    assert_eq!(result.unwrap().len(), 0);
}

// ============================================================================
// Operation Builder Tests
// ============================================================================

#[test]
fn test_operation_builder_no_config() {
    let result = OperationBuilder::new().execute(|| Ok(100));

    assert!(result.is_ok());
    assert_eq!(result.unwrap(), 100);
}

#[test]
fn test_operation_builder_with_retry() {
    let mut attempts = 0;
    let retry_config = RetryConfig {
        max_attempts: 3,
        initial_delay_ms: 1,
        max_delay_ms: 10,
        backoff_multiplier: 2.0,
    };

    let result = OperationBuilder::new()
        .with_retry(retry_config)
        .execute(|| {
            attempts += 1;
            if attempts < 2 {
                Err(CloudIOError::new(ErrorKind::Network, "Retry"))
            } else {
                Ok("success")
            }
        });

    assert!(result.is_ok());
    assert_eq!(result.unwrap(), "success");
    assert_eq!(attempts, 2);
}

#[test]
fn test_operation_builder_with_timeout() {
    let result = OperationBuilder::new()
        .with_timeout(Duration::from_secs(5))
        .execute(|| Ok(42));

    assert!(result.is_ok());
    assert_eq!(result.unwrap(), 42);
}

#[test]
fn test_operation_builder_with_both() {
    let retry_config = RetryConfig {
        max_attempts: 2,
        initial_delay_ms: 1,
        max_delay_ms: 10,
        backoff_multiplier: 2.0,
    };

    let result = OperationBuilder::new()
        .with_retry(retry_config)
        .with_timeout(Duration::from_secs(5))
        .execute(|| Ok(123));

    assert!(result.is_ok());
    assert_eq!(result.unwrap(), 123);
}

#[test]
fn test_operation_builder_default() {
    let builder = OperationBuilder::default();
    let result = builder.execute(|| Ok(42));
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), 42);
}

// ============================================================================
// Operation Context Tests
// ============================================================================

#[test]
fn test_operation_context() {
    let mut context = OperationContext::new("test_operation");
    assert_eq!(context.operation_name, "test_operation");
    assert_eq!(context.retry_count, 0);

    context.add_metadata("key1", "value1");
    context.add_metadata("key2", "value2");
    assert_eq!(context.metadata.len(), 2);
    assert_eq!(context.metadata.get("key1").unwrap(), "value1");

    context.increment_retry();
    context.increment_retry();
    assert_eq!(context.retry_count, 2);

    // Verify that elapsed time can be retrieved
    let elapsed = context.elapsed();
    assert!(elapsed >= Duration::from_millis(0));
}

#[test]
fn test_run_with_context() {
    let context = OperationContext::new("upload_file");

    let result = run_with_context(context, |ctx| {
        ctx.add_metadata("file", "test.txt");
        ctx.increment_retry();
        Ok("uploaded")
    });

    assert!(result.is_ok());
    let (value, final_context) = result.unwrap();
    assert_eq!(value, "uploaded");
    assert_eq!(final_context.retry_count, 1);
    assert_eq!(final_context.metadata.get("file").unwrap(), "test.txt");
}

#[test]
fn test_run_with_context_error() {
    let context = OperationContext::new("failing_operation");

    let result: CloudResult<(i32, OperationContext)> = run_with_context(context, |_ctx| {
        Err(CloudIOError::new(
            ErrorKind::InternalError,
            "Operation failed",
        ))
    });

    assert!(result.is_err());
    assert_eq!(result.unwrap_err().kind, ErrorKind::InternalError);
}

// ============================================================================
// Complex Integration Tests
// ============================================================================

#[test]
fn test_complex_nested_operation() {
    // Simulate a complex operation combining multiple helpers
    let items = vec!["file1.txt", "file2.txt", "file3.txt"];
    let batch_config = BatchConfig {
        chunk_size: 2,
        parallel: false,
    };
    let retry_config = RetryConfig {
        max_attempts: 2,
        initial_delay_ms: 1,
        max_delay_ms: 10,
        backoff_multiplier: 2.0,
    };

    let result = run_batch_operation(&items, &batch_config, |chunk| {
        // Each batch operation has retry logic
        let chunk_results: CloudResult<Vec<String>> = chunk
            .iter()
            .map(|&file| {
                run_with_retry(&retry_config, || {
                    // Simulate file upload
                    Ok(format!("uploaded:{file}"))
                })
            })
            .collect();
        chunk_results
    });

    assert!(result.is_ok());
    let results = result.unwrap();
    assert_eq!(results.len(), 3);
    assert_eq!(results[0], "uploaded:file1.txt");
    assert_eq!(results[2], "uploaded:file3.txt");
}

#[test]
fn test_builder_with_context() {
    let context = OperationContext::new("complex_upload");
    let retry_config = RetryConfig {
        max_attempts: 2,
        initial_delay_ms: 1,
        max_delay_ms: 10,
        backoff_multiplier: 2.0,
    };

    let result = run_with_context(context, |ctx| {
        ctx.add_metadata("operation_type", "batch_upload");

        OperationBuilder::new()
            .with_retry(retry_config)
            .with_timeout(Duration::from_secs(10))
            .execute(|| {
                ctx.increment_retry();
                Ok(42)
            })
    });

    assert!(result.is_ok());
    let (value, final_context) = result.unwrap();
    assert_eq!(value, 42);
    assert_eq!(final_context.retry_count, 1);
    assert_eq!(
        final_context.metadata.get("operation_type").unwrap(),
        "batch_upload"
    );
}

// ============================================================================
// Additional Error Handling and Edge Case Tests
// ============================================================================

#[test]
fn test_run_with_retry_all_error_kinds() {
    let config = RetryConfig::default();

    // Test different error kinds
    let error_kinds = vec![
        ErrorKind::Authentication,
        ErrorKind::Authorization,
        ErrorKind::NotFound,
        ErrorKind::AlreadyExists,
        ErrorKind::InvalidInput,
        ErrorKind::Network,
        ErrorKind::Timeout,
        ErrorKind::ServiceUnavailable,
        ErrorKind::RateLimited,
        ErrorKind::InternalError,
        ErrorKind::Other,
    ];

    for kind in error_kinds {
        let result: CloudResult<i32> = run_with_retry(&config, || {
            Err(CloudIOError::new(kind.clone(), "Test error"))
        });

        // All non-retryable errors should fail on first attempt
        assert!(result.is_err());
    }
}

#[test]
fn test_run_batch_operation_single_item() {
    let items = vec![42];
    let config = BatchConfig {
        chunk_size: 10,
        parallel: false,
    };

    let result = run_batch_operation(&items, &config, |chunk| {
        Ok(chunk.iter().map(|x| x * 2).collect())
    });

    assert!(result.is_ok());
    let results = result.unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0], 84);
}

#[test]
fn test_run_batch_operation_exact_chunk_size() {
    let items = vec![1, 2, 3, 4, 5];
    let config = BatchConfig {
        chunk_size: 5,
        parallel: false,
    };

    let result = run_batch_operation(&items, &config, |chunk| {
        Ok(chunk.iter().map(|x| x * 2).collect())
    });

    assert!(result.is_ok());
    let results = result.unwrap();
    assert_eq!(results.len(), 5);
}

#[test]
fn test_run_paginated_operation_with_error() {
    let config = PaginationConfig {
        page_size: 5,
        max_pages: Some(10),
    };

    let result = run_paginated_operation(&config, |page, _size| {
        if page == 2 {
            Err(CloudIOError::new(
                ErrorKind::Network,
                "Failed to fetch page 2",
            ))
        } else {
            Ok((vec![1, 2, 3], true))
        }
    });

    assert!(result.is_err());
}

#[test]
fn test_run_paginated_operation_stops_when_no_more() {
    let config = PaginationConfig {
        page_size: 5,
        max_pages: Some(100),
    };

    let result = run_paginated_operation(&config, |page, size| {
        let items: Vec<i32> = (0..size).map(u32::cast_signed).collect();
        let has_more = page < 2; // Only 3 pages total
        Ok((items, has_more))
    });

    assert!(result.is_ok());
    let items = result.unwrap();
    assert_eq!(items.len(), 15); // 3 pages * 5 items
}

#[test]
fn test_cloud_io_executor_no_config() {
    let result = CloudIOExecutor::new().execute(|| Ok(42));

    assert!(result.is_ok());
    assert_eq!(result.unwrap(), 42);
}

#[test]
fn test_cloud_io_executor_with_retry() {
    let mut attempts = 0;
    let retry_config = RetryConfig {
        max_attempts: 3,
        initial_delay_ms: 1,
        max_delay_ms: 10,
        backoff_multiplier: 2.0,
    };

    let result = CloudIOExecutor::new().with_retry(retry_config).execute(|| {
        attempts += 1;
        if attempts < 2 {
            Err(CloudIOError::new(ErrorKind::Network, "Retry"))
        } else {
            Ok("success")
        }
    });

    assert!(result.is_ok());
    assert_eq!(result.unwrap(), "success");
    assert_eq!(attempts, 2);
}

#[test]
fn test_cloud_io_executor_with_timeout() {
    let result = CloudIOExecutor::new()
        .with_timeout(Duration::from_secs(5))
        .execute(|| Ok(100));

    assert!(result.is_ok());
    assert_eq!(result.unwrap(), 100);
}

#[test]
fn test_cloud_io_executor_with_retry_and_timeout() {
    let retry_config = RetryConfig {
        max_attempts: 2,
        initial_delay_ms: 1,
        max_delay_ms: 10,
        backoff_multiplier: 2.0,
    };

    let result = CloudIOExecutor::new()
        .with_retry(retry_config)
        .with_timeout(Duration::from_secs(5))
        .execute(|| Ok(123));

    assert!(result.is_ok());
    assert_eq!(result.unwrap(), 123);
}

#[test]
fn test_cloud_io_executor_default() {
    let executor = CloudIOExecutor::default();
    let result = executor.execute(|| Ok(42));
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), 42);
}

#[test]
fn test_run_cloud_io_with_retry_success() {
    let config = RetryConfig::default();
    let mut attempts = 0;

    let result = run_cloud_io_with_retry(&config, || {
        attempts += 1;
        if attempts < 2 {
            Err(CloudIOError::new(ErrorKind::Network, "Temporary"))
        } else {
            Ok(vec![1, 2, 3])
        }
    });

    assert!(result.is_ok());
    assert_eq!(result.unwrap(), vec![1, 2, 3]);
    assert_eq!(attempts, 2);
}

#[test]
fn test_run_cloud_io_batch_success() {
    let config = RetryConfig::default();
    let items = vec!["a", "b", "c"];

    let result = run_cloud_io_batch(&config, &items, |item| Ok(format!("processed:{item}")));

    assert!(result.is_ok());
    let results = result.unwrap();
    assert_eq!(results.len(), 3);
    assert_eq!(results[0], "processed:a");
}

#[test]
fn test_run_cloud_io_batch_with_retry() {
    let config = RetryConfig {
        max_attempts: 3,
        initial_delay_ms: 1,
        max_delay_ms: 10,
        backoff_multiplier: 2.0,
    };
    let items = vec![1, 2, 3];
    let mut fail_count = 0;

    let result = run_cloud_io_batch(&config, &items, |item| {
        if *item == 2 && fail_count < 1 {
            fail_count += 1;
            Err(CloudIOError::new(ErrorKind::Network, "Transient error"))
        } else {
            Ok(*item * 2)
        }
    });

    assert!(result.is_ok());
    let results = result.unwrap();
    assert_eq!(results, vec![2, 4, 6]);
}

#[test]
fn test_run_cloud_io_batch_empty() {
    let config = RetryConfig::default();
    let items: Vec<i32> = vec![];

    let result = run_cloud_io_batch(&config, &items, |item| Ok(*item * 2));

    assert!(result.is_ok());
    assert_eq!(result.unwrap().len(), 0);
}

#[test]
fn test_run_cloud_io_paginated_basic() {
    let config = PaginationConfig {
        page_size: 3,
        max_pages: Some(2),
    };

    let result = run_cloud_io_paginated(&config, |page, size| {
        let start = page * size;
        let items: Vec<String> = (start..start + size).map(|i| format!("item{i}")).collect();
        Ok((items, true))
    });

    assert!(result.is_ok());
    let items = result.unwrap();
    assert_eq!(items.len(), 6); // 2 pages * 3 items
}

#[test]
fn test_run_cloud_io_with_retry_and_timeout_success() {
    let retry_config = RetryConfig::default();
    let timeout = Duration::from_secs(5);

    let result =
        run_cloud_io_with_retry_and_timeout(&retry_config, timeout, || Ok("completed".to_string()));

    assert!(result.is_ok());
    assert_eq!(result.unwrap(), "completed");
}

#[test]
fn test_operation_context_metadata() {
    let mut context = OperationContext::new("test");

    context.add_metadata("key1", "value1");
    context.add_metadata("key2", 42.to_string());
    context.add_metadata("key3", String::from("value3"));

    assert_eq!(context.metadata.len(), 3);
    assert_eq!(context.metadata.get("key1").unwrap(), "value1");
    assert_eq!(context.metadata.get("key2").unwrap(), "42");
    assert_eq!(context.metadata.get("key3").unwrap(), "value3");
}

#[test]
fn test_operation_context_elapsed_time() {
    use std::thread;

    let context = OperationContext::new("timed_operation");

    // Wait a small amount
    thread::sleep(Duration::from_millis(10));

    let elapsed = context.elapsed();
    assert!(elapsed >= Duration::from_millis(10));
}

#[test]
fn test_batch_config_custom() {
    let config = BatchConfig {
        chunk_size: 50,
        parallel: true,
    };

    assert_eq!(config.chunk_size, 50);
    assert!(config.parallel);
}

#[test]
fn test_run_parallel_single_operation() {
    let operations: Vec<Box<dyn FnOnce() -> CloudResult<i32> + Send>> = vec![Box::new(|| Ok(42))];

    let results = run_parallel(operations);
    assert!(results.is_ok());
    let values = results.unwrap();
    assert_eq!(values, vec![42]);
}

#[test]
fn test_run_with_retry_first_success() {
    let config = RetryConfig::default();

    let result = run_with_retry(&config, || Ok("immediate success"));

    assert!(result.is_ok());
    assert_eq!(result.unwrap(), "immediate success");
}

#[test]
fn test_operation_builder_chaining() {
    let retry_config = RetryConfig {
        max_attempts: 2,
        initial_delay_ms: 1,
        max_delay_ms: 10,
        backoff_multiplier: 2.0,
    };

    let result = OperationBuilder::new()
        .with_retry(retry_config)
        .with_timeout(Duration::from_secs(5))
        .execute(|| Ok(999));

    assert!(result.is_ok());
    assert_eq!(result.unwrap(), 999);
}

#[test]
fn test_run_with_context_multiple_retries() {
    let context = OperationContext::new("multi_retry");
    let mut attempts = 0;

    let result = run_with_context(context, |ctx| {
        attempts += 1;
        ctx.increment_retry();
        if attempts < 3 {
            Err(CloudIOError::new(ErrorKind::Network, "Retry"))
        } else {
            Ok("finally succeeded")
        }
    });

    // Note: run_with_context doesn't automatically retry, it just tracks context
    // So this will fail on first attempt
    assert!(result.is_err());
}

#[test]
fn test_run_cloud_io_batch_with_error() {
    let config = RetryConfig {
        max_attempts: 1,
        initial_delay_ms: 1,
        max_delay_ms: 10,
        backoff_multiplier: 2.0,
    };
    let items = vec![1, 2, 3];

    let result = run_cloud_io_batch(&config, &items, |item| {
        if *item == 2 {
            Err(CloudIOError::new(
                ErrorKind::InvalidInput,
                "Cannot process 2",
            ))
        } else {
            Ok(*item * 2)
        }
    });

    assert!(result.is_err());
}
