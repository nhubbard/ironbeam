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
