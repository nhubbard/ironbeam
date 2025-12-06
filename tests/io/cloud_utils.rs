use ironbeam::io::cloud::helpers::*;
use ironbeam::io::cloud::traits::{CloudIOError, ErrorKind};

#[test]
fn test_retry_with_backoff() {
    let config = RetryConfig::default();
    let mut attempts = 0;

    let result = retry_with_backoff(&config, || {
        attempts += 1;
        if attempts < 3 {
            Err(CloudIOError::new(ErrorKind::Network, "Temporary failure"))
        } else {
            Ok(42)
        }
    });

    assert!(result.is_ok());
    assert_eq!(result.unwrap(), 42);
    assert_eq!(attempts, 3);
}

#[test]
fn test_batch_in_chunks() {
    let items: Vec<i32> = (1..=10).collect();
    let result = batch_in_chunks(&items, 3, |chunk| Ok(chunk.iter().map(|x| x * 2).collect()));

    assert!(result.is_ok());
    let results = result.unwrap();
    assert_eq!(results.len(), 10);
    assert_eq!(results[0], 2);
    assert_eq!(results[9], 20);
}

#[test]
fn test_parse_resource_uri() {
    let (provider, parts) = parse_resource_uri("s3://my-bucket/my-key").unwrap();
    assert_eq!(provider, "s3");
    assert_eq!(parts, vec!["my-bucket", "my-key"]);

    let (provider, parts) = parse_resource_uri("bigquery://project/dataset/table").unwrap();
    assert_eq!(provider, "bigquery");
    assert_eq!(parts, vec!["project", "dataset", "table"]);
}

#[test]
fn test_validate_resource_name() {
    assert!(validate_resource_name("my-resource").is_ok());
    assert!(validate_resource_name("my_resource").is_ok());
    assert!(validate_resource_name("my.resource").is_ok());
    assert!(validate_resource_name("").is_err());
    assert!(validate_resource_name("invalid name with spaces").is_err());
}

#[test]
fn test_validate_key_path() {
    assert!(validate_key_path("path/to/key").is_ok());
    assert!(validate_key_path("key").is_ok());
    assert!(validate_key_path("").is_err());
    assert!(validate_key_path("/absolute/path").is_err());
}

#[test]
fn test_connection_pool() {
    let mut pool = ConnectionPool::new(2);

    // Acquire the first connection
    let conn1 = pool.acquire(|| Ok(1)).unwrap();
    assert_eq!(conn1, 1);
    assert_eq!(pool.size(), 0);

    // Release and reacquire
    pool.release(conn1);
    assert_eq!(pool.size(), 1);

    let conn2 = pool.acquire(|| Ok(2)).unwrap();
    assert_eq!(conn2, 1); // Should get the released connection
    assert_eq!(pool.size(), 0);
}

#[test]
fn test_pagination() {
    let config = PaginationConfig {
        page_size: 10,
        max_pages: Some(3),
    };

    let result = paginate(&config, |page, page_size| {
        let start = u64::from(page * page_size);
        let end = start + u64::from(page_size);
        let items: Vec<i32> = (start..end)
            .map(|x| i32::try_from(x).unwrap_or(0))
            .collect();
        let has_more = page < 5;
        Ok((items, has_more))
    });

    assert!(result.is_ok());
    let items = result.unwrap();
    assert_eq!(items.len(), 30); // 3 pages * 10 items
}
