// Integration tests for cloud IO abstractions
//
// These tests demonstrate real-world usage patterns with fake implementations
// that run entirely within the test process, simulating a complete cloud environment.

use anyhow::Result;
use ironbeam::io::cloud::*;
use std::collections::HashMap;

// ============================================================================
// Object Storage Tests
// ============================================================================

#[test]
fn test_object_storage_upload_download() -> Result<()> {
    let storage = FakeObjectIO::new();

    // Upload
    let data = b"Hello, World!";
    storage.put_object("test-bucket", "data/file.txt", data)?;

    // Download
    let downloaded = storage.get_object("test-bucket", "data/file.txt")?;
    assert_eq!(downloaded, data);

    Ok(())
}

#[test]
fn test_object_storage_list() -> Result<()> {
    let storage = FakeObjectIO::new();

    // Upload multiple files
    storage.put_object("bucket", "dir1/file1.txt", b"data1")?;
    storage.put_object("bucket", "dir1/file2.txt", b"data2")?;
    storage.put_object("bucket", "dir2/file3.txt", b"data3")?;

    // List all
    let all_objects = storage.list_objects("bucket", None)?;
    assert_eq!(all_objects.len(), 3);

    // List with prefix
    let dir1_objects = storage.list_objects("bucket", Some("dir1/"))?;
    assert_eq!(dir1_objects.len(), 2);
    assert!(dir1_objects.iter().all(|o| o.key.starts_with("dir1/")));

    Ok(())
}

#[test]
fn test_object_storage_copy() -> Result<()> {
    let storage = FakeObjectIO::new();

    storage.put_object("bucket", "source.txt", b"original")?;
    storage.copy_object("bucket", "source.txt", "bucket", "dest.txt")?;

    let copied = storage.get_object("bucket", "dest.txt")?;
    assert_eq!(copied, b"original");

    // Original still exists
    assert!(storage.object_exists("bucket", "source.txt")?);

    Ok(())
}

#[test]
fn test_object_storage_not_found() {
    let storage = FakeObjectIO::new();
    let result = storage.get_object("bucket", "nonexistent.txt");
    assert!(result.is_err());
    assert_eq!(result.unwrap_err().kind, ErrorKind::NotFound);
}

// ============================================================================
// Key-Value Store Tests
// ============================================================================

#[test]
fn test_kv_basic_operations() -> Result<()> {
    let kv = FakeKeyValueIO::new();

    let mut doc = HashMap::new();
    doc.insert("name".to_string(), "Alice".to_string());
    doc.insert("age".to_string(), "30".to_string());

    // Put
    kv.put("users", "user1", doc.clone())?;

    // Get
    let retrieved = kv.get("users", "user1")?.unwrap();
    assert_eq!(retrieved.data.get("name").unwrap(), "Alice");
    assert_eq!(retrieved.data.get("age").unwrap(), "30");

    // Exists
    assert!(kv.exists("users", "user1")?);
    assert!(!kv.exists("users", "user999")?);

    // Delete
    kv.delete("users", "user1")?;
    assert!(!kv.exists("users", "user1")?);

    Ok(())
}

#[test]
fn test_kv_query() -> Result<()> {
    let kv = FakeKeyValueIO::new();

    // Add test data
    for i in 1..=5 {
        let mut doc = HashMap::new();
        doc.insert("id".to_string(), i.to_string());
        doc.insert(
            "category".to_string(),
            if i % 2 == 0 { "even" } else { "odd" }.to_string(),
        );
        kv.put("numbers", &format!("num{i}"), doc)?;
    }

    // Query for even numbers
    let mut filter = HashMap::new();
    filter.insert("category".to_string(), "even".to_string());
    let results = kv.query("numbers", filter)?;

    assert_eq!(results.len(), 2);
    for doc in results {
        assert_eq!(doc.data.get("category").unwrap(), "even");
    }

    Ok(())
}

#[test]
fn test_kv_batch_operations() -> Result<()> {
    let kv = FakeKeyValueIO::new();

    // Batch put
    let docs = vec![
        (
            "key1".to_string(),
            vec![("field".to_string(), "value1".to_string())]
                .into_iter()
                .collect(),
        ),
        (
            "key2".to_string(),
            vec![("field".to_string(), "value2".to_string())]
                .into_iter()
                .collect(),
        ),
    ];
    kv.batch_put("collection", docs)?;

    // Batch get
    let keys = vec!["key1".to_string(), "key2".to_string(), "key3".to_string()];
    let results = kv.batch_get("collection", keys)?;

    assert_eq!(results.len(), 3);
    assert!(results[0].is_some());
    assert!(results[1].is_some());
    assert!(results[2].is_none());

    Ok(())
}

// ============================================================================
// Queue Tests
// ============================================================================

#[test]
fn test_queue_send_receive() -> Result<()> {
    let queue = FakeQueueIO::new();

    // Send messages
    let msg1_id = queue.send("work-queue", "task1", HashMap::new())?;
    let msg2_id = queue.send("work-queue", "task2", HashMap::new())?;
    assert_ne!(msg1_id, msg2_id);

    // Check size
    assert_eq!(queue.queue_size("work-queue")?, 2);

    // Receive messages
    let messages = queue.receive("work-queue", 10, 30)?;
    assert_eq!(messages.len(), 2);
    assert_eq!(messages[0].body, "task1");
    assert_eq!(messages[1].body, "task2");

    // Queue should be empty after receive
    assert_eq!(queue.queue_size("work-queue")?, 0);

    Ok(())
}

#[test]
fn test_queue_batch_operations() -> Result<()> {
    let queue = FakeQueueIO::new();

    // Batch send
    let messages = vec![
        ("msg1".to_string(), HashMap::new()),
        ("msg2".to_string(), HashMap::new()),
        ("msg3".to_string(), HashMap::new()),
    ];
    let ids = queue.send_batch("queue", messages)?;
    assert_eq!(ids.len(), 3);

    // Receive limited
    let received = queue.receive("queue", 2, 30)?;
    assert_eq!(received.len(), 2);

    Ok(())
}

#[test]
fn test_queue_purge() -> Result<()> {
    let queue = FakeQueueIO::new();

    queue.send("queue", "msg1", HashMap::new())?;
    queue.send("queue", "msg2", HashMap::new())?;
    assert_eq!(queue.queue_size("queue")?, 2);

    queue.purge("queue")?;
    assert_eq!(queue.queue_size("queue")?, 0);

    Ok(())
}

// ============================================================================
// Cache Tests
// ============================================================================

#[test]
fn test_cache_basic_operations() -> Result<()> {
    let cache = FakeCacheIO::new();

    // Set and get
    cache.set("key1", b"value1", None)?;
    let value = cache.get("key1")?.unwrap();
    assert_eq!(value, b"value1");

    // Exists
    assert!(cache.exists("key1")?);
    assert!(!cache.exists("nonexistent")?);

    // Delete
    cache.delete("key1")?;
    assert!(!cache.exists("key1")?);

    Ok(())
}

#[test]
fn test_cache_increment() -> Result<()> {
    let cache = FakeCacheIO::new();

    cache.set("counter", b"10", None)?;

    let val1 = cache.increment("counter", 5)?;
    assert_eq!(val1, 15);

    let val2 = cache.increment("counter", -3)?;
    assert_eq!(val2, 12);

    Ok(())
}

#[test]
fn test_cache_batch_operations() -> Result<()> {
    let cache = FakeCacheIO::new();

    // Batch set
    let items = vec![
        ("key1".to_string(), b"val1".to_vec(), None),
        ("key2".to_string(), b"val2".to_vec(), Some(60)),
    ];
    cache.set_batch(items)?;

    // Batch get
    let keys = vec!["key1".to_string(), "key2".to_string(), "key3".to_string()];
    let results = cache.get_batch(keys)?;

    assert_eq!(results.len(), 3);
    assert_eq!(results[0].as_ref().unwrap(), b"val1");
    assert_eq!(results[1].as_ref().unwrap(), b"val2");
    assert!(results[2].is_none());

    Ok(())
}

// ============================================================================
// Search Tests
// ============================================================================

#[test]
fn test_search_index_and_search() -> Result<()> {
    let search = FakeSearchIO::new();

    // Index documents
    let mut doc1 = HashMap::new();
    doc1.insert("title".to_string(), "Rust Programming".to_string());
    doc1.insert("content".to_string(), "Learn Rust language".to_string());
    search.index("docs", "doc1", doc1)?;

    let mut doc2 = HashMap::new();
    doc2.insert("title".to_string(), "Python Guide".to_string());
    doc2.insert("content".to_string(), "Python basics".to_string());
    search.index("docs", "doc2", doc2)?;

    // Search
    let query = SearchQuery {
        query: "Rust".to_string(),
        filters: HashMap::new(),
        limit: 10,
        offset: 0,
    };
    let results = search.search("docs", query)?;

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].id, "doc1");

    Ok(())
}

#[test]
fn test_search_with_filters() -> Result<()> {
    let search = FakeSearchIO::new();

    // Index with categories
    for i in 1..=5 {
        let mut doc = HashMap::new();
        doc.insert("content".to_string(), format!("document {i}"));
        doc.insert(
            "category".to_string(),
            if i % 2 == 0 { "even" } else { "odd" }.to_string(),
        );
        search.index("docs", &format!("doc{i}"), doc)?;
    }

    // Search with filter
    let mut filters = HashMap::new();
    filters.insert("category".to_string(), "even".to_string());
    let query = SearchQuery {
        query: "document".to_string(),
        filters,
        limit: 10,
        offset: 0,
    };
    let results = search.search("docs", query)?;

    assert_eq!(results.len(), 2);
    for hit in results {
        assert_eq!(hit.fields.get("category").unwrap(), "even");
    }

    Ok(())
}

#[test]
fn test_search_pagination() -> Result<()> {
    let search = FakeSearchIO::new();

    // Index many documents
    for i in 1..=10 {
        let mut doc = HashMap::new();
        doc.insert("content".to_string(), "test document".to_string());
        search.index("docs", &format!("doc{i}"), doc)?;
    }

    // First page
    let query1 = SearchQuery {
        query: "test".to_string(),
        filters: HashMap::new(),
        limit: 3,
        offset: 0,
    };
    let results1 = search.search("docs", query1)?;
    assert_eq!(results1.len(), 3);

    // Second page
    let query2 = SearchQuery {
        query: "test".to_string(),
        filters: HashMap::new(),
        limit: 3,
        offset: 3,
    };
    let results2 = search.search("docs", query2)?;
    assert_eq!(results2.len(), 3);

    // Verify different results
    assert_ne!(results1[0].id, results2[0].id);

    Ok(())
}

// ============================================================================
// Graph Database Tests
// ============================================================================

#[test]
fn test_graph_nodes() -> Result<()> {
    let graph = FakeGraphIO::new();

    // Add node
    let mut props = HashMap::new();
    props.insert("name".to_string(), "Alice".to_string());
    let node_id = graph.add_node(vec!["Person".to_string()], props.clone())?;

    // Get node
    let node = graph.get_node(&node_id)?.unwrap();
    assert_eq!(node.labels, vec!["Person"]);
    assert_eq!(node.properties.get("name").unwrap(), "Alice");

    // Update node
    let mut update = HashMap::new();
    update.insert("age".to_string(), "30".to_string());
    graph.update_node(&node_id, update)?;

    let updated = graph.get_node(&node_id)?.unwrap();
    assert_eq!(updated.properties.get("age").unwrap(), "30");

    // Delete node
    graph.delete_node(&node_id)?;
    assert!(graph.get_node(&node_id)?.is_none());

    Ok(())
}

#[test]
fn test_graph_edges_and_neighbors() -> Result<()> {
    let graph = FakeGraphIO::new();

    // Create nodes
    let alice = graph.add_node(
        vec!["Person".to_string()],
        vec![("name".to_string(), "Alice".to_string())]
            .into_iter()
            .collect(),
    )?;
    let bob = graph.add_node(
        vec!["Person".to_string()],
        vec![("name".to_string(), "Bob".to_string())]
            .into_iter()
            .collect(),
    )?;
    let charlie = graph.add_node(
        vec!["Person".to_string()],
        vec![("name".to_string(), "Charlie".to_string())]
            .into_iter()
            .collect(),
    )?;

    // Create edges
    graph.add_edge(&alice, &bob, "KNOWS", HashMap::new())?;
    graph.add_edge(&bob, &charlie, "KNOWS", HashMap::new())?;

    // Find outgoing neighbors
    let alice_neighbors = graph.get_neighbors(&alice, EdgeDirection::Outgoing)?;
    assert_eq!(alice_neighbors.len(), 1);
    assert_eq!(alice_neighbors[0].properties.get("name").unwrap(), "Bob");

    // Find incoming neighbors
    let bob_incoming = graph.get_neighbors(&bob, EdgeDirection::Incoming)?;
    assert_eq!(bob_incoming.len(), 1);
    assert_eq!(bob_incoming[0].properties.get("name").unwrap(), "Alice");

    // Find all neighbors
    let bob_all = graph.get_neighbors(&bob, EdgeDirection::Both)?;
    assert_eq!(bob_all.len(), 2);

    Ok(())
}

// ============================================================================
// Compute Tests
// ============================================================================

#[test]
fn test_compute_invoke_sync() -> Result<()> {
    let compute = FakeComputeIO::new();

    // Register function
    compute.register_function("uppercase", |input| {
        String::from_utf8_lossy(input).to_uppercase().into_bytes()
    });

    // Invoke
    let input = b"hello world";
    let result = compute.invoke("uppercase", input)?;

    assert_eq!(result.status_code, 200);
    assert_eq!(result.output, b"HELLO WORLD");
    assert!(result.execution_time_ms > 0);

    Ok(())
}

#[test]
fn test_compute_invoke_async() -> Result<()> {
    let compute = FakeComputeIO::new();

    compute.register_function("process", <[u8]>::to_vec);

    let invocation_id = compute.invoke_async("process", b"data")?;
    assert!(!invocation_id.is_empty());

    let status = compute.get_invocation_status(&invocation_id)?;
    assert_eq!(status, InvocationStatus::Succeeded);

    Ok(())
}

#[test]
fn test_compute_list_functions() -> Result<()> {
    let compute = FakeComputeIO::new();

    compute.register_function("func1", <[u8]>::to_vec);
    compute.register_function("func2", <[u8]>::to_vec);

    let functions = compute.list_functions()?;
    assert_eq!(functions.len(), 2);
    assert!(functions.contains(&"func1".to_string()));
    assert!(functions.contains(&"func2".to_string()));

    Ok(())
}

// ============================================================================
// Intelligence (ML) Tests
// ============================================================================

#[test]
fn test_intelligence_predict() -> Result<()> {
    let ai = FakeIntelligenceIO::new();

    // Register a sentiment model
    ai.register_model("sentiment", |input| {
        let text = String::from_utf8_lossy(input);
        let sentiment = if text.contains("great") || text.contains("excellent") {
            "positive"
        } else if text.contains("bad") || text.contains("terrible") {
            "negative"
        } else {
            "neutral"
        };
        format!(r#"{{"sentiment": "{sentiment}"}}"#).into_bytes()
    });

    // Predict positive
    let input = InferenceInput {
        data: b"This is a great product!".to_vec(),
        content_type: "text/plain".to_string(),
    };
    let output = ai.predict("sentiment", input)?;
    assert!(String::from_utf8_lossy(&output.data).contains("positive"));

    // Predict negative
    let input2 = InferenceInput {
        data: b"This is terrible".to_vec(),
        content_type: "text/plain".to_string(),
    };
    let output2 = ai.predict("sentiment", input2)?;
    assert!(String::from_utf8_lossy(&output2.data).contains("negative"));

    Ok(())
}

#[test]
fn test_intelligence_batch_predict() -> Result<()> {
    let ai = FakeIntelligenceIO::new();

    ai.register_model("echo", <[u8]>::to_vec);

    let inputs = vec![
        InferenceInput {
            data: b"input1".to_vec(),
            content_type: "text/plain".to_string(),
        },
        InferenceInput {
            data: b"input2".to_vec(),
            content_type: "text/plain".to_string(),
        },
    ];

    let outputs = ai.predict_batch("echo", inputs)?;
    assert_eq!(outputs.len(), 2);
    assert_eq!(outputs[0].data, b"input1");
    assert_eq!(outputs[1].data, b"input2");

    Ok(())
}

// ============================================================================
// Notification Tests
// ============================================================================

#[test]
fn test_notification_send() -> Result<()> {
    let notif = FakeNotificationIO::new();

    let notification = Notification {
        target: "user@example.com".to_string(),
        subject: Some("Test".to_string()),
        message: "Hello!".to_string(),
        attributes: HashMap::new(),
    };

    let result = notif.send(notification)?;
    assert_eq!(result.status, NotificationStatus::Sent);
    assert!(!result.message_id.is_empty());

    Ok(())
}

#[test]
fn test_notification_topics() -> Result<()> {
    let notif = FakeNotificationIO::new();

    // Create topic
    let topic_id = notif.create_topic("alerts")?;
    assert_eq!(topic_id, "alerts");

    // Subscribe
    let sub_id = notif.subscribe("alerts", "user@example.com", "email")?;
    assert!(!sub_id.is_empty());

    // Unsubscribe
    notif.unsubscribe(&sub_id)?;

    // Delete topic
    notif.delete_topic("alerts")?;

    Ok(())
}

// ============================================================================
// PubSub Tests
// ============================================================================

#[test]
fn test_pubsub_publish_pull() -> Result<()> {
    let pubsub = FakePubSubIO::new();

    // Create subscription
    pubsub.subscribe("events", "my-subscription")?;

    // Publish
    let msg_id = pubsub.publish("events", b"event data", HashMap::new())?;
    assert!(!msg_id.is_empty());

    // Pull (note: fake implementation doesn't deliver to subscriptions automatically)
    assert!(pubsub.topic_exists("events")?);

    Ok(())
}

#[test]
fn test_pubsub_batch_publish() -> Result<()> {
    let pubsub = FakePubSubIO::new();

    let messages = vec![
        (b"msg1".to_vec(), HashMap::new()),
        (b"msg2".to_vec(), HashMap::new()),
    ];

    let ids = pubsub.publish_batch("topic", messages)?;
    assert_eq!(ids.len(), 2);
    assert_ne!(ids[0], ids[1]);

    Ok(())
}

// ============================================================================
// Warehouse Tests
// ============================================================================

#[test]
fn test_warehouse_query() -> Result<()> {
    let warehouse = FakeWarehouseIO::new();

    // Setup test table
    let schema = vec![
        ("id".to_string(), "INTEGER".to_string()),
        ("name".to_string(), "STRING".to_string()),
    ];
    let data = vec![
        vec!["1".to_string(), "Alice".to_string()],
        vec!["2".to_string(), "Bob".to_string()],
    ];
    warehouse.add_table("users", schema, data);

    // Query
    let result = warehouse.query("SELECT * FROM users")?;
    assert_eq!(result.row_count, 2);
    assert_eq!(result.columns, vec!["id", "name"]);
    assert_eq!(result.rows[0], vec!["1", "Alice"]);

    Ok(())
}

#[test]
fn test_warehouse_table_operations() -> Result<()> {
    let warehouse = FakeWarehouseIO::new();

    warehouse.add_table(
        "test_table",
        vec![("col1".to_string(), "INT".to_string())],
        vec![],
    );

    assert!(warehouse.table_exists("test_table")?);
    assert!(!warehouse.table_exists("nonexistent")?);

    let schema = warehouse.get_schema("test_table")?;
    assert_eq!(schema.len(), 1);
    assert_eq!(schema[0].0, "col1");

    Ok(())
}

// ============================================================================
// Database Tests
// ============================================================================

#[test]
fn test_database_query() -> Result<()> {
    let db = FakeDatabaseIO::new();

    db.add_table("users", vec![("id".to_string(), "INTEGER".to_string())]);

    let result = db.query("SELECT * FROM users", vec![])?;
    assert_eq!(result.len(), 0);

    Ok(())
}

#[test]
fn test_database_transaction() -> Result<()> {
    let db = FakeDatabaseIO::new();

    let mut tx = db.begin_transaction()?;
    tx.execute("INSERT INTO test VALUES (1)", vec![])?;
    tx.commit()?;

    Ok(())
}

// ============================================================================
// Metric Tests
// ============================================================================

#[test]
fn test_metrics_put_and_query() -> Result<()> {
    let metrics = FakeMetricIO::new();

    // Put metrics
    let metric_point_1 = MetricPoint {
        name: "requests".to_string(),
        value: 100.0,
        timestamp: 1000,
        tags: vec![("service".to_string(), "api".to_string())]
            .into_iter()
            .collect(),
    };
    metrics.put_metric("prod", metric_point_1)?;

    let metric_point_2 = MetricPoint {
        name: "requests".to_string(),
        value: 150.0,
        timestamp: 2000,
        tags: vec![("service".to_string(), "api".to_string())]
            .into_iter()
            .collect(),
    };
    metrics.put_metric("prod", metric_point_2)?;

    // Query
    let query = MetricQuery {
        metric_name: "requests".to_string(),
        start_time: 0,
        end_time: 3000,
        aggregation: None,
        tags: vec![("service".to_string(), "api".to_string())]
            .into_iter()
            .collect(),
    };
    let results = metrics.query_metrics("prod", query)?;

    assert_eq!(results.len(), 2);
    assert!((results[0].value - 100.0).abs() < f64::EPSILON);
    assert!((results[1].value - 150.0).abs() < f64::EPSILON);

    Ok(())
}

// ============================================================================
// Config Tests
// ============================================================================

#[test]
fn test_config_operations() -> Result<()> {
    let config = FakeConfigIO::new();

    // Set
    config.set("api_key", "secret123", true)?;
    config.set("timeout", "30", false)?;

    // Get
    let api_key = config.get("api_key")?;
    assert_eq!(api_key.value, "secret123");
    assert!(api_key.is_secret);

    let timeout = config.get("timeout")?;
    assert_eq!(timeout.value, "30");
    assert!(!timeout.is_secret);

    // List
    let keys = config.list(None)?;
    assert_eq!(keys.len(), 2);

    // List with prefix
    config.set("api_endpoint", "https://api.example.com", false)?;
    let api_keys = config.list(Some("api_"))?;
    assert_eq!(api_keys.len(), 2);

    // Delete
    config.delete("timeout")?;
    assert!(config.get("timeout").is_err());

    Ok(())
}

// ============================================================================
// End-to-End Workflow Test
// ============================================================================

#[test]
fn test_complete_workflow() -> Result<()> {
    // Simulate a complete data processing workflow using multiple cloud services

    let object_storage = FakeObjectIO::new();
    let queue = FakeQueueIO::new();
    let kv_store = FakeKeyValueIO::new();
    let metrics = FakeMetricIO::new();

    // Step 1: Upload raw data
    let raw_data = b"user123,action,timestamp";
    object_storage.put_object("data-lake", "raw/events.csv", raw_data)?;

    // Step 2: Queue processing task
    let mut task_attrs = HashMap::new();
    task_attrs.insert("file".to_string(), "raw/events.csv".to_string());
    queue.send("processing-queue", "process_file", task_attrs)?;

    // Step 3: Process task (simulated)
    let messages = queue.receive("processing-queue", 1, 30)?;
    assert_eq!(messages.len(), 1);

    // Step 4: Store results
    let mut result = HashMap::new();
    result.insert("user_id".to_string(), "user123".to_string());
    result.insert("processed".to_string(), "true".to_string());
    kv_store.put("processed_events", "event1", result)?;

    // Step 5: Record metrics
    let metric = MetricPoint {
        name: "events_processed".to_string(),
        value: 1.0,
        timestamp: 1000,
        tags: HashMap::new(),
    };
    metrics.put_metric("pipeline", metric)?;

    // Verify end state
    assert!(object_storage.object_exists("data-lake", "raw/events.csv")?);
    assert!(kv_store.exists("processed_events", "event1")?);
    assert_eq!(queue.queue_size("processing-queue")?, 0);

    let metric_results = metrics.query_metrics(
        "pipeline",
        MetricQuery {
            metric_name: "events_processed".to_string(),
            start_time: 0,
            end_time: 2000,
            aggregation: None,
            tags: HashMap::new(),
        },
    )?;
    assert_eq!(metric_results.len(), 1);

    Ok(())
}

// ============================================================================
// Credentials and Config Tests
// ============================================================================

#[test]
fn test_credentials() {
    let creds = FakeCredentials {
        identifier: "test-user".to_string(),
        credential_type: "api_key".to_string(),
    };

    assert_eq!(creds.identifier(), "test-user");
    assert_eq!(creds.credential_type(), "api_key");
    assert!(creds.validate().is_ok());

    // Test invalid credentials
    let invalid = FakeCredentials {
        identifier: String::new(),
        credential_type: "api_key".to_string(),
    };
    assert!(invalid.validate().is_err());
}

#[test]
fn test_config() {
    let config = FakeConfig {
        region: Some("us-west-2".to_string()),
        endpoint: None,
        timeout_secs: 60,
        max_retries: 5,
    };

    assert_eq!(config.region(), Some("us-west-2"));
    assert_eq!(config.endpoint(), None);
    assert_eq!(config.timeout_secs(), 60);
    assert_eq!(config.max_retries(), 5);
}
