//! Tests for cloud IO traits and error handling.

use ironbeam::io::cloud::traits::*;
use std::collections::HashMap;

#[test]
fn test_cloud_io_error_creation() {
    let error = CloudIOError::new(ErrorKind::NotFound, "Resource not found");
    assert_eq!(error.kind, ErrorKind::NotFound);
    assert_eq!(error.message, "Resource not found");
    assert!(error.source.is_none());
}

#[test]
fn test_cloud_io_error_with_source() {
    let error =
        CloudIOError::new(ErrorKind::Network, "Connection failed").with_source("TCP timeout");
    assert_eq!(error.kind, ErrorKind::Network);
    assert_eq!(error.source.as_deref(), Some("TCP timeout"));
}

#[test]
fn test_cloud_io_error_display() {
    let error = CloudIOError::new(ErrorKind::Authentication, "Invalid credentials");
    let display = format!("{error}");
    assert!(display.contains("Authentication"));
    assert!(display.contains("Invalid credentials"));
}

#[test]
fn test_error_kind_equality() {
    assert_eq!(ErrorKind::NotFound, ErrorKind::NotFound);
    assert_ne!(ErrorKind::NotFound, ErrorKind::AlreadyExists);
}

#[test]
fn test_all_error_kinds() {
    let kinds = vec![
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

    for kind in kinds {
        let error = CloudIOError::new(kind.clone(), "Test error");
        assert_eq!(error.kind, kind);
    }
}

#[test]
fn test_resource_id_new() {
    let resource = ResourceId::new("aws", "s3", "my-bucket");
    assert_eq!(resource.provider, "aws");
    assert_eq!(resource.resource_type, "s3");
    assert_eq!(resource.name, "my-bucket");
    assert!(resource.namespace.is_none());
}

#[test]
fn test_resource_id_with_namespace() {
    let resource = ResourceId::new("gcp", "bigquery", "my-table").with_namespace("my-project");
    assert_eq!(resource.namespace.as_deref(), Some("my-project"));
}

#[test]
fn test_resource_id_display_without_namespace() {
    let resource = ResourceId::new("aws", "s3", "my-bucket");
    assert_eq!(format!("{resource}"), "aws:s3:my-bucket");
}

#[test]
fn test_resource_id_display_with_namespace() {
    let resource = ResourceId::new("gcp", "bigquery", "my-table").with_namespace("my-project");
    assert_eq!(format!("{resource}"), "gcp:bigquery:my-project/my-table");
}

#[test]
fn test_resource_id_equality() {
    let r1 = ResourceId::new("aws", "s3", "bucket");
    let r2 = ResourceId::new("aws", "s3", "bucket");
    let r3 = ResourceId::new("aws", "s3", "other-bucket");

    assert_eq!(r1, r2);
    assert_ne!(r1, r3);
}

#[test]
fn test_query_result_creation() {
    let result = QueryResult {
        columns: vec!["id".to_string(), "name".to_string()],
        rows: vec![
            vec!["1".to_string(), "Alice".to_string()],
            vec!["2".to_string(), "Bob".to_string()],
        ],
        row_count: 2,
    };

    assert_eq!(result.columns.len(), 2);
    assert_eq!(result.rows.len(), 2);
    assert_eq!(result.row_count, 2);
}

#[test]
fn test_object_metadata_creation() {
    let mut custom_metadata = HashMap::new();
    custom_metadata.insert("author".to_string(), "test".to_string());

    let metadata = ObjectMetadata {
        key: "file.txt".to_string(),
        size: 1024,
        content_type: Some("text/plain".to_string()),
        last_modified: Some(1234567890),
        etag: Some("abc123".to_string()),
        custom_metadata,
    };

    assert_eq!(metadata.key, "file.txt");
    assert_eq!(metadata.size, 1024);
    assert_eq!(metadata.content_type.as_deref(), Some("text/plain"));
}

#[test]
fn test_message_creation() {
    let mut attributes = HashMap::new();
    attributes.insert("priority".to_string(), "high".to_string());

    let message = Message {
        id: "msg-123".to_string(),
        data: b"test data".to_vec(),
        attributes,
        publish_time: Some(1234567890),
    };

    assert_eq!(message.id, "msg-123");
    assert_eq!(message.data, b"test data");
    assert!(message.attributes.contains_key("priority"));
}

#[test]
fn test_document_creation() {
    let mut data = HashMap::new();
    data.insert("name".to_string(), "John".to_string());
    data.insert("age".to_string(), "30".to_string());

    let doc = Document {
        key: "user:123".to_string(),
        data,
        version: Some("v1".to_string()),
    };

    assert_eq!(doc.key, "user:123");
    assert_eq!(doc.data.get("name").unwrap(), "John");
    assert_eq!(doc.version.as_deref(), Some("v1"));
}

#[test]
fn test_search_hit_creation() {
    let mut fields = HashMap::new();
    fields.insert("title".to_string(), "Test Document".to_string());

    let hit = SearchHit {
        id: "doc-1".to_string(),
        score: 0.95,
        fields,
    };

    assert_eq!(hit.id, "doc-1");
    assert_eq!(hit.score, 0.95);
    assert!(hit.fields.contains_key("title"));
}

#[test]
fn test_search_query_creation() {
    let mut filters = HashMap::new();
    filters.insert("category".to_string(), "tech".to_string());

    let query = SearchQuery {
        query: "test search".to_string(),
        filters,
        limit: 10,
        offset: 0,
    };

    assert_eq!(query.query, "test search");
    assert_eq!(query.limit, 10);
    assert_eq!(query.offset, 0);
}

#[test]
fn test_metric_point_creation() {
    let mut tags = HashMap::new();
    tags.insert("host".to_string(), "server1".to_string());

    let metric = MetricPoint {
        name: "cpu_usage".to_string(),
        value: 75.5,
        timestamp: 1234567890,
        tags,
    };

    assert_eq!(metric.name, "cpu_usage");
    assert_eq!(metric.value, 75.5);
    assert_eq!(metric.timestamp, 1234567890);
}

#[test]
fn test_metric_query_creation() {
    let mut tags = HashMap::new();
    tags.insert("region".to_string(), "us-east-1".to_string());

    let query = MetricQuery {
        metric_name: "requests".to_string(),
        start_time: 1000,
        end_time: 2000,
        aggregation: Some("avg".to_string()),
        tags,
    };

    assert_eq!(query.metric_name, "requests");
    assert_eq!(query.aggregation.as_deref(), Some("avg"));
}

#[test]
fn test_config_value_creation() {
    let config = ConfigValue {
        key: "api_key".to_string(),
        value: "secret123".to_string(),
        version: Some("v1".to_string()),
        is_secret: true,
    };

    assert_eq!(config.key, "api_key");
    assert!(config.is_secret);
}

#[test]
fn test_queue_message_creation() {
    let mut attributes = HashMap::new();
    attributes.insert("type".to_string(), "order".to_string());

    let message = QueueMessage {
        id: "msg-456".to_string(),
        receipt_handle: "handle-123".to_string(),
        body: "test message body".to_string(),
        attributes,
        receive_count: 1,
    };

    assert_eq!(message.id, "msg-456");
    assert_eq!(message.receive_count, 1);
}

#[test]
fn test_graph_node_creation() {
    let mut properties = HashMap::new();
    properties.insert("name".to_string(), "Alice".to_string());

    let node = GraphNode {
        id: "node-1".to_string(),
        labels: vec!["Person".to_string()],
        properties,
    };

    assert_eq!(node.id, "node-1");
    assert_eq!(node.labels.len(), 1);
}

#[test]
fn test_graph_edge_creation() {
    let mut properties = HashMap::new();
    properties.insert("since".to_string(), "2020".to_string());

    let edge = GraphEdge {
        id: "edge-1".to_string(),
        label: "KNOWS".to_string(),
        from_node: "node-1".to_string(),
        to_node: "node-2".to_string(),
        properties,
    };

    assert_eq!(edge.id, "edge-1");
    assert_eq!(edge.label, "KNOWS");
}

#[test]
fn test_edge_direction_variants() {
    assert_eq!(EdgeDirection::Outgoing, EdgeDirection::Outgoing);
    assert_eq!(EdgeDirection::Incoming, EdgeDirection::Incoming);
    assert_eq!(EdgeDirection::Both, EdgeDirection::Both);
    assert_ne!(EdgeDirection::Outgoing, EdgeDirection::Incoming);
}

#[test]
fn test_compute_result_creation() {
    let result = ComputeResult {
        status_code: 200,
        output: b"function output".to_vec(),
        logs: Some("execution logs".to_string()),
        execution_time_ms: 150,
    };

    assert_eq!(result.status_code, 200);
    assert_eq!(result.execution_time_ms, 150);
}

#[test]
fn test_invocation_status_variants() {
    let statuses = vec![
        InvocationStatus::Pending,
        InvocationStatus::Running,
        InvocationStatus::Succeeded,
        InvocationStatus::Failed,
        InvocationStatus::TimedOut,
    ];

    for status in statuses {
        assert_eq!(status, status);
    }
}

#[test]
fn test_notification_creation() {
    let mut attributes = HashMap::new();
    attributes.insert("urgent".to_string(), "true".to_string());

    let notification = Notification {
        target: "user@example.com".to_string(),
        subject: Some("Test Subject".to_string()),
        message: "Test message".to_string(),
        attributes,
    };

    assert_eq!(notification.target, "user@example.com");
    assert_eq!(notification.subject.as_deref(), Some("Test Subject"));
}

#[test]
fn test_notification_result_creation() {
    let result = NotificationResult {
        message_id: "notif-123".to_string(),
        status: NotificationStatus::Sent,
    };

    assert_eq!(result.message_id, "notif-123");
    assert_eq!(result.status, NotificationStatus::Sent);
}

#[test]
fn test_notification_status_variants() {
    assert_eq!(NotificationStatus::Sent, NotificationStatus::Sent);
    assert_ne!(NotificationStatus::Sent, NotificationStatus::Failed);
}

#[test]
fn test_inference_input_creation() {
    let input = InferenceInput {
        data: b"input data".to_vec(),
        content_type: "application/json".to_string(),
    };

    assert_eq!(input.data, b"input data");
    assert_eq!(input.content_type, "application/json");
}

#[test]
fn test_inference_output_creation() {
    let output = InferenceOutput {
        data: b"prediction result".to_vec(),
        content_type: "application/json".to_string(),
        model_version: Some("v2.1".to_string()),
        inference_time_ms: 45,
    };

    assert_eq!(output.data, b"prediction result");
    assert_eq!(output.inference_time_ms, 45);
}

#[test]
fn test_cloud_result_ok() {
    let result: CloudResult<i32> = Ok(42);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), 42);
}

#[test]
fn test_cloud_result_err() {
    let result: CloudResult<i32> = Err(CloudIOError::new(ErrorKind::NotFound, "Not found"));
    assert!(result.is_err());
}
