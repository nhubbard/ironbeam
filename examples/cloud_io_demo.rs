// Example demonstrating the cloud IO abstraction layer
//
// This shows how to use fake implementations for testing and how end-users
// can implement their own cloud IO solutions using the generic traits.

use ironbeam::io::cloud::{
    CacheIO, ComputeIO, EdgeDirection, FakeCacheIO, FakeComputeIO, FakeGraphIO, FakeIntelligenceIO,
    FakeKeyValueIO, FakeObjectIO, FakeQueueIO, FakeSearchIO, GraphIO, InferenceInput,
    IntelligenceIO, KeyValueIO, ObjectIO, QueueIO, SearchIO, SearchQuery,
};
use std::collections::HashMap;

fn main() {
    println!("=== Cloud IO Demonstration ===\n");

    // Example 1: Object Storage (S3, GCS, Azure Blob)
    object_storage_example();

    // Example 2: Key-Value Store (DynamoDB, Firestore)
    key_value_example();

    // Example 3: Message Queue (SQS, Cloud Tasks)
    queue_example();

    // Example 4: Cache (Redis, Memcached)
    cache_example();

    // Example 5: Search (Elasticsearch, CloudWatch Logs)
    search_example();

    // Example 6: Graph Database (Neptune, Neo4j)
    graph_example();

    // Example 7: Serverless Compute (Lambda, Cloud Functions)
    compute_example();

    // Example 8: ML Inference (SageMaker, Vertex AI)
    intelligence_example();
}

fn object_storage_example() {
    println!("--- Object Storage Example ---");
    let storage = FakeObjectIO::new();

    // Upload some data
    storage
        .put_object("my-bucket", "data/file.txt", b"Hello, Cloud!")
        .expect("Failed to put object");

    // List objects
    let objects = storage
        .list_objects("my-bucket", Some("data/"))
        .expect("Failed to list objects");
    println!("Found {} objects", objects.len());

    // Download data
    let data = storage
        .get_object("my-bucket", "data/file.txt")
        .expect("Failed to get object");
    println!("Downloaded: {}", String::from_utf8_lossy(&data));

    // Copy object
    storage
        .copy_object("my-bucket", "data/file.txt", "my-bucket", "backup/file.txt")
        .expect("Failed to copy object");

    println!("✓ Object storage operations completed\n");
}

fn key_value_example() {
    println!("--- Key-Value Store Example ---");
    let kv = FakeKeyValueIO::new();

    // Put some documents
    let mut user1 = HashMap::new();
    user1.insert("name".to_string(), "Alice".to_string());
    user1.insert("age".to_string(), "30".to_string());
    kv.put("users", "user1", user1)
        .expect("Failed to put document");

    let mut user2 = HashMap::new();
    user2.insert("name".to_string(), "Bob".to_string());
    user2.insert("age".to_string(), "25".to_string());
    kv.put("users", "user2", user2)
        .expect("Failed to put document");

    // Get a document
    let doc = kv.get("users", "user1").expect("Failed to get document");
    if let Some(doc) = doc {
        println!("User: {}", doc.data.get("name").unwrap());
    }

    // Query documents
    let mut filter = HashMap::new();
    filter.insert("age".to_string(), "30".to_string());
    let results = kv.query("users", filter).expect("Failed to query");
    println!("Found {} users with age=30", results.len());

    println!("✓ Key-value operations completed\n");
}

fn queue_example() {
    println!("--- Message Queue Example ---");
    let queue = FakeQueueIO::new();

    // Send messages
    queue
        .send("my-queue", "Task 1", HashMap::new())
        .expect("Failed to send message");
    queue
        .send("my-queue", "Task 2", HashMap::new())
        .expect("Failed to send message");

    println!("Queue size: {}", queue.queue_size("my-queue").unwrap());

    // Receive messages
    let messages = queue
        .receive("my-queue", 10, 30)
        .expect("Failed to receive messages");
    println!("Received {} messages", messages.len());

    for msg in &messages {
        println!("  - {}", msg.body);
        queue
            .delete("my-queue", &msg.receipt_handle)
            .expect("Failed to delete message");
    }

    println!("✓ Queue operations completed\n");
}

fn cache_example() {
    println!("--- Cache Example ---");
    let cache = FakeCacheIO::new();

    // Set some values
    cache
        .set("user:123", b"Alice", Some(3600))
        .expect("Failed to set cache");
    cache
        .set("user:456", b"Bob", None)
        .expect("Failed to set cache");

    // Get values
    if let Some(value) = cache.get("user:123").expect("Failed to get cache") {
        println!("Cached user: {}", String::from_utf8_lossy(&value));
    }

    // Increment counter
    cache
        .set("counter", b"10", None)
        .expect("Failed to set counter");
    let new_val = cache.increment("counter", 5).expect("Failed to increment");
    println!("Counter value: {new_val}");

    println!("✓ Cache operations completed\n");
}

fn search_example() {
    println!("--- Search/Log Example ---");
    let search = FakeSearchIO::new();

    // Index some documents
    let mut doc1 = HashMap::new();
    doc1.insert("title".to_string(), "Getting started with Rust".to_string());
    doc1.insert(
        "content".to_string(),
        "Rust is a systems programming language".to_string(),
    );
    search.index("docs", "doc1", doc1).expect("Failed to index");

    let mut doc2 = HashMap::new();
    doc2.insert("title".to_string(), "Advanced Rust patterns".to_string());
    doc2.insert(
        "content".to_string(),
        "Learn about Rust patterns".to_string(),
    );
    search.index("docs", "doc2", doc2).expect("Failed to index");

    // Search
    let query = SearchQuery {
        query: "Rust".to_string(),
        filters: HashMap::new(),
        limit: 10,
        offset: 0,
    };
    let results = search.search("docs", query).expect("Failed to search");
    println!("Search found {} results", results.len());
    for hit in results {
        println!("  - {} (score: {})", hit.id, hit.score);
    }

    println!("✓ Search operations completed\n");
}

fn graph_example() {
    println!("--- Graph Database Example ---");
    let graph = FakeGraphIO::new();

    // Add nodes
    let alice = graph
        .add_node(vec!["Person".to_string()], {
            let mut props = HashMap::new();
            props.insert("name".to_string(), "Alice".to_string());
            props
        })
        .expect("Failed to add node");

    let bob = graph
        .add_node(vec!["Person".to_string()], {
            let mut props = HashMap::new();
            props.insert("name".to_string(), "Bob".to_string());
            props
        })
        .expect("Failed to add node");

    // Add edge
    graph
        .add_edge(&alice, &bob, "KNOWS", HashMap::new())
        .expect("Failed to add edge");

    // Find neighbors
    let neighbors = graph
        .get_neighbors(&alice, EdgeDirection::Outgoing)
        .expect("Failed to get neighbors");
    println!("Alice knows {} people", neighbors.len());

    println!("✓ Graph operations completed\n");
}

fn compute_example() {
    println!("--- Serverless Compute Example ---");
    let compute = FakeComputeIO::new();

    // Register a fake function
    compute.register_function("echo", <[u8]>::to_vec);

    // Invoke synchronously
    let result = compute
        .invoke("echo", b"Hello, Lambda!")
        .expect("Failed to invoke function");
    println!(
        "Function returned: {}",
        String::from_utf8_lossy(&result.output)
    );
    println!("Execution time: {}ms", result.execution_time_ms);

    // Invoke asynchronously
    let invocation_id = compute
        .invoke_async("echo", b"Async call")
        .expect("Failed to invoke async");
    println!("Async invocation ID: {invocation_id}");

    println!("✓ Compute operations completed\n");
}

fn intelligence_example() {
    println!("--- ML Inference Example ---");
    let ai = FakeIntelligenceIO::new();

    // Register a fake model (simple echo for demonstration)
    ai.register_model("sentiment", |input| {
        format!(
            "{{\"sentiment\": \"positive\", \"input\": \"{}\"}}",
            String::from_utf8_lossy(input)
        )
        .into_bytes()
    });

    // Run inference
    let input = InferenceInput {
        data: b"This is a great product!".to_vec(),
        content_type: "text/plain".to_string(),
    };

    let output = ai.predict("sentiment", input).expect("Failed to predict");
    println!("Model output: {}", String::from_utf8_lossy(&output.data));
    println!("Inference time: {}ms", output.inference_time_ms);

    println!("✓ Intelligence operations completed\n");
}
