# Google Cloud clients

`ironbeam-cloud-gcp` provides service clients for Cloud Storage, Pub/Sub,
BigQuery, and Secret Manager. It uses Google's Rust client crates and
Application Default Credentials (ADC). The clients are intended for an
application that builds or runs an Ironbeam pipeline and needs to work with
Google Cloud resources. The separate `ironbeam-cloud-object-store` crate
provides the provider-neutral `gs://` object router used by Ironbeam I/O.

## Installation and authentication

Select only the services your application uses:

```toml
[dependencies]
ironbeam-cloud-gcp = { version = "5.0.0", default-features = false, features = ["storage", "pubsub"] }
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

Until the 5.0.0 packages are published, replace `version = "5.0.0"` with
`path = "/path/to/ironbeam/crates/ironbeam-cloud-gcp"` in a local checkout.
Alternatively, enable `cloud-gcp` on the root `ironbeam` package; that
feature includes all four clients. The root also has individual
`cloud-gcp-storage`, `cloud-gcp-pubsub`, `cloud-gcp-bigquery`, and
`cloud-gcp-secrets` flags. Its clients are then available under
`ironbeam::v5::gcp`.

For a local account, configure ADC with `gcloud auth application-default login`.
For a deployed worker or service, attach a service account to its environment.
The client builders obtain credentials when they connect, and requests use
that identity. Grant it permissions for the exact buckets, topics,
subscriptions, BigQuery jobs and data, or secrets the application needs.
The project ID is the project in which short Pub/Sub and Secret Manager
resource names are expanded. BigQuery jobs also use it as their default
project. Bucket names are global and do not inherit it.

```rust
use ironbeam_cloud_gcp::GcpProject;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let project = GcpProject::new("my-project")?.with_location("US")?;
    let storage = project.storage().await?;
    storage.write_new("my-bucket", "runs/input.jsonl", b"{\"id\":1}\n".to_vec()).await?;
    let body = storage.read_bytes("my-bucket", "runs/input.jsonl").await?;
    println!("{} bytes", body.len());
    Ok(())
}
```

`write_new` uses an object-generation precondition: it creates an object only
if that name does not already have a current generation. It returns an error
for an existing object. Both methods hold the whole object in memory. Use
`storage.client()` for streaming transfers, metadata, and other Storage
operations. `gs://my-bucket` is accepted in place of `my-bucket`.

## Pub/Sub

The following example publishes an event and consumes messages from an
existing subscription. A topic can be created with `create_topic("events")`,
and a subscription with `create_subscription("events", "events-worker")`.
Pub/Sub acknowledges a
message only after the application calls `ack()` on its handler.

```rust
use ironbeam_cloud_gcp::GcpProject;
use std::collections::HashMap;

# async fn example() -> Result<(), Box<dyn std::error::Error>> {
let project = GcpProject::new("my-project")?;
let pubsub = project.pubsub().await?;
let id = pubsub.publish("events", b"ready".to_vec(), HashMap::new()).await?;
println!("published {id}");

let mut messages = pubsub.stream("events-worker")?;
if let Some((message, handler)) = messages.next().await.transpose()? {
    println!("received {} bytes", message.data.len());
    handler.ack();
}
# Ok(()) }
```

Short topic and subscription IDs are expanded within the configured project.
Fully qualified `projects/PROJECT/topics/NAME` and
`projects/PROJECT/subscriptions/NAME` names are also accepted. The publisher
is reused for subsequent calls to the same topic.

## BigQuery

`query_rows` waits for the query job and collects its rows in memory. Read
values with the row's typed `get` method. Set the job location when the
dataset requires one; use the underlying `client()` for parameterized queries,
large streaming results, and advanced jobs.

```rust
use ironbeam_cloud_gcp::GcpProject;

# async fn example() -> Result<(), Box<dyn std::error::Error>> {
let project = GcpProject::new("my-project")?.with_location("US")?;
let bigquery = project.bigquery().await?;
for row in bigquery.query_rows("SELECT 1 AS answer").await? {
    let answer: i64 = row.get("answer")?;
    println!("{answer}");
}
# Ok(()) }
```

`execute` runs a statement and waits for completion without returning rows.
The calling identity needs permission to create jobs and to access the
referenced datasets and tables.

## Secret Manager

`access_latest` returns bytes; `access_latest_utf8` decodes text. Creating a
secret uses automatic replication, then `add_version` writes its first value.
The underlying `client()` exposes explicit replication, versions, and IAM
operations.

```rust
use ironbeam_cloud_gcp::GcpProject;

# async fn example() -> Result<(), Box<dyn std::error::Error>> {
let project = GcpProject::new("my-project")?;
let secrets = project.secrets().await?;
let token = secrets.access_latest_utf8("api-token").await?;
println!("loaded token with {} characters", token.len());
# Ok(()) }
```

Secret values should not be printed or recorded in pipeline logs. A worker
that accesses a secret needs its own service identity with access to that
secret. A local ADC login does not grant a deployed Dataflow worker access.

## Execution scope

These clients connect to Google Cloud resources from application or worker
code. They do not provision a Dataflow job, transfer local credentials to a
worker, or change the Beam graph lowering contract. The current portable
proof runs against local Prism; remote Dataflow operation and provider
consistency require a separate integration proof. The GCP adapters are
compile and unit tested without live Google Cloud credentials.
