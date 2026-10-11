# Ironbeam cloud objects

`ironbeam-cloud-objects` is the small provider-neutral object storage contract.
It contains `ObjectIO`, `ObjectStores`, URI parsing, error and metadata types,
and a filesystem provider for local jobs. It has no provider SDK or legacy
cloud-service dependencies.

```rust
use ironbeam_cloud_objects::{LocalObjectIO, ObjectStores};

let mut objects = ObjectStores::new();
objects.register("file", LocalObjectIO::new("./object-data")?)?;
objects.write("file://results/run-1/output", b"done")?;
objects.create("file://results/run-1/manifest", b"version-1")?;
let copied = objects.copy_prefix("file://results/run-1/", "file://archive/run-1/")?;
assert_eq!(copied, 2);
# Ok::<(), ironbeam_cloud_objects::CloudIOError>(())
```

Opt-in `ironbeam-cloud-object-store` registers enabled S3, GCS, and Azure Blob
providers with `object_stores_from_env()`. The previous `ironbeam-cloud` crate
has been retired from the v5 workspace; published Ironbeam 4.1.1 retains its
old imports for migration. Prefix copy completes a listing before
copying, but a failed copy can leave a partial destination.

`create` is an atomic, durable create-if-absent operation for immutable
manifests and finalization records. The local provider and opt-in hosted
S3, GCS, and Azure adapters implement it. Custom providers must implement
`ObjectIO::put_object_if_absent` to use it; the default fails closed.
