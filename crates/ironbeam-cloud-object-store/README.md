# Hosted object storage for Ironbeam

This opt-in crate connects Ironbeam's provider-neutral `ObjectIO` contract to
Apache `object_store`. Enable only the providers an application uses:

```toml
ironbeam-cloud-objects = "5.0.0"
ironbeam-cloud-object-store = { version = "5.0.0", features = ["gcp"] }
```

```rust,ignore
use ironbeam_cloud_object_store::object_stores_from_env;

let objects = object_stores_from_env()?;
objects.write("gs://my-bucket/output/results.jsonl", b"{\"id\":1}\n")?;
let bytes = objects.read("gs://my-bucket/output/results.jsonl")?;
```

The router also recognizes `s3://` with `aws` enabled and `az://` with
`azure` enabled. The returned `ObjectStores` can register a local or custom
provider alongside those hosted clients. Provider constructors read their
normal environment configuration; bucket or container clients are built on
first use and cached. `RemoteObjectIO::new`
accepts any `object_store` client factory, including local emulators and
custom S3-compatible endpoints.

`read`, `write`, and cross-bucket copies buffer a complete object in memory.
`ObjectIO::open_object` streams reads, and
`ObjectIO::put_object_if_absent_from` streams a conditional write through a
temporary object. The latter copies to the final name only when that name is
absent. A process lost before cleanup may leave a temporary object under
`__ironbeam_uploads/`; it cannot expose a partial final result. Hosted-provider
credentials and live service behavior require separate integration tests; the
package's local tests use Apache `object_store`'s in-memory implementation.
