# Hosted object storage for Ironbeam

This opt-in crate connects Ironbeam's provider-neutral `ObjectIO` contract to
Apache `object_store`. Enable only the providers an application uses:

```toml
ironbeam-cloud = "0.1"
ironbeam-cloud-object-store = { version = "0.1", features = ["gcp"] }
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

This is a bounded object interface. Reads, writes, and cross-bucket copies
buffer a complete object in memory. The v5 JSONL graph adapters still commit a
whole final result, so they are not yet safe as independently committed output
bundles on remote Fn API workers. Hosted-provider credentials and live service
behavior require separate integration tests; the package's local tests use
Apache `object_store`'s in-memory implementation.
