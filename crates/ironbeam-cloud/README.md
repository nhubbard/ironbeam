# Ironbeam cloud objects

`ironbeam-cloud` now includes a URI-based object client. Applications register
an `ObjectIO` implementation once for each URI scheme, then read, write, list,
copy, and inspect objects through the same API. A filesystem-backed adapter is
included for local development and tests.

```rust
use ironbeam_cloud::{LocalObjectIO, ObjectStores};

let mut objects = ObjectStores::new();
objects.register("file", LocalObjectIO::new("./object-data")?)?;
objects.write("file://results/run-1/output.jsonl", b"{\"id\":1}\n")?;
let bytes = objects.read("file://results/run-1/output.jsonl")?;
let files = objects.list("file://results/run-1/")?;
assert_eq!(files.len(), 1);
assert_eq!(bytes, b"{\"id\":1}\n");
# Ok::<(), ironbeam_cloud::CloudIOError>(())
```

The registry also accepts implementations for `gs`, `s3`, `az`, or other
schemes. `provider("scheme")` returns a clonable `ObjectIO` handle for the
existing JSONL cloud source and sink helpers. The registry owns no credentials;
each provider handles its own authentication and retries. Cross-provider
`copy` streams the whole source object through the caller, so it is intended
for bounded objects only.

The local adapter maps `file://bucket/key` to `root/bucket/key`. It checks
relative paths and existing symlinks to keep operations inside its root. It
does not provide distributed consistency, cloud authentication, or an atomic
commit protocol for parallel workers.
