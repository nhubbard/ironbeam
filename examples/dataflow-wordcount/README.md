# Dataflow WordCount example

This package builds one Ironbeam graph in both the Dataflow driver and its
portable worker. It counts words from four in-memory lines and writes the
counts to worker logs. The local unit test checks the graph and Direct result.

Running against hosted Dataflow requires a Google Cloud project, a staging
bucket, a temporary bucket path, application credentials, and a Linux x86-64
worker binary built from this package. The driver checks the binary format
before staging it. For example:

```sh
export GOOGLE_CLOUD_PROJECT=example-project
export IRONBEAM_STAGING_LOCATION=gs://example-bucket/staging
export IRONBEAM_TEMP_LOCATION=gs://example-bucket/tmp
export IRONBEAM_WORKER_BINARY=/path/to/linux-x86_64/ironbeam-dataflow-wordcount
cargo run -p ironbeam-dataflow-wordcount --release
```

The local Prism suite exercises the portable graph and worker without cloud
credentials. A hosted Dataflow run has not been verified, so the example is
an integration starting point rather than a demonstrated cloud deployment.
