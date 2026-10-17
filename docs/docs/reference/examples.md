# Example programs

The repository contains runnable examples. Commands in the table are run from the repository root. Some examples require the feature shown in their command.

| Example | Demonstrates | Command |
| --- | --- | --- |
| [Explain pipeline](https://github.com/nhubbard/ironbeam/blob/main/examples/explain_pipeline.rs) | Graph planning | `cargo run --example explain_pipeline` |
| [ETL pipeline](https://github.com/nhubbard/ironbeam/blob/main/examples/etl_pipeline.rs) | CSV and JSONL | `cargo run --example etl_pipeline --features io-csv` |
| [Windowing aggregations](https://github.com/nhubbard/ironbeam/blob/main/examples/windowing_aggregations.rs) | Event time and windows | `cargo run --example windowing_aggregations` |
| [Multi-output](https://github.com/nhubbard/ironbeam/blob/main/examples/multi_output.rs) | Branches and paired results | `cargo run --example multi_output` |
| [Checkpoint recovery](https://github.com/nhubbard/ironbeam/blob/main/examples/v5_checkpoint_recovery.rs) | Durable Direct result | `cargo run --example v5_checkpoint_recovery` |
| [Testing pipeline](https://github.com/nhubbard/ironbeam/blob/main/examples/testing_pipeline.rs) | Builders and assertions | `cargo run --example testing_pipeline --features v5-testing` |
| [Cloud I/O](https://github.com/nhubbard/ironbeam/blob/main/examples/cloud_io_demo.rs) | Object storage | `cargo run --example cloud_io_demo` |
| [Compressed I/O](https://github.com/nhubbard/ironbeam/blob/main/examples/compressed_io.rs) | Pluggable codecs | `cargo run --example compressed_io --features compression-gzip` |
| [Dataflow WordCount](https://github.com/nhubbard/ironbeam/tree/main/examples/dataflow-wordcount) | Portable graph and worker entry point | `cargo run -p ironbeam-dataflow-wordcount` |

The [Prism proof](https://github.com/nhubbard/ironbeam/blob/main/examples/prism-proof/README.md) exercises the portable runner and worker.
