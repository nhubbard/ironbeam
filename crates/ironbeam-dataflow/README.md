# Ironbeam Dataflow integration

`ironbeam-dataflow` prepares Beam portable graphs and submits jobs to Google
Cloud Dataflow. It uses `ironbeam-graphx` for graph lowering and
`ironbeam-harness` for the Fn API worker. Applications must build the same
pipeline in the driver and worker processes; the graph and handler identities
are checked before a worker opens a source.

The [WordCount example](../../examples/dataflow-wordcount) contains an
application entry point. The [portable execution guide](../../docs/docs/guides/portable.md)
describes the local Prism proof and its boundaries. Cloud submission code has
unit and mock-service tests, while a live hosted Dataflow job has not been
verified.

This crate and its companion Beam crates were imported from the local
`codex/ironbeam-v5-dataflow-alignment` branch of
`vgough/ironbeam-dataflow` at commit `348f0dc`. The source branch was not
modified during the import. These crates retain their Apache-2.0 license;
the repository contains its text in `LICENSE-APACHE`. The vendored Apache Beam
protocol definitions retain their original headers and notice.
