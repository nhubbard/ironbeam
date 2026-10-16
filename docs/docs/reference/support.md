# Ironbeam 5.0.0 support boundary

This page distinguishes features implemented in Ironbeam 5 from the narrower
set verified with the local Apache Beam Prism runner. A passing local Prism
job tests Beam protobuf lowering and a separate Rust worker process. It does
not establish that a hosted service has the same scheduling, credentials, or
storage behavior.

| Area | Verified behavior | Limit for 5.0.0 |
| --- | --- | --- |
| Bounded pipelines | Sources, elementwise transforms, grouping, joins, folds, side inputs, and output paths have Direct and Prism fixtures. | A Rust callback used by a portable worker needs a registered semantic identity and stable coder. |
| Event time | Fixed, offset, sliding, and selected session windows have shared fixtures. | Complex merged-session lateness and all nested trigger schedules are not claimed. |
| Streaming | Replayable sources, count and processing-time firings, keyed state, timer families, and timed partial batch completion have focused proofs. | A source that never completes needs a firing policy for partial global batches. General user-defined SDF splitting is not implemented. Global-window expiration and a final partial byte batch on finite completion work on Direct, but the pinned Prism results are intermittent. |
| Recovery | Direct restores supported coded state and source cursors. Supervised Prism replaces a failed execution child and checks exact committed output. | The Fn Control parent itself is not replaced within a running Prism job. |
| File I/O | The separate format crates have local round trips; selected formats run through local Prism. | A local file proof does not establish remote provider consistency. |
| Cloud resources | Provider-neutral object contracts and GCP Storage, Pub/Sub, BigQuery, and Secret Manager clients compile and have local tests. | Live GCP service calls and hosted Dataflow execution have not been integration tested. |
| Dataflow | The driver can stage artifacts, construct a portable job request, submit it, and poll the job service. | Hosted worker lifecycle and final-result authorization need a service proof before production use. |

Ironbeam's feature inventory is measured against portable Beam semantics and
I/O relevant to Rust applications. The repository does not assign a numeric
parity percentage. The [release gate report](https://github.com/nhubbard/ironbeam/blob/main/V5_RELEASE_GATE_STATUS.md)
records the exact test cases and known runner differences.

## What to test in an application

For a Direct application, run the real source and sink against a disposable
dataset. Restart a job from its checkpoint if it has an unbounded source or
stateful transform. A replay-safe sink should produce the same final records
after a retry.

For a portable application, use the same pipeline-building function in the
driver and worker. Run it against local Prism with representative input,
including a late event, a partial final batch, and at least one retried bundle
when those behaviors matter. Verify values and event timestamps; a successful
job status alone does not prove that a final-result sink published rows.

For cloud resources, test credentials, permissions, and provider consistency
in the target environment. The local GCP client tests validate construction
and request handling without contacting live services.
