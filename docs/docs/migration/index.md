# Migration from Ironbeam 4

Ironbeam 5 retains the pipeline and typed collection model while changing graph identity, error handling, and execution boundaries. The root package on this branch exposes v5 behavior. Published Ironbeam 4.1.1 remains the comparison reference for compatibility tests; its implementation is not embedded in the v5 facade.

## Main changes

| Area | Ironbeam 4 | Ironbeam 5 |
| --- | --- | --- |
| Graph construction | Many unnamed collection operations | Named, fallible sources, transforms, and outputs |
| Local execution | Collection helpers on the original implementation | Direct runner and collection extension traits |
| File formats | Broad root-package feature set | Separate format crates and opt-in root features |
| Custom batches | Physical partition boundaries could be observable | Explicit batch size and semantic version |
| Checkpoints | Progress markers and graph reruns | Coded state and source progress at supported boundaries |
| Event time | Millisecond examples | Nanosecond metadata |
| Portable ordering | Encounter order could be assumed locally | Explicit order keys for portable ordered operations |

The [API mapping](api.md) covers common calls. [Operational changes](operations.md) describes checkpoints, side effects, and portability. The repository's [full migration inventory](https://github.com/nhubbard/ironbeam/blob/main/V4_TO_V5_MIGRATION.md) includes less common helpers.

## Migration sequence

A bounded Direct run gives a stable comparison point for element results. The v5 graph then needs a unique key at each source, transform, and terminal, with construction errors handled as `Result` values. Format features can be selected independently. Any timestamp used as event-time metadata must be expressed in nanoseconds.

Ordered reductions and batch handlers require a separate review: v5 batch sizes are explicit, and portable ordering requires an order key. A v4 checkpoint cannot be reused. For a graph intended to run through Beam, the local Prism fixture tests the supported portable path; hosted deployment remains a separate operational step.
