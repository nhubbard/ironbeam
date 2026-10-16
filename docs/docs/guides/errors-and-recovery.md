# Errors and recovery

Ironbeam reports graph and execution failures through a structured `Error`.
The error includes a category, a message, and, when available, the graph key
and runner that encountered it. A retry classification records whether the
same operation may be attempted again. A successful retry still depends on
the source and sink contracts of the job.

## Error categories

| Category | Meaning | Usual response |
| --- | --- | --- |
| `Graph` | A pipeline node cannot be added or validated, often because a key is duplicated or a contract is incomplete. | Correct the graph before running it again. |
| `UnsupportedCapability` | The selected runner cannot execute a node or graph shape. | Choose a supported operation or runner. |
| `Incompatible` | A wire format or execution identity differs from the expected contract. | Restore the matching build and configuration, or begin a new job. |
| `Resource` | A Direct job exceeded a declared resource limit. | Review the job budget and the operation's materialization needs. |
| `Execution` | A handler, source, or runner step failed while the job ran. | Inspect the named node and the underlying failure. |
| `Io` | Reading or writing a resource failed. | Check the resource and the sink's retry contract. |
| `Recovery` | A checkpoint or replay state could not be resumed safely. | Keep the failed state for inspection; resume with compatible state or start a new job. |
| `Cancelled` | The job stopped after a cancellation request. | Treat committed output according to the sink's contract. |

Graph, unsupported-capability, incompatibility, resource, and recovery errors
are classified as permanent by their constructors. Execution and I/O errors
start with an unknown retry class; the category alone does not authorize a
retry. The `node` field identifies the affected stable graph key when one is
known, and `backend` identifies the runner when the failure arose there.

## Construction and preflight

Source, transform, and terminal methods return `Result` when adding nodes.
A duplicate graph key therefore fails at the call that attempts to add it.
Runner validation can also reject an unsupported operation before it opens a
source or sink. For portable execution, preflight checks coders, registered
handlers, and supported Beam lowering. The [API reference](../reference/api.md)
identifies the common construction methods.

## Restarting a Direct job

A Direct checkpoint can retain coded state and source progress. Recovery
checks the saved graph, source, coder, and binary build identities before
continuing. Changing one of those contracts without a compatible migration
causes a recovery error. A v4 checkpoint cannot be loaded by v5. The
[checkpoint guide](checkpoints.md) describes the configuration and the state
that is retained.

Checkpointing does not make an external append sink replay safe. A job that
may retry a bundle needs an idempotent sink or a finalization protocol that
accepts only committed attempts. The [I/O guide](io.md) describes finalized
results and format-specific behavior.

## Portable worker failure

In supervised mode, a stable Beam Fn Control parent runs bundle work in an
execution child. If that child dies, the parent can replay its buffered input
and forward only the successful attempt's output. This path requires a
replayable source, a safe sink, and a durable journal for any finalizing sink.
The parent has a 64 MiB input buffer limit per bundle. Loss of the Control
parent fails the active local Prism job; it is outside the proved recovery
boundary. The [portable execution guide](portable.md) describes how the mode
is enabled and what its local tests establish.
