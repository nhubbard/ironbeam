# Portable execution

Ironbeam's logical graph can be lowered to Apache Beam protobuf by the separate `ironbeam-dataflow` integration. The v5 proof uses local Prism and a separately launched worker. This path is intended to check graph, coder, source, transform, and sink behavior without cloud credentials.

A portable pipeline needs stable node keys, a named output, coders available to the worker, and operations supported by the lowering. Explicit order keys are required for portable ordered folds and larger keyed ordered batches. Registered custom handlers need semantic versions; a bare custom marker does not execute. These conditions are checked before starting unsupported source or sink work.

The [Prism proof README](https://github.com/vgough/ironbeam-dataflow/blob/main/examples/prism-proof/README.md) gives the fixture command and prerequisites. The [release gate report](https://github.com/nhubbard/ironbeam/blob/main/V5_RELEASE_GATE_STATUS.md) records which local cases passed and which hosted capabilities remain outside the 5.0 proof.
