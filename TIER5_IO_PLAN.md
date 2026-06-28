# Tier 5 I/O Implementation Plan (5.2 – 5.6)

This document is the implementation guide for features 5.2 through 5.6 of
`FEATURE_PARITY_PLAN.md`. Each new format follows the same pattern established
by the MsgPack connector (5.1).

---

## Reference: The MsgPack Pattern

Every new format must replicate the following structure exactly.

### Files per format

| File                      | Purpose                                                                 |
|---------------------------|-------------------------------------------------------------------------|
| `src/io/<format>.rs`      | Low-level I/O primitives + `VecOps` adapter                             |
| `src/helpers/<format>.rs` | Pipeline-level helpers (`read_<format>`, `PCollection::write_<format>`) |
| `tests/io/<format>.rs`    | Integration tests (starts with `#![cfg(feature = "io-<format>")]`)      |

### Layer structure in `src/io/<format>.rs`

1. **Always-compiled `<Format>Shards` struct** — stores `path: PathBuf`,
   `ranges: Vec<(u64, u64)>` (0-based record-count ranges, end-exclusive),
   `total_records: u64`. No crate-specific types; compiles without the feature.
2. **Private helpers** — behind `#[cfg(feature = "io-<format>")]`.
3. **Public `read_<format>_vec`, `write_<format>_vec`, `build_<format>_shards`,
   `read_<format>_range`** — fully implemented behind `#[cfg(feature = "io-<format>")]`.
4. **Public `write_<format>_par`** — behind
   `#[cfg(all(feature = "parallel-io", feature = "io-<format>"))]`.
5. **Stubs** — behind `#[cfg(not(feature = "io-<format>"))]` (and
   `#[cfg(all(feature = "parallel-io", not(feature = "io-<format>")))]` for par);
   identical signatures, body is `anyhow::bail!("the \`io-<format>\` feature is not enabled")`.
6. **Always-compiled `<Format>VecOps<T>`** — implements `VecOps` by calling
   `read_<format>_range`; produces `None` when the range read fails (disabled stub).

### Layer structure in `src/helpers/<format>.rs`

1. Re-export everything from the io module:
   `pub use crate::io::<format>::{<Format>Shards, <Format>VecOps, build_<format>_shards, read_<format>_vec, write_<format>_vec};`
2. `read_<format><T>(p, path)` — eager glob-aware source that loads all matching files
   and returns `PCollection<T>`.
3. `read_<format>_streaming<T>(p, path, records_per_shard)` — builds `<Format>Shards`
   and inserts a `Node::Source`.
4. `impl<T: Element + Serialize> PCollection<T> { fn write_<format>(path) -> Result<usize> }`
5. `#[cfg(feature = "parallel-io")] impl<...> PCollection<T> { fn write_<format>_par(path, shards) -> Result<usize> }`

### Wiring in existing files

| File                 | Change                                                                                           |
|----------------------|--------------------------------------------------------------------------------------------------|
| `Cargo.toml`         | Add `dep:<crate>`, add `io-<format> = ["dep:<crate>"]` under `# Additional I/O formats (Tier 5)` |
| `src/io/mod.rs`      | `pub mod <format>;`                                                                              |
| `src/helpers/mod.rs` | `pub mod <format>;` + `pub use <format>::*;` in the glob-use block                               |
| `src/lib.rs`         | Add re-exports matching the msgpack block (lines 607–612)                                        |
| `tests/io/mod.rs`    | `mod <format>;`                                                                                  |

### Documentation requirements

- `src/io/<format>.rs` module doc must mirror the msgpack module doc:
  - bullet list of what the module provides
  - `# Feature gating` section explaining the stub ABI contract
  - format-specific notes (wire format, compression, shard unit)
- All public items require doc comments with `# Errors` sections.
- No dead links: `cargo doc --no-deps` must pass without warnings.

### Test requirements

Every test file must achieve coverage parity with `tests/io/msgpack.rs`:

| Test group         | What to cover                                                                                                                                             |
|--------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------|
| Vector I/O         | roundtrip, empty file, parent dir creation, file-not-found, corrupt data, compressed roundtrip                                                            |
| Error paths        | mkdir failure (parent is file), create failure (path is dir), shards/range file-not-found, glob error propagation, par-write mkdir failure                |
| Parallel writing   | empty, single shard, multiple shards (deterministic order), auto-shard sizing, mkdir failure                                                              |
| Sharding / ranges  | non-empty sharding, zero rps clamping, empty file, corrupt propagation, full and sub-range reads                                                          |
| VecOps             | `len`, `split`, `clone_any` on N records; empty VecOps                                                                                                    |
| High-level helpers | pipeline roundtrip, parallel pipeline write, glob concat in sorted order, glob no-match, streaming wordcount, streaming empty, streaming parallel collect |

---

## Pre-Implementation: Verify Feature 5.5 (Parquet) Status

Before implementing any new code, check `src/io/parquet.rs` and
`src/helpers/parquet.rs`:

- `write_parquet_vec<T: Serialize>`, `read_parquet_vec<T: DeserializeOwned>` — present
- `build_parquet_shards`, `read_parquet_row_group_range` — present
- `ParquetShards`, `ParquetVecOps<T>` — present
- `read_parquet_streaming<T>` (glob-aware streaming helper) — present
- `PCollection::write_parquet` — present
- **Missing**: `read_parquet<T>` eager glob helper (non-streaming)
- **Missing**: `PCollection::write_parquet_par` parallel write helper
- **Missing**: `RecordBatch`-level API (`read_parquet_batches`, `write_parquet_batches`)

**Action for 5.5**: The serde-based row API is complete. Add the two missing helpers
(`read_parquet` eager glob + `write_parquet_par`) to `src/helpers/parquet.rs` and
update `src/lib.rs` re-exports. Skip the `RecordBatch`-level API (it would be a
separate sub-feature requiring a new `io-arrow-parquet` flag and is out of scope for
this plan). Then update `FEATURE_PARITY_PLAN.md` to move 5.5 into the Implemented table.

---

## Implementation Order

Implement in order of ascending complexity. Each feature must pass `cargo clippy`,
`cargo fmt`, `cargo doc --no-deps`, and `cargo llvm-cov` before moving to the next.

1. **5.5 Parquet** — gap-fill only (add two helpers). Lowest risk.
2. **5.2 CBOR** — nearly identical to MsgPack; only difference is CBOR serializer API.
3. **5.4 Protocol Buffers** — serde-free `prost::Message` encode/decode path.
4. **5.6 TFRecord** — custom CRC-32C masked framing + raw-bytes + proto-Example support.
5. **5.3 Arrow IPC** — most complex; mixed ABI (RecordBatch vs generic T), two VecOps
   adapters.

---

## 5.5 Parquet — Gap-Fill

### Cargo.toml
No new dependencies. `io-parquet` already exists and is in `default`.

### Changes

**`src/helpers/parquet.rs`** — add two new items:

```rust
/// Eager glob-aware read; mirrors read_msgpack behaviour.
pub fn read_parquet<T>(p: &Pipeline, path: impl AsRef<Path>) -> Result<PCollection<T>>
where T: Element + DeserializeOwned { ... }

#[cfg(feature = "parallel-io")]
impl<T: Element + Serialize + Send + Sync> PCollection<T> {
    pub fn write_parquet_par(self, path: impl AsRef<Path>, shards: Option<usize>) -> Result<usize> { ... }
}
```

`read_parquet` follows the same glob-detection logic as `read_msgpack`: check for
`[*?[]` with a regex, then either `expand_glob` + concat or single-file load.

`write_parquet_par` must be implemented. Parquet is NOT byte-concatenable, so the
parallel strategy is: serialize each shard in parallel to a temp Parquet file, then
copy record-batch groups into the final file sequentially. Given the existing
`parquet` crate API (which reads/writes `RecordBatch` via `serde_arrow`), the
simplest approach is to serialize each shard sequentially but in parallel threads,
writing to numbered temp files (`*.parquet.partN`), then use `read_parquet_vec` to
merge all temp files in order into the final output file. Clean up temp files after.

**`src/lib.rs`** — add alongside the existing parquet re-exports:
```rust
pub use helpers::parquet::read_parquet;
#[cfg(feature = "parallel-io")]
pub use io::parquet::write_parquet_par; // if adding to io layer, else helpers
```

**`tests/io/parquet.rs`** — add tests for `read_parquet` (glob, single file,
no-match error) and `write_parquet_par` (empty, single shard, multi-shard determinism).

---

## 5.2 CBOR I/O

### Overview

CBOR (Concise Binary Object Representation, RFC 8949) is self-delimiting at the value
level. Like MsgPack, a concatenation of CBOR values is a valid multi-record stream.
The implementation is a near-direct port of `src/io/msgpack.rs`.

### Cargo.toml

```toml
# Under "# Additional I/O formats (Tier 5)":
io-cbor = ["dep:ciborium"]

# Under [dependencies]:
ciborium = { version = "0.2", optional = true }
```

### Wire Format Notes

- **Encoding**: `ciborium::ser::into_writer(value, writer)` — writes a single CBOR
  value to `writer`.
- **Decoding**: `ciborium::de::from_reader::<T, _>(reader)` — reads one CBOR value.
- **Clean EOF detection**: `ciborium::de::Error<io::Error>` wraps an inner `io::Error`.
  A clean EOF (stream exhausted at a record boundary) surfaces as an `io::Error` with
  kind `ErrorKind::UnexpectedEof`. Write a `is_clean_eof(err: &ciborium::de::Error<io::Error>) -> bool`
  helper that matches `ciborium::de::Error::Io(e) if e.kind() == ErrorKind::UnexpectedEof`.
  **Verify this during implementation** by writing a round-trip test that checks the
  clean EOF path explicitly.
- **Compression**: auto-detect via `crate::io::compression::{auto_detect_reader, auto_detect_writer}`.
- **Concatenability**: shard byte streams are directly byte-concatenable.

### Files

**`src/io/cbor.rs`** — full implementation following `msgpack.rs` structure exactly:
- `CborShards { path: PathBuf, ranges: Vec<(u64, u64)>, total_records: u64 }` (always compiled)
- Private helpers (feature-gated): `is_clean_eof`, `cbor_read_loop<T, R>`,
  `cbor_count_records<R>`, `make_cbor_shards`, `open_cbor_reader`
- Public API (feature-gated): `read_cbor_vec<T>`, `write_cbor_vec<T>`,
  `write_cbor_par<T>` (parallel-io + io-cbor), `build_cbor_shards`,
  `read_cbor_range<T>`
- Stubs (not-feature-gated): same signatures, `anyhow::bail!`
- `CborVecOps<T>` (always compiled)

**`src/helpers/cbor.rs`** — re-exports + pipeline helpers:
- `read_cbor<T>(p, path)` — eager glob source
- `read_cbor_streaming<T>(p, path, records_per_shard)` — streaming source
- `impl PCollection<T: Serialize> { write_cbor(path) }`
- `#[cfg(parallel-io)] impl PCollection<T: Serialize + Send + Sync> { write_cbor_par(path, shards) }`

**`tests/io/cbor.rs`** — full coverage as per the test-requirements table above.

### Wiring

- `src/io/mod.rs`: `pub mod cbor;`
- `src/helpers/mod.rs`: `pub mod cbor;` + `pub use cbor::*;`
- `src/lib.rs`: add block analogous to lines 607–612 for msgpack, pointing at cbor
- `tests/io/mod.rs`: `mod cbor;`

---

## 5.4 Protocol Buffers I/O

### Overview

Protocol Buffers records use a **length-delimited** binary framing: a varint-encoded
byte length followed by the raw protobuf bytes. This allows record counting and
sequential range reading without a global index.

`prost`-generated types implement `prost::Message + Default` but **not** Serde. The
entire `src/io/protobuf.rs` module works through `prost::Message::encode` /
`prost::Message::decode` instead of `serde`. The `ProtoShards` struct uses only
standard types (no prost types), so it is always compiled.

### Cargo.toml

```toml
# Under "# Additional I/O formats (Tier 5)":
io-protobuf = ["dep:prost"]

# Under [dependencies]:
prost = { version = "0.13", optional = true }
```

### Wire Format Notes

- **Record framing**: write the encoded bytes length as a varint, then the raw bytes.
  Use `prost::encode_length_delimiter(len, writer)?` and
  `prost::decode_length_delimiter(&mut reader)?` from prost's `length_delimiter`
  helpers.
- **Encoding**: `msg.encode(&mut buf)?; write varint(buf.len()); write buf`
- **Decoding**: `len = decode_varint(reader); buf = read_exact(reader, len); T::decode(buf)`
- **Clean EOF**: at record boundary, the next varint read returns `Err` with an
  `io::Error` of kind `UnexpectedEof` or the reader returns 0 bytes. Write an
  `is_clean_eof` helper that checks `io::ErrorKind::UnexpectedEof`.
- **Compression**: auto-detect as with other formats.
- **Concatenability**: yes — the varint+bytes framing is byte-concatenable.

### Type Constraint Differences

Where msgpack uses `T: Serialize + DeserializeOwned`, protobuf uses:
- reads: `T: prost::Message + Default`
- writes: `T: prost::Message`
- parallel: `T: prost::Message + Send + Sync`

The `ProtoVecOps<T>` constraint is:
`T: prost::Message + Default + Clone + Send + Sync + 'static`.

In stubs (not-feature-gated), these bounds must still appear in the signature. Use
`prost::Message` as a bound in stub signatures via conditional compilation:
- Feature enabled: `T: prost::Message + Default`
- Feature disabled: We cannot reference `prost::Message` without the crate. Therefore
  **the stubs must use a different approach**.

**Stub strategy for prost-specific bounds**: because `prost::Message` is behind a
feature gate, stub functions for the disabled case must use a trait bound that does
not reference prost types. Use the Avro strategy: **no stubs** for functions whose
signatures require `prost::Message`. These functions simply do not compile when the
feature is off. This is acceptable because:
1. The `ProtoShards` struct (always compiled) has no prost types — it can be
   unconditionally referenced.
2. `build_proto_shards` and `read_proto_range` do reference prost types (`T: prost::Message`)
   only in `read_proto_range` but not in `build_proto_shards`.
3. The pipeline-level helpers are fully feature-gated in `src/helpers/protobuf.rs`
   so the runner never references them unconditionally.

**Revised stub policy**: provide stubs **only** for `build_proto_shards` (which
returns `ProtoShards` without involving prost types). All other functions
(`read_proto_vec`, `write_proto_vec`, `write_proto_par`, `read_proto_range`) are
fully feature-gated with no stubs.

`ProtoVecOps<T>` is always compiled but its `split` / `clone_any` methods return
`None` when the feature is off (because `read_proto_range` is not available), identical
to the MsgPack pattern.

### Files

**`src/io/protobuf.rs`**:
- `ProtoShards { path: PathBuf, ranges: Vec<(u64, u64)>, total_records: u64 }` (always compiled)
- Private helpers (feature-gated): `is_clean_eof`, `proto_read_loop<T>`,
  `proto_count_records`, `make_proto_shards`, `open_proto_reader`
- `build_proto_shards(path, rps)` — fully gated + stub (return type has no prost types)
- `read_proto_vec<T: prost::Message + Default>` — fully gated, no stub
- `write_proto_vec<T: prost::Message>` — fully gated, no stub
- `write_proto_par<T: prost::Message + Send + Sync>` — fully gated (parallel-io + io-protobuf), no stub
- `read_proto_range<T: prost::Message + Default>` — fully gated, no stub
- `ProtoVecOps<T>` (always compiled)

**`src/helpers/protobuf.rs`** — fully gated on `#[cfg(feature = "io-protobuf")]`:
- `read_proto<T: prost::Message + Default>(p, path)` — eager glob source
- `read_proto_streaming<T: prost::Message + Default>(p, path, rps)` — streaming source
- `impl<T: Element + prost::Message> PCollection<T> { write_proto(path) }`
- `#[cfg(parallel-io)] write_proto_par`

**`tests/io/protobuf.rs`** — `#![cfg(feature = "io-protobuf")]`, full test coverage.
Use a minimal hand-written `prost::Message` struct (derive) as the test record type to
avoid needing a `.proto` file or `protoc`.

Example test type:
```rust
#[derive(Clone, PartialEq, prost::Message)]
struct TestMsg {
    #[prost(string, tag = "1")]
    pub name: String,
    #[prost(int64, tag = "2")]
    pub value: i64,
}
```

### Wiring

Same as CBOR: `src/io/mod.rs`, `src/helpers/mod.rs`, `src/lib.rs`, `tests/io/mod.rs`.

---

## 5.6 TFRecord I/O

### Overview

TFRecord is a simple length-prefixed binary container with masked CRC-32C checksums.
It does not require TensorFlow. Each record is either raw bytes or a `tf.Example`
protobuf message.

### Cargo.toml

```toml
# Under "# Additional I/O formats (Tier 5)":
io-tfrecord = ["dep:crc32c"]

# Under [dependencies]:
crc32c = { version = "0.6", optional = true }
```

The `tf.Example` proto helpers require both `io-tfrecord` and `io-protobuf`. They are
gated with `#[cfg(all(feature = "io-tfrecord", feature = "io-protobuf"))]` and
require no additional crate beyond `prost` (already pulled in by `io-protobuf`).

### Wire Format

Each record:
```
┌──────────────────────────────────────────────────────┐
│  uint64LE   length of data payload                   │  8 bytes
│  uint32LE   masked CRC-32C of the 8 length bytes     │  4 bytes
│  <bytes>    raw data payload (length bytes)          │
│  uint32LE   masked CRC-32C of the raw data bytes     │  4 bytes
└──────────────────────────────────────────────────────┘
```

**CRC masking**:
```rust
fn mask_crc(crc: u32) -> u32 {
    ((crc >> 15) | (crc << 17)).wrapping_add(0xa282ead8_u32)
}
fn unmask_crc(masked: u32) -> u32 {
    let rot = masked.wrapping_sub(0xa282ead8_u32);
    (rot >> 17) | (rot << 15)
}
```

**Read algorithm** for one record (propagate `io::Error` with `ErrorKind::UnexpectedEof`
as a clean EOF):
1. Read 8 bytes → `length_bytes: [u8; 8]`; if EOF at byte 0 → clean EOF.
2. Read 4 bytes → `len_crc_bytes`; compute `crc32c(length_bytes)`, compare after
   masking; return error on mismatch.
3. `length = u64::from_le_bytes(length_bytes)`
4. `data = read_exact(length as usize)` → raw payload bytes.
5. Read 4 bytes → `data_crc_bytes`; compute `crc32c(&data)`, compare after masking.
6. Return `data`.

**Write algorithm** for one record:
1. `length = data.len() as u64`
2. Write `length.to_le_bytes()` (8 bytes).
3. Write `mask_crc(crc32c::crc32c(&length.to_le_bytes()))` as little-endian u32 (4 bytes).
4. Write `data` bytes.
5. Write `mask_crc(crc32c::crc32c(&data))` as little-endian u32 (4 bytes).

**Counting records** (for `build_tfrecord_shards`): read only the 8-byte length +
4-byte length-CRC, verify, skip `length` data bytes + 4 data-CRC bytes. O(n) in
record count, O(1) in memory.

**Range reading**: from the beginning, skip records before `start` using the fast
count path above, then fully decode records in `[start, end)`.

### Element Type

The primary element type is `Vec<u8>` (raw bytes). This avoids adding the `bytes`
crate dependency.

### `tf.Example` Support

Bundle hand-written prost structs (no `protoc` needed) in
`src/io/tfrecord_proto.rs` (committed to the repo). This file is compiled when both
`io-tfrecord` and `io-protobuf` features are enabled:

```rust
// src/io/tfrecord_proto.rs — generated-equivalent prost structs for tf.Example
#[cfg(all(feature = "io-tfrecord", feature = "io-protobuf"))]
pub mod example {
    #[derive(Clone, PartialEq, prost::Message)]
    pub struct BytesList { #[prost(bytes = "vec", repeated, tag = "1")] pub value: Vec<Vec<u8>> }
    #[derive(Clone, PartialEq, prost::Message)]
    pub struct FloatList { #[prost(float, repeated, tag = "1")] pub value: Vec<f32> }
    #[derive(Clone, PartialEq, prost::Message)]
    pub struct Int64List { #[prost(int64, repeated, tag = "1")] pub value: Vec<i64> }
    // Feature oneof + Features map + Example wrapper ...
}
```

The `Feature` oneof and `Features` map require `prost`'s `oneof` and `btree_map`
field annotations. Implement following the prost documentation for oneofs and maps.

### Files

**`src/io/tfrecord_proto.rs`** — bundled proto structs (described above).

**`src/io/tfrecord.rs`**:
- `TFRecordShards { path: PathBuf, ranges: Vec<(u64, u64)>, total_records: u64 }` (always)
- `mask_crc(u32) -> u32`, `unmask_crc(u32) -> u32` (always, trivially testable)
- Private helpers (feature-gated): `read_tfrecord_entry<R>`,
  `write_tfrecord_entry<W>`, `tfrecord_count_records`, `make_tfrecord_shards`,
  `open_tfrecord_reader`
- `read_tfrecord_vec(path)` → `Result<Vec<Vec<u8>>>` (feature-gated + stub)
- `write_tfrecord_vec(path, &[Vec<u8>])` → `Result<usize>` (feature-gated + stub)
- `write_tfrecord_par(path, &[Vec<u8>], shards)` → `Result<usize>` (parallel + feature + stub)
- `build_tfrecord_shards(path, rps)` → `Result<TFRecordShards>` (feature-gated + stub)
- `read_tfrecord_range(shards, start, end)` → `Result<Vec<Vec<u8>>>` (feature-gated + stub)
- `read_tfrecord_examples_vec(path)` → gated on `all(io-tfrecord, io-protobuf)`, no stub
- `TFRecordVecOps` (always compiled): `VecOps` over `TFRecordShards`, element type `Vec<u8>`

**`src/helpers/tfrecord.rs`**:
- `read_tfrecord(p, path)` — eager glob source, `PCollection<Vec<u8>>`
- `read_tfrecord_streaming(p, path, rps)` — streaming source
- `impl PCollection<Vec<u8>> { write_tfrecord(path) }`
- `#[cfg(parallel-io)] write_tfrecord_par(path, shards)`
- `#[cfg(all(io-tfrecord, io-protobuf))]` — `read_tfrecord_examples(p, path)` returning
  `PCollection<example::Example>`

**`tests/io/tfrecord.rs`** — full test coverage.

Additional test groups for TFRecord specifically:
- `mask_crc` / `unmask_crc` round-trip (pure unit tests)
- CRC mismatch on length bytes → error propagation
- CRC mismatch on data bytes → error propagation
- Truncated record (data shorter than declared length) → error
- `#[cfg(all(feature = "io-tfrecord", feature = "io-protobuf"))]` block:
  - `read_tfrecord_examples` roundtrip via `write_tfrecord_vec` (encode Example to bytes) then read

### Wiring

Same wiring as CBOR. `src/lib.rs` re-exports:
```rust
pub use io::tfrecord::{read_tfrecord_vec, write_tfrecord_vec, TFRecordShards};
pub use helpers::tfrecord::{read_tfrecord, read_tfrecord_streaming};
#[cfg(feature = "parallel-io")]
pub use io::tfrecord::write_tfrecord_par;
#[cfg(all(feature = "io-tfrecord", feature = "io-protobuf"))]
pub use helpers::tfrecord::read_tfrecord_examples;
```

---

## 5.3 Arrow IPC I/O

### Overview

Arrow IPC is the most structurally different format because `RecordBatch` is not
Serde-compatible. The module provides **two parallel APIs**:

- **Batch-level API**: element type is `RecordBatch`. Fully feature-gated (no stubs
  possible because `RecordBatch` is a crate type).
- **Row-level API**: element type is `T: Serialize + DeserializeOwned`, converted to/from
  `RecordBatch` via `serde_arrow`. Fully feature-gated when reading, since
  `serde_arrow::from_record_batch` is needed.

Because both APIs require `arrow` types in function signatures, this module uses the
**Avro strategy**: no stubs at all. All public functions are fully gated with
`#[cfg(feature = "io-arrow")]`. The `ArrowShards` struct (always compiled) stores
only primitive types.

### Cargo.toml

```toml
# Under "# Additional I/O formats (Tier 5)":
io-arrow = ["dep:arrow", "dep:serde_arrow"]

# arrow and serde_arrow are already present as optional deps (shared with io-parquet):
# arrow = { version = "59", optional = true }
# serde_arrow = { version = "0.14", optional = true, features = ["arrow-59"] }
# No new [dependencies] lines needed.
```

**Important**: `io-arrow` shares `arrow` and `serde_arrow` with `io-parquet`. Cargo
deduplicates them. The two features do NOT conflict.

### Wire Format Notes

Use the Arrow IPC **file format** (magic + schema + record batches + footer).
- Writer: `arrow::ipc::writer::FileWriter::try_new(file, &schema)?`
- Reader: `arrow::ipc::reader::FileReader::try_new(file, None)?` (reads all batches)
- Schema inference: `serde_arrow::schema::SchemaLike::from_type::<T>(TracingOptions::default())?`

The IPC file format **requires** the full file to be written before reading, so
shard files (for par write) must be full IPC files (not byte-concatenable byte
streams). The final merged file is also a full IPC file.

**Sharding unit for Arrow IPC**: number of `RecordBatch`es, not individual rows.
`ArrowShards { path: PathBuf, ranges: Vec<(usize, usize)>, total_batches: usize, total_rows: u64 }`.
`ranges` are batch-index ranges `(start_batch, end_batch)`.

**Parallel writing strategy**: serialize each shard as a complete IPC file in
parallel, then open them as readers, extract all batches in order, and write a single
merged IPC file. Delete temp files after.

### Files

**`src/io/arrow_ipc.rs`**:
- `ArrowShards { path: PathBuf, ranges: Vec<(usize, usize)>, total_batches: usize, total_rows: u64 }` (always compiled; no arrow types)
- All other items fully gated with `#[cfg(feature = "io-arrow")]`:
  - `read_arrow_ipc_vec(path) -> Result<Vec<RecordBatch>>` — batch-level read
  - `write_arrow_ipc_vec(path, &[RecordBatch]) -> Result<usize>` — batch-level write
  - `read_arrow_ipc_rows_vec<T: Serialize + DeserializeOwned>(path) -> Result<Vec<T>>` — row-level
  - `write_arrow_ipc_rows_vec<T: Serialize + DeserializeOwned>(path, &[T]) -> Result<usize>` — row-level
  - `write_arrow_ipc_par(path, &[RecordBatch], shards) -> Result<usize>` (parallel-io + io-arrow)
  - `write_arrow_ipc_rows_par<T: Serialize + ...>(path, &[T], shards) -> Result<usize>` (parallel-io + io-arrow)
  - `build_arrow_shards(path, batches_per_shard) -> Result<ArrowShards>`
  - `read_arrow_ipc_range(shards, start_batch, end_batch) -> Result<Vec<RecordBatch>>`
  - `read_arrow_ipc_rows_range<T: DeserializeOwned>(shards, start, end) -> Result<Vec<T>>`
- **Two VecOps adapters**:
  - `ArrowBatchVecOps` — element type `RecordBatch`; fully gated
  - `ArrowRowVecOps<T>` — element type `T: DeserializeOwned`; wrapper around row range reader; fully gated

> **Note**: Because there are no stubs, the runner and helpers must not reference
> Arrow functions unconditionally. Ensure `src/helpers/arrow_ipc.rs` is entirely
> within `#[cfg(feature = "io-arrow")]` guards.

**`src/helpers/arrow_ipc.rs`** — everything inside `#[cfg(feature = "io-arrow")]`:
- `read_arrow_ipc<T: DeserializeOwned + ...>(p, path) -> Result<PCollection<T>>` — row-level, glob
- `read_arrow_ipc_streaming<T>(p, path, batches_per_shard)` — streaming row-level
- `read_arrow_ipc_batches(p, path) -> Result<PCollection<RecordBatch>>` — batch-level, glob
- `impl PCollection<T: Element + Serialize> { write_arrow_ipc_rows(path) -> Result<usize> }`
- `impl PCollection<RecordBatch> { write_arrow_ipc_batches(path) -> Result<usize> }`
  (requires `RecordBatch: Element`, which requires `RecordBatch: Clone + Send + Sync + 'static`)
- `#[cfg(parallel-io)]` parallel write variants

**`tests/io/arrow_ipc.rs`** — `#![cfg(feature = "io-arrow")]`, full test coverage.
Use a simple test struct `#[derive(Clone, Serialize, Deserialize)] struct Row { id: u32, name: String }`.
Additional Arrow-specific test groups:
- Batch roundtrip (`RecordBatch` write → read, schema preserved)
- Row roundtrip (`T` write → read via serde_arrow)
- Multi-batch file (multiple `RecordBatch`es)
- Mixed-type schema (string + int64 + float32 fields)
- `ArrowShards` batch-count ranges

### Wiring

`src/lib.rs` re-exports (all inside `#[cfg(feature = "io-arrow")]`):
```rust
#[cfg(feature = "io-arrow")]
pub use io::arrow_ipc::{ArrowShards, build_arrow_shards};
#[cfg(feature = "io-arrow")]
pub use helpers::arrow_ipc::{read_arrow_ipc, read_arrow_ipc_streaming, read_arrow_ipc_batches};
#[cfg(all(feature = "io-arrow", feature = "parallel-io"))]
pub use io::arrow_ipc::{write_arrow_ipc_par, write_arrow_ipc_rows_par};
```

---

## Quality Checklist (Run After Each Feature)

After implementing each feature, verify:

```sh
# 1. Formatting
cargo fmt

# 2. Clippy (pedantic + nursery, all features, warnings as errors)
cargo clippy --all-targets --all-features --fix --allow-dirty -- \
  -D warnings -W clippy::pedantic -W clippy::nursery

# 3. Documentation
cargo doc --no-deps

# 4. Tests and coverage
cargo llvm-cov --workspace --all-features --html --branch
```

Coverage must not drop below the level measured before starting this work.

Additionally, verify the feature-permutation compilation contract with:
```sh
# Verify each new feature compiles in isolation (no default features)
cargo check --no-default-features --features io-cbor
cargo check --no-default-features --features io-protobuf
cargo check --no-default-features --features io-tfrecord
cargo check --no-default-features --features io-arrow

# Verify stub ABI: cbor, tfrecord compile with feature off
cargo check --no-default-features

# Verify all features together
cargo check --all-features
```

---

## Summary Table

| Feature                | Crate(s) Added                  | Files Created                                                                           | Stubs                               | VecOps                                              |
|------------------------|---------------------------------|-----------------------------------------------------------------------------------------|-------------------------------------|-----------------------------------------------------|
| 5.5 Parquet (gap-fill) | none                            | (edits only)                                                                            | existing                            | existing                                            |
| 5.2 CBOR               | `ciborium`                      | `io/cbor.rs`, `helpers/cbor.rs`, `tests/io/cbor.rs`                                     | full (all functions)                | `CborVecOps<T>` always compiled                     |
| 5.4 Protobuf           | `prost`                         | `io/protobuf.rs`, `helpers/protobuf.rs`, `tests/io/protobuf.rs`                         | partial (`build_proto_shards` only) | `ProtoVecOps<T>` always compiled                    |
| 5.6 TFRecord           | `crc32c`                        | `io/tfrecord_proto.rs`, `io/tfrecord.rs`, `helpers/tfrecord.rs`, `tests/io/tfrecord.rs` | full (raw-bytes functions)          | `TFRecordVecOps` always compiled                    |
| 5.3 Arrow IPC          | `arrow`, `serde_arrow` (shared) | `io/arrow_ipc.rs`, `helpers/arrow_ipc.rs`, `tests/io/arrow_ipc.rs`                      | none                                | `ArrowBatchVecOps`, `ArrowRowVecOps<T>` fully gated |
