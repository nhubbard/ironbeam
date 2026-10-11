# Phase 0: v4 public `PCollection` and transform inventory

Source: `src/collection.rs` and `src/helpers/*.rs` at `main` `d64c26a` (same code on this branch). Each row names a public operation, including source and multi-input free functions. `co_gbk` methods are macro-generated and listed separately below. This is the proposed v5 import and ownership ledger; it does not change v4 exports.

| v4 module | Public operations | Proposed v5 home | Reason / migration impact |
| --- | --- | --- | --- |
| `collection` | `node_id`, `apply_transform` | core graph identity and custom operation; replace `DynOp` partition contract with semantic registration | `node_id` remains build-local; portable identity is separate. |
| `arrow_ipc` | `read_arrow_ipc`, `read_arrow_ipc_streaming`, `read_arrow_ipc_batches`, `write_arrow_ipc_rows`, `write_arrow_ipc_batches`, `write_arrow_ipc_rows_par`, `write_arrow_ipc_batches_par` | I/O extension or free source/sink builder; immediate writes currently execute local collect | v5 import changes if moved from inherent method; see operation matrix. |
| `avro` | `write_avro_with_schema`, `write_avro_par`, `read_avro`, `read_avro_streaming` | I/O extension or free source/sink builder; immediate writes currently execute local collect | v5 import changes if moved from inherent method; see operation matrix. |
| `basic` | `sum_globally`, `min_globally`, `max_globally`, `average_globally`, `mean_globally`, `sum_per_key`, `min_per_key`, `max_per_key`, `average_per_key`, `mean_per_key` | transform extension trait; composed convenience operation | v5 import changes if moved from inherent method; see operation matrix. |
| `batches` | `map_batches`, `batch_elements`, `batch_by_size`, `map_values_batches`, `group_into_batches` | direct extension / runner operation; physical batching, ordering, or dependency semantics need review | v5 import changes if moved from inherent method; see operation matrix. |
| `cbor` | `read_cbor`, `read_cbor_streaming`, `write_cbor`, `write_cbor_par` | I/O extension or free source/sink builder; immediate writes currently execute local collect | v5 import changes if moved from inherent method; see operation matrix. |
| `collect_sorted` | `collect_seq_sorted`, `collect_par_sorted`, `collect_par_sorted_by_key` | direct extension / runner operation; physical batching, ordering, or dependency semantics need review | v5 import changes if moved from inherent method; see operation matrix. |
| `collect_values` | `to_list_globally`, `to_set_globally`, `to_list_per_key`, `to_set_per_key`, `to_dict` | transform extension trait; composed convenience operation | v5 import changes if moved from inherent method; see operation matrix. |
| `combine` | `combine_values`, `combine_values_lifted` | core combine node; lifted variant may be direct optimization | v5 import changes if moved from inherent method; see operation matrix. |
| `combine_global` | `combine_globally`, `combine_globally_lifted` | core combine node; lifted variant may be direct optimization | v5 import changes if moved from inherent method; see operation matrix. |
| `common` | `map`, `filter`, `flat_map` | core inherent; preserve time/window | v5 import changes if moved from inherent method; see operation matrix. |
| `common` | `take`, `first` | review; bounded limit semantics | v5 import changes if moved from inherent method; see operation matrix. |
| `common` | `collect`, `collect_seq`, `collect_par` | direct terminal; move to extension trait or runner call | v5 import changes if moved from inherent method; see operation matrix. |
| `count` | `count_globally`, `count_per_element`, `count_per_key` | transform extension trait; composed convenience operation | v5 import changes if moved from inherent method; see operation matrix. |
| `csv` | `read_csv`, `write_csv`, `write_csv_par`, `read_csv_streaming` | I/O extension or free source/sink builder; immediate writes currently execute local collect | v5 import changes if moved from inherent method; see operation matrix. |
| `dead_letter` | `new`, `map_catching`, `flat_map_catching` | semantic review before ownership; specify window, side-input, error, or multi-input behavior | v5 import changes if moved from inherent method; see operation matrix. |
| `display` | `to_display_string` | transform extension trait; composed convenience operation | v5 import changes if moved from inherent method; see operation matrix. |
| `distinct` | `distinct_count_globally`, `distinct`, `approx_distinct_count`, `approx_count_distinct`, `approx_count_distinct_with_error`, `distinct_by`, `distinct_count_per_key`, `distinct_per_key`, `approx_distinct_count_per_key`, `approx_count_distinct_per_key`, `approx_count_distinct_per_key_with_error` | exact helpers in `ironbeam-transforms`; KMV/HLL helpers in `ironbeam-transform-approx` or `ironbeam::v5::prelude::*` with `v5-approx` | v5 uses named, fallible graph construction; approximate sketches are opt-in. |
| `filter` | `filter_eq`, `filter_ne`, `filter_lt`, `filter_le`, `filter_gt`, `filter_ge`, `filter_range`, `filter_range_inclusive`, `filter_by` | transform extension trait; composed convenience operation | v5 import changes if moved from inherent method; see operation matrix. |
| `flatten` | `flatten` | core free graph operation; validate fan-in types/boundedness | v5 import changes if moved from inherent method; see operation matrix. |
| `joins` | `join_inner`, `join_left`, `join_right`, `join_full` | semantic review before ownership; specify window, side-input, error, or multi-input behavior | v5 import changes if moved from inherent method; see operation matrix. |
| `jsonl` | `read_jsonl`, `write_jsonl`, `read_jsonl_streaming`, `write_jsonl_par` | I/O extension or free source/sink builder; immediate writes currently execute local collect | v5 import changes if moved from inherent method; see operation matrix. |
| `keyed` | `key_by`, `group_by_key` | core inherent | v5 import changes if moved from inherent method; see operation matrix. |
| `keyed` | `with_constant_key`, `with_keys`, `keys`, `values`, `kv_swap` | core convenience trait | v5 import changes if moved from inherent method; see operation matrix. |
| `keyed` | `to_hashmap` | direct terminal | v5 import changes if moved from inherent method; see operation matrix. |
| `latest` | `latest_globally`, `latest_per_key` | transform extension trait; composed convenience operation | v5 import changes if moved from inherent method; see operation matrix. |
| `log_elements` | `log_elements`, `log_elements_with` | transform extension trait; composed convenience operation | v5 import changes if moved from inherent method; see operation matrix. |
| `mongodb` | `read_mongodb`, `read_mongodb_streaming`, `write_mongodb`, `write_mongodb_par` | I/O extension or free source/sink builder; immediate writes currently execute local collect | v5 import changes if moved from inherent method; see operation matrix. |
| `msgpack` | `read_msgpack`, `read_msgpack_streaming`, `write_msgpack`, `write_msgpack_par` | I/O extension or free source/sink builder; immediate writes currently execute local collect | v5 import changes if moved from inherent method; see operation matrix. |
| `named` | `with_name` | core inherent graph metadata | v5 import changes if moved from inherent method; see operation matrix. |
| `parquet` | `read_parquet`, `write_parquet`, `write_parquet_par`, `read_parquet_streaming` | I/O extension or free source/sink builder; immediate writes currently execute local collect | v5 import changes if moved from inherent method; see operation matrix. |
| `protobuf` | `read_proto`, `read_proto_streaming`, `write_proto`, `write_proto_par` | I/O extension or free source/sink builder; immediate writes currently execute local collect | v5 import changes if moved from inherent method; see operation matrix. |
| `regex` | `regex_matches`, `regex_extract`, `regex_extract_kv`, `regex_find`, `regex_replace_all`, `regex_split` | `ironbeam-transform-regex::RegexExt`, or `ironbeam::v5::prelude::*` with `v5-regex` | v5 uses named, fallible graph construction and an opt-in regex dependency. |
| `reshuffle` | `reshuffle` | direct extension / runner operation; physical batching, ordering, or dependency semantics need review | v5 import changes if moved from inherent method; see operation matrix. |
| `sampling` | `sample_reservoir_vec`, `sample_reservoir`, `sample_globally`, `sample_globally_with_seed`, `sample_values_reservoir_vec`, `sample_values_reservoir`, `sample_per_key`, `sample_per_key_with_seed` | transform extension trait; composed convenience operation | v5 import changes if moved from inherent method; see operation matrix. |
| `side_inputs` | `side_vec`, `map_with_side`, `filter_with_side`, `side_hashmap`, `side_singleton`, `side_multimap`, `map_with_side_map`, `filter_with_side_map`, `map_with_singleton`, `filter_with_singleton`, `map_with_side_multimap`, `filter_with_side_multimap` | semantic review before ownership; specify window, side-input, error, or multi-input behavior | v5 import changes if moved from inherent method; see operation matrix. |
| `sql` | `read_sql`, `read_sql_streaming`, `write_sql_with`, `write_sql_par_with` | I/O extension or free source/sink builder; immediate writes currently execute local collect | v5 import changes if moved from inherent method; see operation matrix. |
| `statistical` | `approx_median_globally`, `approx_quantiles_globally`, `approx_median_per_key`, `approx_quantiles_per_key` | transform extension trait; composed convenience operation | v5 import changes if moved from inherent method; see operation matrix. |
| `stdlib` | `from_vec`, `from_iter` | core bounded source builder | v5 import changes if moved from inherent method; see operation matrix. |
| `stdlib` | `from_custom_source` | review; replace `VecOps` payload contract | v5 import changes if moved from inherent method; see operation matrix. |
| `stdlib` | `filter_map` | core convenience trait | v5 import changes if moved from inherent method; see operation matrix. |
| `tee` | `tee`, `tee_n` | direct extension / runner operation; physical batching, ordering, or dependency semantics need review | v5 import changes if moved from inherent method; see operation matrix. |
| `tfrecord` | `read_tfrecord`, `read_tfrecord_streaming`, `read_tfrecord_examples`, `write_tfrecord`, `write_tfrecord_par` | I/O extension or free source/sink builder; immediate writes currently execute local collect | v5 import changes if moved from inherent method; see operation matrix. |
| `timestamped` | `attach_timestamps`, `to_timestamped`, `reify_timestamps` | semantic review before ownership; specify window, side-input, error, or multi-input behavior | v5 import changes if moved from inherent method; see operation matrix. |
| `topk` | `top_k_globally`, `top_k_per_key`, `bottom_k_globally`, `bottom_k_per_key` | transform extension trait; composed convenience operation | v5 import changes if moved from inherent method; see operation matrix. |
| `try_process` | `try_map`, `try_flat_map`, `collect_fail_fast` | semantic review before ownership; specify window, side-input, error, or multi-input behavior | v5 import changes if moved from inherent method; see operation matrix. |
| `tumbling` | `key_by_window`, `group_by_window`, `key_by_window`, `group_by_key_and_window` | semantic review before ownership; specify window, side-input, error, or multi-input behavior | v5 import changes if moved from inherent method; see operation matrix. |
| `validation` | `validate_with_mode`, `validate_skip_invalid`, `validate_fail_fast`, `validate_values_skip_invalid`, `validate_values_with_mode` | semantic review before ownership; specify window, side-input, error, or multi-input behavior | v5 import changes if moved from inherent method; see operation matrix. |
| `values` | `map_values`, `filter_values` | core inherent keyed vocabulary | v5 import changes if moved from inherent method; see operation matrix. |
| `wait_on` | `wait_on` | direct extension / runner operation; physical batching, ordering, or dependency semantics need review | v5 import changes if moved from inherent method; see operation matrix. |
| `windowed_combine` | `combine_per_window`, `sum_per_window`, `count_per_window`, `min_per_window`, `max_per_window`, `average_per_window`, `combine_per_key_and_window`, `sum_per_key_and_window`, `count_per_key_and_window`, `min_per_key_and_window`, `max_per_key_and_window`, `average_per_key_and_window` | semantic review before ownership; specify window, side-input, error, or multi-input behavior | v5 import changes if moved from inherent method; see operation matrix. |
| `xml` | `write_xml`, `write_xml_par`, `read_xml`, `read_xml_streaming` | I/O extension or free source/sink builder; immediate writes currently execute local collect | v5 import changes if moved from inherent method; see operation matrix. |

`co_gbk` generates `cogroup_by_key_2` through `cogroup_by_key_10` via a macro; these are free multi-input functions. Proposed home: core fan-in vocabulary after topology and type validation are defined. `cloud` exports execution utilities and context methods, not `PCollection` operations; keep those with optional I/O support.

## Ownership rules and migration ledger

- v4 helper modules add inherent `PCollection` methods inside one crate. Sibling v5 crates cannot do this. Methods assigned to extension packages require an explicit trait import, re-export through `ironbeam::prelude`, or a free function.
- `collect*` and `to_hashmap` materialize locally. `to_dict` builds a `PCollection<HashMap<...>>` graph node and remains a finite-input convenience transform. v5 should expose a typed direct-runner terminal; a backend submission accepts explicit sinks and optional typed bounded result requests.
- `write_*` methods execute immediately and return counts. v5 deferred sinks should declare append/update/final semantics and commit/replay policy; any immediate helper remains a direct convenience.
- v4 `read_*_streaming` names mean lazy bounded shard reads, not unbounded streaming. Rename or document boundedness explicitly in v5.
- v4 `Timestamped<T>` stores time in the element and uses unsigned milliseconds. v5 event time and windows move to metadata separate from `T`; conversion is explicit.
- `take`, `first`, global aggregation, side inputs, joins, and `wait_on` need finite-input or window/time-alignment validation for unbounded graphs.
- Group, join, sampling, and parallel operations are compared as multisets unless a specific order is guaranteed; sequential elementwise order is checked directly.

## Cargo feature inventory

| v4 feature | v5 mapping |
| --- | --- |
| `default` | facade default selection; reconsider heavy formats before v5 release |
| `io-jsonl, io-csv, io-parquet, io-avro, io-xml` | ironbeam-io format features |
| `io-msgpack, io-cbor, io-protobuf, io-tfrecord, io-arrow, io-sql, io-mongodb` | optional ironbeam-io formats/adapters; keep database clients out of core |
| `compression-gzip, compression-zstd, compression-bzip2, compression-xz` | ironbeam-io codec features |
| `parallel-io` | I/O scheduling or direct runner option; inspect current empty flag before carrying forward |
| `metrics` | direct runner telemetry feature |
| `checkpointing, spilling` | direct runner local persistence features; distinct from portable checkpoint/state schema |
| `coders` | core coder metadata plus concrete Postcard codec; v4 tightens every `Element` to Serde |

Feature test tiers: default, `--no-default-features`, each optional feature, and all features. The default unit/integration tests include testcontainers dependencies; live Docker tests are separately identified by `tests/io/sql_containers.rs` and `tests/io/mongodb.rs` and must not be mistaken for portable unit coverage.
