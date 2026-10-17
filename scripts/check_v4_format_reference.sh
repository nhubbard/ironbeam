#!/usr/bin/env bash
set -euo pipefail

cargo_bin="${IRONBEAM_CARGO:-cargo}"

"$cargo_bin" test -p ironbeam-compat --features v4-arrow,v4-parallel-io --test v4_io_arrow_ipc
"$cargo_bin" test -p ironbeam-compat --features v4-avro,v4-parallel-io --test v4_io_avro
"$cargo_bin" test -p ironbeam-compat --features v4-cbor,v4-gzip,v4-parallel-io --test v4_io_cbor
"$cargo_bin" test -p ironbeam-compat --features v4-gzip,v4-zstd,v4-bzip2,v4-xz --test v4_io_compression
"$cargo_bin" test -p ironbeam-compat --features v4-csv,v4-parallel-io --test v4_io_csv --test v4_io_csv_streaming --test v4_io_glob
"$cargo_bin" test -p ironbeam-compat --features v4-jsonl,v4-parallel-io --test v4_io_jsonl --test v4_io_jsonl_streaming --test v4_io_glob
"$cargo_bin" test -p ironbeam-compat --features v4-msgpack,v4-gzip,v4-parallel-io --test v4_io_msgpack
"$cargo_bin" test -p ironbeam-compat --features v4-parquet,v4-parallel-io --test v4_io_parquet --test v4_io_parquet_streaming --test v4_io_glob
"$cargo_bin" test -p ironbeam-compat --features v4-protobuf,v4-gzip,v4-parallel-io --test v4_io_protobuf
"$cargo_bin" test -p ironbeam-compat --features v4-tfrecord,v4-protobuf,v4-gzip,v4-parallel-io --test v4_io_tfrecord
"$cargo_bin" test -p ironbeam-compat --features v4-xml,v4-gzip,v4-zstd,v4-parallel-io --test v4_io_xml
