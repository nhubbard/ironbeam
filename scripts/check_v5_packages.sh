#!/usr/bin/env bash
set -euo pipefail

# Verify distributable packages against local v5 dependencies. No registry
# publication or crates.io publish dry run occurs here.
cd "$(dirname "$0")/.."
root="$PWD"
packages=(ironbeam-compression ironbeam-cloud-objects ironbeam-cloud ironbeam-cloud-object-store ironbeam-core ironbeam-direct ironbeam-transforms ironbeam-transform-regex ironbeam-transform-approx ironbeam-io-sql ironbeam-io-mongodb ironbeam-io ironbeam-io-jsonl ironbeam-io-protobuf ironbeam-io-tfrecord ironbeam-io-csv ironbeam-io-xml ironbeam-io-msgpack ironbeam-io-cbor ironbeam-io-avro ironbeam-io-parquet ironbeam-io-arrow ironbeam-testing ironbeam-compat)
compression_patch=(--config "patch.crates-io.ironbeam-compression.path=\"$root/crates/ironbeam-compression\"")
cloud_objects_patch=(--config "patch.crates-io.ironbeam-cloud-objects.path=\"$root/crates/ironbeam-cloud-objects\"")
cloud_patch=(--config "patch.crates-io.ironbeam-cloud.path=\"$root/crates/ironbeam-cloud\"")
core_patch=(--config "patch.crates-io.ironbeam-core.path=\"$root/crates/ironbeam-core\"")
direct_patch=(--config "patch.crates-io.ironbeam-direct.path=\"$root/crates/ironbeam-direct\"")
transforms_patch=(--config "patch.crates-io.ironbeam-transforms.path=\"$root/crates/ironbeam-transforms\"")
regex_patch=(--config "patch.crates-io.ironbeam-transform-regex.path=\"$root/crates/ironbeam-transform-regex\"")
approx_patch=(--config "patch.crates-io.ironbeam-transform-approx.path=\"$root/crates/ironbeam-transform-approx\"")

if cargo tree -p ironbeam --no-default-features --features v5-preview --edges normal | grep -E 'ironbeam-cloud v|hyperloglogplus v|ordered-float v|num_cpus v'; then
  echo "V5-only facade unexpectedly depends on the legacy root implementation." >&2
  exit 1
fi
if cargo tree -p ironbeam-io --edges normal | grep -E '(^|[[:space:]])(sqlx|mongodb|bson|arrow|parquet|apache-avro|prost|crc32c) v'; then
  echo "Default ironbeam-io unexpectedly depends on a heavy opt-in adapter." >&2
  exit 1
fi
if cargo tree -p ironbeam-io --no-default-features --features compression-gzip --edges normal | grep -E 'ironbeam-cloud v'; then
  echo "Compression-only v5 I/O unexpectedly depends on legacy cloud helpers." >&2
  exit 1
fi
if cargo tree -p ironbeam-io --all-features --edges normal | grep -E 'ironbeam-cloud v'; then
  echo "Shared v5 I/O unexpectedly depends on legacy cloud helpers." >&2
  exit 1
fi
if cargo tree -p ironbeam-io-jsonl --all-features --edges normal | grep -E 'ironbeam-cloud v'; then
  echo "V5 JSONL unexpectedly depends on legacy cloud helpers." >&2
  exit 1
fi

cargo test --workspace
cargo test -p ironbeam --features legacy-v4,v5-preview --test v5_behavior_corpus --test v5_direct_differential --test v5_facade_preview --test v5_facade_prelude --test v5_tfrecord_examples
cargo test -p ironbeam --no-default-features --features v5-preview --test v5_facade_prelude
cargo test -p ironbeam --no-default-features --features v5-regex --test v5_facade_prelude
cargo test -p ironbeam --no-default-features --features v5-approx --test v5_facade_prelude
cargo test -p ironbeam --no-default-features --features v5-io-csv --test v5_facade_prelude
cargo test -p ironbeam --no-default-features --features io-csv --test v5_facade_prelude
cargo test -p ironbeam --no-default-features --features v5-io-xml --test v5_facade_prelude
cargo test -p ironbeam --no-default-features --features v5-io-msgpack --test v5_facade_prelude
cargo test -p ironbeam --no-default-features --features v5-io-cbor --test v5_facade_prelude
cargo test -p ironbeam --no-default-features --features v5-io-protobuf --test v5_facade_prelude
cargo test -p ironbeam --no-default-features --features v5-io-tfrecord --test v5_facade_prelude
cargo test -p ironbeam --features legacy-v4,v5-io-tfrecord,io-tfrecord,io-protobuf --test v5_tfrecord_examples
cargo test -p ironbeam --no-default-features --features v5-preview,v5-io-arrow --test v5_facade_prelude
cargo test -p ironbeam-io --all-features
cargo test -p ironbeam-compression --all-features
cargo test -p ironbeam-cloud-object-store --all-features
cargo test -p ironbeam-transform-regex --all-features
cargo test -p ironbeam-transform-approx --all-features
cargo test -p ironbeam-io-jsonl --all-features
cargo test -p ironbeam-io-protobuf --all-features
cargo test -p ironbeam-io-tfrecord --all-features
cargo test -p ironbeam-io-csv --all-features
cargo test -p ironbeam-io-xml --all-features
cargo test -p ironbeam-io-msgpack --all-features
cargo test -p ironbeam-io-cbor --all-features
cargo test -p ironbeam-io-avro --all-features
cargo test -p ironbeam-io-parquet --all-features
cargo test -p ironbeam-io-arrow --all-features
cargo test -p ironbeam-io-sql
cargo test -p ironbeam-io-mongodb
cargo doc --workspace --all-features --no-deps
for package in "${packages[@]}"; do
  patches=()
  case "$package" in
    ironbeam-compression|ironbeam-cloud-objects|ironbeam-core) ;;
    ironbeam-cloud) patches=("${compression_patch[@]}" "${cloud_objects_patch[@]}") ;;
    ironbeam-cloud-object-store) patches=("${cloud_objects_patch[@]}") ;;
    ironbeam-direct) patches=("${core_patch[@]}") ;;
    ironbeam-transforms|ironbeam-transform-regex|ironbeam-transform-approx|ironbeam-testing)
      patches=("${core_patch[@]}" "${direct_patch[@]}") ;;
    ironbeam-io-sql|ironbeam-io-mongodb)
      patches=("${core_patch[@]}" "${direct_patch[@]}") ;;
    ironbeam-io)
      patches=("${compression_patch[@]}" "${cloud_objects_patch[@]}" "${core_patch[@]}" "${direct_patch[@]}") ;;
    ironbeam-io-jsonl|ironbeam-io-protobuf|ironbeam-io-tfrecord|ironbeam-io-csv|ironbeam-io-xml|ironbeam-io-msgpack|ironbeam-io-cbor|ironbeam-io-avro)
      patches=("${compression_patch[@]}" "${cloud_objects_patch[@]}" "${cloud_patch[@]}" "${core_patch[@]}" "${direct_patch[@]}" --config "patch.crates-io.ironbeam-io.path=\"$root/crates/ironbeam-io\"") ;;
    ironbeam-io-parquet|ironbeam-io-arrow)
      patches=("${core_patch[@]}" "${direct_patch[@]}" --config "patch.crates-io.ironbeam-io.path=\"$root/crates/ironbeam-io\"") ;;
    ironbeam-compat)
      patches=("${core_patch[@]}" "${direct_patch[@]}" "${transforms_patch[@]}" "${regex_patch[@]}" "${approx_patch[@]}") ;;
  esac
  cargo package -p "$package" --allow-dirty "${patches[@]}"
done
echo "All twenty-four v5 packages verified locally; no crates were published."
