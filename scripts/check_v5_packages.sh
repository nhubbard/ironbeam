#!/usr/bin/env bash
set -euo pipefail

cargo_bin="${IRONBEAM_CARGO:-cargo}"

# Verify distributable packages against local v5 dependencies. No registry
# publication or crates.io publish dry run occurs here.
cd "$(dirname "$0")/.."
root="$PWD"
release_order="$(python3 scripts/check_v5_release_order.py --release --order-only)"
packages=()
while IFS= read -r package; do
  packages+=("$package")
done <<< "$release_order"
compression_patch=(--config "patch.crates-io.ironbeam-compression.path=\"$root/crates/ironbeam-compression\"")
cloud_objects_patch=(--config "patch.crates-io.ironbeam-cloud-objects.path=\"$root/crates/ironbeam-cloud-objects\"")
cloud_service_api_patch=(--config "patch.crates-io.ironbeam-cloud-service-api.path=\"$root/crates/ironbeam-cloud-service-api\"")
cloud_fakes_patch=(--config "patch.crates-io.ironbeam-cloud-fakes.path=\"$root/crates/ironbeam-cloud-fakes\"")
core_patch=(--config "patch.crates-io.ironbeam-core.path=\"$root/crates/ironbeam-core\"")
direct_patch=(--config "patch.crates-io.ironbeam-direct.path=\"$root/crates/ironbeam-direct\"" --config "patch.crates-io.ironbeam-metrics.path=\"$root/crates/ironbeam-metrics\"")
metrics_patch=(--config "patch.crates-io.ironbeam-metrics.path=\"$root/crates/ironbeam-metrics\"")
transforms_patch=(--config "patch.crates-io.ironbeam-transforms.path=\"$root/crates/ironbeam-transforms\"")
regex_patch=(--config "patch.crates-io.ironbeam-transform-regex.path=\"$root/crates/ironbeam-transform-regex\"")
approx_patch=(--config "patch.crates-io.ironbeam-transform-approx.path=\"$root/crates/ironbeam-transform-approx\"")
root_patches=()
for manifest in "$root"/crates/*/Cargo.toml; do
  package_dir="${manifest%/Cargo.toml}"
  package_name="${package_dir##*/}"
  if [[ "$package_name" != ironbeam-runner-contract-tests ]]; then
    root_patches+=(--config "patch.crates-io.$package_name.path=\"$package_dir\"")
  fi
done

if "$cargo_bin" tree -p ironbeam --no-default-features --edges normal | grep -E 'ironbeam-cloud v|hyperloglogplus v|ordered-float v|num_cpus v'; then
  echo "V5-only facade unexpectedly depends on the legacy root implementation." >&2
  exit 1
fi
if "$cargo_bin" tree -p ironbeam --no-default-features --edges normal,dev | grep -E 'testcontainers(-modules)? v'; then
  echo "V5-only root tests unexpectedly depend on the legacy service harness." >&2
  exit 1
fi
if "$cargo_bin" tree -p ironbeam --no-default-features --edges normal,dev | grep -E '(^|[[:space:]])regex v'; then
  echo "V5-only root tests unexpectedly depend on the regex engine." >&2
  exit 1
fi
if "$cargo_bin" tree -p ironbeam --no-default-features --features v5-io-csv --edges normal | grep -E 'ironbeam-cloud(-objects|-object-store)? v'; then
  echo "The isolated CSV facade feature unexpectedly enables cloud packages." >&2
  exit 1
fi
if "$cargo_bin" tree -p ironbeam --no-default-features --features parallel-io --edges normal | grep -E 'ironbeam-io-(avro|csv|xml|msgpack|cbor|protobuf|tfrecord|parquet|arrow) v'; then
  echo "The parallel-io facade feature unexpectedly enables an optional format." >&2
  exit 1
fi
if "$cargo_bin" tree -p ironbeam-io --edges normal | grep -E '(^|[[:space:]])(sqlx|mongodb|bson|arrow|parquet|apache-avro|prost|crc32c) v'; then
  echo "Default ironbeam-io unexpectedly depends on a heavy opt-in adapter." >&2
  exit 1
fi
if "$cargo_bin" tree -p ironbeam-io --no-default-features --features compression-gzip --edges normal | grep -E 'ironbeam-cloud v'; then
  echo "Compression-only v5 I/O unexpectedly depends on legacy cloud helpers." >&2
  exit 1
fi
if "$cargo_bin" tree -p ironbeam-io --all-features --edges normal | grep -E 'ironbeam-cloud v'; then
  echo "Shared v5 I/O unexpectedly depends on legacy cloud helpers." >&2
  exit 1
fi
if "$cargo_bin" tree -p ironbeam-io-jsonl --all-features --edges normal | grep -E 'ironbeam-cloud v'; then
  echo "V5 JSONL unexpectedly depends on legacy cloud helpers." >&2
  exit 1
fi

"$cargo_bin" test --workspace
"$cargo_bin" run --example cloud_io_demo --no-default-features --features cloud
"$cargo_bin" test -p ironbeam-compat --test v4_baseline --test differential --test mean --test count_per_element --test keys_values --test kv_swap --test to_dict --test to_hashmap --test to_display_string
"$cargo_bin" test -p ironbeam --test v5_facade_core --test v5_facade_prelude
"$cargo_bin" test -p ironbeam --no-default-features --test v5_facade_core --test v5_facade_prelude
"$cargo_bin" test -p ironbeam --no-default-features --features coders --test v5_facade_core
"$cargo_bin" test -p ironbeam-coder-postcard
"$cargo_bin" test -p ironbeam --no-default-features --features v5-regex --test v5_facade_prelude
"$cargo_bin" test -p ironbeam --no-default-features --features v5-approx --test v5_facade_prelude
"$cargo_bin" test -p ironbeam --no-default-features --features v5-io-csv --test v5_facade_prelude
"$cargo_bin" test -p ironbeam --no-default-features --features io-csv --test v5_facade_prelude
"$cargo_bin" test -p ironbeam --no-default-features --features v5-io-xml --test v5_facade_prelude
"$cargo_bin" test -p ironbeam --no-default-features --features v5-io-msgpack --test v5_facade_prelude
"$cargo_bin" test -p ironbeam --no-default-features --features v5-io-cbor --test v5_facade_prelude
"$cargo_bin" test -p ironbeam --no-default-features --features v5-io-protobuf --test v5_facade_prelude
"$cargo_bin" test -p ironbeam --no-default-features --features v5-io-tfrecord --test v5_facade_prelude
"$cargo_bin" test -p ironbeam-compat --test tfrecord_wire
"$cargo_bin" test -p ironbeam --no-default-features --features v5-io-arrow --test v5_facade_prelude
"$cargo_bin" test -p ironbeam --no-default-features --features io-avro,parallel-io --test v5_facade_prelude
"$cargo_bin" test -p ironbeam-io --all-features
"$cargo_bin" test -p ironbeam-compression --all-features
"$cargo_bin" test -p ironbeam-cloud-object-store --all-features
"$cargo_bin" test -p ironbeam-transform-regex --all-features
"$cargo_bin" test -p ironbeam-transform-approx --all-features
"$cargo_bin" test -p ironbeam-metrics
"$cargo_bin" test -p ironbeam-direct --features metrics --test custom_metrics
"$cargo_bin" test -p ironbeam-io-jsonl --all-features
"$cargo_bin" test -p ironbeam-io-jsonl --features cloud --test cloud_eager
"$cargo_bin" test -p ironbeam-io-protobuf --all-features
"$cargo_bin" test -p ironbeam-io-tfrecord --all-features
"$cargo_bin" test -p ironbeam-io-csv --all-features
"$cargo_bin" test -p ironbeam-io-xml --all-features
"$cargo_bin" test -p ironbeam-io-msgpack --all-features
"$cargo_bin" test -p ironbeam-io-cbor --all-features
"$cargo_bin" test -p ironbeam-io-avro --all-features
"$cargo_bin" test -p ironbeam-io-parquet --all-features
"$cargo_bin" test -p ironbeam-io-arrow --all-features
"$cargo_bin" test -p ironbeam-io-sql
"$cargo_bin" test -p ironbeam-io-mongodb
"$cargo_bin" doc --workspace --all-features --no-deps
for package in "${packages[@]}"; do
  patches=()
  case "$package" in
    ironbeam-compression|ironbeam-cloud-objects|ironbeam-core|ironbeam-metrics) ;;
    ironbeam-coder-postcard) patches=("${core_patch[@]}" "${direct_patch[@]}") ;;
    ironbeam-cloud-service-api) patches=("${cloud_objects_patch[@]}") ;;
    ironbeam-cloud-fakes) patches=("${cloud_objects_patch[@]}" "${cloud_service_api_patch[@]}") ;;
    ironbeam-cloud-ops) patches=("${cloud_objects_patch[@]}" "${cloud_service_api_patch[@]}" "${cloud_fakes_patch[@]}") ;;
    ironbeam-cloud-object-store) patches=("${cloud_objects_patch[@]}") ;;
    ironbeam-direct) patches=("${core_patch[@]}" "${metrics_patch[@]}") ;;
    ironbeam-transforms|ironbeam-transform-regex|ironbeam-transform-approx|ironbeam-testing)
      patches=("${core_patch[@]}" "${direct_patch[@]}") ;;
    ironbeam-io-sql|ironbeam-io-mongodb)
      patches=("${core_patch[@]}" "${direct_patch[@]}") ;;
    ironbeam-io)
      patches=("${compression_patch[@]}" "${cloud_objects_patch[@]}" "${core_patch[@]}" "${direct_patch[@]}") ;;
    ironbeam-io-jsonl|ironbeam-io-protobuf|ironbeam-io-tfrecord|ironbeam-io-csv|ironbeam-io-xml|ironbeam-io-msgpack|ironbeam-io-cbor|ironbeam-io-avro)
      patches=("${compression_patch[@]}" "${cloud_objects_patch[@]}" "${cloud_service_api_patch[@]}" "${cloud_fakes_patch[@]}" "${core_patch[@]}" "${direct_patch[@]}" --config "patch.crates-io.ironbeam-io.path=\"$root/crates/ironbeam-io\"") ;;
    ironbeam-io-parquet|ironbeam-io-arrow)
      patches=("${core_patch[@]}" "${direct_patch[@]}" --config "patch.crates-io.ironbeam-io.path=\"$root/crates/ironbeam-io\"") ;;
    ironbeam-compat)
      patches=("${core_patch[@]}" "${direct_patch[@]}" "${transforms_patch[@]}" "${regex_patch[@]}" "${approx_patch[@]}" --config "patch.crates-io.ironbeam-io.path=\"$root/crates/ironbeam-io\"" --config "patch.crates-io.ironbeam-io-tfrecord.path=\"$root/crates/ironbeam-io-tfrecord\"") ;;
    ironbeam) patches=("${root_patches[@]}") ;;
  esac
  "$cargo_bin" package -p "$package" --allow-dirty --offline "${patches[@]}"
done
echo "All ${#packages[@]} v5 packages, including the root facade, verified locally; no crates were published."
