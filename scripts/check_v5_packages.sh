#!/usr/bin/env bash
set -euo pipefail

# Verify distributable packages against local v5 dependencies. No registry
# publication or crates.io publish dry run occurs here.
cd "$(dirname "$0")/.."
root="$PWD"
packages=(ironbeam-cloud ironbeam-core ironbeam-direct ironbeam-transforms ironbeam-io-sql ironbeam-io-mongodb ironbeam-io ironbeam-io-avro ironbeam-io-parquet ironbeam-testing ironbeam-compat)
cloud_patch=(--config "patch.crates-io.ironbeam-cloud.path=\"$root/crates/ironbeam-cloud\"")
core_patch=(--config "patch.crates-io.ironbeam-core.path=\"$root/crates/ironbeam-core\"")
direct_patch=(--config "patch.crates-io.ironbeam-direct.path=\"$root/crates/ironbeam-direct\"")
transforms_patch=(--config "patch.crates-io.ironbeam-transforms.path=\"$root/crates/ironbeam-transforms\"")

if cargo tree -p ironbeam-io --edges normal | grep -E '(^|[[:space:]])(sqlx|mongodb|bson|arrow|parquet|apache-avro) v'; then
  echo "Default ironbeam-io unexpectedly depends on a heavy opt-in adapter." >&2
  exit 1
fi

cargo test --workspace
cargo test -p ironbeam --features v5-preview --test v5_behavior_corpus --test v5_direct_differential --test v5_facade_preview --test v5_facade_prelude --test v5_tfrecord_examples
cargo test -p ironbeam --no-default-features --features v5-preview --test v5_facade_prelude
cargo test -p ironbeam-io --all-features
cargo test -p ironbeam-io-avro --all-features
cargo test -p ironbeam-io-parquet --all-features
cargo test -p ironbeam-io-sql
cargo test -p ironbeam-io-mongodb
cargo doc --workspace --all-features --no-deps
for package in "${packages[@]}"; do
  patches=()
  case "$package" in
    ironbeam-cloud|ironbeam-core) ;;
    ironbeam-direct) patches=("${core_patch[@]}") ;;
    ironbeam-transforms|ironbeam-testing)
      patches=("${core_patch[@]}" "${direct_patch[@]}") ;;
    ironbeam-io-sql|ironbeam-io-mongodb)
      patches=("${core_patch[@]}" "${direct_patch[@]}") ;;
    ironbeam-io)
      patches=("${cloud_patch[@]}" "${core_patch[@]}" "${direct_patch[@]}") ;;
    ironbeam-io-avro)
      patches=("${cloud_patch[@]}" "${core_patch[@]}" "${direct_patch[@]}" --config "patch.crates-io.ironbeam-io.path=\"$root/crates/ironbeam-io\"") ;;
    ironbeam-io-parquet)
      patches=("${core_patch[@]}" "${direct_patch[@]}" --config "patch.crates-io.ironbeam-io.path=\"$root/crates/ironbeam-io\"") ;;
    ironbeam-compat)
      patches=("${core_patch[@]}" "${direct_patch[@]}" "${transforms_patch[@]}") ;;
  esac
  cargo package -p "$package" --allow-dirty "${patches[@]}"
done
echo "All eleven v5 packages verified locally; no crates were published."
