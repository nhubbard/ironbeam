#!/usr/bin/env bash
set -euo pipefail

# Verify distributable packages against local v5 dependencies. No registry
# publication or crates.io publish dry run occurs here.
cd "$(dirname "$0")/.."
root="$PWD"
packages=(ironbeam-core ironbeam-direct ironbeam-transforms ironbeam-io ironbeam-compat)
core_patch=(--config "patch.crates-io.ironbeam-core.path=\"$root/crates/ironbeam-core\"")
direct_patch=(--config "patch.crates-io.ironbeam-direct.path=\"$root/crates/ironbeam-direct\"")
transforms_patch=(--config "patch.crates-io.ironbeam-transforms.path=\"$root/crates/ironbeam-transforms\"")

cargo test --workspace
cargo doc --workspace --all-features --no-deps
for package in "${packages[@]}"; do
  patches=()
  case "$package" in
    ironbeam-core) ;;
    ironbeam-direct) patches=("${core_patch[@]}") ;;
    ironbeam-transforms|ironbeam-io)
      patches=("${core_patch[@]}" "${direct_patch[@]}") ;;
    ironbeam-compat)
      patches=("${core_patch[@]}" "${direct_patch[@]}" "${transforms_patch[@]}") ;;
  esac
  cargo package -p "$package" --allow-dirty "${patches[@]}"
done
echo "All five v5 packages verified locally; no crates were published."
