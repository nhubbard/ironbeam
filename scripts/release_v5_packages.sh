#!/usr/bin/env bash
set -euo pipefail

# The root ironbeam facade remains version 4; this script stages only the v5
# leaf packages. Publishing is an explicit, protected workflow action.
mode="${1:-check}"
if [[ "$mode" != check && "$mode" != publish ]]; then
  echo "usage: $0 [check|publish]" >&2
  exit 2
fi

cd "$(dirname "$0")/.."
packages=(ironbeam-core ironbeam-direct ironbeam-transforms ironbeam-io ironbeam-compat)
version=0.1.0

cargo test --workspace
cargo doc --workspace --all-features --no-deps
for package in "${packages[@]}"; do
  cargo package -p "$package" --list --allow-dirty >/dev/null
done

if [[ "$mode" == check ]]; then
  cargo publish -p ironbeam-core --dry-run --allow-dirty
  echo "Dependent publish dry runs follow each preceding crate's registry publication."
  exit 0
fi

if [[ -n "$(git status --porcelain)" ]]; then
  echo "Publishing requires a clean checkout." >&2
  exit 1
fi

for package in "${packages[@]}"; do
  # A newly published dependency may take time to appear in the registry index.
  ready=false
  for attempt in {1..12}; do
    if cargo publish -p "$package" --dry-run; then
      ready=true
      break
    fi
    if [[ "$attempt" -lt 12 ]]; then
      sleep 10
    fi
  done
  if [[ "$ready" != true ]]; then
    echo "Publish dry run never passed for $package." >&2
    exit 1
  fi
  cargo publish -p "$package"
  echo "Published $package $version."
done
