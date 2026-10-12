#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."
mode="${1:-plan}"

if [[ "$mode" == plan ]]; then
  python3 scripts/check_v5_release_order.py --release
  exit 0
fi
if [[ "$mode" != publish ]]; then
  echo "usage: $0 [plan|publish]" >&2
  exit 2
fi

if [[ "${IRONBEAM_RELEASE_CONFIRM:-}" != publish-5.0.0 ]]; then
  echo "Publishing requires IRONBEAM_RELEASE_CONFIRM=publish-5.0.0." >&2
  exit 1
fi
if [[ "$(git branch --show-current)" != main ]]; then
  echo "Publish only from the main branch." >&2
  exit 1
fi
if [[ -n "$(git status --porcelain)" ]]; then
  echo "Publish only from a clean checkout." >&2
  exit 1
fi

release_order="$(python3 scripts/check_v5_release_order.py --release --order-only)"
packages=()
while IFS= read -r package; do
  packages+=("$package")
done <<< "$release_order"

start_at="${IRONBEAM_RELEASE_START_AT:-${packages[0]}}"
found_start=false
for package in "${packages[@]}"; do
  if [[ "$package" == "$start_at" ]]; then
    found_start=true
  fi
done
if [[ "$found_start" != true ]]; then
  echo "Unknown release start package: $start_at" >&2
  exit 1
fi

started=false
for package in "${packages[@]}"; do
  if [[ "$package" == "$start_at" ]]; then
    started=true
  fi
  if [[ "$started" == true ]]; then
    echo "Publishing $package"
    cargo publish -p "$package" --locked
  fi
done
