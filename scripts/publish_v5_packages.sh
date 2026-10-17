#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."
mode="${1:-plan}"
release_version="${IRONBEAM_RELEASE_VERSION:-5.0.0}"

if [[ "$mode" == plan ]]; then
  python3 scripts/check_v5_release_order.py --release
  exit 0
fi
if [[ "$mode" != publish ]]; then
  echo "usage: $0 [plan|publish]" >&2
  exit 2
fi

if [[ "${IRONBEAM_RELEASE_CONFIRM:-}" != "publish-$release_version" ]]; then
  echo "Publishing requires IRONBEAM_RELEASE_CONFIRM=publish-$release_version." >&2
  exit 1
fi
if [[ "$(git branch --show-current)" != main ]]; then
  echo "Publish only from the main branch." >&2
  exit 1
fi
if [[ -z "${CARGO_REGISTRY_TOKEN:-}" ]]; then
  echo "Publishing requires CARGO_REGISTRY_TOKEN." >&2
  exit 1
fi
if [[ -n "$(git status --porcelain)" ]]; then
  echo "Publish only from a clean checkout." >&2
  exit 1
fi

release_order="$(python3 scripts/check_v5_release_order.py --expected-version "$release_version" --order-only)"
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
    registry_url="https://crates.io/api/v1/crates/$package/$release_version"
    status="$(curl --silent --show-error --retry 3 --output /dev/null --write-out '%{http_code}' "$registry_url")"
    case "$status" in
      200) echo "Already published: $package $release_version" ;;
      404)
        echo "Publishing $package $release_version"
        cargo publish -p "$package" --locked
        ;;
      *) echo "Cannot determine registry status for $package: HTTP $status" >&2; exit 1 ;;
    esac
    # Cargo's local index can lag the registry API after a new name is published.
    indexed=false
    for attempt in {1..20}; do
      if cargo info "$package@$release_version" >/dev/null 2>&1; then
        indexed=true
        break
      fi
      sleep 3
    done
    if [[ "$indexed" != true ]]; then
      echo "Registry index did not expose $package $release_version" >&2
      exit 1
    fi
  fi
done
