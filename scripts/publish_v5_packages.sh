#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."
mode="${1:-plan}"
release_version="${IRONBEAM_RELEASE_VERSION:-5.0.0}"

rate_limit_wait_seconds() {
  python3 - "$1" <<'PY'
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
import math
import re
import sys

message = Path(sys.argv[1]).read_text(errors="replace")
if not re.search(r"\b429\b|You have published too many", message, re.IGNORECASE):
    sys.exit(1)

match = re.search(r"Please try again after ([^\n.]+? GMT)", message)
if match:
    try:
        retry_at = parsedate_to_datetime(match.group(1))
        seconds = max(5, math.ceil((retry_at - datetime.now(timezone.utc)).total_seconds()) + 5)
    except (TypeError, ValueError):
        seconds = 610
else:
    # The new-crate bucket normally refills once every ten minutes.
    seconds = 610
print(seconds)
PY
}

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
publish_log=""
trap '[[ -z "$publish_log" ]] || rm -f "$publish_log"' EXIT
for package in "${packages[@]}"; do
  if [[ "$package" == "$start_at" ]]; then
    started=true
  fi
  if [[ "$started" == true ]]; then
    registry_url="https://crates.io/api/v1/crates/$package/$release_version"
    rate_limit_retries=0
    while true; do
      status="$(curl --silent --show-error --retry 3 \
        --user-agent 'ironbeam-publisher/5 (+https://github.com/nhubbard/ironbeam)' \
        --output /dev/null --write-out '%{http_code}' "$registry_url")"
      case "$status" in
        200)
          echo "Already published: $package $release_version"
          break
          ;;
        404)
          echo "Publishing $package $release_version"
          publish_log="$(mktemp)"
          if cargo publish -p "$package" --locked 2>&1 | tee "$publish_log"; then
            rm -f "$publish_log"
            publish_log=""
            break
          fi
          if wait_seconds="$(rate_limit_wait_seconds "$publish_log")"; then
            rm -f "$publish_log"
            publish_log=""
            rate_limit_retries=$((rate_limit_retries + 1))
            if (( rate_limit_retries > 6 )); then
              echo "Rate limit persisted after six waits for $package; stop and resume later." >&2
              exit 1
            fi
            echo "crates.io limited $package; retrying after ${wait_seconds}s."
            sleep "$wait_seconds"
            # An upload can succeed even if Cargo times out waiting for indexing.
            # Recheck the version before attempting another upload.
            continue
          fi
          rm -f "$publish_log"
          publish_log=""
          exit 1
          ;;
        *) echo "Cannot determine registry status for $package: HTTP $status" >&2; exit 1 ;;
      esac
    done
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
    # crates.io asks direct API clients to stay at or below one request per second.
    sleep 1
  fi
done
