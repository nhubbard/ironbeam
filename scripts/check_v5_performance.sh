#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

"${IRONBEAM_CARGO:-cargo}" build --release -p ironbeam --example v5_benchmark_corpus
"${IRONBEAM_CARGO:-cargo}" build --release -p ironbeam-compat --example v4_benchmark_corpus

target_dir="${CARGO_TARGET_DIR:-$repo_root/target}"
python3 -B scripts/compare_v4_v5_benchmarks.py \
  --v4 "$target_dir/release/examples/v4_benchmark_corpus" \
  --v5 "$target_dir/release/examples/v5_benchmark_corpus" \
  --rows 1000000 --warmups 2 --samples 8 --rss \
  --v5-memory-budget-bytes 1073741824 \
  --max-v5-ms source_copy=1.5 \
  --max-v5-ms map_filter=3.5 \
  --max-v5-ms group_by_key=16 \
  --max-v5-ms latest_per_key=16 \
  --max-v5-ms join_inner=160 \
  --max-v5-ms join_duplicates=16 \
  --max-v5-ms jsonl_roundtrip=75 \
  --max-v5-rss-mib source_copy=24 \
  --max-v5-rss-mib map_filter=16 \
  --max-v5-rss-mib group_by_key=32 \
  --max-v5-rss-mib latest_per_key=16 \
  --max-v5-rss-mib join_inner=350 \
  --max-v5-rss-mib join_duplicates=56 \
  --max-v5-rss-mib jsonl_roundtrip=24
