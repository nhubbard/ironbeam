#!/usr/bin/env python3
"""Run the matched v4/v5 release corpus and verify each workload's result."""

from __future__ import annotations

import argparse
import os
import platform
import re
import statistics
import subprocess
import sys
from pathlib import Path


CASES = (
    "source_copy",
    "map_filter",
    "group_by_key",
    "latest_per_key",
    "join_inner",
    "join_duplicates",
    "jsonl_roundtrip",
)
HEADER = "case,rows,elapsed_ns,output_rows,checksum"
RSS_PATTERN = re.compile(r"^\s*(\d+)\s+maximum resident set size\s*$", re.MULTILINE)


def run_case(binary: Path, rows: int, case: str, measure_rss: bool, memory_budget: int | None = None) -> tuple[int, int, int, int | None]:
    command = [str(binary), str(rows), case]
    if measure_rss:
        command = ["/usr/bin/time", "-l", *command]
    environment = os.environ.copy()
    if memory_budget is not None:
        environment["IRONBEAM_BENCH_MEMORY_BUDGET_BYTES"] = str(memory_budget)
    try:
        completed = subprocess.run(command, capture_output=True, text=True, check=True, env=environment)
    except subprocess.CalledProcessError as error:
        raise ValueError(
            f"{binary}: {case} exited {error.returncode}: {error.stderr.strip()}"
        ) from error
    lines = completed.stdout.strip().splitlines()
    if len(lines) != 2 or lines[0] != HEADER:
        raise ValueError(f"{binary}: unexpected benchmark output for {case}: {lines!r}")
    parts = lines[1].split(",")
    if len(parts) != 5 or parts[0] != case:
        raise ValueError(f"{binary}: malformed benchmark row for {case}: {lines[1]!r}")
    measured_rows, elapsed_ns, output_rows, checksum = map(int, parts[1:])
    if measured_rows != rows or elapsed_ns <= 0:
        raise ValueError(f"{binary}: invalid row count or timing for {case}")
    rss = None
    if measure_rss:
        match = RSS_PATTERN.search(completed.stderr)
        if match is None:
            raise ValueError(f"{binary}: peak RSS was unavailable for {case}")
        rss = int(match.group(1))
    return elapsed_ns, output_rows, checksum, rss


def display_mib(value: float | None) -> str:
    return "—" if value is None else f"{value:.2f}"


def case_limits(values: list[str], option: str) -> dict[str, float]:
    limits = {}
    for value in values:
        case, separator, raw_limit = value.partition("=")
        if not separator or case not in CASES or case in limits:
            raise ValueError(f"{option}: expected a unique CASE=LIMIT for one of {', '.join(CASES)}")
        try:
            limit = float(raw_limit)
        except ValueError as error:
            raise ValueError(f"{option}: invalid limit for {case}: {raw_limit}") from error
        if not 0 < limit < float("inf"):
            raise ValueError(f"{option}: limit for {case} must be finite and positive")
        limits[case] = limit
    return limits


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--v4", type=Path, required=True, help="built v4 benchmark executable")
    parser.add_argument("--v5", type=Path, required=True, help="built v5 benchmark executable")
    parser.add_argument("--rows", type=int, default=100_000)
    parser.add_argument("--warmups", type=int, default=2)
    parser.add_argument("--samples", type=int, default=8)
    parser.add_argument("--case", action="append", choices=CASES, dest="cases")
    parser.add_argument("--rss", action="store_true", help="measure macOS peak RSS with /usr/bin/time")
    parser.add_argument("--max-latency-ratio", type=float)
    parser.add_argument("--max-rss-ratio", type=float)
    parser.add_argument("--max-v5-ms", action="append", default=[], metavar="CASE=MS", help="absolute v5 median latency limit for one case; repeat per case")
    parser.add_argument("--max-v5-rss-mib", action="append", default=[], metavar="CASE=MIB", help="absolute v5 median peak RSS limit for one case; repeat per case")
    parser.add_argument("--v5-memory-budget-bytes", type=int, help="override v5 Direct's default 64 MiB job limit for scaling runs")
    args = parser.parse_args()
    if args.rows <= 0 or args.samples <= 0 or args.warmups < 0:
        parser.error("rows and samples must be positive; warmups cannot be negative")
    if args.rss and platform.system() != "Darwin":
        parser.error("--rss currently requires macOS /usr/bin/time -l")
    if args.max_rss_ratio is not None and not args.rss:
        parser.error("--max-rss-ratio requires --rss")
    if args.max_v5_rss_mib and not args.rss:
        parser.error("--max-v5-rss-mib requires --rss")
    if args.max_latency_ratio is not None and args.max_latency_ratio <= 0:
        parser.error("--max-latency-ratio must be positive")
    if args.max_rss_ratio is not None and args.max_rss_ratio <= 0:
        parser.error("--max-rss-ratio must be positive")
    if args.v5_memory_budget_bytes is not None and args.v5_memory_budget_bytes <= 0:
        parser.error("--v5-memory-budget-bytes must be positive")
    latency_limits = case_limits(args.max_v5_ms, "--max-v5-ms")
    rss_limits = case_limits(args.max_v5_rss_mib, "--max-v5-rss-mib")
    selected_cases = args.cases or CASES
    for limit_case in (*latency_limits, *rss_limits):
        if limit_case not in selected_cases:
            parser.error(f"limit supplied for unselected case: {limit_case}")
    binaries = {"v4": args.v4.resolve(), "v5": args.v5.resolve()}
    for binary in binaries.values():
        if not binary.is_file():
            parser.error(f"benchmark executable is missing: {binary}")

    print("| Case | v4 median ms | v5 median ms | v5/v4 | v4 peak MiB | v5 peak MiB |", flush=True)
    print("| --- | ---: | ---: | ---: | ---: | ---: |", flush=True)
    exceeded = []
    for case in selected_cases:
        results: dict[str, list[tuple[int, int, int, int | None]]] = {"v4": [], "v5": []}
        expected_output = None
        for iteration in range(args.warmups + args.samples):
            for version in (("v4", "v5") if iteration % 2 == 0 else ("v5", "v4")):
                result = run_case(
                    binaries[version],
                    args.rows,
                    case,
                    args.rss,
                    args.v5_memory_budget_bytes if version == "v5" else None,
                )
                output = result[1:3]
                if expected_output is None:
                    expected_output = output
                elif output != expected_output:
                    raise ValueError(f"{case}: output count/checksum changed: {output} != {expected_output}")
                if iteration >= args.warmups:
                    results[version].append(result)
        v4_ms = statistics.median(row[0] for row in results["v4"]) / 1_000_000
        v5_ms = statistics.median(row[0] for row in results["v5"]) / 1_000_000
        latency_ratio = v5_ms / v4_ms
        v4_rss = statistics.median(row[3] for row in results["v4"]) / 1_048_576 if args.rss else None
        v5_rss = statistics.median(row[3] for row in results["v5"]) / 1_048_576 if args.rss else None
        rss_ratio = v5_rss / v4_rss if args.rss else None
        print(
            f"| {case} | {v4_ms:.3f} | {v5_ms:.3f} | {latency_ratio:.2f}× | "
            f"{display_mib(v4_rss)} | {display_mib(v5_rss)} |",
            flush=True,
        )
        if args.max_latency_ratio is not None and latency_ratio > args.max_latency_ratio:
            exceeded.append(f"{case} latency {latency_ratio:.2f}×")
        if args.max_rss_ratio is not None and rss_ratio > args.max_rss_ratio:
            exceeded.append(f"{case} RSS {rss_ratio:.2f}×")
        if case in latency_limits and v5_ms > latency_limits[case]:
            exceeded.append(f"{case} v5 latency {v5_ms:.3f} ms > {latency_limits[case]:.3f} ms")
        if case in rss_limits and v5_rss > rss_limits[case]:
            exceeded.append(f"{case} v5 RSS {v5_rss:.2f} MiB > {rss_limits[case]:.2f} MiB")
    if exceeded:
        print("Regression limit exceeded: " + ", ".join(exceeded), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, ValueError, subprocess.CalledProcessError) as error:
        print(f"benchmark comparison failed: {error}", file=sys.stderr)
        raise SystemExit(1) from error
