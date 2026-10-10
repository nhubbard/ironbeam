#!/usr/bin/env python3
"""Enforce package-scoped line and branch coverage against measured baselines."""

import json
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
BASELINES = ROOT / "coverage-baselines.json"
IO_HELPERS = {
    "arrow_ipc.rs", "avro.rs", "cbor.rs", "cloud.rs", "csv.rs",
    "jsonl.rs", "mongodb.rs", "msgpack.rs", "parquet.rs",
    "protobuf.rs", "sql.rs", "tfrecord.rs", "xml.rs",
}
DIRECT_FILES = {
    "checkpoint.rs", "metrics.rs", "planner.rs", "runner.rs",
    "spill.rs", "spill_integration.rs",
}
DIRECT_HELPERS = {"collect_sorted.rs", "reshuffle.rs", "tee.rs", "wait_on.rs"}
V5_PACKAGES = {"ironbeam-core", "ironbeam-direct", "ironbeam-transforms", "ironbeam-io", "ironbeam-io-avro", "ironbeam-io-parquet", "ironbeam-compat"}


def area_for(package: str, relative: Path) -> str:
    if package == "ironbeam-core":
        return "logical-graph"
    if package == "ironbeam-direct":
        return "direct-runner"
    if package == "ironbeam-transforms":
        return "transforms"
    if package in {"ironbeam-io", "ironbeam-io-avro", "ironbeam-io-parquet"}:
        return "bounded-io"
    if package == "ironbeam-compat":
        return "compatibility"
    if package == "ironbeam-runner-contract-tests":
        return "integration-contract"
    parts = relative.parts
    if parts[0] == "testing" or parts[0] == "testing.rs":
        return "testing"
    if parts[0] == "io" or (parts[0] == "helpers" and parts[-1] in IO_HELPERS):
        return "io"
    if parts[-1] in DIRECT_FILES or (parts[0] == "helpers" and parts[-1] in DIRECT_HELPERS):
        return "direct-runner"
    return "core-and-transforms"


def coverage_by_area(
    report: dict, source: Path, package: str
) -> dict[str, dict[str, tuple[int, int]]]:
    source = source.resolve()
    areas: dict[str, dict[str, list[int]]] = {}
    for data in report["data"]:
        for file in data["files"]:
            filename = Path(file["filename"]).resolve()
            if not filename.is_relative_to(source):
                continue
            area = area_for(package, filename.relative_to(source))
            totals = areas.setdefault(area, {"lines": [0, 0], "branches": [0, 0]})
            for kind in ("lines", "branches"):
                summary = file["summary"][kind]
                totals[kind][0] += summary["covered"]
                totals[kind][1] += summary["count"]
    return {
        area: {kind: tuple(values) for kind, values in totals.items()}
        for area, totals in areas.items()
    }


def coverage_for_source(report: dict, source: Path) -> dict[str, tuple[int, int]]:
    areas = coverage_by_area(report, source, "ironbeam")
    return {
        kind: tuple(sum(area[kind][index] for area in areas.values()) for index in (0, 1))
        for kind in ("lines", "branches")
    }


def source_location_coverage(report: dict, source: Path) -> dict[str, tuple[int, int]]:
    """Count each Rust source line/branch once across generic instantiations.

    LLVM reports every monomorphization separately in file summaries. That
    turns a newly used generic API into apparent uncovered lines even when its
    source path is exercised by another type. This keeps the gate about source
    paths while retaining the raw instrumented totals in the area report.
    """
    source = source.resolve()
    lines: dict[tuple[Path, int], bool] = {}
    branches: dict[tuple[Path, int, int, int, int], list[int]] = {}
    for data in report["data"]:
        for file in data["files"]:
            filename = Path(file["filename"]).resolve()
            if not filename.is_relative_to(source):
                continue
            for segment in file["segments"]:
                line, _, count, executable = segment[:4]
                if executable:
                    key = (filename, line)
                    lines[key] = lines.get(key, False) or count > 0
            for branch in file["branches"]:
                key = (filename, *branch[:4])
                counts = branches.setdefault(key, [0, 0])
                counts[0] += branch[4]
                counts[1] += branch[5]
    active_branches = [counts for counts in branches.values() if sum(counts) > 0]
    return {
        "lines": (sum(lines.values()), len(lines)),
        "branches": (
            sum((counts[0] > 0) + (counts[1] > 0) for counts in active_branches),
            2 * len(active_branches),
        ),
    }


def meets_baseline(covered: int, count: int, old_covered: int, old_count: int) -> bool:
    return count > 0 and old_count > 0 and covered * old_count >= old_covered * count


def main() -> int:
    if len(sys.argv) != 3:
        print("usage: check_coverage.py PACKAGE REPORT.json", file=sys.stderr)
        return 2
    package, report_path = sys.argv[1:]
    baselines = json.loads(BASELINES.read_text())
    if package not in baselines:
        print(f"unknown coverage package: {package}", file=sys.stderr)
        return 2
    baseline = baselines[package]
    source = (ROOT / baseline["source"]).resolve()
    report = json.loads(Path(report_path).read_text())
    areas = coverage_by_area(report, source, package)
    if package in V5_PACKAGES:
        actual = source_location_coverage(report, source)
    else:
        actual = {
            kind: tuple(sum(area[kind][index] for area in areas.values()) for index in (0, 1))
            for kind in ("lines", "branches")
        }
    failed = False
    for kind in ("lines", "branches"):
        covered, count = actual[kind]
        old_covered = baseline[kind]["covered"]
        old_count = baseline[kind]["count"]
        if kind == "branches" and old_count == 0 and count == 0:
            print(f"{package} branches: no instrumented branches")
            continue
        if count == 0 or old_count == 0:
            print(f"{package}: missing {kind} measurements", file=sys.stderr)
            failed = True
            continue
        minimum = 100 * old_covered / old_count
        current = 100 * covered / count
        label = "source locations" if package in V5_PACKAGES else "instrumented"
        print(f"{package} {kind} ({label}): {covered}/{count} ({current:.2f}%), baseline {minimum:.2f}%")
        if not meets_baseline(covered, count, old_covered, old_count):
            failed = True
    for area, counts in sorted(areas.items()):
        line_covered, line_count = counts["lines"]
        branch_covered, branch_count = counts["branches"]
        print(
            f"  {area}: lines {line_covered}/{line_count}, "
            f"branches {branch_covered}/{branch_count}"
        )
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
