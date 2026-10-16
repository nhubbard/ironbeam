#!/usr/bin/env python3
"""Require direct Rustdoc on public functions in published handwritten crates."""

from __future__ import annotations

import json
import sys
import tomllib
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
GENERATED = {"beam-proto"}


def package_manifest(path: Path) -> dict:
    with path.open("rb") as source:
        return tomllib.load(source)["package"]


def main() -> int:
    doc_dir = Path(sys.argv[1]) if len(sys.argv) > 1 else ROOT / "target/doc"
    with (ROOT / "Cargo.toml").open("rb") as source:
        members = tomllib.load(source)["workspace"]["members"]

    failures = []
    checked = 0
    for member in members:
        manifest = package_manifest(ROOT / member / "Cargo.toml")
        name = manifest["name"]
        if manifest.get("publish") is False or name in GENERATED:
            continue

        report = doc_dir / f"{name.replace('-', '_')}.json"
        if not report.is_file():
            failures.append(f"{name}: missing rustdoc JSON at {report}")
            continue
        data = json.loads(report.read_text())
        checked += 1
        index = data["index"]
        for item in index.values():
            if (
                item["crate_id"] == 0
                and item["visibility"] == "public"
                and "function" in item["inner"]
                and not (item.get("docs") or "").strip()
            ):
                span = item.get("span") or {}
                failures.append(
                    f"{name}: {span.get('filename', '?')}:{(span.get('begin') or ['?'])[0]} "
                    f"{item.get('name') or '<anonymous>'} lacks Rustdoc"
                )
            if (
                item["crate_id"] == 0
                and item["visibility"] == "public"
                and "trait" in item["inner"]
            ):
                for method_id in item["inner"]["trait"]["items"]:
                    method = index.get(str(method_id))
                    if method is None or "function" not in method["inner"]:
                        continue
                    if (method.get("docs") or "").strip():
                        continue
                    span = method.get("span") or {}
                    failures.append(
                        f"{name}: {span.get('filename', '?')}:"
                        f"{(span.get('begin') or ['?'])[0]} "
                        f"{item.get('name') or '<anonymous>'}::"
                        f"{method.get('name') or '<anonymous>'} lacks Rustdoc"
                    )

    if failures:
        print("\n".join(sorted(failures)), file=sys.stderr)
        return 1
    print(f"Public function Rustdoc: {checked} published handwritten crates passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
