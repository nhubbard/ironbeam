#!/usr/bin/env python3
"""Set every workspace package and local dependency to a Semantic Release version."""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
import tomllib
from pathlib import Path

from check_v5_release_order import release_order


ROOT = Path(__file__).resolve().parent.parent
VERSION = re.compile(r"^\d+\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?$")
PACKAGE_VERSION = re.compile(r'(?m)^(version\s*=\s*")([^"\n]+)(")')
INLINE_VERSION = re.compile(r'(\bversion\s*=\s*")([^"\n]+)(")')


def prepare(root: Path, version: str, update_lock: bool = True) -> None:
    if not VERSION.fullmatch(version) or int(version.split(".", 1)[0]) < 5:
        raise ValueError(f"expected a v5 or later release version, got {version!r}")
    release_order(root)
    with (root / "Cargo.toml").open("rb") as source:
        workspace = tomllib.load(source)
    paths = [root / member / "Cargo.toml" for member in workspace["workspace"]["members"]]
    versions = set()
    changes = {}
    for path in paths:
        original = path.read_text()
        with path.open("rb") as source:
            parsed = tomllib.load(source)
        previous = parsed["package"]["version"]
        versions.add(previous)
        # Only the package section and path dependencies belong to this workspace.
        lines = original.splitlines(keepends=True)
        section = ""
        replacements = 0
        for index, line in enumerate(lines):
            heading = re.match(r"^\[([^]]+)\]", line)
            if heading:
                section = heading.group(1)
            if section == "package" and line.startswith("version"):
                lines[index], count = PACKAGE_VERSION.subn(lambda match: match[1] + version + match[3], line)
                replacements += count
            elif section in ("dependencies", "build-dependencies", "dev-dependencies") and "path" in line:
                lines[index], _ = INLINE_VERSION.subn(lambda match: match[1] + version + match[3], line)
            elif path == root / "Cargo.toml" and section == "workspace.dependencies" and "path" in line:
                lines[index], _ = INLINE_VERSION.subn(lambda match: match[1] + version + match[3], line)
        if replacements != 1:
            raise ValueError(f"expected one package version in {path}")
        modified = "".join(lines)
        with_version = tomllib.loads(modified)
        if with_version["package"]["version"] != version:
            raise ValueError(f"failed to update {path}")
        changes[path] = modified
    if len(versions) != 1:
        raise ValueError(f"workspace package versions differ: {sorted(versions)}")
    for path, modified in changes.items():
        if path.read_text() != modified:
            path.write_text(modified)
    if update_lock and versions != {version}:
        subprocess.run(["cargo", "update", "--workspace"], cwd=root, check=True)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("version")
    parser.add_argument("--root", type=Path, default=ROOT)
    parser.add_argument("--skip-lock", action="store_true", help="fixture testing only")
    args = parser.parse_args()
    try:
        prepare(args.root, args.version, not args.skip_lock)
    except (OSError, KeyError, ValueError, subprocess.CalledProcessError, tomllib.TOMLDecodeError) as error:
        print(f"workspace version preparation failed: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
