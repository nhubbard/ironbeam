#!/usr/bin/env python3
"""Validate local package versions and print a dependency-safe publish order.

This command never contacts crates.io or publishes a package. It includes
path-based dev dependencies because Cargo verifies them during packaging.
"""

from __future__ import annotations

import argparse
import heapq
import sys
import tomllib
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent


def manifest(path: Path) -> dict:
    with path.open("rb") as source:
        return tomllib.load(source)


def release_order(root: Path = ROOT) -> list[tuple[str, str]]:
    workspace = manifest(root / "Cargo.toml")
    workspace_dependencies = workspace.get("workspace", {}).get("dependencies", {})
    package_paths = [
        (root / member / "Cargo.toml").resolve()
        for member in workspace["workspace"]["members"]
    ]
    packages: dict[str, tuple[Path, dict]] = {}
    for path in package_paths:
        data = manifest(path)
        package = data["package"]
        name = package["name"]
        if name in packages:
            raise ValueError(f"duplicate workspace package: {name}")
        packages[name] = (path, data)

    publishable = {
        name: (path, data)
        for name, (path, data) in packages.items()
        if data["package"].get("publish") not in (False, [])
    }
    dependencies = {name: set() for name in publishable}
    followers = {name: set() for name in publishable}
    for name, (path, data) in publishable.items():
        for section in ("dependencies", "build-dependencies", "dev-dependencies"):
            for alias, specification in data.get(section, {}).items():
                from_workspace = isinstance(specification, dict) and specification.get("workspace")
                if from_workspace:
                    inherited = workspace_dependencies.get(alias)
                    if inherited is None:
                        raise ValueError(f"{name}: missing workspace dependency {alias}")
                    specification = inherited
                if not isinstance(specification, dict) or "path" not in specification:
                    continue
                base = root if from_workspace else path.parent
                dependency_path = (base / specification["path"] / "Cargo.toml").resolve()
                dependency = manifest(dependency_path)["package"]
                dependency_name = dependency["name"]
                if specification.get("package", dependency_name) != dependency_name:
                    raise ValueError(f"{name}: {alias} aliases the wrong package")
                if dependency_name not in publishable:
                    raise ValueError(f"{name}: {dependency_name} is not publishable")
                expected = dependency["version"]
                if specification.get("version") != expected:
                    raise ValueError(
                        f"{name}: {dependency_name} requires {specification.get('version')!r}; "
                        f"local package is {expected!r}"
                    )
                dependencies[name].add(dependency_name)
                followers[dependency_name].add(name)

    # The public facade is the last package even when a helper is independent.
    if "ironbeam" in dependencies:
        for name in publishable:
            if name != "ironbeam":
                dependencies["ironbeam"].add(name)
                followers[name].add("ironbeam")

    ready = [name for name, before in dependencies.items() if not before]
    heapq.heapify(ready)
    order = []
    while ready:
        name = heapq.heappop(ready)
        order.append((name, publishable[name][1]["package"]["version"]))
        for follower in sorted(followers[name]):
            dependencies[follower].remove(name)
            if not dependencies[follower]:
                heapq.heappush(ready, follower)
    if len(order) != len(publishable):
        raise ValueError("workspace package dependencies contain a cycle")
    return order


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--order-only", action="store_true", help="print package names only")
    parser.add_argument("--release", action="store_true", help="require a uniform v5 or later release version")
    parser.add_argument("--expected-version", help="require every publishable package at this version")
    args = parser.parse_args()
    try:
        order = release_order()
        if args.release:
            if order[-1][0] != "ironbeam" or int(order[-1][1].split(".", 1)[0]) < 5:
                raise ValueError("the public ironbeam package must be v5 or later")
            if any(version != order[-1][1] for _, version in order):
                raise ValueError("all publishable packages must share the release version")
        if args.expected_version:
            mismatches = [(name, version) for name, version in order if version != args.expected_version]
            if mismatches:
                raise ValueError(f"packages do not match {args.expected_version}: {mismatches}")
    except (OSError, KeyError, ValueError, tomllib.TOMLDecodeError) as error:
        print(f"release order invalid: {error}", file=sys.stderr)
        return 1
    for name, version in order:
        print(name if args.order_only else f"{name} {version}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
