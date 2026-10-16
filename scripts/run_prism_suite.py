#!/usr/bin/env python3
"""Run every nondestructive Prism fixture against short-lived job services."""

from __future__ import annotations

import argparse
import json
import os
import signal
import socket
import subprocess
import sys
import time
from pathlib import Path


PANIC = "panic: sync: WaitGroup is reused before previous Wait has returned"
EXCLUDED = {
    # This deliberate Control-owner loss has its own fresh-Prism workflow step.
    "prism_fails_closed_on_supervised_worker_loss_with_active_bundles",
    # The workflow repeats this residual-scheduling case eight times first.
    "prism_runs_production_unbounded_source_with_residual_resume",
}


def test_binary() -> Path:
    command = [
        "cargo", "test", "-p", "ironbeam-prism-proof", "--test", "prism_v5_run",
        "--locked", "--no-run", "--message-format=json",
    ]
    result = subprocess.run(command, text=True, capture_output=True, check=True)
    binaries = []
    for line in result.stdout.splitlines():
        try:
            message = json.loads(line)
        except json.JSONDecodeError:
            continue
        if (
            message.get("reason") == "compiler-artifact"
            and message.get("target", {}).get("name") == "prism_v5_run"
            and message.get("executable")
        ):
            binaries.append(Path(message["executable"]))
    if len(binaries) != 1:
        raise RuntimeError(f"expected one Prism test binary, found {len(binaries)}\n{result.stderr}")
    return binaries[0]


def suite_names(binary: Path) -> list[str]:
    output = subprocess.run([str(binary), "--list"], text=True, capture_output=True, check=True)
    names = [line.removesuffix(": test") for line in output.stdout.splitlines() if line.endswith(": test")]
    ignored_output = subprocess.run(
        [str(binary), "--list", "--ignored"], text=True, capture_output=True, check=True
    )
    ignored = {
        line.removesuffix(": test")
        for line in ignored_output.stdout.splitlines()
        if line.endswith(": test")
    }
    if len(names) != len(set(names)) or not EXCLUDED.issubset(names):
        raise RuntimeError("Prism fixture listing has duplicate or missing expected test names")
    if not ignored.issubset(names):
        raise RuntimeError("ignored Prism fixtures are missing from the full listing")
    print(f"Excluded {len(EXCLUDED)} separately run and {len(ignored)} ignored fixtures", flush=True)
    return [name for name in names if name not in EXCLUDED and name not in ignored]


def port_open(port: int) -> bool:
    with socket.socket() as connection:
        connection.settimeout(0.2)
        return connection.connect_ex(("127.0.0.1", port)) == 0


def wait_for_port(port: int, expected: bool, deadline_seconds: int = 60) -> None:
    deadline = time.monotonic() + deadline_seconds
    while time.monotonic() < deadline:
        if port_open(port) is expected:
            return
        time.sleep(0.2)
    raise RuntimeError(f"Prism port {port} did not become {'ready' if expected else 'free'}")


def stop_prior(pid_file: Path, port: int) -> None:
    if pid_file.is_file():
        pid = int(pid_file.read_text().strip())
        try:
            os.kill(pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
    wait_for_port(port, False)


def start_prism(prism: Path, pid_file: Path, log_dir: Path, port: int, index: int) -> Path:
    log_path = log_dir / f"prism-shard-{index}.log"
    with log_path.open("wb") as log:
        process = subprocess.Popen(
            [str(prism), f"-job_port={port}", "-serve_http=false"],
            stdout=log,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
    pid_file.write_text(f"{process.pid}\n")
    try:
        wait_for_port(port, True)
    except RuntimeError:
        print(log_path.read_text(errors="replace"), file=sys.stderr)
        raise
    return log_path


def run_suite(prism: Path, pid_file: Path, log_dir: Path, port: int, shard_size: int) -> None:
    binary = test_binary()
    names = suite_names(binary)
    print(f"Running {len(names)} listed Prism fixtures in shards of {shard_size}", flush=True)
    stop_prior(pid_file, port)
    shard = 0
    log_path: Path | None = None
    for number, name in enumerate(names):
        if number % shard_size == 0:
            if log_path is not None:
                stop_prior(pid_file, port)
            shard += 1
            log_path = start_prism(prism, pid_file, log_dir, port, shard)
        command = [str(binary), name, "--exact", "--test-threads=1"]
        result = subprocess.run(command, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        if result.returncode and log_path is not None and PANIC in log_path.read_text(errors="replace"):
            print(f"Pinned Prism panicked during {name}; retrying that fixture once on a fresh service", flush=True)
            stop_prior(pid_file, port)
            shard += 1
            log_path = start_prism(prism, pid_file, log_dir, port, shard)
            result = subprocess.run(command, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        if result.returncode:
            print(result.stdout, file=sys.stderr)
            if log_path is not None:
                print(log_path.read_text(errors="replace"), file=sys.stderr)
            raise RuntimeError(f"Prism fixture failed: {name}")
        print(f"[{number + 1}/{len(names)}] {name}: ok", flush=True)
    print(f"All {len(names)} listed nondestructive Prism fixtures passed", flush=True)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--prism", type=Path, required=True)
    parser.add_argument("--pid-file", type=Path, required=True)
    parser.add_argument("--log-dir", type=Path, required=True)
    parser.add_argument("--port", type=int, default=8073)
    parser.add_argument("--shard-size", type=int, default=25)
    args = parser.parse_args()
    if args.shard_size < 1:
        parser.error("shard size must be positive")
    try:
        run_suite(args.prism, args.pid_file, args.log_dir, args.port, args.shard_size)
    except (OSError, RuntimeError, subprocess.CalledProcessError) as error:
        print(error, file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
