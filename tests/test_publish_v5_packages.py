"""Release-script tests that never contact crates.io or upload a crate."""

from datetime import datetime, timedelta, timezone
from email.utils import format_datetime
from pathlib import Path
import os
import subprocess
import tempfile
import textwrap
import unittest


ROOT = Path(__file__).resolve().parents[1]
PUBLISH_SCRIPT = ROOT / "scripts" / "publish_v5_packages.sh"


class PublishRateLimitTests(unittest.TestCase):
    def run_publish(self, *, statuses: str, first_publish_error: str):
        with tempfile.TemporaryDirectory() as directory:
            temporary = Path(directory)
            binaries = temporary / "bin"
            binaries.mkdir()

            def executable(name: str, source: str):
                path = binaries / name
                path.write_text("#!/bin/sh\n" + textwrap.dedent(source))
                path.chmod(0o755)

            executable(
                "git",
                """
                if [ "$1" = branch ]; then echo main; fi
                exit 0
                """,
            )
            executable(
                "curl",
                """
                count_file="$MOCK_DIR/curl-count"
                count=0
                if [ -f "$count_file" ]; then count=$(cat "$count_file"); fi
                count=$((count + 1))
                echo "$count" > "$count_file"
                printf '%s\n' "$MOCK_CURL_STATUSES" | sed -n "${count}p"
                """,
            )
            executable(
                "cargo",
                """
                if [ "$1" = info ]; then exit 0; fi
                if [ "$1" != publish ]; then exit 2; fi
                count_file="$MOCK_DIR/publish-count"
                count=0
                if [ -f "$count_file" ]; then count=$(cat "$count_file"); fi
                count=$((count + 1))
                echo "$count" > "$count_file"
                if [ "$count" -eq 1 ]; then
                  printf '%s\n' "$MOCK_FIRST_PUBLISH_ERROR" >&2
                  exit 101
                fi
                exit 0
                """,
            )
            executable("sleep", 'echo "$1" >> "$MOCK_DIR/sleeps"')

            environment = os.environ.copy()
            environment.update(
                {
                    "PATH": f"{binaries}:{environment['PATH']}",
                    "MOCK_DIR": directory,
                    "MOCK_CURL_STATUSES": statuses,
                    "MOCK_FIRST_PUBLISH_ERROR": first_publish_error,
                    "CARGO_REGISTRY_TOKEN": "test-token",
                    "IRONBEAM_RELEASE_CONFIRM": "publish-5.0.0",
                    "IRONBEAM_RELEASE_START_AT": "ironbeam",
                }
            )
            result = subprocess.run(
                ["bash", str(PUBLISH_SCRIPT), "publish"],
                cwd=ROOT,
                env=environment,
                text=True,
                capture_output=True,
                check=False,
            )
            count_file = temporary / "publish-count"
            sleeps_file = temporary / "sleeps"
            return (
                result,
                int(count_file.read_text()) if count_file.exists() else 0,
                sleeps_file.read_text().splitlines() if sleeps_file.exists() else [],
            )

    def test_waits_for_rate_limit_then_retries(self):
        retry_at = format_datetime(datetime.now(timezone.utc) + timedelta(minutes=2), usegmt=True)
        message = f"error: got 429. Please try again after {retry_at} and see the rate-limit docs."
        result, attempts, sleeps = self.run_publish(
            statuses="404\n404", first_publish_error=message
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(attempts, 2)
        self.assertGreaterEqual(int(sleeps[0]), 120)
        self.assertLessEqual(int(sleeps[0]), 125)

    def test_rechecks_registry_before_retrying_upload(self):
        message = "error: got 429; publishing was rate limited"
        result, attempts, sleeps = self.run_publish(
            statuses="404\n200", first_publish_error=message
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(attempts, 1)
        self.assertEqual(sleeps[0], "610")
        self.assertIn("Already published: ironbeam 5.0.0", result.stdout)

    def test_other_publish_failures_stop_without_waiting(self):
        result, attempts, sleeps = self.run_publish(
            statuses="404", first_publish_error="error: package verification failed"
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(attempts, 1)
        self.assertEqual(sleeps, [])


if __name__ == "__main__":
    unittest.main()
