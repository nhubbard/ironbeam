import os
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))

from check_v5_release_order import release_order


class ReleaseOrderTests(unittest.TestCase):
    def test_publish_script_requires_explicit_confirmation(self):
        environment = dict(os.environ)
        environment.pop("IRONBEAM_RELEASE_CONFIRM", None)
        result = subprocess.run(
            ["bash", "scripts/publish_v5_packages.sh", "publish"],
            cwd=ROOT,
            env=environment,
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("requires IRONBEAM_RELEASE_CONFIRM", result.stderr)

    def test_current_workspace_has_public_facade_last(self):
        order = release_order()
        self.assertEqual(len(order), 35)
        packages = [name for name, _ in order]
        self.assertLess(packages.index("ironbeam-coder-row"), packages.index("ironbeam-harness"))
        self.assertEqual(order[-1], ("ironbeam", "5.0.0"))

    def workspace(self, dependency_version="5.0.0", reverse_dependency=False):
        temporary = tempfile.TemporaryDirectory()
        root = Path(temporary.name)
        base = root / "crates" / "ironbeam-core"
        base.mkdir(parents=True)
        (root / "Cargo.toml").write_text(
            '[workspace]\nmembers = [".", "crates/ironbeam-core"]\n'
            '[package]\nname = "ironbeam"\nversion = "5.0.0"\n'
            '[dependencies]\n'
            f'ironbeam-core = {{ path = "crates/ironbeam-core", version = "{dependency_version}" }}\n'
        )
        (base / "Cargo.toml").write_text(
            '[package]\nname = "ironbeam-core"\nversion = "5.0.0"\n'
            + (
                '[dependencies]\nironbeam = { path = "../..", version = "5.0.0" }\n'
                if reverse_dependency
                else ""
            )
        )
        return temporary, root

    def test_dependency_before_public_facade(self):
        temporary, root = self.workspace()
        with temporary:
            self.assertEqual(
                release_order(root),
                [("ironbeam-core", "5.0.0"), ("ironbeam", "5.0.0")],
            )

    def test_workspace_inherited_local_dependency(self):
        temporary, root = self.workspace()
        with temporary:
            manifest = root / "Cargo.toml"
            source = manifest.read_text().replace(
                '[package]\n',
                '[workspace.dependencies]\n'
                'ironbeam-core = { path = "crates/ironbeam-core", version = "5.0.0" }\n'
                '[package]\n',
                1,
            ).replace(
                '[dependencies]\nironbeam-core = { path = "crates/ironbeam-core", version = "5.0.0" }',
                '[dependencies]\nironbeam-core = { workspace = true }',
            )
            manifest.write_text(source)
            self.assertEqual(release_order(root)[0], ("ironbeam-core", "5.0.0"))

    def test_rejects_stale_local_version_requirement(self):
        temporary, root = self.workspace(dependency_version="5.1.0")
        with temporary, self.assertRaisesRegex(ValueError, "requires '5.1.0'"):
            release_order(root)

    def test_rejects_path_only_dependency_in_nonpublishable_package(self):
        temporary, root = self.workspace()
        with temporary:
            helper = root / "crates" / "contract-tests"
            helper.mkdir()
            (helper / "Cargo.toml").write_text(
                '[package]\nname = "contract-tests"\nversion = "5.0.0"\npublish = false\n'
                '[dev-dependencies]\n'
                'ironbeam-core = { path = "../ironbeam-core" }\n'
            )
            manifest = root / "Cargo.toml"
            manifest.write_text(
                manifest.read_text().replace(
                    '"crates/ironbeam-core"]',
                    '"crates/ironbeam-core", "crates/contract-tests"]',
                )
            )
            with self.assertRaisesRegex(ValueError, "requires None"):
                release_order(root)

    def test_rejects_dependency_cycle(self):
        temporary, root = self.workspace(reverse_dependency=True)
        with temporary, self.assertRaisesRegex(ValueError, "cycle"):
            release_order(root)

    def test_rejects_git_dependency(self):
        temporary, root = self.workspace()
        with temporary:
            manifest = root / "Cargo.toml"
            manifest.write_text(
                manifest.read_text()
                + '[dev-dependencies]\nremote = { git = "https://example.invalid/remote" }\n'
            )
            with self.assertRaisesRegex(ValueError, "Git dependency"):
                release_order(root)

    def test_rejects_registry_patch(self):
        temporary, root = self.workspace()
        with temporary:
            manifest = root / "Cargo.toml"
            manifest.write_text(
                manifest.read_text()
                + '[patch.crates-io]\nremote = { path = "../remote" }\n'
            )
            with self.assertRaisesRegex(ValueError, "patches"):
                release_order(root)

    def test_publish_resumes_after_a_package_reaches_registry(self):
        temporary, root = self.workspace()
        with temporary:
            source = ROOT / "scripts"
            scripts = root / "scripts"
            scripts.mkdir()
            for name in ("check_v5_release_order.py", "publish_v5_packages.sh"):
                shutil.copyfile(source / name, scripts / name)
            commands = root / "bin"
            commands.mkdir()
            stubs = {
                "git": '#!/bin/sh\nif [ "$1" = branch ]; then echo main; fi\n',
                "curl": '#!/bin/bash\ncase "${!#}" in *ironbeam-core*) echo 200;; *) echo 404;; esac\n',
                "cargo": '#!/bin/sh\necho "$*" >> "$CALLS_FILE"\n',
            }
            for name, body in stubs.items():
                path = commands / name
                path.write_text(body)
                path.chmod(0o755)
            calls = root / "calls.txt"
            environment = dict(os.environ)
            environment.update({
                "PATH": f"{commands}:{environment['PATH']}",
                "CALLS_FILE": str(calls),
                "CARGO_REGISTRY_TOKEN": "fixture-token",
                "IRONBEAM_RELEASE_CONFIRM": "publish-5.0.0",
            })
            result = subprocess.run(
                ["bash", str(scripts / "publish_v5_packages.sh"), "publish"],
                cwd=root, env=environment, capture_output=True, text=True, check=False,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(calls.read_text().splitlines(), [
                "info ironbeam-core@5.0.0",
                "publish -p ironbeam --locked",
                "info ironbeam@5.0.0",
            ])


if __name__ == "__main__":
    unittest.main()
