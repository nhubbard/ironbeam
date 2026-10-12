import os
import subprocess
import tempfile
import unittest
from pathlib import Path

from check_v5_release_order import release_order


class ReleaseOrderTests(unittest.TestCase):
    def test_publish_script_requires_explicit_confirmation(self):
        environment = dict(os.environ)
        environment.pop("IRONBEAM_RELEASE_CONFIRM", None)
        result = subprocess.run(
            ["bash", "scripts/publish_v5_packages.sh", "publish"],
            cwd=Path(__file__).resolve().parent.parent,
            env=environment,
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("requires IRONBEAM_RELEASE_CONFIRM", result.stderr)

    def test_current_workspace_has_public_facade_last(self):
        order = release_order()
        self.assertEqual(len(order), 29)
        self.assertEqual(order[-1], ("ironbeam", "5.0.0"))

    def workspace(self, dependency_version="0.1.0", reverse_dependency=False):
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
            '[package]\nname = "ironbeam-core"\nversion = "0.1.0"\n'
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
                [("ironbeam-core", "0.1.0"), ("ironbeam", "5.0.0")],
            )

    def test_rejects_stale_local_version_requirement(self):
        temporary, root = self.workspace(dependency_version="0.2.0")
        with temporary, self.assertRaisesRegex(ValueError, "requires '0.2.0'"):
            release_order(root)

    def test_rejects_dependency_cycle(self):
        temporary, root = self.workspace(reverse_dependency=True)
        with temporary, self.assertRaisesRegex(ValueError, "cycle"):
            release_order(root)


if __name__ == "__main__":
    unittest.main()
