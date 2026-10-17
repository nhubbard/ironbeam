import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))

from check_v5_release_order import release_order
from prepare_workspace_release import prepare


class PrepareWorkspaceReleaseTests(unittest.TestCase):
    def workspace(self):
        temporary = tempfile.TemporaryDirectory()
        root = Path(temporary.name)
        (root / "crates" / "ironbeam-core").mkdir(parents=True)
        (root / "Cargo.toml").write_text(
            '[workspace]\nmembers = [".", "crates/ironbeam-core"]\n'
            '[workspace.dependencies]\n'
            'ironbeam-core = { version = "5.0.0", path = "crates/ironbeam-core" }\n'
            '[package]\nname = "ironbeam"\nversion = "5.0.0"\n'
            '[dependencies]\n'
            'ironbeam-core = { workspace = true }\n'
            'serde = { version = "1.0", features = ["derive"] }\n'
        )
        (root / "crates" / "ironbeam-core" / "Cargo.toml").write_text(
            '[package]\nname = "ironbeam-core"\nversion = "5.0.0"\n'
        )
        return temporary, root

    def test_updates_every_package_and_only_local_dependencies(self):
        temporary, root = self.workspace()
        with temporary:
            prepare(root, "5.1.0", update_lock=False)
            self.assertEqual(release_order(root), [
                ("ironbeam-core", "5.1.0"), ("ironbeam", "5.1.0")
            ])
            self.assertIn('serde = { version = "1.0"', (root / "Cargo.toml").read_text())
            self.assertIn('ironbeam-core = { version = "5.1.0", path', (root / "Cargo.toml").read_text())
            prepare(root, "5.1.0", update_lock=False)

    def test_rejects_four_x_without_changing_files(self):
        temporary, root = self.workspace()
        with temporary:
            original = (root / "Cargo.toml").read_text()
            with self.assertRaisesRegex(ValueError, "v5 or later"):
                prepare(root, "4.1.2", update_lock=False)
            self.assertEqual((root / "Cargo.toml").read_text(), original)

    def test_rejects_mixed_starting_versions(self):
        temporary, root = self.workspace()
        with temporary:
            core = root / "crates" / "ironbeam-core" / "Cargo.toml"
            core.write_text(core.read_text().replace("5.0.0", "5.0.1"))
            with self.assertRaisesRegex(ValueError, "requires"):
                prepare(root, "5.1.0", update_lock=False)
            self.assertIn('version = "5.0.0"', (root / "Cargo.toml").read_text())


if __name__ == "__main__":
    unittest.main()
