import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))

from check_public_rustdoc import inspect_crate


class PublicRustdocTests(unittest.TestCase):
    def test_public_trait_method_needs_its_own_docs(self):
        data = {
            "index": {
                "1": {
                    "crate_id": 0,
                    "visibility": "public",
                    "name": "Runner",
                    "inner": {"trait": {"items": [2]}},
                },
                "2": {
                    "crate_id": 0,
                    "visibility": "default",
                    "name": "run",
                    "inner": {"function": {}},
                    "docs": None,
                    "span": {"filename": "src/lib.rs", "begin": [12, 1]},
                },
            }
        }
        failures, functions, methods = inspect_crate(data, "fixture")
        self.assertEqual(functions, 0)
        self.assertEqual(methods, 1)
        self.assertEqual(
            failures,
            ["fixture: src/lib.rs:12 Runner::run lacks Rustdoc"],
        )

        data["index"]["2"]["docs"] = "Run one bundle."
        self.assertEqual(inspect_crate(data, "fixture"), ([], 0, 1))

    def test_public_function_needs_docs_but_foreign_function_is_ignored(self):
        data = {
            "index": {
                "1": {
                    "crate_id": 0,
                    "visibility": "public",
                    "name": "build",
                    "inner": {"function": {}},
                    "docs": "",
                },
                "2": {
                    "crate_id": 1,
                    "visibility": "public",
                    "name": "external",
                    "inner": {"function": {}},
                },
            }
        }
        failures, functions, methods = inspect_crate(data, "fixture")
        self.assertEqual((functions, methods), (1, 0))
        self.assertEqual(failures, ["fixture: ?:? build lacks Rustdoc"])


if __name__ == "__main__":
    unittest.main()
