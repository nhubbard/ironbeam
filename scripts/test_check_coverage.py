import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from check_coverage import area_for, coverage_for_source, meets_baseline


class CoverageGateTests(unittest.TestCase):
    def test_facade_areas_are_distinct(self):
        self.assertEqual(area_for("ironbeam", Path("runner.rs")), "direct-runner")
        self.assertEqual(area_for("ironbeam", Path("io/jsonl.rs")), "io")
        self.assertEqual(area_for("ironbeam", Path("collection.rs")), "core-and-transforms")
        self.assertEqual(area_for("ironbeam", Path("testing/assertions.rs")), "testing")
        self.assertEqual(
            area_for("ironbeam-runner-contract-tests", Path("lib.rs")), "integration-contract"
        )

    def test_ratio_gate_accepts_baseline_and_rejects_regression(self):
        self.assertTrue(meets_baseline(47, 54, 47, 54))
        self.assertTrue(meets_baseline(94, 108, 47, 54))
        self.assertFalse(meets_baseline(46, 54, 47, 54))
        self.assertFalse(meets_baseline(0, 0, 47, 54))

    def test_only_requested_package_source_is_measured(self):
        with TemporaryDirectory() as directory:
            source = Path(directory) / "crate" / "src"
            report = {
                "data": [
                    {
                        "files": [
                            {
                                "filename": str(source / "lib.rs"),
                                "summary": {
                                    "lines": {"covered": 3, "count": 4},
                                    "branches": {"covered": 1, "count": 2},
                                },
                            },
                            {
                                "filename": str(Path(directory) / "other" / "src" / "lib.rs"),
                                "summary": {
                                    "lines": {"covered": 99, "count": 100},
                                    "branches": {"covered": 99, "count": 100},
                                },
                            },
                        ]
                    }
                ]
            }
            self.assertEqual(coverage_for_source(report, source), {"lines": (3, 4), "branches": (1, 2)})


if __name__ == "__main__":
    unittest.main()
