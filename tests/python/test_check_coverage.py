import sys
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))

from check_coverage import (
    area_for,
    coverage_by_area,
    meets_baseline,
    source_location_coverage,
)


class CoverageGateTests(unittest.TestCase):
    def test_implementation_packages_have_distinct_areas(self):
        self.assertEqual(area_for("ironbeam-core", Path("lib.rs")), "logical-graph")
        self.assertEqual(area_for("ironbeam-direct", Path("lib.rs")), "direct-runner")
        self.assertEqual(area_for("ironbeam-io-jsonl", Path("lib.rs")), "bounded-io")
        self.assertEqual(area_for("ironbeam-testing", Path("lib.rs")), "testing")
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
            self.assertEqual(
                coverage_by_area(report, source, "ironbeam-core"),
                {"logical-graph": {"lines": (3, 4), "branches": (1, 2)}},
            )

    def test_unreached_branch_sites_count_as_uncovered(self):
        with TemporaryDirectory() as directory:
            source = Path(directory) / "src"
            report = {
                "data": [
                    {
                        "files": [
                            {
                                "filename": str(source / "lib.rs"),
                                "segments": [[1, 1, 1, True], [2, 1, 0, True]],
                                "branches": [[1, 1, 1, 10, 1, 0], [2, 1, 2, 10, 0, 0]],
                            }
                        ]
                    }
                ]
            }
            self.assertEqual(
                source_location_coverage(report, source),
                {"lines": (1, 2), "branches": (1, 4)},
            )


if __name__ == "__main__":
    unittest.main()
