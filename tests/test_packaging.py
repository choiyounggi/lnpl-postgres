import importlib.util
import pathlib
import tomllib
import unittest

from packaging.requirements import Requirement
from packaging.version import Version

ROOT = pathlib.Path(__file__).resolve().parent.parent

_spec = importlib.util.spec_from_file_location(
    "check_lnpl_range", ROOT / "scripts" / "check_lnpl_range.py"
)
check_lnpl_range = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(check_lnpl_range)


def _pyproject(*deps):
    return {"project": {"dependencies": list(deps)}}


def _index(**releases):
    return {"releases": releases}


_FILE = [{"yanked": False}]


class PyprojectTest(unittest.TestCase):
    def setUp(self):
        with open(ROOT / "pyproject.toml", "rb") as f:
            self.pyproject = tomllib.load(f)

    def test_lnpl_is_a_pypi_range_over_0_9(self):
        req = check_lnpl_range.lnpl_requirement(self.pyproject)
        self.assertIsNone(req.url)
        self.assertTrue(req.specifier.contains(Version("0.9.0")))
        self.assertFalse(req.specifier.contains(Version("0.10.0")))

    def test_no_dependency_is_a_direct_reference(self):
        # PyPI rejects an upload whose metadata carries a direct reference.
        deps = self.pyproject["project"]["dependencies"]
        self.assertEqual([d for d in deps if Requirement(d).url], [])

    def test_requires_python_matches_lnpl_floor(self):
        # lnpl 0.9 requires Python >=3.11; a lower floor would promise an
        # install that pip cannot resolve.
        self.assertEqual(self.pyproject["project"]["requires-python"], ">=3.11")


class LnplRequirementTest(unittest.TestCase):
    def test_returns_the_lnpl_requirement(self):
        req = check_lnpl_range.lnpl_requirement(_pyproject("lnpl>=0.9,<0.10", "x>=1"))
        self.assertEqual(str(req.specifier), "<0.10,>=0.9")

    def test_rejects_a_direct_reference(self):
        with self.assertRaisesRegex(ValueError, "direct reference"):
            check_lnpl_range.lnpl_requirement(
                _pyproject("lnpl @ git+https://github.com/choiyounggi/linkly@main")
            )

    def test_rejects_a_missing_lnpl(self):
        with self.assertRaisesRegex(ValueError, "found 0"):
            check_lnpl_range.lnpl_requirement(_pyproject("x>=1"))

    def test_rejects_an_empty_dependency_list(self):
        with self.assertRaisesRegex(ValueError, "found 0"):
            check_lnpl_range.lnpl_requirement({"project": {}})

    def test_rejects_a_duplicated_lnpl(self):
        with self.assertRaisesRegex(ValueError, "found 2"):
            check_lnpl_range.lnpl_requirement(_pyproject("lnpl>=0.9", "lnpl<0.10"))

    def test_rejects_an_unbounded_lnpl(self):
        with self.assertRaisesRegex(ValueError, "no version range"):
            check_lnpl_range.lnpl_requirement(_pyproject("lnpl"))


class LatestReleaseTest(unittest.TestCase):
    def test_picks_the_highest_stable_version(self):
        latest = check_lnpl_range.latest_release(
            _index(**{"0.9.0": _FILE, "0.10.0": _FILE, "0.9.1": _FILE})
        )
        self.assertEqual(latest, Version("0.10.0"))

    def test_skips_prereleases_yanked_and_fileless_versions(self):
        latest = check_lnpl_range.latest_release(
            _index(
                **{
                    "0.9.0": _FILE,
                    "0.10.0rc1": _FILE,
                    "0.11.0": [{"yanked": True}],
                    "0.12.0": [],
                    "not-a-version": _FILE,
                }
            )
        )
        self.assertEqual(latest, Version("0.9.0"))

    def test_rejects_an_index_with_no_stable_release(self):
        with self.assertRaisesRegex(ValueError, "no stable"):
            check_lnpl_range.latest_release(_index())


class CheckTest(unittest.TestCase):
    def test_passes_when_the_range_admits_the_latest(self):
        code, message = check_lnpl_range.check(
            _pyproject("lnpl>=0.9,<0.10"), _index(**{"0.9.3": _FILE})
        )
        self.assertEqual(code, 0)
        self.assertIn("0.9.3", message)

    def test_fails_when_a_newer_minor_is_excluded(self):
        code, message = check_lnpl_range.check(
            _pyproject("lnpl>=0.9,<0.10"), _index(**{"0.9.3": _FILE, "0.10.0": _FILE})
        )
        self.assertEqual(code, 1)
        self.assertIn("lnpl 0.10.0 is on PyPI", message)

    def test_fails_at_the_exact_upper_bound(self):
        code, _ = check_lnpl_range.check(
            _pyproject("lnpl>=0.9,<0.10"), _index(**{"0.10": _FILE})
        )
        self.assertEqual(code, 1)


if __name__ == "__main__":
    unittest.main()
