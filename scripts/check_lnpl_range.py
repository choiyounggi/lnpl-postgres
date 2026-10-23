"""Check that pyproject.toml's lnpl requirement admits the latest lnpl on PyPI.

linkly-sync's latest-release job runs this on every PR. ci.yml installs the
newest lnpl inside the range and runs the suite, so the two together mean
"works with the latest lnpl release". When linkly ships a release the range
excludes (a new minor), this fails until the range is widened and tested.

usage: python scripts/check_lnpl_range.py [pyproject.toml]
exit 0: the range admits the latest release. exit 1: it does not, or the
lnpl requirement is missing, duplicated, unbounded or a direct reference.
"""

import json
import sys
import tomllib
import urllib.request

from packaging.requirements import Requirement
from packaging.version import InvalidVersion, Version

PYPI_URL = "https://pypi.org/pypi/lnpl/json"


def lnpl_requirement(pyproject):
    """Return the single lnpl Requirement from a parsed pyproject dict."""
    deps = pyproject.get("project", {}).get("dependencies", [])
    found = [r for r in (Requirement(d) for d in deps) if r.name == "lnpl"]
    if len(found) != 1:
        raise ValueError(f"expected exactly one lnpl dependency, found {len(found)}")
    req = found[0]
    if req.url:
        raise ValueError(
            f"lnpl is a direct reference ({req.url}); PyPI rejects uploads that carry one"
        )
    if not str(req.specifier):
        raise ValueError("lnpl has no version range")
    return req


def latest_release(index):
    """Return the newest stable, non-yanked version in a PyPI JSON index."""
    versions = []
    for raw, files in index.get("releases", {}).items():
        if not files or all(f.get("yanked") for f in files):
            continue
        try:
            version = Version(raw)
        except InvalidVersion:
            continue
        if not version.is_prerelease:
            versions.append(version)
    if not versions:
        raise ValueError("PyPI lists no stable lnpl release")
    return max(versions)


def check(pyproject, index):
    """Return (exit code, message) for a parsed pyproject and a PyPI index."""
    req = lnpl_requirement(pyproject)
    latest = latest_release(index)
    if req.specifier.contains(latest):
        return 0, f"{req} admits lnpl {latest}, the latest on PyPI"
    return 1, (
        f"lnpl {latest} is on PyPI but {req} excludes it: widen the range, "
        "run the suite against it, and release"
    )


def main(argv):
    path = argv[1] if len(argv) > 1 else "pyproject.toml"
    try:
        with open(path, "rb") as f:
            pyproject = tomllib.load(f)
        with urllib.request.urlopen(PYPI_URL, timeout=30) as resp:
            index = json.load(resp)
        code, message = check(pyproject, index)
    except (OSError, ValueError, KeyError) as exc:
        print(f"::error::{exc}")
        return 1
    print(message if code == 0 else f"::error::{message}")
    return code


if __name__ == "__main__":
    sys.exit(main(sys.argv))
