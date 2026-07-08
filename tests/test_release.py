"""Release hygiene: the version is declared in two places; they must agree,
and the changelog must document the current version."""

from __future__ import annotations

import re
from pathlib import Path

import argo2prefect

ROOT = Path(__file__).parents[1]


def _pyproject_version() -> str:
    # No tomllib on the 3.10 floor; the version line is trivially regexable.
    text = (ROOT / "pyproject.toml").read_text(encoding="utf-8")
    match = re.search(r'^version = "([^"]+)"$', text, re.MULTILINE)
    assert match, "version not found in pyproject.toml"
    return match.group(1)


def test_versions_agree() -> None:
    assert argo2prefect.__version__ == _pyproject_version()


def test_changelog_covers_current_version() -> None:
    changelog = (ROOT / "CHANGELOG.md").read_text(encoding="utf-8")
    assert re.search(rf"^## \[{re.escape(_pyproject_version())}\]", changelog, re.MULTILINE), (
        "CHANGELOG.md has no entry for the current version"
    )
