"""Shared pytest fixtures and environment setup.

Prefect is configured to use an isolated, ephemeral home directory so the
end-to-end tests never touch a developer's real Prefect profile/database. This
must run before any test imports Prefect, which is why it lives at module import
time in ``conftest.py``.
"""

from __future__ import annotations

import os
import tempfile
from pathlib import Path

import pytest

_PREFECT_HOME = tempfile.mkdtemp(prefix="argo2prefect-tests-")
os.environ.setdefault("PREFECT_HOME", _PREFECT_HOME)
os.environ.setdefault("PREFECT_LOGGING_LEVEL", "ERROR")
os.environ.setdefault(
    "PREFECT_API_DATABASE_CONNECTION_URL", f"sqlite+aiosqlite:///{_PREFECT_HOME}/prefect.db"
)

EXAMPLES_DIR = Path(__file__).resolve().parents[1] / "examples" / "argo"


@pytest.fixture
def examples_dir() -> Path:
    return EXAMPLES_DIR


@pytest.fixture
def example_text():
    def _read(name: str) -> str:
        return (EXAMPLES_DIR / name).read_text(encoding="utf-8")

    return _read
