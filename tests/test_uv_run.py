"""Prove the PEP 723 header makes generated flows runnable via `uv run`.

This is the end-user story: a data engineer with only `uv` installed runs
`uv run flow.py` and the flow executes with no manual venv/dependency setup.

The test is skipped when `uv` is not on PATH. It runs the flow with the `shell`
runtime using host `echo`, so no Docker/cluster is needed. `uv` will resolve and
install Prefect into an isolated, cached environment on first run (network +
time), which is exactly the behavior we are verifying.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys

import pytest

from argo2prefect import convert
from argo2prefect.generator import GeneratorOptions

uv = shutil.which("uv")
pytestmark = pytest.mark.skipif(uv is None, reason="uv is not installed")


def test_uv_run_executes_generated_flow(example_text, tmp_path):
    code = convert(example_text("dag-diamond.yaml"), GeneratorOptions(runtime="shell"))
    flow_file = tmp_path / "dag_diamond_flow.py"
    flow_file.write_text(code, encoding="utf-8")

    env = dict(os.environ)
    env["PREFECT_HOME"] = str(tmp_path / "prefect_home")
    env["PREFECT_LOGGING_LEVEL"] = "ERROR"

    # --no-project: use only the script's inline (PEP 723) metadata, ignoring any
    # surrounding project. The default __main__ does a one-off run (no --serve).
    result = subprocess.run(
        [uv, "run", "--no-project", str(flow_file)],
        cwd=str(tmp_path),
        env=env,
        capture_output=True,
        text=True,
        timeout=600,
    )
    assert result.returncode == 0, (
        f"uv run failed (code {result.returncode}).\n"
        f"STDOUT:\n{result.stdout}\n\nSTDERR:\n{result.stderr}"
    )
