"""End-to-end tests: generate code, import it, and actually run the flow.

These require Prefect (the generated code's runtime dependency). They are skipped
automatically if Prefect is not installed. They use the ``shell`` runtime with
host-available commands (``echo``/``sh``) so no Docker or cluster is needed.
"""

from __future__ import annotations

import importlib.util
import uuid
from pathlib import Path

import pytest

pytest.importorskip("prefect")
pytest.importorskip("prefect_shell")

from argo2prefect import convert  # noqa: E402
from argo2prefect.generator import GeneratorOptions  # noqa: E402


def _load_module(code: str, tmp_path: Path):
    path = tmp_path / f"generated_{uuid.uuid4().hex}.py"
    path.write_text(code, encoding="utf-8")
    spec = importlib.util.spec_from_file_location(path.stem, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_run_dag_diamond(example_text, tmp_path):
    code = convert(example_text("dag-diamond.yaml"), GeneratorOptions(runtime="shell"))
    module = _load_module(code, tmp_path)
    assert module.dag_diamond_flow() is None


def test_run_steps_with_parameter(example_text, tmp_path):
    code = convert(example_text("steps-hello.yaml"), GeneratorOptions(runtime="shell"))
    module = _load_module(code, tmp_path)
    assert module.steps_hello_flow(name="Tester") is None


def test_run_loops(example_text, tmp_path):
    code = convert(example_text("loops-map.yaml"), GeneratorOptions(runtime="shell"))
    module = _load_module(code, tmp_path)
    assert module.loops_map_flow() is None


SH_SCRIPT_WORKFLOW = """
apiVersion: argoproj.io/v1alpha1
kind: Workflow
metadata:
  name: sh-demo
spec:
  entrypoint: main
  arguments:
    parameters:
      - name: who
        value: world
  templates:
    - name: main
      steps:
        - - name: greet
            template: say
            arguments:
              parameters:
                - name: who
                  value: "{{workflow.parameters.who}}"
    - name: say
      inputs:
        parameters:
          - name: who
      script:
        image: alpine
        command: [sh]
        source: |
          echo "hello {{inputs.parameters.who}}"
"""


def test_run_shell_script_with_param(tmp_path):
    code = convert(SH_SCRIPT_WORKFLOW, GeneratorOptions(runtime="shell"))
    module = _load_module(code, tmp_path)
    assert module.sh_demo_flow(who="Mihir") is None
