"""Phase 3: output quality — typed params, named outputs, formatting."""

from __future__ import annotations

from pathlib import Path

from argo2prefect.generator import GeneratorOptions, format_code, generate_module, generate_project
from argo2prefect.parser import parse_workflows
from argo2prefect.project import load_project

TYPED_PARAMS = """
kind: Workflow
metadata: {name: typed}
spec:
  entrypoint: main
  arguments:
    parameters:
      - {name: retries, value: "5"}
      - {name: rate, value: "0.25"}
      - {name: label, value: "prod"}
  templates:
    - name: main
      inputs:
        parameters: [{name: retries}]
      container: {image: alpine, command: [echo], args: ["{{inputs.parameters.retries}}"]}
"""


def test_main_flow_params_are_typed() -> None:
    code, _ = generate_module(parse_workflows(TYPED_PARAMS))
    assert "retries: int = 5" in code
    assert "rate: float = 0.25" in code
    assert 'label: str = "prod"' in code
    # ...but Argo string semantics are preserved inside.
    assert '"retries": str(retries)' in code
    assert '"label": label' in code
    assert "return main(retries=str(retries))" in code


NAMED_OUTPUT = """
kind: Workflow
metadata: {name: outputs}
spec:
  entrypoint: main
  templates:
    - name: main
      steps:
        - - name: produce
            template: producer
        - - name: consume
            template: consumer
            arguments:
              parameters:
                - name: v
                  value: "{{steps.produce.outputs.parameters.answer}}"
    - name: producer
      outputs:
        parameters:
          - name: answer
            valueFrom: {path: /tmp/answer.txt}
      container: {image: alpine, command: [sh, -c], args: ["echo 42 > /tmp/answer.txt"]}
    - name: consumer
      inputs:
        parameters: [{name: v}]
      container: {image: alpine, command: [echo], args: ["{{inputs.parameters.v}}"]}
"""


def test_named_output_params_fail_loudly_not_silently() -> None:
    code, _ = generate_module(parse_workflows(NAMED_OUTPUT))
    # The wrong-but-plausible v1 behavior was to substitute stdout silently.
    assert "_argo_output_param(produce_fut.result(), 'answer')" in code
    assert "def _argo_output_param(" in code
    assert "named output parameters are not migrated automatically" in code


def test_stdout_output_still_direct() -> None:
    manifest = NAMED_OUTPUT.replace("outputs.parameters.answer", "outputs.result")
    code, _ = generate_module(parse_workflows(manifest))
    assert "_argo_output_param" not in code
    # A whole-string reference passes the future through lazily.
    assert "v=produce_fut" in code


def test_project_output_is_ruff_formatted(tmp_path: Path) -> None:
    src = tmp_path / "m"
    src.mkdir()
    (src / "wf.yaml").write_text(TYPED_PARAMS, encoding="utf-8")
    out = generate_project(load_project([src]))
    for name, code in out.files.items():
        assert format_code(code) == code, f"{name} is not format-stable"


def test_stable_todo_codes_present() -> None:
    manifest = """
kind: Workflow
metadata: {name: todo-demo}
spec:
  entrypoint: main
  templates:
    - name: main
      dag:
        tasks:
          - name: gated
            template: t
            depends: "x.Failed"
          - name: x
            template: t
    - name: t
      container: {image: alpine, command: [echo], args: [hi]}
"""
    code, _ = generate_module(parse_workflows(manifest), GeneratorOptions())
    assert "TODO(A2P-102)" in code
