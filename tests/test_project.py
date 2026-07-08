"""Project linking: cross-file templateRef, workflowTemplateRef, depends."""

from __future__ import annotations

from pathlib import Path

import pytest

from argo2prefect.generator import generate_code, generate_project
from argo2prefect.parser import depends_is_plain, depends_task_names, parse_workflows
from argo2prefect.project import load_project, load_project_from_text

LIBRARY = """
apiVersion: argoproj.io/v1alpha1
kind: WorkflowTemplate
metadata:
  name: shared-lib
spec:
  entrypoint: greet
  arguments:
    parameters:
      - name: audience
        value: world
  templates:
    - name: greet
      inputs:
        parameters:
          - name: audience
      container:
        image: alpine
        command: [echo]
        args: ["hello {{inputs.parameters.audience}}"]
"""

CALLER = """
apiVersion: argoproj.io/v1alpha1
kind: Workflow
metadata:
  name: caller
spec:
  entrypoint: main
  templates:
    - name: main
      dag:
        tasks:
          - name: hello
            templateRef:
              name: shared-lib
              template: greet
            arguments:
              parameters:
                - name: audience
                  value: "callers"
"""

REF_WORKFLOW = """
apiVersion: argoproj.io/v1alpha1
kind: Workflow
metadata:
  name: ref-caller
spec:
  workflowTemplateRef:
    name: shared-lib
  arguments:
    parameters:
      - name: audience
        value: "override"
"""


def _write_project(tmp_path: Path, **files: str) -> Path:
    src = tmp_path / "manifests"
    src.mkdir()
    for name, text in files.items():
        (src / f"{name}.yaml").write_text(text, encoding="utf-8")
    return src


# --------------------------------------------------------------------------- #
# depends parsing
# --------------------------------------------------------------------------- #
def test_depends_task_names() -> None:
    assert depends_task_names("A && (B.Succeeded || C.Failed)") == ["A", "B", "C"]
    assert depends_task_names("!flaky-task.Errored") == ["flaky-task"]


def test_depends_is_plain() -> None:
    assert depends_is_plain("A && B")
    assert not depends_is_plain("A || B")
    assert not depends_is_plain("A.Failed && B")


def test_depends_becomes_dependencies() -> None:
    manifest = """
kind: Workflow
metadata: {name: dep-test}
spec:
  entrypoint: main
  templates:
    - name: main
      dag:
        tasks:
          - name: a
            template: t
          - name: b
            template: t
            depends: "a.Succeeded"
    - name: t
      container: {image: alpine, command: [echo], args: [hi]}
"""
    wf = parse_workflows(manifest)[0]
    dag = wf.template_by_name("main")
    b = next(c for c in dag.dag_tasks if c.name == "b")
    assert b.dependencies == ["a"]
    code = generate_code([wf])
    assert "wait_for=[_f for _f in [a_fut]" in code


def test_non_plain_depends_flagged() -> None:
    manifest = """
kind: Workflow
metadata: {name: dep-flag}
spec:
  entrypoint: main
  templates:
    - name: main
      dag:
        tasks:
          - name: a
            template: t
          - name: b
            template: t
            depends: "a.Failed || a.Succeeded"
    - name: t
      container: {image: alpine, command: [echo], args: [hi]}
"""
    code = generate_code(parse_workflows(manifest))
    assert "TODO(A2P-102): Argo gated this on `depends:" in code
    assert "a.Failed || a.Succeeded" in code


# --------------------------------------------------------------------------- #
# Cross-file linking
# --------------------------------------------------------------------------- #
def test_cross_file_template_ref_resolves(tmp_path) -> None:
    src = _write_project(tmp_path, lib=LIBRARY, caller=CALLER)
    project = load_project([src])
    out = generate_project(project)

    assert set(out.files) == {"shared_templates.py", "caller_flow.py"}
    shared = out.files["shared_templates.py"]
    caller = out.files["caller_flow.py"]
    assert "def greet(" in shared
    assert "from shared_templates import WORKFLOW_PARAMETERS, greet" in caller
    assert "greet.submit(audience=" in caller
    assert "NotImplementedError" not in caller


def test_same_file_template_ref_resolves() -> None:
    project = load_project_from_text(LIBRARY + "\n---\n" + CALLER)
    out = generate_project(project)
    caller = out.files["workflows_flow.py"]
    assert "NotImplementedError" not in caller
    assert "from shared_templates import" in caller


def test_workflow_template_ref_inherits_entrypoint_and_args(tmp_path) -> None:
    src = _write_project(tmp_path, lib=LIBRARY, ref=REF_WORKFLOW)
    project = load_project([src])
    wf = next(w for f in project.files for w in f.runnable)
    assert wf.entrypoint == "greet"
    assert [p.name for p in wf.arguments] == ["audience"]
    assert wf.arguments[0].value == "override"

    out = generate_project(project)
    ref_flow = out.files["ref_flow.py"]
    assert "return greet(audience=audience)" in ref_flow
    assert "NotImplementedError" not in ref_flow


def test_unresolved_template_ref_still_stubs_with_guidance() -> None:
    project = load_project_from_text(CALLER)
    out = generate_project(project)
    code = out.files["workflows_flow.py"]
    assert "NotImplementedError" in code
    assert "not in this conversion's input" in code


def test_library_manifests_get_no_deployment_plans(tmp_path) -> None:
    src = _write_project(tmp_path, lib=LIBRARY, caller=CALLER)
    out = generate_project(load_project([src]))
    assert [p.name for p in out.plans] == ["caller"]
    assert out.plans[0].entrypoint_file == "caller_flow.py"


def test_duplicate_library_names_warn(tmp_path) -> None:
    other = LIBRARY.replace("kind: WorkflowTemplate", "kind: ClusterWorkflowTemplate")
    src = _write_project(tmp_path, lib=LIBRARY, lib2=other)
    project = load_project([src])
    assert any("Duplicate template library name" in w for w in project.warnings)


# --------------------------------------------------------------------------- #
# End to end: the generated pair actually runs
# --------------------------------------------------------------------------- #
def test_linked_project_runs(tmp_path, monkeypatch) -> None:
    pytest.importorskip("prefect")
    pytest.importorskip("prefect_shell")
    import importlib

    from argo2prefect.generator import GeneratorOptions

    src = _write_project(tmp_path, lib=LIBRARY, caller=CALLER)
    out = generate_project(load_project([src]), GeneratorOptions(runtime="shell", serve=False))
    out_dir = tmp_path / "flows"
    out_dir.mkdir()
    for filename, code in out.files.items():
        (out_dir / filename).write_text(code, encoding="utf-8")

    monkeypatch.syspath_prepend(str(out_dir))
    module = importlib.import_module("caller_flow")
    try:
        assert module.caller_flow() is None
    finally:
        import sys

        sys.modules.pop("caller_flow", None)
        sys.modules.pop("shared_templates", None)
