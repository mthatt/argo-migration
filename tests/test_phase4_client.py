"""Phase 4: assess grading, migration report, verify, and convert UX."""

from __future__ import annotations

import json
from pathlib import Path

from argo2prefect.assess import assess_project, render_json, render_markdown
from argo2prefect.cli import main
from argo2prefect.project import load_project_from_text

CLEAN = """
kind: Workflow
metadata: {name: clean-wf}
spec:
  entrypoint: main
  templates:
    - name: main
      container: {image: alpine, command: [echo], args: [hi]}
"""

REVIEW = """
kind: Workflow
metadata: {name: review-wf}
spec:
  entrypoint: main
  templates:
    - name: main
      steps:
        - - name: maybe
            template: echo
            when: "{{workflow.parameters.mode}} == fast"
    - name: echo
      container: {image: alpine, command: [echo], args: [hi]}
"""

MANUAL = """
kind: Workflow
metadata: {name: manual-wf}
spec:
  entrypoint: main
  templates:
    - name: main
      dag:
        tasks:
          - name: missing
            templateRef: {name: not-in-project, template: nope}
"""


def test_assessment_grades() -> None:
    project = load_project_from_text(CLEAN + "\n---\n" + REVIEW + "\n---\n" + MANUAL)
    assessment = assess_project(project)
    grades = {wf.name: wf.grade for wf in assessment.workflows}
    assert grades == {"clean-wf": "automatic", "review-wf": "review", "manual-wf": "manual"}

    review = next(wf for wf in assessment.workflows if wf.name == "review-wf")
    assert "A2P-101" in review.todos
    assert review.features.get("conditions") == 1
    assert assessment.total_minutes > 0


def test_assessment_renders() -> None:
    project = load_project_from_text(CLEAN + "\n---\n" + MANUAL)
    assessment = assess_project(project)
    md = render_markdown(assessment)
    assert "migration assessment" in md
    assert "**manual**" in md and "**automatic**" in md

    payload = json.loads(render_json(assessment))
    assert payload["summary"]["grades"]["manual"] == 1
    assert payload["todo_legend"]["A2P-108"]


def _write_manifests(tmp_path: Path) -> Path:
    src = tmp_path / "manifests"
    src.mkdir()
    (src / "review.yaml").write_text(REVIEW, encoding="utf-8")
    return src


def test_cli_assess_writes_reports(tmp_path, capsys) -> None:
    src = _write_manifests(tmp_path)
    out = tmp_path / "report"
    rc = main(["assess", str(src), "-o", str(out)])
    assert rc == 0
    assert (out / "ASSESSMENT.md").exists()
    assert (out / "ASSESSMENT.json").exists()
    assert (out / "ASSESSMENT.html").exists()


def test_cli_convert_writes_migration_report(tmp_path) -> None:
    src = _write_manifests(tmp_path)
    out = tmp_path / "flows"
    rc = main(["convert", str(src), "-o", str(out), "-q"])
    assert rc == 0
    report = (out / "MIGRATION_REPORT.md").read_text(encoding="utf-8")
    assert "A2P-101" in report
    assert "review_flow.py:" in report  # file:line anchors


def test_cli_convert_dry_run_writes_nothing(tmp_path) -> None:
    src = _write_manifests(tmp_path)
    out = tmp_path / "flows"
    rc = main(["convert", str(src), "-o", str(out), "-q", "--dry-run"])
    assert rc == 0
    assert not out.exists()


def test_cli_convert_refuses_overwrite_without_force(tmp_path) -> None:
    src = _write_manifests(tmp_path)
    out = tmp_path / "flows"
    assert main(["convert", str(src), "-o", str(out), "-q"]) == 0
    assert main(["convert", str(src), "-o", str(out), "-q"]) == 2  # no --force
    assert main(["convert", str(src), "-o", str(out), "-q", "--force"]) == 0


def test_cli_verify_reports_broken_module(tmp_path, capsys) -> None:
    good = tmp_path / "good.py"
    good.write_text("x = 1\n", encoding="utf-8")
    bad = tmp_path / "bad.py"
    bad.write_text("import definitely_not_a_real_module_xyz\n", encoding="utf-8")
    rc = main(["verify", str(tmp_path)])
    out = capsys.readouterr().out
    assert rc == 1
    assert "ok   good.py" in out
    assert "FAIL bad.py" in out
