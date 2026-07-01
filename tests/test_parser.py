from __future__ import annotations

import pytest

from argo2prefect import parse_workflows
from argo2prefect.models import TemplateKind
from argo2prefect.parser import ParseError


def test_parse_dag_diamond(example_text):
    workflows = parse_workflows(example_text("dag-diamond.yaml"))
    assert len(workflows) == 1
    wf = workflows[0]
    assert wf.kind == "Workflow"
    assert wf.name == "dag-diamond"
    assert wf.entrypoint == "diamond"
    assert [p.name for p in wf.arguments] == ["greeting"]

    echo = wf.template_by_name("echo")
    assert echo is not None
    assert echo.kind == TemplateKind.CONTAINER
    assert echo.container.image == "alpine:3.18"
    assert echo.container.command == ["echo"]
    assert [p.name for p in echo.inputs] == ["message"]

    diamond = wf.template_by_name("diamond")
    assert diamond.kind == TemplateKind.DAG
    assert [t.name for t in diamond.dag_tasks] == ["A", "B", "C", "D"]
    d = next(t for t in diamond.dag_tasks if t.name == "D")
    assert set(d.dependencies) == {"B", "C"}
    b = next(t for t in diamond.dag_tasks if t.name == "B")
    assert b.arguments[0].value == "B saw: {{tasks.A.outputs.result}}"


def test_parse_cron_workflow(example_text):
    wf = parse_workflows(example_text("cron-backup.yaml"))[0]
    assert wf.kind == "CronWorkflow"
    assert wf.schedule == "0 2 * * *"
    assert wf.timezone == "America/New_York"
    assert wf.entrypoint == "backup"
    backup = wf.template_by_name("backup")
    assert backup.kind == TemplateKind.CONTAINER
    assert "DB_URL" in backup.container.env
    assert backup.container.env["DB_URL"] == "{{inputs.parameters.database_url}}"


def test_parse_steps_groups(example_text):
    wf = parse_workflows(example_text("steps-hello.yaml"))[0]
    template = wf.template_by_name("hello-hello-hello")
    assert template.kind == TemplateKind.STEPS
    assert len(template.step_groups) == 2
    assert len(template.step_groups[0]) == 1
    assert len(template.step_groups[1]) == 2


def test_parse_with_items(example_text):
    wf = parse_workflows(example_text("loops-map.yaml"))[0]
    call = wf.template_by_name("loop-map").step_groups[0][0]
    assert call.with_items == ["apple", "banana", "cherry"]


def test_parse_script_interpreter(example_text):
    wf = parse_workflows(example_text("script-python.yaml"))[0]
    template = wf.template_by_name("gen-random")
    assert template.kind == TemplateKind.SCRIPT
    assert template.script.interpreter == "python"
    assert "random" in template.script.source


def test_multi_document(example_text):
    text = example_text("dag-diamond.yaml") + "\n---\n" + example_text("steps-hello.yaml")
    workflows = parse_workflows(text)
    assert [w.name for w in workflows] == ["dag-diamond", "steps-hello"]


def test_parse_error_on_non_workflow():
    with pytest.raises(ParseError):
        parse_workflows("kind: ConfigMap\nmetadata:\n  name: x\n")


def test_generate_name_only():
    wf = parse_workflows(
        "apiVersion: argoproj.io/v1alpha1\n"
        "kind: Workflow\n"
        "metadata:\n  generateName: hello-\n"
        "spec:\n  entrypoint: a\n  templates:\n    - name: a\n      container:\n        image: alpine\n"
    )[0]
    assert wf.display_name == "hello"
