from __future__ import annotations

from argo2prefect.expressions import Scope, translate_condition, translate_value


def test_plain_literal_becomes_string():
    assert translate_value("hello world", Scope()) == '"hello world"'


def test_single_input_is_bare_identifier():
    scope = Scope(inputs={"message": "message"})
    assert translate_value("{{inputs.parameters.message}}", scope) == "message"


def test_embedded_input_becomes_fstring():
    scope = Scope(inputs={"message": "message"})
    assert translate_value("hi {{inputs.parameters.message}}!", scope) == 'f"hi {message}!"'


def test_workflow_param_uses_single_quoted_subscript():
    out = translate_value("{{workflow.parameters.greeting}} there", Scope())
    assert out == "f\"{WORKFLOW_PARAMETERS['greeting']} there\""
    # Critical: no double-quote nested inside the double-quoted f-string (3.9 safe).
    assert '"]' not in out


def test_whole_task_output_passes_future():
    assert translate_value("{{tasks.A.outputs.result}}", Scope()) == "a_fut"


def test_embedded_task_output_resolves_result():
    out = translate_value("got {{tasks.gen-data.outputs.result}}", Scope())
    assert out == 'f"got {gen_data_fut.result()}"'


def test_item_and_item_key():
    scope = Scope(item_var="_item")
    assert translate_value("{{item}}", scope) == "_item"
    assert translate_value("{{item.name}}", scope) == "_item['name']"


def test_unresolved_expression_is_kept_literal_with_warning():
    scope = Scope()
    out = translate_value("{{outputs.artifacts.foo}}", scope)
    assert out == '"{{outputs.artifacts.foo}}"'
    assert scope.warnings


def test_condition_translation():
    scope = Scope()
    out = translate_condition('{{steps.flip.outputs.result}} == "heads" && 1 < 2', scope)
    assert "flip_fut.result()" in out
    assert '== "heads"' in out
    assert " and " in out


def test_workflow_name_uses_runtime():
    scope = Scope()
    out = translate_value("{{workflow.name}}", scope)
    assert out == "flow_run.name"
    assert "flow_run" in scope.used_runtime
