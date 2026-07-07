"""Phase 2 coverage: retries, timeouts, hooks, sync, memoize, sequences,
inline templates, cron extras, artifact locations, and expr-lang."""

from __future__ import annotations

from argo2prefect.deploy import DeployOptions, render_prefect_yaml
from argo2prefect.expressions import Scope, translate_condition, translate_value
from argo2prefect.generator import generate_module
from argo2prefect.parser import parse_workflows


def _convert(manifest: str) -> str:
    code, _plans = generate_module(parse_workflows(manifest))
    return code


def _wrap(templates: str, extra_spec: str = "") -> str:
    return f"""
kind: Workflow
metadata: {{name: feature-test}}
spec:
  entrypoint: main
{extra_spec}
  templates:
{templates}
"""


ECHO = """
    - name: echo
      container: {image: alpine, command: [echo], args: [hi]}
"""


# --------------------------------------------------------------------------- #
# Retries / timeouts / memoize
# --------------------------------------------------------------------------- #
def test_full_retry_strategy() -> None:
    code = _convert(
        _wrap("""
    - name: main
      retryStrategy:
        limit: 4
        retryPolicy: OnError
        backoff:
          duration: "30s"
          factor: 2
          maxDuration: "1h"
      container: {image: alpine, command: [echo], args: [hi]}
""")
    )
    assert "retries=4" in code
    assert "retry_delay_seconds=exponential_backoff(backoff_factor=30)" in code
    assert "from prefect.tasks import exponential_backoff" in code
    assert "retryPolicy 'OnError'" in code  # flagged in header
    assert "caps retry backoff at 1h" in code


def test_constant_retry_delay() -> None:
    code = _convert(
        _wrap("""
    - name: main
      retryStrategy:
        limit: 2
        backoff: {duration: "2m"}
      container: {image: alpine, command: [echo], args: [hi]}
""")
    )
    assert "retries=2" in code
    assert "retry_delay_seconds=120" in code


def test_timeouts_template_and_workflow() -> None:
    code = _convert(
        _wrap(
            """
    - name: main
      activeDeadlineSeconds: 300
      container: {image: alpine, command: [echo], args: [hi]}
""",
            extra_spec="  activeDeadlineSeconds: 3600",
        )
    )
    assert "timeout_seconds=300" in code  # task
    assert "timeout_seconds=3600" in code  # main flow


def test_memoize_maps_to_cache_policy() -> None:
    code = _convert(
        _wrap("""
    - name: main
      memoize:
        key: "{{inputs.parameters.x}}"
        maxAge: "1h"
      inputs:
        parameters: [{name: x}]
      container: {image: alpine, command: [echo], args: ["{{inputs.parameters.x}}"]}
""")
    )
    assert "cache_policy=INPUTS" in code
    assert "cache_expiration=timedelta(seconds=3600)" in code
    assert "from prefect.cache_policies import INPUTS" in code


# --------------------------------------------------------------------------- #
# onExit / synchronization
# --------------------------------------------------------------------------- #
def test_on_exit_becomes_state_hooks() -> None:
    code = _convert(
        _wrap(
            ECHO
            + """
    - name: main
      steps:
        - - name: run
            template: echo
    - name: notify
      container: {image: alpine, command: [echo], args: [done]}
""",
            extra_spec="  onExit: notify",
        )
    )
    assert "def _feature_test_flow_on_exit(flow, flow_run, state):" in code
    assert "on_completion=[_feature_test_flow_on_exit]" in code
    assert "on_failure=[_feature_test_flow_on_exit]" in code


def test_mutex_becomes_concurrency_guard() -> None:
    code = _convert(
        _wrap("""
    - name: main
      synchronization:
        mutex: {name: my-lock}
      container: {image: alpine, command: [echo], args: [hi]}
""")
    )
    assert 'with concurrency("my-lock"):  # Argo mutex' in code
    assert "from prefect.concurrency.sync import concurrency" in code
    assert "prefect gcl create my-lock" in code


# --------------------------------------------------------------------------- #
# withSequence / inline templates
# --------------------------------------------------------------------------- #
def test_with_sequence_generates_items() -> None:
    code = _convert(
        _wrap(
            """
    - name: main
      steps:
        - - name: gen
            template: show
            arguments:
              parameters: [{name: n, value: "{{item}}"}]
            withSequence: {start: "3", end: "1", format: "num-%d"}
    - name: show
      inputs:
        parameters: [{name: n}]
      container: {image: alpine, command: [echo], args: ["{{inputs.parameters.n}}"]}
"""
        )
    )
    assert '_items = _argo_sequence(start="3", end="1", fmt="num-%d")' in code
    assert "def _argo_sequence(" in code
    assert "show.map(" in code


def test_inline_template_is_hoisted() -> None:
    code = _convert(
        _wrap("""
    - name: main
      dag:
        tasks:
          - name: quick
            inline:
              container: {image: alpine, command: [echo], args: [inline-hi]}
""")
    )
    assert "NotImplementedError" not in code
    assert "def quick_inline(" in code
    assert "quick_fut = quick_inline.submit(" in code


# --------------------------------------------------------------------------- #
# Cron extras / artifacts
# --------------------------------------------------------------------------- #
CRON_MULTI = """
kind: CronWorkflow
metadata: {name: multi-cron}
spec:
  schedules: ["0 1 * * *", "0 13 * * *"]
  timezone: America/New_York
  suspend: true
  workflowSpec:
    entrypoint: main
    templates:
      - name: main
        container: {image: alpine, command: [echo], args: [tick]}
"""


def test_cron_multiple_schedules_and_suspend() -> None:
    code, plans = generate_module(parse_workflows(CRON_MULTI))
    assert plans[0].schedules == ["0 1 * * *", "0 13 * * *"]
    assert plans[0].suspended is True
    assert "paused=True" in code

    plans[0].entrypoint_file = "multi_flow.py"
    yaml_text = render_prefect_yaml(plans, DeployOptions())
    assert yaml_text.count("- cron:") == 2
    assert yaml_text.count("active: false") == 2


def test_artifact_todos_carry_location() -> None:
    code = _convert(
        _wrap("""
    - name: main
      inputs:
        artifacts:
          - name: data
            path: /tmp/data.csv
            s3: {bucket: my-bucket, key: exports/data.csv}
      container: {image: alpine, command: [cat], args: [/tmp/data.csv]}
""")
    )
    assert "input artifact 'data' from s3://my-bucket/exports/data.csv" in code


# --------------------------------------------------------------------------- #
# Expression translation
# --------------------------------------------------------------------------- #
def test_expr_lang_condition() -> None:
    scope = Scope(inputs={"should-print": "should_print"})
    cond = translate_condition("{{= inputs.parameters['should-print'] == 'true'}}", scope)
    assert cond == "should_print == 'true'"


def test_bare_words_are_quoted() -> None:
    scope = Scope(inputs={"coin": "coin"})
    assert translate_condition("{{inputs.parameters.coin}} == heads", scope) == 'coin == "heads"'


def test_untranslatable_condition_is_false() -> None:
    scope = Scope()
    cond = translate_condition("{{=jsonpath(workflow.parameters.j, '$.x') == 'y'}}", scope)
    assert cond == "False"
    assert any("port the gate manually" in w for w in scope.warnings)


def test_expr_lang_literals_and_functions() -> None:
    scope = Scope(inputs={"n": "n"})
    assert translate_value("{{= asInt(inputs.parameters.n) + 1}}", scope) == "int(n) + 1"
    cond = translate_condition("{{= inputs.parameters.n != nil && true}}", scope)
    assert cond == "n != None and True"


# --------------------------------------------------------------------------- #
# End to end: sequence + inline template actually run
# --------------------------------------------------------------------------- #
def test_sequence_and_inline_run(tmp_path):
    import importlib.util

    import pytest

    pytest.importorskip("prefect")
    pytest.importorskip("prefect_shell")
    from argo2prefect.generator import GeneratorOptions

    manifest = _wrap("""
    - name: main
      dag:
        tasks:
          - name: fan
            template: show
            arguments:
              parameters: [{name: n, value: "{{item}}"}]
            withSequence: {count: "2"}
          - name: quick
            depends: fan
            inline:
              container: {image: alpine, command: [echo], args: [inline-done]}
    - name: show
      inputs:
        parameters: [{name: n}]
      container: {image: alpine, command: [echo], args: ["{{inputs.parameters.n}}"]}
""")
    code, _ = generate_module(parse_workflows(manifest), GeneratorOptions(runtime="shell"))
    path = tmp_path / "phase2_e2e.py"
    path.write_text(code, encoding="utf-8")
    spec = importlib.util.spec_from_file_location(path.stem, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    assert module.feature_test_flow() is None
