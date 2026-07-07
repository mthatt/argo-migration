from __future__ import annotations

import pytest

from argo2prefect import convert
from argo2prefect.generator import GeneratorOptions

EXAMPLES = [
    "dag-diamond.yaml",
    "steps-hello.yaml",
    "cron-backup.yaml",
    "loops-map.yaml",
    "script-python.yaml",
]
RUNTIMES = ["shell", "docker", "kubernetes"]


@pytest.mark.parametrize("name", EXAMPLES)
@pytest.mark.parametrize("runtime", RUNTIMES)
def test_generated_code_compiles(example_text, name, runtime):
    code = convert(example_text(name), GeneratorOptions(runtime=runtime))
    # compile() raises SyntaxError on any invalid output (stricter than ast.parse
    # for some f-string issues), so this guards the whole code path on Python 3.9+.
    compile(code, name, "exec")
    assert "from __future__ import annotations" in code


def test_dag_structure(example_text):
    code = convert(example_text("dag-diamond.yaml"))
    assert '@task(name="echo")' in code
    assert "def diamond(" in code
    assert "wait_for=" in code
    assert "WORKFLOW_PARAMETERS['greeting']" in code
    assert ".submit(" in code


def test_cron_serves_with_schedule(example_text):
    code = convert(example_text("cron-backup.yaml"))
    assert ".serve(" in code
    assert 'cron="0 2 * * *"' in code
    assert 'timezone="America/New_York"' in code


def test_loops_use_map(example_text):
    code = convert(example_text("loops-map.yaml"))
    assert ".map(" in code


def test_no_serve_option(example_text):
    code = convert(example_text("dag-diamond.yaml"), GeneratorOptions(serve=False))
    assert "__main__" not in code


def test_kubernetes_runtime_uses_k8s_client(example_text):
    code = convert(example_text("cron-backup.yaml"), GeneratorOptions(runtime="kubernetes"))
    assert "_run_k8s_job(" in code
    assert "from kubernetes import client" in code
    assert "delete_namespaced_job" in code  # jobs never leak
    assert '"kubernetes>=29"' in code  # PEP 723 dep


def test_docker_runtime_uses_docker_sdk(example_text):
    code = convert(example_text("steps-hello.yaml"), GeneratorOptions(runtime="docker"))
    assert "docker.from_env()" in code
    assert "_client.containers.run(" in code
    assert "remove=True" in code
    assert '"docker>=7"' in code  # PEP 723 dep


def test_invalid_runtime_rejected():
    with pytest.raises(ValueError):
        GeneratorOptions(runtime="bogus")


def test_pep723_metadata_present_by_default(example_text):
    code = convert(example_text("dag-diamond.yaml"))
    assert code.startswith("# /// script")
    assert '"prefect>=3,<4"' in code
    # The default (docker) runtime needs the docker SDK.
    assert '"docker>=7"' in code
    # Shell-based flows need prefect-shell at runtime.
    shell = convert(example_text("dag-diamond.yaml"), GeneratorOptions(runtime="shell"))
    assert '"prefect-shell>=0.3"' in shell


def test_pep723_metadata_can_be_disabled(example_text):
    code = convert(example_text("dag-diamond.yaml"), GeneratorOptions(script_metadata=False))
    assert "# /// script" not in code


def test_main_block_runs_once_and_supports_serve(example_text):
    code = convert(example_text("dag-diamond.yaml"))
    assert '__name__ == "__main__"' in code
    assert '"--serve" in sys.argv' in code
    assert "dag_diamond_flow()" in code  # one-off run path
    assert "dag_diamond_flow.serve(" in code  # deploy path


def test_retry_strategy_becomes_retries():
    manifest = (
        "apiVersion: argoproj.io/v1alpha1\n"
        "kind: Workflow\n"
        "metadata:\n  name: retry-demo\n"
        "spec:\n  entrypoint: a\n  templates:\n"
        "    - name: a\n      retryStrategy:\n        limit: 3\n"
        "      container:\n        image: alpine\n        command: [echo, hi]\n"
    )
    code = convert(manifest)
    assert "retries=3" in code
