from __future__ import annotations

from argo2prefect.cli import main


def test_cli_convert_to_stdout(capsys, examples_dir):
    rc = main(["convert", str(examples_dir / "dag-diamond.yaml"), "-q"])
    assert rc == 0
    out = capsys.readouterr().out
    assert "def dag_diamond_flow(" in out


def test_cli_inspect(capsys, examples_dir):
    rc = main(["inspect", str(examples_dir / "cron-backup.yaml")])
    assert rc == 0
    out = capsys.readouterr().out
    assert "CronWorkflow" in out
    assert "schedule:" in out
    assert "0 2 * * *" in out


def test_cli_convert_to_file(tmp_path, examples_dir):
    out_file = tmp_path / "flow.py"
    rc = main(
        ["convert", str(examples_dir / "steps-hello.yaml"), "-o", str(out_file), "-q"]
    )
    assert rc == 0
    assert out_file.exists()
    assert "def steps_hello_flow(" in out_file.read_text(encoding="utf-8")


def test_cli_convert_directory(tmp_path, examples_dir):
    rc = main(["convert", str(examples_dir), "-o", str(tmp_path), "-q"])
    assert rc == 0
    generated = sorted(p.name for p in tmp_path.glob("*_flow.py"))
    assert "dag-diamond_flow.py" in generated
    assert len(generated) >= 5


def test_cli_unknown_input_errors():
    rc = main(["convert", "/nonexistent/path/to/file.yaml"])
    assert rc == 2


def test_cli_emit_prefect_yaml(tmp_path, examples_dir):
    import yaml

    rc = main(
        [
            "convert",
            str(examples_dir),
            "-o",
            str(tmp_path),
            "--emit-prefect-yaml",
            "--work-pool",
            "my-pool",
            "--source-repo",
            "https://github.com/acme/flows",
            "-q",
        ]
    )
    assert rc == 0

    prefect_yaml = tmp_path / "prefect.yaml"
    deploy_md = tmp_path / "DEPLOY.md"
    assert prefect_yaml.exists() and deploy_md.exists()

    doc = yaml.safe_load(prefect_yaml.read_text(encoding="utf-8"))
    deployments = {d["name"]: d for d in doc["deployments"]}
    # Every deployment points at a generated flow file and the shared work pool.
    assert deployments, "expected at least one deployment"
    for dep in deployments.values():
        assert dep["entrypoint"].endswith(f":{dep['entrypoint'].split(':')[1]}")
        assert dep["work_pool"]["name"] == "my-pool"
    # The CronWorkflow keeps its schedule.
    assert deployments["nightly-backup"]["schedules"][0]["cron"] == "0 2 * * *"
    # git_clone pull step wired from --source-repo.
    assert doc["pull"][0]["prefect.deployments.steps.git_clone"][
        "repository"
    ] == "https://github.com/acme/flows"


def test_cli_emit_prefect_yaml_requires_file_output(examples_dir):
    rc = main(["convert", str(examples_dir / "dag-diamond.yaml"), "--emit-prefect-yaml"])
    assert rc == 2
