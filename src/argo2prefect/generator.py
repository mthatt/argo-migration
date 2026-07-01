"""Render the :mod:`argo2prefect.models` IR as runnable Prefect 3 Python.

Mapping overview (Argo -> Prefect):

* container / script / resource / http / suspend template -> ``@task`` function
* dag / steps template                                    -> ``@flow`` subflow
* dag ``dependencies`` / step ordering                    -> ``.submit()`` + ``wait_for``
* ``withItems`` / ``withParam``                           -> ``.map()`` / ``unmapped``
* ``when``                                                -> ``if`` guard
* workflow ``arguments``                                  -> main flow parameters
* CronWorkflow ``schedule``                               -> ``flow.serve(cron=...)``

Where Argo semantics have no faithful 1:1 Prefect equivalent (artifacts,
output-parameter extraction, regex conditions, ...) the generator emits a clear
``# TODO`` and records a warning instead of guessing.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from typing import Optional

from .expressions import Scope, translate_condition, translate_value
from .models import (
    ContainerSpec,
    ScriptSpec,
    Template,
    TemplateKind,
    Workflow,
)
from .naming import sanitize_identifier, unique

GENERATED_BY = "argo2prefect"

_ITEM_REF = re.compile(r"\{\{\s*item(\.|\s|\})")
_WF_PARAM_REF = re.compile(r"workflow\.parameters\.([A-Za-z0-9_.\-]+)")


@dataclass
class GeneratorOptions:
    """Knobs for code generation.

    ``runtime`` controls how container/script work is executed:

    * ``"shell"``      - run the command on the Prefect worker host.
    * ``"docker"``     - run it via ``docker run`` (faithful to the image).
    * ``"kubernetes"`` - submit a Kubernetes ``Job`` via ``kubectl`` (closest to
      Argo; requires ``kubectl`` + cluster access on the worker).

    ``script_metadata`` embeds a PEP 723 inline-metadata block so the generated
    file is self-bootstrapping with ``uv run flow.py`` (no manual venv/install).
    """

    runtime: str = "docker"
    serve: bool = True
    include_header: bool = True
    script_metadata: bool = True

    def __post_init__(self) -> None:
        if self.runtime not in ("shell", "docker", "kubernetes"):
            raise ValueError(
                f"Unknown runtime {self.runtime!r}; expected shell|docker|kubernetes."
            )


@dataclass
class DeploymentPlan:
    """Everything needed to declare one Prefect deployment for a workflow.

    Emitted alongside the generated code so a caller (the CLI) can render a
    project-level ``prefect.yaml`` without re-deriving the generator's naming.
    ``entrypoint_file`` is filled in by the caller once it knows the ``.py``
    filename the module was written to.
    """

    name: str
    flow_func: str
    schedule: Optional[str] = None
    timezone: Optional[str] = None
    parameters: dict[str, str] = field(default_factory=dict)
    entrypoint_file: Optional[str] = None


class _Code:
    """Tiny indentation-aware source buffer (4-space indents)."""

    def __init__(self) -> None:
        self.lines: list[str] = []

    def add(self, text: str = "", indent: int = 0) -> None:
        self.lines.append(("    " * indent + text) if text else "")

    def render(self) -> str:
        return "\n".join(self.lines).rstrip() + "\n"


@dataclass
class _GenState:
    options: GeneratorOptions
    imports: set[str] = field(default_factory=set)
    helpers: set[str] = field(default_factory=set)
    used_runtime: set[str] = field(default_factory=set)
    warnings: list[str] = field(default_factory=list)
    func_names: set[str] = field(default_factory=set)
    workflow_params: dict[str, str] = field(default_factory=dict)

    def base_scope(self, inputs: Optional[dict[str, str]] = None) -> Scope:
        return Scope(
            inputs=inputs or {},
            workflow_params=dict(self.workflow_params),
            used_runtime=self.used_runtime,
            warnings=self.warnings,
        )


def generate_code(workflows: list[Workflow], options: GeneratorOptions | None = None) -> str:
    """Convert parsed :class:`Workflow` objects into a single Prefect module."""
    code, _plans = generate_module(workflows, options)
    return code


def generate_module(
    workflows: list[Workflow], options: GeneratorOptions | None = None
) -> tuple[str, list[DeploymentPlan]]:
    """Generate a Prefect module and the deployment plan for each workflow.

    Returns the source code plus one :class:`DeploymentPlan` per workflow (in
    input order), so callers can build a ``prefect.yaml`` that references the
    exact flow-function names this module defines.
    """
    if not workflows:
        raise ValueError("No workflows to generate from.")
    gen = _GenState(options or GeneratorOptions())

    # Collect workflow parameters (declared + referenced) so deep `{{workflow.*}}`
    # references resolve against a shared, runtime-overridable dict.
    wp_defaults: dict[str, str] = {}
    for wf in workflows:
        for param in wf.arguments:
            wp_defaults.setdefault(param.name, param.value or param.default or "")
        for name in sorted(_referenced_workflow_params(wf)):
            wp_defaults.setdefault(name, "")
    gen.workflow_params = {
        name: f"WORKFLOW_PARAMETERS[{_squote(name)}]" for name in wp_defaults
    }

    body = _Code()
    served: list[tuple[Workflow, str]] = []
    plans: list[DeploymentPlan] = []

    for index, wf in enumerate(workflows):
        for note in wf.warnings:
            gen.warnings.append(f"[{wf.display_name}] {note}")
        func_of = _assign_func_names(wf, gen)
        if index:
            body.add()
            body.add(f"# {'=' * 70}")
            body.add(f"# Workflow: {wf.display_name} ({wf.kind})")
            body.add(f"# {'=' * 70}")

        for template in wf.templates:
            if not template.is_composite:
                _emit_task(body, wf, template, func_of, gen)
        for template in wf.templates:
            if template.is_composite:
                _emit_composite_flow(body, wf, template, func_of, gen)

        main_name = _emit_main_flow(body, wf, func_of, gen)
        served.append((wf, main_name))
        plans.append(
            DeploymentPlan(
                name=wf.display_name,
                flow_func=main_name,
                schedule=wf.schedule,
                timezone=wf.timezone,
                parameters={
                    p.name: (p.value if p.value is not None else (p.default or ""))
                    for p in wf.arguments
                },
            )
        )

    header = _build_header(gen, wp_defaults, workflows)
    footer = _build_footer(gen, served) if gen.options.serve else ""
    return header + "\n" + body.render() + footer, plans


# --------------------------------------------------------------------------- #
# Naming
# --------------------------------------------------------------------------- #
def _assign_func_names(wf: Workflow, gen: _GenState) -> dict[str, str]:
    func_of: dict[str, str] = {}
    for template in wf.templates:
        base = sanitize_identifier(template.name, prefix="template")
        func_of[template.name] = unique(base, gen.func_names)
    return func_of


# --------------------------------------------------------------------------- #
# Leaf templates -> @task
# --------------------------------------------------------------------------- #
def _emit_task(
    code: _Code, wf: Workflow, template: Template, func_of: dict[str, str], gen: _GenState
) -> None:
    func = func_of[template.name]
    inputs = {p.name: sanitize_identifier(p.name) for p in template.inputs}
    scope = gen.base_scope(inputs)

    decorator = f'@task(name="{template.name}"'
    if template.retry_limit:
        decorator += f", retries={template.retry_limit}"
    decorator += ")"

    code.add()
    code.add(decorator)
    code.add(f"def {func}({_signature(template, gen)}):")
    doc = f"Argo template '{template.name}' ({template.kind.value})."
    code.add(f'"""{doc}"""', 1)

    kind = template.kind
    if kind == TemplateKind.CONTAINER and template.container is not None:
        _emit_container_body(code, 1, template.container, scope, gen, template.name)
    elif kind == TemplateKind.SCRIPT and template.script is not None:
        _emit_script_body(code, 1, template.script, scope, gen, template)
    elif kind == TemplateKind.RESOURCE and template.resource is not None:
        _emit_resource_body(code, 1, template, scope, gen)
    elif kind == TemplateKind.HTTP and template.http is not None:
        _emit_http_body(code, 1, template, scope, gen)
    elif kind == TemplateKind.SUSPEND:
        _emit_suspend_body(code, 1, template, gen)
    else:
        _emit_stub_body(code, 1, template, gen)


def _signature(template: Template, gen: _GenState) -> str:
    parts = []
    for param in template.inputs:
        ident = sanitize_identifier(param.name)
        default = json.dumps(param.default) if param.default is not None else '""'
        parts.append(f"{ident}: str = {default}")
    return ", ".join(parts)


def _emit_container_body(
    code: _Code,
    ind: int,
    spec: ContainerSpec,
    scope: Scope,
    gen: _GenState,
    name: str,
    *,
    return_value: bool = True,
) -> None:
    command_exprs = [translate_value(tok, scope) for tok in (spec.command + spec.args)]
    env_exprs = {k: translate_value(v, scope) for k, v in spec.env.items()}
    image = spec.image or "alpine:latest"
    runtime = gen.options.runtime

    if not command_exprs:
        code.add("# NOTE: container had no command/args; using the image entrypoint.", ind)

    if runtime == "shell":
        gen.imports.add("shell")
        gen.imports.add("shlex")
        code.add(f"# image '{image}' is ignored in shell runtime (runs on the worker host)", ind)
        code.add(f"_argv = [{', '.join(command_exprs)}]", ind)
        _emit_env_dict(code, ind, env_exprs)
        code.add("_out = ShellOperation(commands=[shlex.join(_argv)], env=_env).run()", ind)
    elif runtime == "docker":
        gen.imports.add("shell")
        gen.imports.add("shlex")
        _emit_env_dict(code, ind, env_exprs)
        code.add('_parts = ["docker", "run", "--rm"]', ind)
        code.add("for _k, _v in _env.items():", ind)
        code.add('_parts += ["-e", f"{_k}={_v}"]', ind + 1)
        code.add(f"_parts += [{json.dumps(image)}{_comma(command_exprs)}]", ind)
        code.add("_out = ShellOperation(commands=[shlex.join(_parts)]).run()", ind)
    else:  # kubernetes
        _emit_kubectl_job(code, ind, image, command_exprs, env_exprs, scope, gen, name)

    if return_value:
        code.add('return "\\n".join(_out)', ind)


def _emit_script_body(
    code: _Code, ind: int, spec: ScriptSpec, scope: Scope, gen: _GenState, template: Template
) -> None:
    gen.helpers.add("render")
    inputs_map = ", ".join(
        f"{json.dumps(p.name)}: {sanitize_identifier(p.name)}" for p in template.inputs
    )
    code.add(f"_script = _render_argo({json.dumps(spec.source)}, {{{inputs_map}}})", ind)
    env_exprs = {k: translate_value(v, scope) for k, v in spec.env.items()}
    interpreter = spec.interpreter or "sh"
    runtime = gen.options.runtime
    image = spec.image or "python:3-slim"

    if runtime == "kubernetes":
        command_exprs = [json.dumps(interpreter), json.dumps("-c"), "_script"]
        _emit_kubectl_job(code, ind, image, command_exprs, env_exprs, scope, gen, template.name)
        code.add('return "\\n".join(_out)', ind)
        return

    gen.imports.add("shell")
    gen.imports.add("shlex")
    gen.imports.add("os")
    gen.imports.add("tempfile")
    _emit_env_dict(code, ind, env_exprs)
    code.add(f"_fd, _path = tempfile.mkstemp(suffix={json.dumps(_suffix(interpreter))})", ind)
    code.add('with os.fdopen(_fd, "w") as _fh:', ind)
    code.add("_fh.write(_script)", ind + 1)
    code.add("try:", ind)
    if runtime == "shell":
        code.add(
            f"_cmd = shlex.join([{json.dumps(interpreter)}, _path])", ind + 1
        )
        code.add("_out = ShellOperation(commands=[_cmd], env=_env).run()", ind + 1)
    else:  # docker
        code.add('_parts = ["docker", "run", "--rm", "-i"]', ind + 1)
        code.add("for _k, _v in _env.items():", ind + 1)
        code.add('_parts += ["-e", f"{_k}={_v}"]', ind + 2)
        code.add(f"_parts += [{json.dumps(image)}, {json.dumps(interpreter)}]", ind + 1)
        code.add(
            '_cmd = shlex.join(_parts) + " < " + shlex.quote(_path)', ind + 1
        )
        code.add("_out = ShellOperation(commands=[_cmd]).run()", ind + 1)
    code.add("finally:", ind)
    code.add("os.unlink(_path)", ind + 1)
    code.add('return "\\n".join(_out)', ind)


def _emit_kubectl_job(
    code: _Code,
    ind: int,
    image: str,
    command_exprs: list[str],
    env_exprs: dict[str, str],
    scope: Scope,
    gen: _GenState,
    name: str,
) -> None:
    gen.imports.add("shell")
    gen.imports.add("json")
    gen.imports.add("os")
    gen.imports.add("tempfile")
    gen.imports.add("uuid")
    safe = sanitize_identifier(name).replace("_", "-")
    code.add(f'_job_name = "{safe}-" + uuid.uuid4().hex[:8]', ind)
    code.add(f"_env = [{_env_list(env_exprs)}]", ind)
    code.add("_container = {", ind)
    code.add('"name": "main",', ind + 1)
    code.add(f'"image": {json.dumps(image)},', ind + 1)
    if command_exprs:
        code.add(f'"command": [{", ".join(command_exprs)}],', ind + 1)
    code.add('"env": _env,', ind + 1)
    code.add("}", ind)
    code.add("_manifest = {", ind)
    code.add('"apiVersion": "batch/v1",', ind + 1)
    code.add('"kind": "Job",', ind + 1)
    code.add('"metadata": {"name": _job_name},', ind + 1)
    code.add('"spec": {', ind + 1)
    code.add('"backoffLimit": 0,', ind + 2)
    code.add('"template": {"spec": {"restartPolicy": "Never", "containers": [_container]}},', ind + 2)
    code.add("},", ind + 1)
    code.add("}", ind)
    code.add('_fd, _path = tempfile.mkstemp(suffix=".json")', ind)
    code.add('with os.fdopen(_fd, "w") as _fh:', ind)
    code.add("json.dump(_manifest, _fh)", ind + 1)
    code.add("try:", ind)
    code.add("ShellOperation(commands=[", ind + 1)
    code.add('f"kubectl apply -f {_path}",', ind + 2)
    code.add('f"kubectl wait --for=condition=complete --timeout=1h job/{_job_name}",', ind + 2)
    code.add("]).run()", ind + 1)
    code.add(
        '_out = ShellOperation(commands=[f"kubectl logs job/{_job_name}"]).run()', ind + 1
    )
    code.add("finally:", ind)
    code.add("os.unlink(_path)", ind + 1)


def _emit_resource_body(
    code: _Code, ind: int, template: Template, scope: Scope, gen: _GenState
) -> None:
    assert template.resource is not None
    gen.imports.add("shell")
    gen.imports.add("os")
    gen.imports.add("tempfile")
    gen.helpers.add("render")
    res = template.resource
    inputs_map = ", ".join(
        f"{json.dumps(p.name)}: {sanitize_identifier(p.name)}" for p in template.inputs
    )
    code.add(f"_manifest = _render_argo({json.dumps(res.manifest)}, {{{inputs_map}}})", ind)
    code.add('_fd, _path = tempfile.mkstemp(suffix=".yaml")', ind)
    code.add('with os.fdopen(_fd, "w") as _fh:', ind)
    code.add("_fh.write(_manifest)", ind + 1)
    code.add("try:", ind)
    code.add(
        f"_out = ShellOperation(commands=[f\"kubectl {res.action} -f {{_path}}\"]).run()",
        ind + 1,
    )
    code.add("finally:", ind)
    code.add("os.unlink(_path)", ind + 1)
    code.add('return "\\n".join(_out)', ind)


def _emit_http_body(
    code: _Code, ind: int, template: Template, scope: Scope, gen: _GenState
) -> None:
    assert template.http is not None
    gen.imports.add("urllib")
    http = template.http
    url_expr = translate_value(http.url, scope)
    code.add(f"_url = {url_expr}", ind)
    code.add(f"_req = urllib.request.Request(_url, method={json.dumps(http.method)})", ind)
    for key, value in http.headers.items():
        code.add(f"_req.add_header({json.dumps(key)}, {translate_value(value, scope)})", ind)
    if http.body is not None:
        code.add(f"_data = ({translate_value(http.body, scope)}).encode()", ind)
    else:
        code.add("_data = None", ind)
    code.add("with urllib.request.urlopen(_req, data=_data) as _resp:", ind)
    code.add("return _resp.read().decode()", ind + 1)


def _emit_suspend_body(code: _Code, ind: int, template: Template, gen: _GenState) -> None:
    duration = template.suspend.duration if template.suspend else None
    seconds = _duration_to_seconds(duration)
    if seconds is None:
        code.add("# TODO: indefinite suspend. Use prefect.flow_runs.pause_flow_run() if needed.", ind)
        code.add("return None", ind)
    else:
        gen.imports.add("time")
        code.add(f"time.sleep({seconds})", ind)
        code.add("return None", ind)


def _emit_stub_body(code: _Code, ind: int, template: Template, gen: _GenState) -> None:
    gen.warnings.append(
        f"Template '{template.name}' ({template.kind.value}) emitted as a stub; implement manually."
    )
    code.add(
        f"raise NotImplementedError("
        f"{json.dumps(f'Argo template {template.name!r} of type {template.kind.value!r} needs manual migration.')})",
        ind,
    )


# --------------------------------------------------------------------------- #
# Composite templates -> @flow
# --------------------------------------------------------------------------- #
def _emit_composite_flow(
    code: _Code, wf: Workflow, template: Template, func_of: dict[str, str], gen: _GenState
) -> None:
    func = func_of[template.name]
    inputs = {p.name: sanitize_identifier(p.name) for p in template.inputs}
    scope = gen.base_scope(inputs)

    code.add()
    code.add(f'@flow(name="{template.name}")')
    code.add(f"def {func}({_signature(template, gen)}):")
    code.add(f'"""Argo {template.kind.value} template \'{template.name}\'."""', 1)

    sync_calls = _composite_sync_calls(wf, template)

    if template.kind == TemplateKind.DAG:
        ordered = _topo_sort(template.dag_tasks)
        for call in ordered:
            prev = [
                f"{sanitize_identifier(d)}_fut"
                for d in call.dependencies
                if d not in sync_calls
            ]
            _emit_call(code, 1, wf, call, scope, func_of, gen, prev, sync_calls)
    else:  # STEPS
        prev_group: list[str] = []
        for group in template.step_groups:
            current: list[str] = []
            for call in group:
                waits = [w for w in prev_group]
                _emit_call(code, 1, wf, call, scope, func_of, gen, waits, sync_calls)
                if call.name not in sync_calls:
                    current.append(f"{sanitize_identifier(call.name)}_fut")
            prev_group = current

    code.add("return None", 1)


def _composite_sync_calls(wf: Workflow, template: Template) -> set[str]:
    """Names of calls whose target is itself composite (run inline, not submitted)."""
    sync: set[str] = set()
    calls = template.dag_tasks if template.kind == TemplateKind.DAG else [
        c for group in template.step_groups for c in group
    ]
    for call in calls:
        target = wf.template_by_name(call.template)
        if target is not None and target.is_composite:
            sync.add(call.name)
    return sync


def _emit_call(
    code: _Code,
    ind: int,
    wf: Workflow,
    call,
    scope: Scope,
    func_of: dict[str, str],
    gen: _GenState,
    wait_names: list[str],
    sync_calls: set[str],
) -> None:
    fut_var = f"{sanitize_identifier(call.name)}_fut"
    target = wf.template_by_name(call.template)

    code.add(f"# {call.name} -> template '{call.template}'", ind)
    if target is None:
        gen.warnings.append(
            f"Call '{call.name}' references unknown template '{call.template}' "
            "(cross-file templateRef?); emitted as a stub."
        )
        code.add(
            f"raise NotImplementedError({json.dumps(f'Unresolved templateRef: {call.template}')})",
            ind,
        )
        code.add(f"{fut_var} = None", ind)
        return

    target_func = func_of[call.template]
    is_sync = call.name in sync_calls
    wait_clause = _wait_clause(wait_names)

    body_ind = ind
    if call.when:
        cond = translate_condition(call.when, scope)
        code.add(f"{fut_var} = None  # conditional", ind)
        code.add(f"if {cond}:  # TODO(argo2prefect): review translated condition", ind)
        body_ind = ind + 1

    looped = call.with_items is not None or bool(call.with_param)
    if looped and not is_sync:
        _emit_mapped_call(code, body_ind, call, target, target_func, scope, gen, wait_clause, fut_var)
    elif is_sync:
        kwargs = _build_kwargs(call, target, scope, gen)
        if looped:
            gen.warnings.append(
                f"Call '{call.name}' loops over a composite template; emitted as a sequential loop."
            )
        code.add(f"{fut_var} = {target_func}({kwargs})", body_ind)
    else:
        kwargs = _build_kwargs(call, target, scope, gen)
        args = ", ".join(filter(None, [kwargs, wait_clause]))
        code.add(f"{fut_var} = {target_func}.submit({args})", body_ind)


def _emit_mapped_call(
    code: _Code,
    ind: int,
    call,
    target: Template,
    target_func: str,
    scope: Scope,
    gen: _GenState,
    wait_clause: str,
    fut_var: str,
) -> None:
    gen.imports.add("unmapped")
    if call.with_items is not None:
        code.add(f"_items = {_py_literal(call.with_items)}", ind)
    else:
        gen.helpers.add("as_list")
        code.add(f"_items = _as_list({translate_value(call.with_param, scope)})", ind)

    item_scope = gen.base_scope(scope.inputs)
    item_scope.item_var = "_item"
    valid = {p.name for p in target.inputs}
    mapped_kwargs: list[str] = []
    has_mapped = False
    for arg in call.arguments:
        if arg.name not in valid:
            continue
        ident = sanitize_identifier(arg.name)
        if _references_item(arg.value):
            has_mapped = True
            expr = translate_value(arg.value, item_scope)
            mapped_kwargs.append(f"{ident}=[{expr} for _item in _items]")
        else:
            mapped_kwargs.append(f"{ident}=unmapped({translate_value(arg.value, scope)})")

    if not has_mapped:
        gen.warnings.append(
            f"Call '{call.name}' loops but no argument uses {{{{item}}}}; emitted a per-item submit loop."
        )
        code.add("_futs = []", ind)
        code.add("for _item in _items:", ind)
        loop_kwargs = ", ".join(mapped_kwargs).replace("unmapped(", "(")
        args = ", ".join(filter(None, [loop_kwargs, wait_clause]))
        code.add(f"_futs.append({target_func}.submit({args}))", ind + 1)
        code.add(f"{fut_var} = _futs", ind)
        return

    args = ", ".join(filter(None, [", ".join(mapped_kwargs), wait_clause]))
    code.add(f"{fut_var} = {target_func}.map({args})", ind)


def _build_kwargs(call, target: Template, scope: Scope, gen: _GenState) -> str:
    valid = {p.name for p in target.inputs}
    parts: list[str] = []
    for arg in call.arguments:
        if arg.name not in valid:
            gen.warnings.append(
                f"Argument '{arg.name}' for call '{call.name}' is not a declared input "
                f"of template '{target.name}'; dropped."
            )
            continue
        parts.append(f"{sanitize_identifier(arg.name)}={translate_value(arg.value, scope)}")
    return ", ".join(parts)


def _wait_clause(wait_names: list[str]) -> str:
    if not wait_names:
        return ""
    joined = ", ".join(wait_names)
    return f"wait_for=[_f for _f in [{joined}] if _f is not None]"


# --------------------------------------------------------------------------- #
# Main flow + entrypoint wiring
# --------------------------------------------------------------------------- #
def _emit_main_flow(code: _Code, wf: Workflow, func_of: dict[str, str], gen: _GenState) -> str:
    entry = _resolve_entrypoint(wf, gen)
    main_name = unique(
        sanitize_identifier(wf.display_name, prefix="main_flow") + "_flow", gen.func_names
    )

    params = []
    for param in wf.arguments:
        ident = sanitize_identifier(param.name)
        default = json.dumps(param.value if param.value is not None else (param.default or ""))
        params.append(f"{ident}: str = {default}")

    code.add()
    code.add(f'@flow(name="{wf.display_name}")')
    code.add(f"def {main_name}({', '.join(params)}):")
    code.add(f'"""Entry point for Argo {wf.kind} \'{wf.display_name}\'."""', 1)

    if wf.arguments:
        updates = ", ".join(
            f"{json.dumps(p.name)}: {sanitize_identifier(p.name)}" for p in wf.arguments
        )
        code.add(f"WORKFLOW_PARAMETERS.update({{{updates}}})", 1)

    if entry is None:
        code.add("# TODO: no entrypoint template found; nothing to run.", 1)
        code.add("return None", 1)
        return main_name

    entry_func = func_of[entry.name]
    kwargs = _entrypoint_kwargs(wf, entry)
    code.add(f"return {entry_func}({kwargs})", 1)
    return main_name


def _resolve_entrypoint(wf: Workflow, gen: _GenState) -> Optional[Template]:
    if wf.entrypoint:
        target = wf.template_by_name(wf.entrypoint)
        if target is not None:
            return target
        gen.warnings.append(
            f"[{wf.display_name}] entrypoint '{wf.entrypoint}' not found; using a fallback."
        )
    for template in wf.templates:
        if template.is_composite:
            return template
    return wf.templates[0] if wf.templates else None


def _entrypoint_kwargs(wf: Workflow, entry: Template) -> str:
    wf_arg_names = {p.name for p in wf.arguments}
    parts: list[str] = []
    for param in entry.inputs:
        ident = sanitize_identifier(param.name)
        if param.name in wf_arg_names:
            parts.append(f"{ident}={ident}")
        elif param.default is not None:
            parts.append(f"{ident}={json.dumps(param.default)}")
    return ", ".join(parts)


# --------------------------------------------------------------------------- #
# Header / footer / helpers
# --------------------------------------------------------------------------- #
def _build_header(gen: _GenState, wp_defaults: dict[str, str], workflows: list[Workflow]) -> str:
    code = _Code()

    if gen.options.script_metadata:
        # PEP 723 inline script metadata: `uv run flow.py` auto-installs these
        # and runs the flow with no manual environment setup. Harmless to others.
        deps = ['"prefect>=3,<4"']
        if "shell" in gen.imports:
            deps.append('"prefect-shell>=0.3"')
        code.add("# /// script")
        code.add('# requires-python = ">=3.9"')
        code.add(f"# dependencies = [{', '.join(deps)}]")
        code.add("# ///")

    if gen.options.include_header:
        sources = ", ".join(f"{w.display_name} ({w.kind})" for w in workflows)
        code.add('"""Prefect flows generated by argo2prefect.')
        code.add()
        code.add(f"Source: {sources}")
        code.add(f"Runtime: {gen.options.runtime}")
        if gen.warnings:
            code.add()
            code.add("Review the following before running in production:")
            for warning in _dedupe(gen.warnings):
                code.add(f"  - {warning}")
        code.add('"""')
        code.add()

    # Stringize annotations so generated type hints stay valid on Python 3.9+.
    code.add("from __future__ import annotations")
    code.add()

    stdlib = sorted(
        name
        for name in gen.imports
        if name in {"json", "os", "shlex", "tempfile", "time", "uuid"}
    )
    for name in stdlib:
        code.add(f"import {name}")
    if "urllib" in gen.imports:
        code.add("import urllib.request")

    prefect_imports = ["flow", "task"]
    if "unmapped" in gen.imports:
        prefect_imports.append("unmapped")
    code.add(f"from prefect import {', '.join(prefect_imports)}")
    if gen.used_runtime:
        code.add(f"from prefect.runtime import {', '.join(sorted(gen.used_runtime))}")
    if "shell" in gen.imports:
        code.add("from prefect_shell import ShellOperation")

    code.add()
    code.add(f"WORKFLOW_PARAMETERS = {_py_literal(wp_defaults)}")

    if "render" in gen.helpers:
        code.add()
        code.add(_RENDER_HELPER.rstrip())
    if "as_list" in gen.helpers:
        code.add()
        code.add(_AS_LIST_HELPER.rstrip())

    return code.render()


def _build_footer(gen: _GenState, served: list[tuple[Workflow, str]]) -> str:
    code = _Code()
    code.add()
    code.add()
    code.add('if __name__ == "__main__":')
    if len(served) == 1:
        wf, func = served[0]
        serve_args = ['name="' + wf.display_name + '"']
        if wf.schedule:
            serve_args.append(f"cron={json.dumps(wf.schedule)}")
        if wf.timezone:
            serve_args.append(f"timezone={json.dumps(wf.timezone)}")
        code.add("import sys", 1)
        code.add()
        code.add('if "--serve" in sys.argv:', 1)
        code.add(f"{func}.serve({', '.join(serve_args)})", 2)
        code.add("else:", 1)
        code.add("# One-off local run with default parameters.", 2)
        code.add("# Pass --serve to deploy this flow on the schedule above instead.", 2)
        code.add(f"{func}()", 2)
    else:
        gen.imports.add("serve")
        deployments = []
        for wf, func in served:
            d_args = [f'name="{wf.display_name}"']
            if wf.schedule:
                d_args.append(f"cron={json.dumps(wf.schedule)}")
            deployments.append(f"{func}.to_deployment({', '.join(d_args)})")
        code.add("from prefect import serve", 1)
        code.add("serve(", 1)
        for dep in deployments:
            code.add(f"{dep},", 2)
        code.add(")", 1)
    return code.render()


# --------------------------------------------------------------------------- #
# Small utilities
# --------------------------------------------------------------------------- #
def _emit_env_dict(code: _Code, ind: int, env_exprs: dict[str, str]) -> None:
    if not env_exprs:
        code.add("_env = {}", ind)
        return
    items = ", ".join(f"{json.dumps(k)}: {v}" for k, v in env_exprs.items())
    code.add(f"_env = {{{items}}}", ind)


def _env_list(env_exprs: dict[str, str]) -> str:
    return ", ".join(
        f'{{"name": {json.dumps(k)}, "value": {v}}}' for k, v in env_exprs.items()
    )


def _comma(items: list[str]) -> str:
    return (", " + ", ".join(items)) if items else ""


def _topo_sort(tasks: list) -> list:
    """Order DAG tasks so dependencies precede dependents (stable, cycle-safe)."""
    by_name = {t.name: t for t in tasks}
    visited: set[str] = set()
    ordered: list = []

    def visit(name: str, stack: set[str]) -> None:
        if name in visited or name not in by_name or name in stack:
            return
        stack.add(name)
        for dep in by_name[name].dependencies:
            visit(dep, stack)
        stack.discard(name)
        visited.add(name)
        ordered.append(by_name[name])

    for task in tasks:
        visit(task.name, set())
    return ordered


def _references_item(value: Optional[str]) -> bool:
    return bool(value and _ITEM_REF.search(value))


def _referenced_workflow_params(wf: Workflow) -> set[str]:
    blob = json.dumps(wf.raw)
    return set(_WF_PARAM_REF.findall(blob))


def _duration_to_seconds(duration: Optional[str]) -> Optional[int]:
    if not duration:
        return None
    text = str(duration).strip()
    multipliers = {"s": 1, "m": 60, "h": 3600, "d": 86400}
    if text and text[-1] in multipliers:
        try:
            return int(float(text[:-1]) * multipliers[text[-1]])
        except ValueError:
            return None
    try:
        return int(float(text))
    except ValueError:
        return None


def _suffix(interpreter: str) -> str:
    return {"python": ".py", "node": ".js", "bash": ".sh", "sh": ".sh"}.get(interpreter, ".sh")


def _py_literal(value) -> str:
    """A valid Python literal for primitive/list/dict YAML values (uses repr)."""
    return repr(value)


def _squote(text: str) -> str:
    """Single-quoted string literal.

    Used for dict subscripts that may be embedded inside double-quoted f-strings
    (e.g. ``WORKFLOW_PARAMETERS['x']``), which keeps the output valid on Python
    < 3.12 where reusing the outer quote char inside an f-string is a syntax error.
    """
    return "'" + text.replace("\\", "\\\\").replace("'", "\\'") + "'"


def _dedupe(items: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for item in items:
        if item not in seen:
            seen.add(item)
            out.append(item)
    return out


_RENDER_HELPER = '''
import re as _re


def _render_argo(_text, _inputs=None):
    """Substitute Argo `{{inputs.parameters.*}}` / `{{workflow.parameters.*}}` placeholders."""
    _inputs = _inputs or {}

    def _sub(_m):
        _key = _m.group(1).strip()
        if _key.startswith("inputs.parameters."):
            return str(_inputs.get(_key[len("inputs.parameters."):], ""))
        if _key.startswith("workflow.parameters."):
            return str(WORKFLOW_PARAMETERS.get(_key[len("workflow.parameters."):], ""))
        return _m.group(0)

    return _re.sub(r"\\{\\{\\s*(.*?)\\s*\\}\\}", _sub, _text)
'''

_AS_LIST_HELPER = '''
def _as_list(_value):
    """Coerce an Argo `withParam` value (often a JSON string) into a list."""
    if isinstance(_value, str):
        import json as _json

        return _json.loads(_value)
    return list(_value)
'''
