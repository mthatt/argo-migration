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
import subprocess
import sys
from dataclasses import dataclass, field, replace

from .expressions import Scope, translate_condition, translate_value
from .models import (
    Call,
    ContainerSpec,
    ScriptSpec,
    Template,
    TemplateKind,
    Workflow,
)
from .naming import sanitize_identifier, unique
from .parser import depends_is_plain
from .project import Project

GENERATED_BY = "argo2prefect"

_ITEM_REF = re.compile(r"\{\{\s*item(\.|\s|\})")
_WF_PARAM_REF = re.compile(r"workflow\.parameters\.([A-Za-z0-9_.\-]+)")


@dataclass
class GeneratorOptions:
    """Knobs for code generation.

    ``runtime`` controls how container/script work is executed:

    * ``"shell"``      - run the command on the Prefect worker host.
    * ``"docker"``     - run it via ``docker run`` (faithful to the image).
    * ``"kubernetes"`` - submit a Kubernetes ``Job`` via the official client
      (closest to Argo; needs kubeconfig or in-cluster credentials on the
      worker; the Job is polled to completion and always cleaned up).

    ``script_metadata`` embeds a PEP 723 inline-metadata block so the generated
    file is self-bootstrapping with ``uv run flow.py`` (no manual venv/install).
    """

    runtime: str = "docker"
    serve: bool = True
    include_header: bool = True
    script_metadata: bool = True

    def __post_init__(self) -> None:
        if self.runtime not in ("shell", "docker", "kubernetes"):
            raise ValueError(f"Unknown runtime {self.runtime!r}; expected shell|docker|kubernetes.")


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
    schedule: str | None = None
    schedules: list[str] = field(default_factory=list)
    timezone: str | None = None
    suspended: bool = False
    parameters: dict[str, str] = field(default_factory=dict)
    entrypoint_file: str | None = None


SHARED_MODULE_NAME = "shared_templates"


@dataclass
class SharedModuleInfo:
    """What workflow modules need to know about the shared-templates module.

    Produced by :func:`generate_shared_module`; consumed by
    :func:`generate_module` so per-workflow files can import and call the
    functions generated for ``WorkflowTemplate`` / ``ClusterWorkflowTemplate``
    manifests instead of stubbing ``templateRef`` call sites.
    """

    module_name: str = SHARED_MODULE_NAME
    #: (library manifest name, template name) -> generated function name.
    func_of: dict[tuple[str, str], str] = field(default_factory=dict)
    #: Library manifest name -> its parsed Workflow (for input signatures).
    manifests: dict[str, Workflow] = field(default_factory=dict)

    def lookup(self, library: str, template: str) -> tuple[Template, str] | None:
        manifest = self.manifests.get(library)
        if manifest is None:
            return None
        target = manifest.template_by_name(template)
        func = self.func_of.get((library, template))
        if target is None or func is None:
            return None
        return target, func


@dataclass
class ProjectOutput:
    """Result of :func:`generate_project`: output filename -> module source."""

    files: dict[str, str] = field(default_factory=dict)
    plans: list[DeploymentPlan] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)


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
    # Cross-manifest resolution within this module: (manifest name, template
    # name) -> (template, function name). Covers multi-document inputs.
    module_registry: dict[tuple[str, str], tuple[Template, str]] = field(default_factory=dict)
    # The shared-templates module of the surrounding project, if any.
    shared: SharedModuleInfo | None = None
    # Function names to import from the shared module (collected as used).
    shared_imports: set[str] = field(default_factory=set)

    def base_scope(self, inputs: dict[str, str] | None = None) -> Scope:
        return Scope(
            inputs=inputs or {},
            workflow_params=dict(self.workflow_params),
            used_runtime=self.used_runtime,
            helpers=self.helpers,
            warnings=self.warnings,
        )

    def resolve_call(
        self, wf: Workflow, call: Call, func_of: dict[str, str]
    ) -> tuple[Template, str] | None:
        """Find the template + generated function a call refers to.

        Resolution order: inline template in the same manifest, then a
        library manifest generated into this module (multi-document input),
        then the project's shared-templates module (recorded as an import).
        """
        if not call.template_ref:
            target = wf.template_by_name(call.template)
            if target is not None:
                return target, func_of[call.template]
            return None
        hit = self.module_registry.get((call.template_ref, call.template))
        if hit is not None:
            return hit
        if self.shared is not None:
            shared_hit = self.shared.lookup(call.template_ref, call.template)
            if shared_hit is not None:
                self.shared_imports.add(shared_hit[1])
                return shared_hit
        return None


def generate_code(workflows: list[Workflow], options: GeneratorOptions | None = None) -> str:
    """Convert parsed :class:`Workflow` objects into a single Prefect module."""
    code, _plans = generate_module(workflows, options)
    return code


def format_code(code: str) -> str:
    """Format generated source with ruff (best effort).

    Falls back to the unformatted code if ruff is unavailable or errors, so
    formatting can never break a conversion.
    """
    try:
        proc = subprocess.run(
            [sys.executable, "-m", "ruff", "format", "--stdin-filename", "generated_flow.py", "-"],
            input=code,
            capture_output=True,
            text=True,
            timeout=30,
        )
    except Exception:
        return code
    if proc.returncode == 0 and proc.stdout:
        return proc.stdout
    return code


def generate_module(
    workflows: list[Workflow],
    options: GeneratorOptions | None = None,
    shared: SharedModuleInfo | None = None,
) -> tuple[str, list[DeploymentPlan]]:
    """Generate a Prefect module and the deployment plan for each workflow.

    Returns the source code plus one :class:`DeploymentPlan` per workflow (in
    input order), so callers can build a ``prefect.yaml`` that references the
    exact flow-function names this module defines.

    ``shared`` connects the module to a project's shared-templates module:
    ``templateRef`` call sites then resolve to imported functions instead of
    stubs (see :func:`generate_project`).
    """
    gen = _GenState(options or GeneratorOptions(), shared=shared)
    code, plans = _generate_module(workflows, gen)
    return code, plans


def generate_shared_module(
    libraries: list[Workflow],
    options: GeneratorOptions | None = None,
    extra_workflow_params: dict[str, str] | None = None,
) -> tuple[str, SharedModuleInfo]:
    """Generate the shared module for a project's template libraries.

    ``extra_workflow_params`` merges the whole project's workflow-parameter
    defaults into this module's ``WORKFLOW_PARAMETERS`` dict, which workflow
    modules import so every module reads and writes the same dict.
    """
    opts = replace(options or GeneratorOptions(), serve=False)
    gen = _GenState(opts)
    code, _plans = _generate_module(libraries, gen, extra_workflow_params)
    return code, SharedModuleInfo(
        func_of={key: func for key, (_tmpl, func) in gen.module_registry.items()},
        manifests={wf.display_name: wf for wf in libraries},
    )


def generate_project(project: Project, options: GeneratorOptions | None = None) -> ProjectOutput:
    """Generate every module for a linked project.

    ``WorkflowTemplate`` / ``ClusterWorkflowTemplate`` manifests are emitted
    once into ``shared_templates.py``; each source file with runnable
    workflows becomes ``<stem>_flow.py``, importing shared functions as
    needed. Library manifests get no deployment plans (they are code, not
    schedulable workloads); runnable workflows keep exactly one plan each.
    """
    out = ProjectOutput(warnings=list(project.warnings))
    libraries = project.libraries

    shared_info: SharedModuleInfo | None = None
    if libraries:
        # The shared module owns the project-wide WORKFLOW_PARAMETERS dict, so
        # seed it with defaults from every manifest, not just the libraries.
        all_defaults = _collect_wp_defaults([wf for file in project.files for wf in file.workflows])
        shared_code, shared_info = generate_shared_module(
            libraries, options, extra_workflow_params=all_defaults
        )
        out.files[f"{SHARED_MODULE_NAME}.py"] = format_code(shared_code)

    for file in project.files:
        runnable = file.runnable
        if not runnable:
            continue
        code, plans = generate_module(runnable, options, shared=shared_info)
        filename = _unique_filename(f"{file.name}_flow.py", out.files)
        out.files[filename] = format_code(code)
        for plan in plans:
            plan.entrypoint_file = filename
        out.plans.extend(plans)

    if not out.files:
        raise ValueError("No workflows to generate from.")
    return out


def _collect_wp_defaults(workflows: list[Workflow]) -> dict[str, str]:
    """Workflow-parameter defaults, declared or referenced, across manifests."""
    wp_defaults: dict[str, str] = {}
    for wf in workflows:
        for param in wf.arguments:
            wp_defaults.setdefault(param.name, param.value or param.default or "")
        for name in sorted(_referenced_workflow_params(wf)):
            wp_defaults.setdefault(name, "")
    return wp_defaults


def _unique_filename(name: str, existing: dict[str, str]) -> str:
    if name not in existing:
        return name
    stem, _, suffix = name.rpartition(".")
    counter = 2
    while f"{stem}_{counter}.{suffix}" in existing:
        counter += 1
    return f"{stem}_{counter}.{suffix}"


def _generate_module(
    workflows: list[Workflow],
    gen: _GenState,
    extra_workflow_params: dict[str, str] | None = None,
) -> tuple[str, list[DeploymentPlan]]:
    if not workflows:
        raise ValueError("No workflows to generate from.")

    # Collect workflow parameters (declared + referenced) so deep `{{workflow.*}}`
    # references resolve against a shared, runtime-overridable dict. The
    # module's own declared defaults win over project-wide extras.
    wp_defaults = _collect_wp_defaults(workflows)
    for name, value in (extra_workflow_params or {}).items():
        if not wp_defaults.get(name):
            wp_defaults[name] = value
    gen.workflow_params = {name: f"WORKFLOW_PARAMETERS[{_squote(name)}]" for name in wp_defaults}

    body = _Code()
    served: list[tuple[Workflow, str]] = []
    plans: list[DeploymentPlan] = []

    # Assign every function name up front so cross-manifest references within
    # this module (multi-document inputs) resolve regardless of order.
    func_maps: list[dict[str, str]] = []
    for wf in workflows:
        func_of = _assign_func_names(wf, gen)
        func_maps.append(func_of)
        for template in wf.templates:
            gen.module_registry.setdefault(
                (wf.display_name, template.name), (template, func_of[template.name])
            )

    for index, wf in enumerate(workflows):
        for note in wf.warnings:
            gen.warnings.append(f"[{wf.display_name}] {note}")
        func_of = func_maps[index]
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
                schedules=list(wf.schedules),
                timezone=wf.timezone,
                suspended=wf.suspended,
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

    args = [f'name="{template.name}"']
    args += _behavior_decorator_args(template, gen)
    decorator = f"@task({', '.join(args)})"

    code.add()
    code.add(decorator)
    code.add(f"def {func}({_signature(template, gen)}):")
    doc = f"Argo template '{template.name}' ({template.kind.value})."
    code.add(f'"""{doc}"""', 1)

    ind = _emit_artifact_todos(code, 1, template, gen)
    ind = _emit_sync_guard(code, ind, template.synchronization, gen)

    kind = template.kind
    if kind == TemplateKind.CONTAINER and template.container is not None:
        _emit_container_body(code, ind, template.container, scope, gen, template.name)
    elif kind == TemplateKind.SCRIPT and template.script is not None:
        _emit_script_body(code, ind, template.script, scope, gen, template)
    elif kind == TemplateKind.RESOURCE and template.resource is not None:
        _emit_resource_body(code, ind, template, scope, gen)
    elif kind == TemplateKind.HTTP and template.http is not None:
        _emit_http_body(code, ind, template, scope, gen)
    elif kind == TemplateKind.SUSPEND:
        _emit_suspend_body(code, ind, template, gen)
    else:
        _emit_stub_body(code, ind, template, gen)


def _behavior_decorator_args(template: Template, gen: _GenState) -> list[str]:
    """Decorator arguments shared by tasks and (composite) flows: retries,
    timeouts, and caching — the Argo semantics with direct Prefect knobs."""
    args: list[str] = []
    retry = template.retry
    if retry and retry.limit:
        args.append(f"retries={retry.limit}")
        if retry.backoff_factor:
            gen.imports.add("exponential_backoff")
            base = _duration_to_seconds(retry.backoff_duration) or 1
            args.append(f"retry_delay_seconds=exponential_backoff(backoff_factor={base})")
            if retry.backoff_max:
                gen.warnings.append(
                    f"Template '{template.name}' caps retry backoff at "
                    f"{retry.backoff_max}; Prefect's exponential_backoff has no cap — review."
                )
        elif retry.backoff_duration:
            seconds = _duration_to_seconds(retry.backoff_duration)
            if seconds:
                args.append(f"retry_delay_seconds={seconds}")
        if retry.policy and retry.policy != "OnFailure":
            gen.warnings.append(
                f"Template '{template.name}' uses retryPolicy '{retry.policy}'; Prefect "
                "retries on any task failure — review if error/failure distinction matters."
            )
    if template.timeout_seconds:
        args.append(f"timeout_seconds={template.timeout_seconds}")
    if template.memoize:
        gen.imports.add("INPUTS")
        args.append("cache_policy=INPUTS")
        max_age = _duration_to_seconds(template.memoize.max_age)
        if max_age:
            gen.imports.add("timedelta")
            args.append(f"cache_expiration=timedelta(seconds={max_age})")
        gen.warnings.append(
            f"Template '{template.name}' memoize key '{template.memoize.key}' is "
            "approximated with cache_policy=INPUTS (hashes ALL inputs); review."
        )
    return args


def _emit_artifact_todos(code: _Code, ind: int, template: Template, gen: _GenState) -> int:
    """Anchor artifact follow-up work in the task body, with the storage
    location when the manifest declares one."""
    for direction, todo_code, artifacts in (
        ("input", "A2P-103", template.input_artifacts),
        ("output", "A2P-104", template.output_artifacts),
    ):
        for art in artifacts:
            where = f" from {art.storage}://{art.location}" if art.storage else ""
            path = f" at '{art.path}'" if art.path else ""
            verb = "fetch it before the command runs" if direction == "input" else "publish it"
            code.add(
                f"# TODO({todo_code}): {direction} artifact '{art.name}'{where}{path}; {verb}.",
                ind,
            )
    if template.input_artifacts or template.output_artifacts:
        gen.warnings.append(
            f"Template '{template.name}' uses artifacts; storage wiring is left as "
            "TODOs in the task body."
        )
    return ind


def _emit_sync_guard(code: _Code, ind: int, sync, gen: _GenState) -> int:
    """Open a Prefect concurrency guard for an Argo mutex/semaphore; returns
    the body indent to use inside the guard."""
    if sync is None:
        return ind
    gen.imports.add("concurrency")
    code.add(f"with concurrency({json.dumps(sync.name)}):  # Argo {sync.kind}", ind)
    gen.warnings.append(
        f"Create the global concurrency limit '{sync.name}' before running "
        f"(`prefect gcl create {sync.name} --limit <N>`); it guards an Argo {sync.kind}."
    )
    return ind + 1


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
        if return_value:
            code.add('return "\\n".join(_out)', ind)
    elif runtime == "docker":
        gen.imports.add("docker")
        _emit_env_dict(code, ind, env_exprs)
        command = f"[{', '.join(command_exprs)}]" if command_exprs else "None"
        _emit_docker_run(code, ind, image, command)
        if return_value:
            code.add('return _out.decode().rstrip("\\n")', ind)
    else:  # kubernetes
        gen.helpers.add("k8s_job")
        _emit_env_dict(code, ind, env_exprs)
        command = f"[{', '.join(command_exprs)}]" if command_exprs else "None"
        safe = sanitize_identifier(name).replace("_", "-")
        code.add(
            f"_out = _run_k8s_job({json.dumps(image)}, {command}, _env, {json.dumps(safe)})", ind
        )
        if return_value:
            code.add('return _out.rstrip("\\n")', ind)


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
        gen.helpers.add("k8s_job")
        _emit_env_dict(code, ind, env_exprs)
        safe = sanitize_identifier(template.name).replace("_", "-")
        code.add(
            f"_out = _run_k8s_job({json.dumps(image)}, "
            f'[{json.dumps(interpreter)}, "-c", _script], _env, {json.dumps(safe)})',
            ind,
        )
        code.add('return _out.rstrip("\\n")', ind)
        return

    if runtime == "docker":
        # The script goes straight into the container command — no temp file.
        gen.imports.add("docker")
        _emit_env_dict(code, ind, env_exprs)
        _emit_docker_run(code, ind, image, f'[{json.dumps(interpreter)}, "-c", _script]')
        code.add('return _out.decode().rstrip("\\n")', ind)
        return

    # shell: write the script to a temp file and run it on the worker host.
    gen.imports.add("shell")
    gen.imports.add("shlex")
    gen.imports.add("os")
    gen.imports.add("tempfile")
    _emit_env_dict(code, ind, env_exprs)
    code.add(f"_fd, _path = tempfile.mkstemp(suffix={json.dumps(_suffix(interpreter))})", ind)
    code.add('with os.fdopen(_fd, "w") as _fh:', ind)
    code.add("_fh.write(_script)", ind + 1)
    code.add("try:", ind)
    code.add(f"_cmd = shlex.join([{json.dumps(interpreter)}, _path])", ind + 1)
    code.add("_out = ShellOperation(commands=[_cmd], env=_env).run()", ind + 1)
    code.add("finally:", ind)
    code.add("os.unlink(_path)", ind + 1)
    code.add('return "\\n".join(_out)', ind)


def _emit_docker_run(code: _Code, ind: int, image: str, command: str) -> None:
    """Run a container via the Docker SDK and bind its combined logs to _out."""
    code.add("_client = docker.from_env()", ind)
    code.add("_out = _client.containers.run(", ind)
    code.add(f"{json.dumps(image)},", ind + 1)
    code.add(f"{command},", ind + 1)
    code.add("environment=_env,", ind + 1)
    code.add("remove=True,", ind + 1)
    code.add("stdout=True,", ind + 1)
    code.add("stderr=True,", ind + 1)
    code.add(")", ind)


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
        f'_out = ShellOperation(commands=[f"kubectl {res.action} -f {{_path}}"]).run()',
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
        code.add(
            "# TODO(A2P-107): indefinite suspend. Use prefect.flow_runs.pause_flow_run() if needed.",
            ind,
        )
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
        f"# TODO(A2P-112): template type '{template.kind.value}' has no automatic migration.", ind
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

    args = [f'name="{template.name}"']
    retry = template.retry
    if retry and retry.limit:
        args.append(f"retries={retry.limit}")
    if template.timeout_seconds:
        args.append(f"timeout_seconds={template.timeout_seconds}")

    code.add()
    code.add(f"@flow({', '.join(args)})")
    code.add(f"def {func}({_signature(template, gen)}):")
    code.add(f'"""Argo {template.kind.value} template \'{template.name}\'."""', 1)

    ind = _emit_sync_guard(code, 1, template.synchronization, gen)
    sync_calls = _composite_sync_calls(wf, template, func_of, gen)

    if template.kind == TemplateKind.DAG:
        ordered = _topo_sort(template.dag_tasks)
        for call in ordered:
            prev = [
                f"{sanitize_identifier(d)}_fut" for d in call.dependencies if d not in sync_calls
            ]
            _emit_call(code, ind, wf, call, scope, func_of, gen, prev, sync_calls)
    else:  # STEPS
        prev_group: list[str] = []
        for group in template.step_groups:
            current: list[str] = []
            for call in group:
                waits = [w for w in prev_group]
                _emit_call(code, ind, wf, call, scope, func_of, gen, waits, sync_calls)
                if call.name not in sync_calls:
                    current.append(f"{sanitize_identifier(call.name)}_fut")
            prev_group = current

    code.add("return None", ind)


def _composite_sync_calls(
    wf: Workflow, template: Template, func_of: dict[str, str], gen: _GenState
) -> set[str]:
    """Names of calls whose target is itself composite (run inline, not submitted)."""
    sync: set[str] = set()
    calls = (
        template.dag_tasks
        if template.kind == TemplateKind.DAG
        else [c for group in template.step_groups for c in group]
    )
    for call in calls:
        resolved = gen.resolve_call(wf, call, func_of)
        if resolved is not None and resolved[0].is_composite:
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
    resolved = gen.resolve_call(wf, call, func_of)

    ref_note = f" (templateRef '{call.template_ref}')" if call.template_ref else ""
    code.add(f"# {call.name} -> template '{call.template}'{ref_note}", ind)
    if resolved is None:
        where = (
            f"in WorkflowTemplate '{call.template_ref}' (not in this conversion's input)"
            if call.template_ref
            else "in this manifest"
        )
        gen.warnings.append(
            f"Call '{call.name}' references template '{call.template}' {where}; "
            "emitted as a stub. Include the referenced manifest in the conversion "
            "input to resolve it."
        )
        code.add("# TODO(A2P-108): unresolved templateRef; include the referenced manifest.", ind)
        code.add(
            f"raise NotImplementedError({json.dumps(f'Unresolved templateRef: {call.template}')})",
            ind,
        )
        code.add(f"{fut_var} = None", ind)
        return

    target, target_func = resolved
    is_sync = call.name in sync_calls
    wait_clause = _wait_clause(wait_names)

    if call.depends and not depends_is_plain(call.depends):
        gen.warnings.append(
            f"Call '{call.name}' uses `depends: {call.depends}`; dependency edges are "
            "preserved, but status-based gating (Failed/Errored/||) needs manual review."
        )
        code.add(
            f"# TODO(A2P-102): Argo gated this on `depends: {call.depends}`.",
            ind,
        )
        code.add(
            "#   wait_for waits for upstream completion; add success/failure checks to match.",
            ind,
        )

    body_ind = ind
    if call.when:
        cond = translate_condition(call.when, scope)
        code.add(f"{fut_var} = None  # conditional", ind)
        if cond == "False":
            code.add(f"if False:  # TODO(A2P-110): untranslatable Argo condition: {call.when}", ind)
        else:
            code.add(f"if {cond}:  # TODO(A2P-101): review translated condition", ind)
        body_ind = ind + 1

    looped = call.with_items is not None or bool(call.with_param) or call.with_sequence is not None
    if looped and not is_sync:
        _emit_mapped_call(
            code, body_ind, call, target, target_func, scope, gen, wait_clause, fut_var
        )
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
    elif call.with_sequence is not None:
        gen.helpers.add("sequence")
        seq = call.with_sequence
        seq_args = [
            f"{field_name}={translate_value(value, scope)}"
            for field_name, value in (
                ("count", seq.count),
                ("start", seq.start),
                ("end", seq.end),
            )
            if value is not None
        ]
        if seq.format:
            seq_args.append(f"fmt={json.dumps(seq.format)}")
        code.add(f"_items = _argo_sequence({', '.join(seq_args)})", ind)
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
    resolved_entry = _resolve_entrypoint(wf, func_of, gen)
    main_name = unique(
        sanitize_identifier(wf.display_name, prefix="main_flow") + "_flow", gen.func_names
    )

    # Typed signatures at the API boundary (what a client sees in the Prefect
    # UI); values are normalized back to strings for the parameter dict, since
    # Argo semantics are string-based throughout.
    params = []
    typed_idents: set[str] = set()
    for param in wf.arguments:
        ident = sanitize_identifier(param.name)
        raw_default = param.value if param.value is not None else (param.default or "")
        annotation, literal = _infer_param_type(raw_default)
        if annotation != "str":
            typed_idents.add(ident)
        params.append(f"{ident}: {annotation} = {literal}")

    hook_name = _emit_exit_hook(code, wf, func_of, gen, main_name)

    flow_args = [f'name="{wf.display_name}"']
    if wf.timeout_seconds:
        flow_args.append(f"timeout_seconds={wf.timeout_seconds}")
    if hook_name:
        # Argo onExit runs on success AND failure; attach to both hooks.
        flow_args.append(f"on_completion=[{hook_name}]")
        flow_args.append(f"on_failure=[{hook_name}]")

    code.add()
    code.add(f"@flow({', '.join(flow_args)})")
    code.add(f"def {main_name}({', '.join(params)}):")
    code.add(f'"""Entry point for Argo {wf.kind} \'{wf.display_name}\'."""', 1)

    if wf.arguments:
        updates = ", ".join(
            f"{json.dumps(p.name)}: {_as_str(sanitize_identifier(p.name), typed_idents)}"
            for p in wf.arguments
        )
        code.add(f"WORKFLOW_PARAMETERS.update({{{updates}}})", 1)

    if resolved_entry is None:
        code.add("# TODO(A2P-109): no entrypoint template found; nothing to run.", 1)
        code.add("return None", 1)
        return main_name

    ind = _emit_sync_guard(code, 1, wf.synchronization, gen)
    entry, entry_func = resolved_entry
    kwargs = _entrypoint_kwargs(wf, entry, typed_idents)
    code.add(f"return {entry_func}({kwargs})", ind)
    return main_name


_INT_RE = re.compile(r"-?\d+")
_FLOAT_RE = re.compile(r"-?\d+\.\d+")


def _infer_param_type(default: str) -> tuple[str, str]:
    """Infer a (annotation, default-literal) pair from an Argo string default.

    Only numeric types are inferred: Argo booleans stay strings because every
    comparison in translated conditions is string-based (`x == "true"`).
    """
    if _INT_RE.fullmatch(default):
        return "int", default
    if _FLOAT_RE.fullmatch(default):
        return "float", default
    return "str", json.dumps(default)


def _as_str(ident: str, typed_idents: set[str]) -> str:
    return f"str({ident})" if ident in typed_idents else ident


def _emit_exit_hook(
    code: _Code, wf: Workflow, func_of: dict[str, str], gen: _GenState, main_name: str
) -> str | None:
    """Emit a state hook for the workflow's ``onExit`` handler, if any.

    Returns the hook function's name, to be attached to the main flow's
    ``on_completion`` and ``on_failure`` (Argo exit handlers run regardless
    of outcome).
    """
    if not wf.on_exit:
        return None
    exit_template = wf.template_by_name(wf.on_exit)
    if exit_template is None:
        gen.warnings.append(
            f"[{wf.display_name}] onExit handler '{wf.on_exit}' not found in this "
            "manifest; wire a Prefect on_completion/on_failure hook manually."
        )
        return None
    exit_func = func_of[exit_template.name]
    hook_name = unique(f"_{main_name}_on_exit", gen.func_names)
    code.add()
    code.add(f"def {hook_name}(flow, flow_run, state):")
    code.add(f'"""Argo onExit handler \'{wf.on_exit}\' (runs on success and failure)."""', 1)
    if any(p.default is None for p in exit_template.inputs):
        code.add(
            "# TODO(A2P-106): the exit template has required inputs; Argo passed",
            1,
        )
        code.add("#   `{{workflow.*}}` context here — supply equivalents from `state`.", 1)
    code.add(f"{exit_func}()", 1)
    return hook_name


def _resolve_entrypoint(
    wf: Workflow, func_of: dict[str, str], gen: _GenState
) -> tuple[Template, str] | None:
    """Find the template + function the main flow should invoke.

    A spec-level ``workflowTemplateRef`` resolves into the project's shared
    module (the referenced library's entrypoint, or the workflow's own
    ``entrypoint`` override looked up inside that library).
    """
    ref = wf.workflow_template_ref
    if ref:
        if gen.shared is not None:
            entry_name = wf.entrypoint or (
                gen.shared.manifests[ref].entrypoint if ref in gen.shared.manifests else None
            )
            if entry_name:
                hit = gen.shared.lookup(ref, entry_name)
                if hit is not None:
                    gen.shared_imports.add(hit[1])
                    return hit[0], hit[1]
        gen.warnings.append(
            f"[{wf.display_name}] workflowTemplateRef '{ref}' could not be resolved; "
            "include the referenced manifest in the conversion input."
        )
    if wf.entrypoint:
        target = wf.template_by_name(wf.entrypoint)
        if target is not None:
            return target, func_of[wf.entrypoint]
        if not ref:
            gen.warnings.append(
                f"[{wf.display_name}] entrypoint '{wf.entrypoint}' not found; using a fallback."
            )
    for template in wf.templates:
        if template.is_composite:
            return template, func_of[template.name]
    if wf.templates:
        first = wf.templates[0]
        return first, func_of[first.name]
    return None


def _entrypoint_kwargs(wf: Workflow, entry: Template, typed_idents: set[str]) -> str:
    wf_arg_names = {p.name for p in wf.arguments}
    parts: list[str] = []
    for param in entry.inputs:
        ident = sanitize_identifier(param.name)
        if param.name in wf_arg_names:
            # Template inputs are str-typed; normalize inferred numerics.
            parts.append(f"{ident}={_as_str(ident, typed_idents)}")
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
        # Modules importing the shared module inherit its runtime needs too.
        if "shell" in gen.imports or gen.shared_imports:
            deps.append('"prefect-shell>=0.3"')
        if "docker" in gen.imports or (gen.shared_imports and gen.options.runtime == "docker"):
            deps.append('"docker>=7"')
        if "k8s_job" in gen.helpers or (gen.shared_imports and gen.options.runtime == "kubernetes"):
            deps.append('"kubernetes>=29"')
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
        name for name in gen.imports if name in {"json", "os", "shlex", "tempfile", "time", "uuid"}
    )
    for name in stdlib:
        code.add(f"import {name}")
    if "urllib" in gen.imports:
        code.add("import urllib.request")

    if "timedelta" in gen.imports:
        code.add("from datetime import timedelta")
    if "docker" in gen.imports:
        code.add()
        code.add("import docker")
        code.add()
    prefect_imports = ["flow", "task"]
    if "unmapped" in gen.imports:
        prefect_imports.append("unmapped")
    code.add(f"from prefect import {', '.join(prefect_imports)}")
    if "INPUTS" in gen.imports:
        code.add("from prefect.cache_policies import INPUTS")
    if "concurrency" in gen.imports:
        code.add("from prefect.concurrency.sync import concurrency")
    if "exponential_backoff" in gen.imports:
        code.add("from prefect.tasks import exponential_backoff")
    if gen.used_runtime:
        code.add(f"from prefect.runtime import {', '.join(sorted(gen.used_runtime))}")
    if "shell" in gen.imports:
        code.add("from prefect_shell import ShellOperation")

    code.add()
    if gen.shared_imports and gen.shared is not None:
        # One WORKFLOW_PARAMETERS dict per project: this module and the shared
        # templates it calls must read/write the same object, so import it.
        names = ", ".join(["WORKFLOW_PARAMETERS", *sorted(gen.shared_imports)])
        code.add(f"from {gen.shared.module_name} import {names}")
        if wp_defaults:
            code.add()
            code.add(f"WORKFLOW_PARAMETERS.update({_py_literal(wp_defaults)})")
    else:
        code.add(f"WORKFLOW_PARAMETERS = {_py_literal(wp_defaults)}")

    if "render" in gen.helpers:
        code.add()
        code.add(_RENDER_HELPER.rstrip())
    if "as_list" in gen.helpers:
        code.add()
        code.add(_AS_LIST_HELPER.rstrip())
    if "sequence" in gen.helpers:
        code.add()
        code.add(_SEQUENCE_HELPER.rstrip())
    if "k8s_job" in gen.helpers:
        code.add()
        code.add(_K8S_JOB_HELPER.rstrip())
    if "output_param" in gen.helpers:
        code.add()
        code.add(_OUTPUT_PARAM_HELPER.rstrip())

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
        if wf.suspended:
            serve_args.append("paused=True")
        code.add("import sys", 1)
        code.add()
        code.add('if "--serve" in sys.argv:', 1)
        if wf.suspended:
            code.add("# paused=True because the source CronWorkflow was suspended.", 2)
        if len(wf.schedules) > 1:
            extra = ", ".join(wf.schedules[1:])
            code.add(f"# NOTE: the CronWorkflow had additional schedules ({extra});", 2)
            code.add("#   serve() takes one cron — use prefect.yaml for the full set.", 2)
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


def _references_item(value: str | None) -> bool:
    return bool(value and _ITEM_REF.search(value))


def _referenced_workflow_params(wf: Workflow) -> set[str]:
    blob = json.dumps(wf.raw)
    return set(_WF_PARAM_REF.findall(blob))


def _duration_to_seconds(duration: str | None) -> int | None:
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

_SEQUENCE_HELPER = '''
def _argo_sequence(count=None, start=None, end=None, fmt=None):
    """Expand an Argo `withSequence` into its list of string items."""
    if count is not None:
        _values = range(int(count))
    else:
        _s, _e = int(start or 0), int(end or 0)
        _values = range(_s, _e + 1) if _e >= _s else range(_s, _e - 1, -1)
    return [(fmt % _v) if fmt else str(_v) for _v in _values]
'''

_K8S_JOB_HELPER = '''
import os as _os
import time as _time
import uuid as _uuid


def _run_k8s_job(_image, _command, _env, _name_prefix, _timeout=3600):
    """Run a Kubernetes Job and return its pod logs.

    Uses in-cluster config when available, falling back to the local
    kubeconfig. The Job is always deleted afterwards (no leaked Jobs).
    TODO(A2P-111): namespace comes from $A2P_NAMESPACE (default "default").
    """
    from kubernetes import client as _k8s, config as _k8s_config

    try:
        _k8s_config.load_incluster_config()
    except Exception:
        _k8s_config.load_kube_config()
    _ns = _os.environ.get("A2P_NAMESPACE", "default")
    _name = f"{_name_prefix}-{_uuid.uuid4().hex[:8]}"
    _container = {
        "name": "main",
        "image": _image,
        "env": [{"name": _k, "value": str(_v)} for _k, _v in (_env or {}).items()],
    }
    if _command:
        _container["command"] = list(_command)
    _batch = _k8s.BatchV1Api()
    _core = _k8s.CoreV1Api()
    _batch.create_namespaced_job(
        _ns,
        {
            "apiVersion": "batch/v1",
            "kind": "Job",
            "metadata": {"name": _name},
            "spec": {
                "backoffLimit": 0,
                "template": {"spec": {"restartPolicy": "Never", "containers": [_container]}},
            },
        },
    )
    try:
        _deadline = _time.time() + _timeout
        while True:
            _status = _batch.read_namespaced_job(_name, _ns).status
            if _status.succeeded:
                break
            if _status.failed:
                raise RuntimeError(f"Kubernetes job {_name} failed")
            if _time.time() > _deadline:
                raise TimeoutError(f"Kubernetes job {_name} did not finish in {_timeout}s")
            _time.sleep(2)
        _pods = _core.list_namespaced_pod(_ns, label_selector=f"job-name={_name}")
        return _core.read_namespaced_pod_log(_pods.items[0].metadata.name, namespace=_ns)
    finally:
        _batch.delete_namespaced_job(_name, _ns, propagation_policy="Background")
'''

_OUTPUT_PARAM_HELPER = '''
def _argo_output_param(_stdout, _name):
    """Argo read this output parameter from a file (`valueFrom`); only stdout
    (`outputs.result`) is captured automatically. TODO(A2P-105)."""
    raise NotImplementedError(
        f"Output parameter {_name!r} came from a file in Argo (valueFrom.path/jsonPath). "
        "Map it manually - only stdout (outputs.result) is migrated automatically."
    )
'''
