"""Parse Argo Workflows YAML manifests into the :mod:`argo2prefect.models` IR.

Supports the manifest kinds that carry a workflow spec:

* ``Workflow``
* ``WorkflowTemplate`` / ``ClusterWorkflowTemplate``
* ``CronWorkflow`` (schedule + ``workflowSpec``)

The parser is intentionally lenient: unknown template types are still captured
(as :class:`~argo2prefect.models.TemplateKind.UNKNOWN` with their raw body) so
the generator can flag them rather than crash.
"""

from __future__ import annotations

import re
from typing import Any

import yaml

from .models import (
    Artifact,
    Call,
    ContainerSpec,
    HTTPSpec,
    Memoization,
    Parameter,
    ResourceSpec,
    RetryPolicy,
    ScriptSpec,
    SequenceSpec,
    SuspendSpec,
    Synchronization,
    Template,
    TemplateKind,
    Workflow,
)

WORKFLOW_KINDS = {
    "Workflow",
    "WorkflowTemplate",
    "ClusterWorkflowTemplate",
    "CronWorkflow",
}


class ParseError(ValueError):
    """Raised when the input cannot be interpreted as an Argo manifest."""


def parse_workflows(yaml_text: str) -> list[Workflow]:
    """Parse one or more Argo manifests from a (possibly multi-document) string.

    Documents whose ``kind`` is not workflow-related are skipped. A
    :class:`ParseError` is raised only if *no* usable workflow document is found.
    """
    try:
        documents = list(yaml.safe_load_all(yaml_text))
    except yaml.YAMLError as exc:  # pragma: no cover - passthrough of yaml msg
        raise ParseError(f"Invalid YAML: {exc}") from exc

    workflows: list[Workflow] = []
    skipped: list[str] = []
    for doc in documents:
        if not isinstance(doc, dict):
            continue
        kind = doc.get("kind")
        if kind in WORKFLOW_KINDS:
            workflows.append(parse_workflow_dict(doc))
        elif "spec" in doc and _spec_for_kind(kind, doc.get("spec", {})).get("templates"):
            # Kind missing/unknown but it quacks like a workflow.
            workflows.append(parse_workflow_dict(doc))
        elif kind:
            skipped.append(str(kind))

    if not workflows:
        detail = f" (skipped kinds: {', '.join(skipped)})" if skipped else ""
        raise ParseError(
            f"No Argo Workflow manifest found. Expected one of {sorted(WORKFLOW_KINDS)}{detail}."
        )
    return workflows


def parse_workflow_dict(doc: dict[str, Any]) -> Workflow:
    """Parse a single already-loaded YAML document into a :class:`Workflow`."""
    kind = str(doc.get("kind", "Workflow"))
    metadata = doc.get("metadata") or {}
    spec = doc.get("spec") or {}
    warnings: list[str] = []

    schedule: str | None = None
    schedules: list[str] = []
    timezone: str | None = None
    suspended = False
    if kind == "CronWorkflow":
        if spec.get("schedule"):
            schedules.append(str(spec["schedule"]))
        for extra in spec.get("schedules") or []:
            if str(extra) not in schedules:
                schedules.append(str(extra))
        schedule = schedules[0] if schedules else None
        timezone = spec.get("timezone")
        suspended = bool(spec.get("suspend"))
        if spec.get("concurrencyPolicy") in ("Forbid", "Replace"):
            warnings.append(
                f"CronWorkflow concurrencyPolicy '{spec['concurrencyPolicy']}' is not "
                "migrated; add a Prefect concurrency limit on the deployment to match."
            )
        if spec.get("startingDeadlineSeconds") is not None:
            warnings.append(
                "CronWorkflow startingDeadlineSeconds is not migrated; Prefect handles "
                "late runs via its own scheduling semantics."
            )

    work_spec = _spec_for_kind(kind, spec)

    wt_ref = work_spec.get("workflowTemplateRef")
    wt_ref_name: str | None = None
    wt_ref_cluster = False
    if isinstance(wt_ref, dict) and wt_ref.get("name"):
        wt_ref_name = str(wt_ref["name"])
        wt_ref_cluster = bool(wt_ref.get("clusterScope"))

    hoisted: list[Template] = []
    templates = [_parse_template(t, warnings, hoisted) for t in (work_spec.get("templates") or [])]
    templates.extend(hoisted)

    workflow = Workflow(
        kind=kind,
        name=str(metadata.get("name") or "").strip(),
        generate_name=metadata.get("generateName"),
        namespace=metadata.get("namespace"),
        entrypoint=work_spec.get("entrypoint"),
        arguments=_parse_parameters((work_spec.get("arguments") or {}).get("parameters")),
        templates=templates,
        schedule=schedule,
        schedules=schedules,
        timezone=timezone,
        suspended=suspended,
        timeout_seconds=_maybe_int(work_spec.get("activeDeadlineSeconds")),
        on_exit=work_spec.get("onExit"),
        synchronization=_parse_synchronization(work_spec.get("synchronization"), warnings),
        workflow_template_ref=wt_ref_name,
        cluster_workflow_template_ref=wt_ref_cluster,
        labels={str(k): str(v) for k, v in (metadata.get("labels") or {}).items()},
        warnings=warnings,
        raw=doc,
    )

    if not workflow.name and not workflow.generate_name:
        workflow.name = "workflow"

    if (work_spec.get("arguments") or {}).get("artifacts"):
        warnings.append(
            "Workflow-level artifacts are not migrated automatically; wire up "
            "Prefect storage/results manually."
        )
    if work_spec.get("volumes") or work_spec.get("volumeClaimTemplates"):
        warnings.append(
            "Volumes / volumeClaimTemplates are not migrated; configure storage "
            "on your Prefect work pool or infrastructure block."
        )
    return workflow


def _spec_for_kind(kind: str | None, spec: dict[str, Any]) -> dict[str, Any]:
    """Return the spec that actually holds ``templates``/``entrypoint``.

    For ``CronWorkflow`` this is nested under ``workflowSpec``.
    """
    if kind == "CronWorkflow":
        return spec.get("workflowSpec") or {}
    return spec


def _first_schedule(schedules: Any) -> str | None:
    if isinstance(schedules, list) and schedules:
        return str(schedules[0])
    return None


def _parse_template(
    raw: dict[str, Any], warnings: list[str], hoisted: list[Template] | None = None
) -> Template:
    """Parse one template. ``hoisted`` collects templates synthesized from
    ``inline:`` task/step definitions so the workflow can adopt them."""
    if hoisted is None:
        hoisted = []
    name = str(raw.get("name", "template"))
    template = Template(
        name=name,
        inputs=_parse_parameters((raw.get("inputs") or {}).get("parameters")),
        input_artifacts=_parse_artifacts((raw.get("inputs") or {}).get("artifacts")),
        outputs=_parse_parameters((raw.get("outputs") or {}).get("parameters")),
        output_artifacts=_parse_artifacts((raw.get("outputs") or {}).get("artifacts")),
        raw=raw,
    )

    retry = raw.get("retryStrategy")
    if isinstance(retry, dict):
        backoff = retry.get("backoff") or {}
        template.retry = RetryPolicy(
            limit=_maybe_int(retry.get("limit")),
            policy=retry.get("retryPolicy"),
            backoff_duration=backoff.get("duration"),
            backoff_factor=_maybe_int(backoff.get("factor")),
            backoff_max=backoff.get("maxDuration"),
        )

    template.timeout_seconds = _maybe_int(raw.get("activeDeadlineSeconds"))
    template.synchronization = _parse_synchronization(raw.get("synchronization"), warnings)

    memoize = raw.get("memoize")
    if isinstance(memoize, dict):
        template.memoize = Memoization(
            key=str(memoize.get("key", "")),
            max_age=memoize.get("maxAge"),
        )

    if "container" in raw:
        template.kind = TemplateKind.CONTAINER
        template.container = _parse_container(raw["container"])
    elif "script" in raw:
        template.kind = TemplateKind.SCRIPT
        template.script = _parse_script(raw["script"])
    elif "dag" in raw:
        template.kind = TemplateKind.DAG
        template.dag_tasks = [
            _parse_call(t, warnings, hoisted) for t in (raw["dag"].get("tasks") or [])
        ]
    elif "steps" in raw:
        template.kind = TemplateKind.STEPS
        template.step_groups = _parse_steps(raw["steps"], warnings, hoisted)
    elif "resource" in raw:
        template.kind = TemplateKind.RESOURCE
        template.resource = _parse_resource(raw["resource"])
    elif "http" in raw:
        template.kind = TemplateKind.HTTP
        template.http = _parse_http(raw["http"])
    elif "suspend" in raw:
        template.kind = TemplateKind.SUSPEND
        template.suspend = SuspendSpec(duration=(raw["suspend"] or {}).get("duration"))
    elif "containerSet" in raw:
        template.kind = TemplateKind.CONTAINER_SET
        warnings.append(
            f"Template '{name}' uses containerSet; emitted as a single shell task. "
            "Review the multi-container ordering."
        )
    elif "data" in raw:
        template.kind = TemplateKind.DATA
        warnings.append(
            f"Template '{name}' is a data template; artifact sourcing must be ported manually."
        )
    else:
        template.kind = TemplateKind.UNKNOWN
        warnings.append(f"Template '{name}' has an unrecognised type; emitted as a stub.")

    return template


def _parse_container(raw: dict[str, Any]) -> ContainerSpec:
    return ContainerSpec(
        image=str(raw.get("image", "")),
        command=_str_list(raw.get("command")),
        args=_str_list(raw.get("args")),
        env=_parse_env(raw.get("env")),
        working_dir=raw.get("workingDir"),
    )


def _parse_script(raw: dict[str, Any]) -> ScriptSpec:
    command = _str_list(raw.get("command"))
    return ScriptSpec(
        image=str(raw.get("image", "")),
        command=command,
        env=_parse_env(raw.get("env")),
        source=str(raw.get("source", "")),
        interpreter=_detect_interpreter(command, raw.get("image", "")),
    )


def _parse_resource(raw: dict[str, Any]) -> ResourceSpec:
    return ResourceSpec(
        action=str(raw.get("action", "apply")),
        manifest=str(raw.get("manifest", "")),
        flags=_str_list(raw.get("flags")),
    )


def _parse_http(raw: dict[str, Any]) -> HTTPSpec:
    headers = {}
    for header in raw.get("headers") or []:
        if isinstance(header, dict) and "name" in header:
            headers[str(header["name"])] = str(header.get("value", ""))
    return HTTPSpec(
        method=str(raw.get("method", "GET")).upper(),
        url=str(raw.get("url", "")),
        headers=headers,
        body=raw.get("body"),
        success_condition=raw.get("successCondition"),
    )


def _parse_steps(raw_steps: Any, warnings: list[str], hoisted: list[Template]) -> list[list[Call]]:
    """Argo ``steps`` is a list of groups; each group runs in parallel.

    The YAML shape is ``list[ list[stepDict] | stepDict ]`` so we normalise each
    group to a list.
    """
    groups: list[list[Call]] = []
    for group in raw_steps or []:
        entries = group if isinstance(group, list) else [group]
        groups.append([_parse_call(step, warnings, hoisted) for step in entries])
    return groups


#: Status qualifiers allowed after a task name in a ``depends`` expression.
_DEPENDS_STATUSES = {
    "Succeeded",
    "Failed",
    "Errored",
    "Skipped",
    "Omitted",
    "Daemoned",
    "AnySucceeded",
    "AllFailed",
}

_DEPENDS_TOKEN = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.\-]*")


def depends_task_names(depends: str) -> list[str]:
    """Extract the upstream task names referenced by a ``depends`` expression.

    ``depends: "A && (B.Succeeded || C.Failed)"`` -> ``["A", "B", "C"]``.
    Order of first appearance is preserved.
    """
    names: list[str] = []
    for token in _DEPENDS_TOKEN.findall(depends):
        name, _, qualifier = token.partition(".")
        if qualifier and qualifier not in _DEPENDS_STATUSES:
            # Dotted names that are not status qualifiers are kept whole.
            name = token
        if name not in names:
            names.append(name)
    return names


def depends_is_plain(depends: str) -> bool:
    """True when ``depends`` means the same as ``dependencies``: all upstream
    tasks succeeded (only ``&&``, no status qualifiers, no ``||``/``!``)."""
    if "||" in depends or "!" in depends:
        return False
    stripped = re.sub(r"[()&\s]", " ", depends)
    return all(
        "." not in token or token.rpartition(".")[2] not in _DEPENDS_STATUSES
        for token in stripped.split()
    )


def _parse_call(
    raw: dict[str, Any],
    warnings: list[str] | None = None,
    hoisted: list[Template] | None = None,
) -> Call:
    if warnings is None:
        warnings = []
    if hoisted is None:
        hoisted = []
    call_name = str(raw.get("name", "step"))

    template_ref = None
    ref = raw.get("templateRef")
    if isinstance(ref, dict):
        template_ref = ref.get("template") or ref.get("name")

    # An `inline:` task/step defines its template in place. Hoist it into the
    # workflow under a synthesized name so the normal call path applies.
    template_name = raw.get("template") or template_ref
    if not template_name and isinstance(raw.get("inline"), dict):
        inline_raw = dict(raw["inline"])
        inline_raw.setdefault("name", f"{call_name}-inline")
        inline = _parse_template(inline_raw, warnings, hoisted)
        inline.name = str(inline_raw["name"])
        hoisted.append(inline)
        template_name = inline.name

    with_items = raw.get("withItems")
    if with_items is not None and not isinstance(with_items, list):
        with_items = [with_items]

    with_sequence = None
    seq = raw.get("withSequence")
    if isinstance(seq, dict):
        with_sequence = SequenceSpec(
            count=None if seq.get("count") is None else str(seq["count"]),
            start=None if seq.get("start") is None else str(seq["start"]),
            end=None if seq.get("end") is None else str(seq["end"]),
            format=seq.get("format"),
        )

    dependencies = _str_list(raw.get("dependencies"))
    depends = raw.get("depends")
    if depends is not None:
        depends = str(depends)
        for name in depends_task_names(depends):
            if name not in dependencies:
                dependencies.append(name)

    on_exit = raw.get("onExit")
    if on_exit:
        warnings.append(
            f"Step/task '{call_name}' has an onExit hook ('{on_exit}'); Prefect task-level "
            "hooks (on_failure/on_completion) must be wired manually."
        )

    return Call(
        name=call_name,
        template=str(template_name or ""),
        arguments=_parse_parameters((raw.get("arguments") or {}).get("parameters")),
        dependencies=dependencies,
        when=raw.get("when"),
        with_items=with_items,
        with_param=raw.get("withParam"),
        with_sequence=with_sequence,
        template_ref=ref.get("name") if isinstance(ref, dict) else None,
        depends=depends,
        on_exit=str(on_exit) if on_exit else None,
    )


def _parse_parameters(raw: Any) -> list[Parameter]:
    params: list[Parameter] = []
    for item in raw or []:
        if not isinstance(item, dict) or "name" not in item:
            continue
        value = item.get("value")
        if value is None and "valueFrom" in item:
            # Outputs frequently use valueFrom (path/jsonPath); keep a marker.
            value = None
        params.append(
            Parameter(
                name=str(item["name"]),
                value=None if value is None else _scalar_to_str(value),
                default=None
                if item.get("default") is None
                else _scalar_to_str(item.get("default")),
            )
        )
    return params


#: Artifact storage keys we can identify, with how to describe the location.
_ARTIFACT_STORES = ("s3", "gcs", "oss", "azure", "http", "git", "artifactory", "hdfs", "raw")


def _parse_artifacts(raw: Any) -> list[Artifact]:
    artifacts: list[Artifact] = []
    for item in raw or []:
        if not isinstance(item, dict) or "name" not in item:
            continue
        storage: str | None = None
        location: str | None = None
        for store in _ARTIFACT_STORES:
            spec = item.get(store)
            if isinstance(spec, dict):
                storage = store
                location = _artifact_location(store, spec)
                break
        artifacts.append(
            Artifact(
                name=str(item["name"]),
                path=item.get("path"),
                from_expression=item.get("from"),
                storage=storage,
                location=location,
            )
        )
    return artifacts


def _artifact_location(store: str, spec: dict[str, Any]) -> str | None:
    if store in ("s3", "gcs", "oss"):
        bucket, key = spec.get("bucket"), spec.get("key")
        return f"{bucket}/{key}" if bucket and key else (key or bucket)
    if store == "http":
        return spec.get("url")
    if store == "git":
        return spec.get("repo")
    if store == "azure":
        container, blob = spec.get("container"), spec.get("blob")
        return f"{container}/{blob}" if container and blob else (blob or container)
    if store == "hdfs":
        return spec.get("path")
    return None


def _parse_synchronization(raw: Any, warnings: list[str]) -> Synchronization | None:
    """Parse a ``synchronization`` block (mutex or semaphore, singular or the
    newer plural form). Only the first guard is modelled; extras are flagged."""
    if not isinstance(raw, dict):
        return None
    mutexes = raw.get("mutexes") or ([raw["mutex"]] if isinstance(raw.get("mutex"), dict) else [])
    semaphores = raw.get("semaphores") or (
        [raw["semaphore"]] if isinstance(raw.get("semaphore"), dict) else []
    )
    if len(mutexes) + len(semaphores) > 1:
        warnings.append("Multiple synchronization guards on one scope; only the first is migrated.")
    if mutexes:
        name = str(mutexes[0].get("name", "mutex"))
        return Synchronization(kind="mutex", name=name)
    if semaphores:
        ref = semaphores[0].get("configMapKeyRef") or {}
        name = f"{ref.get('name', 'semaphore')}/{ref.get('key', 'limit')}"
        return Synchronization(kind="semaphore", name=name)
    return None


def _maybe_int(value: Any) -> int | None:
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _parse_env(raw: Any) -> dict[str, str]:
    env: dict[str, str] = {}
    for item in raw or []:
        if not isinstance(item, dict) or "name" not in item:
            continue
        if "value" in item:
            env[str(item["name"])] = _scalar_to_str(item["value"])
        elif "valueFrom" in item:
            # Reference (secret/configmap/field); leave a resolvable-looking marker.
            env[str(item["name"])] = f"{{{{env.valueFrom.{item['name']}}}}}"
    return env


def _detect_interpreter(command: list[str], image: str) -> str:
    """Best-effort interpreter detection for ``script`` templates."""
    if command:
        exe = command[0].rsplit("/", 1)[-1]
        if exe in {"python", "python2", "python3"} or exe.startswith("python"):
            return "python"
        if exe in {"bash", "sh", "zsh", "dash"}:
            return exe
        if exe in {"node", "nodejs"}:
            return "node"
        return exe
    image_l = image.lower()
    if "python" in image_l:
        return "python"
    if "node" in image_l:
        return "node"
    return "sh"


def _str_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [_scalar_to_str(v) for v in value]
    return [_scalar_to_str(value)]


def _scalar_to_str(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)
