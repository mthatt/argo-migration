"""Typed intermediate representation (IR) for Argo Workflows.

The parser converts raw Argo YAML into these dataclasses, and the generator
consumes them. Keeping a clean IR in the middle means the two ends can evolve
independently and makes the conversion logic easy to unit test.

Only the subset of the (very large) Argo schema that is meaningful for a
Prefect migration is modelled. Anything unrecognised is preserved on
``Template.raw`` / ``Workflow.raw`` so the generator can emit a faithful
``# TODO`` instead of silently dropping it.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Optional


class TemplateKind(str, Enum):
    """The flavour of an Argo template.

    Argo templates are polymorphic: exactly one of these "template types" is
    set on any given template. We collapse them into an enum so the generator
    can switch on a single field.
    """

    CONTAINER = "container"
    SCRIPT = "script"
    DAG = "dag"
    STEPS = "steps"
    RESOURCE = "resource"
    HTTP = "http"
    SUSPEND = "suspend"
    CONTAINER_SET = "containerSet"
    DATA = "data"
    UNKNOWN = "unknown"


@dataclass
class Parameter:
    """An Argo parameter, used for inputs, outputs and call arguments.

    * ``value`` holds a literal or an ``{{...}}`` expression (call arguments and
      workflow-level parameter values).
    * ``default`` holds an input parameter's default value.
    """

    name: str
    value: Optional[str] = None
    default: Optional[str] = None


@dataclass
class Artifact:
    """An Argo artifact. We do not migrate artifact storage, but we track them
    so the generator can warn about manual follow-up."""

    name: str
    path: Optional[str] = None
    from_expression: Optional[str] = None


@dataclass
class ContainerSpec:
    """A container template (``template.container``)."""

    image: str = ""
    command: list[str] = field(default_factory=list)
    args: list[str] = field(default_factory=list)
    env: dict[str, str] = field(default_factory=dict)
    working_dir: Optional[str] = None


@dataclass
class ScriptSpec:
    """A script template (``template.script``).

    A script is a container plus an inline ``source`` body. ``interpreter`` is
    derived from ``command`` (e.g. ``["python"]`` -> ``"python"``) so the
    generator can decide whether to inline Python or shell out.
    """

    image: str = ""
    command: list[str] = field(default_factory=list)
    env: dict[str, str] = field(default_factory=dict)
    source: str = ""
    interpreter: str = "sh"


@dataclass
class ResourceSpec:
    """A ``resource`` template that manipulates Kubernetes objects."""

    action: str = "apply"
    manifest: str = ""
    flags: list[str] = field(default_factory=list)


@dataclass
class HTTPSpec:
    """An ``http`` template that performs an HTTP request."""

    method: str = "GET"
    url: str = ""
    headers: dict[str, str] = field(default_factory=dict)
    body: Optional[str] = None
    success_condition: Optional[str] = None


@dataclass
class SuspendSpec:
    """A ``suspend`` template that pauses the workflow."""

    duration: Optional[str] = None


@dataclass
class Call:
    """A reference to another template from within a ``dag`` or ``steps``.

    Models both DAG tasks and step entries since they share a shape. The
    distinction (parallel grouping vs. explicit ``dependencies``) is captured by
    the containing template, not here.
    """

    name: str
    template: str
    arguments: list[Parameter] = field(default_factory=list)
    dependencies: list[str] = field(default_factory=list)
    when: Optional[str] = None
    with_items: Optional[list[Any]] = None
    with_param: Optional[str] = None
    # A step/task may invoke another workflow template instead of an inline one.
    template_ref: Optional[str] = None


@dataclass
class Template:
    """A single Argo template in its IR form."""

    name: str
    kind: TemplateKind = TemplateKind.UNKNOWN
    inputs: list[Parameter] = field(default_factory=list)
    input_artifacts: list[Artifact] = field(default_factory=list)
    outputs: list[Parameter] = field(default_factory=list)
    output_artifacts: list[Artifact] = field(default_factory=list)

    container: Optional[ContainerSpec] = None
    script: Optional[ScriptSpec] = None
    resource: Optional[ResourceSpec] = None
    http: Optional[HTTPSpec] = None
    suspend: Optional[SuspendSpec] = None

    # For kind == DAG.
    dag_tasks: list[Call] = field(default_factory=list)
    # For kind == STEPS: an ordered list of parallel groups.
    step_groups: list[list[Call]] = field(default_factory=list)

    # Retry / resource hints carried through for the generator.
    retry_limit: Optional[int] = None

    raw: dict[str, Any] = field(default_factory=dict)

    @property
    def is_composite(self) -> bool:
        """True if this template orchestrates other templates (DAG or steps)."""
        return self.kind in (TemplateKind.DAG, TemplateKind.STEPS)


@dataclass
class Workflow:
    """A whole Argo manifest (Workflow / WorkflowTemplate / CronWorkflow / ...)."""

    kind: str = "Workflow"
    name: str = "workflow"
    generate_name: Optional[str] = None
    namespace: Optional[str] = None
    entrypoint: Optional[str] = None
    arguments: list[Parameter] = field(default_factory=list)
    templates: list[Template] = field(default_factory=list)

    # Populated for CronWorkflow.
    schedule: Optional[str] = None
    timezone: Optional[str] = None

    labels: dict[str, str] = field(default_factory=dict)
    warnings: list[str] = field(default_factory=list)
    raw: dict[str, Any] = field(default_factory=dict)

    def template_by_name(self, name: str) -> Optional[Template]:
        for template in self.templates:
            if template.name == name:
                return template
        return None

    @property
    def display_name(self) -> str:
        """A stable, human-meaningful name even when only ``generateName`` is set."""
        return self.name or (self.generate_name or "workflow").rstrip("-")
