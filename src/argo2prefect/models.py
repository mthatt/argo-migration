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
from typing import Any


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
    value: str | None = None
    default: str | None = None


@dataclass
class Artifact:
    """An Argo artifact. We do not migrate artifact storage, but we identify
    where it lives so warnings and TODOs are specific enough to act on."""

    name: str
    path: str | None = None
    from_expression: str | None = None
    # Storage backend ("s3", "gcs", "http", "git", "raw", ...) and a
    # human-readable location like "bucket/key" or a URL, when declared.
    storage: str | None = None
    location: str | None = None


@dataclass
class SequenceSpec:
    """A ``withSequence`` loop source. Values are strings and may contain
    ``{{...}}`` expressions."""

    count: str | None = None
    start: str | None = None
    end: str | None = None
    format: str | None = None


@dataclass
class RetryPolicy:
    """A template's ``retryStrategy`` in full."""

    limit: int | None = None
    policy: str | None = None  # OnFailure (default) / OnError / Always / ...
    backoff_duration: str | None = None  # base delay, e.g. "1m"
    backoff_factor: int | None = None  # exponential multiplier
    backoff_max: str | None = None  # cap, e.g. "1h"


@dataclass
class Synchronization:
    """A mutex or semaphore guard (template- or workflow-level)."""

    kind: str  # "mutex" | "semaphore"
    name: str


@dataclass
class Memoization:
    """A template's ``memoize`` cache configuration."""

    key: str = ""
    max_age: str | None = None


@dataclass
class ContainerSpec:
    """A container template (``template.container``)."""

    image: str = ""
    command: list[str] = field(default_factory=list)
    args: list[str] = field(default_factory=list)
    env: dict[str, str] = field(default_factory=dict)
    working_dir: str | None = None


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
    body: str | None = None
    success_condition: str | None = None


@dataclass
class SuspendSpec:
    """A ``suspend`` template that pauses the workflow."""

    duration: str | None = None


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
    when: str | None = None
    with_items: list[Any] | None = None
    with_param: str | None = None
    with_sequence: SequenceSpec | None = None
    # Task/step-level exit hook template name (flagged, not yet generated).
    on_exit: str | None = None
    # A step/task may invoke another workflow template instead of an inline one.
    # ``template_ref`` is the referenced WorkflowTemplate's name; ``template``
    # then names the template *inside* it.
    template_ref: str | None = None
    # Raw ``depends`` expression (DAG tasks only). Dependency *edges* extracted
    # from it are merged into ``dependencies``; status qualifiers / OR logic
    # beyond "all upstream succeeded" are flagged for review by the generator.
    depends: str | None = None


@dataclass
class Template:
    """A single Argo template in its IR form."""

    name: str
    kind: TemplateKind = TemplateKind.UNKNOWN
    inputs: list[Parameter] = field(default_factory=list)
    input_artifacts: list[Artifact] = field(default_factory=list)
    outputs: list[Parameter] = field(default_factory=list)
    output_artifacts: list[Artifact] = field(default_factory=list)

    container: ContainerSpec | None = None
    script: ScriptSpec | None = None
    resource: ResourceSpec | None = None
    http: HTTPSpec | None = None
    suspend: SuspendSpec | None = None

    # For kind == DAG.
    dag_tasks: list[Call] = field(default_factory=list)
    # For kind == STEPS: an ordered list of parallel groups.
    step_groups: list[list[Call]] = field(default_factory=list)

    # Behavioral knobs carried through for the generator.
    retry: RetryPolicy | None = None
    timeout_seconds: int | None = None  # activeDeadlineSeconds
    synchronization: Synchronization | None = None
    memoize: Memoization | None = None

    raw: dict[str, Any] = field(default_factory=dict)

    @property
    def retry_limit(self) -> int | None:
        """Backward-compatible accessor for the plain retry count."""
        return self.retry.limit if self.retry else None

    @property
    def is_composite(self) -> bool:
        """True if this template orchestrates other templates (DAG or steps)."""
        return self.kind in (TemplateKind.DAG, TemplateKind.STEPS)


@dataclass
class Workflow:
    """A whole Argo manifest (Workflow / WorkflowTemplate / CronWorkflow / ...)."""

    kind: str = "Workflow"
    name: str = "workflow"
    generate_name: str | None = None
    namespace: str | None = None
    entrypoint: str | None = None
    arguments: list[Parameter] = field(default_factory=list)
    templates: list[Template] = field(default_factory=list)

    # Populated for CronWorkflow. ``schedule`` is the first schedule (kept for
    # convenience); ``schedules`` carries all of them.
    schedule: str | None = None
    schedules: list[str] = field(default_factory=list)
    timezone: str | None = None
    suspended: bool = False

    # Workflow-level behavior.
    timeout_seconds: int | None = None  # spec.activeDeadlineSeconds
    on_exit: str | None = None  # exit-handler template name
    synchronization: Synchronization | None = None

    # Spec-level ``workflowTemplateRef``: the whole spec (entrypoint, templates)
    # comes from the named WorkflowTemplate; this manifest only overrides
    # arguments/metadata. Resolved by :mod:`argo2prefect.project`.
    workflow_template_ref: str | None = None
    cluster_workflow_template_ref: bool = False

    labels: dict[str, str] = field(default_factory=dict)
    warnings: list[str] = field(default_factory=list)
    raw: dict[str, Any] = field(default_factory=dict)

    def template_by_name(self, name: str) -> Template | None:
        for template in self.templates:
            if template.name == name:
                return template
        return None

    @property
    def display_name(self) -> str:
        """A stable, human-meaningful name even when only ``generateName`` is set."""
        return self.name or (self.generate_name or "workflow").rstrip("-")
