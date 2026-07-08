"""Project-level loading and cross-manifest linking.

v1 converted one manifest at a time, so any ``templateRef`` into a shared
``WorkflowTemplate`` became a stub. A :class:`Project` instead loads *all*
manifests in scope, builds a registry of ``WorkflowTemplate`` /
``ClusterWorkflowTemplate`` libraries, and resolves references across files:

* per-call ``templateRef`` -> the referenced template's generated function in
  the shared module (handled by the generator via :class:`SharedModuleInfo`).
* spec-level ``workflowTemplateRef`` -> the workflow inherits the referenced
  template's entrypoint/templates; its own ``arguments`` win on conflict
  (resolved here, at the IR level).

Namespaces are not modelled yet: the registry is keyed by manifest name only,
and a name collision between libraries is flagged rather than guessed at.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

from .models import Workflow
from .parser import ParseError, parse_workflows

#: Manifest kinds that act as shared template libraries.
LIBRARY_KINDS = {"WorkflowTemplate", "ClusterWorkflowTemplate"}

YAML_SUFFIXES = {".yaml", ".yml"}


@dataclass
class ProjectFile:
    """All workflow documents parsed from one source file (or text input)."""

    name: str  # output-naming stem, e.g. "etl-pipeline" for etl-pipeline.yaml
    workflows: list[Workflow]
    path: Path | None = None

    @property
    def runnable(self) -> list[Workflow]:
        return [wf for wf in self.workflows if wf.kind not in LIBRARY_KINDS]

    @property
    def libraries(self) -> list[Workflow]:
        return [wf for wf in self.workflows if wf.kind in LIBRARY_KINDS]


@dataclass
class Project:
    """Every manifest in a conversion run, plus the shared-template registry."""

    files: list[ProjectFile] = field(default_factory=list)
    #: WorkflowTemplate/ClusterWorkflowTemplate manifests by metadata name.
    registry: dict[str, Workflow] = field(default_factory=dict)
    #: Files that contained no workflow documents, with the parser's reason.
    skipped: list[tuple[str, str]] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    @property
    def libraries(self) -> list[Workflow]:
        seen: set[int] = set()
        out: list[Workflow] = []
        for file in self.files:
            for wf in file.libraries:
                if id(wf) not in seen:
                    seen.add(id(wf))
                    out.append(wf)
        return out

    @property
    def has_runnable(self) -> bool:
        return any(file.runnable for file in self.files)


def load_project(paths: list[Path]) -> Project:
    """Parse every YAML file (recursing into directories) into a Project."""
    project = Project()
    for path in _expand(paths):
        try:
            workflows = parse_workflows(path.read_text(encoding="utf-8"))
        except ParseError as exc:
            project.skipped.append((path.name, str(exc)))
            continue
        _add_file(project, ProjectFile(name=path.stem, workflows=workflows, path=path))
    _resolve(project)
    return project


def load_project_from_text(text: str, name: str = "workflows") -> Project:
    """Build a single-source project (stdin / API input). Raises ParseError."""
    project = Project()
    _add_file(project, ProjectFile(name=name, workflows=parse_workflows(text)))
    _resolve(project)
    return project


def _expand(paths: list[Path]) -> list[Path]:
    files: list[Path] = []
    for path in paths:
        if path.is_dir():
            files.extend(
                p
                for p in sorted(path.rglob("*"))
                if p.is_file() and p.suffix.lower() in YAML_SUFFIXES
            )
        else:
            files.append(path)
    return files


def _add_file(project: Project, file: ProjectFile) -> None:
    project.files.append(file)
    for wf in file.libraries:
        existing = project.registry.get(wf.display_name)
        if existing is not None and existing is not wf:
            project.warnings.append(
                f"Duplicate template library name '{wf.display_name}' "
                f"({existing.kind} and {wf.kind}); the latest definition wins."
            )
        project.registry[wf.display_name] = wf


def _resolve(project: Project) -> None:
    """Resolve spec-level ``workflowTemplateRef`` argument inheritance.

    The referenced library's arguments become defaults behind the workflow's
    own (Argo merge semantics: the Workflow's arguments win). Entrypoint and
    template lookup stay by-reference and are resolved by the generator via
    the shared module, so shared code is emitted exactly once.
    """
    for file in project.files:
        for wf in file.runnable:
            ref = wf.workflow_template_ref
            if not ref:
                continue
            library = project.registry.get(ref)
            if library is None:
                wf.warnings.append(
                    f"workflowTemplateRef '{ref}' not found in the project; "
                    "add its manifest to the conversion input."
                )
                continue
            own = {p.name for p in wf.arguments}
            wf.arguments.extend(p for p in library.arguments if p.name not in own)
            if not wf.entrypoint:
                wf.entrypoint = library.entrypoint
