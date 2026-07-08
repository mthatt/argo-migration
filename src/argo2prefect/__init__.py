"""argo2prefect: migrate Argo Workflows manifests into Prefect 3 flows.

The public API mirrors the three pipeline stages:

* :func:`parse_workflows` - read Argo YAML into a typed intermediate
  representation (see :mod:`argo2prefect.models`).
* :func:`generate_code` - render that representation as runnable Prefect 3
  Python source.
* :func:`convert` - convenience wrapper that runs both stages on a YAML string.
"""

from __future__ import annotations

from .assess import Assessment, assess_project
from .generator import (
    DeploymentPlan,
    GeneratorOptions,
    ProjectOutput,
    generate_code,
    generate_module,
    generate_project,
)
from .models import Workflow
from .parser import ParseError, parse_workflows
from .project import Project, load_project, load_project_from_text

__all__ = [
    "Workflow",
    "ParseError",
    "GeneratorOptions",
    "DeploymentPlan",
    "Project",
    "ProjectOutput",
    "Assessment",
    "assess_project",
    "parse_workflows",
    "generate_code",
    "generate_module",
    "generate_project",
    "load_project",
    "load_project_from_text",
    "convert",
]

__version__ = "0.1.0"


def convert(yaml_text: str, options: GeneratorOptions | None = None) -> str:
    """Parse Argo YAML and return generated Prefect Python source.

    This is a thin convenience wrapper over :func:`parse_workflows` followed by
    :func:`generate_code`. For multi-document YAML the first workflow-like
    document is used as the primary flow and the rest are emitted alongside it.
    """
    workflows = parse_workflows(yaml_text)
    return generate_code(workflows, options or GeneratorOptions())
