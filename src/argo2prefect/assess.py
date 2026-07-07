"""Fleet assessment: grade every workflow before generating any code.

The first question in a migration engagement is not "what does the code look
like" but "how big is this, and what needs a human". :func:`assess_project`
answers it by running the full conversion pipeline in memory and grading each
manifest:

* ``automatic`` — converts with no warnings and no TODOs.
* ``review``    — converts fully, but carries TODOs/warnings a human should
                  read (translated conditions, retry-policy nuances, ...).
* ``manual``    — parts could not be converted (stubs that raise until
                  implemented).

Grades come from the *actual generator output* — the same warnings and stable
``TODO(A2P-###)`` codes that land in the generated files — so the assessment
can never drift from what conversion really does.
"""

from __future__ import annotations

import html
import json
import re
from dataclasses import asdict, dataclass, field

from .generator import (
    GeneratorOptions,
    SharedModuleInfo,
    generate_module,
    generate_shared_module,
)
from .models import Workflow
from .project import Project
from .todos import TODO_CODES

_TODO_RE = re.compile(r"TODO\((A2P-\d+)\)")

#: Rough per-grade review effort in minutes, plus a per-TODO increment.
#: A rule of thumb for scoping conversations, not a promise.
_BASE_MINUTES = {"automatic": 5, "review": 20, "manual": 90}
_MINUTES_PER_TODO = 5


@dataclass
class WorkflowAssessment:
    name: str
    kind: str
    source: str
    grade: str
    features: dict[str, int] = field(default_factory=dict)
    warnings: list[str] = field(default_factory=list)
    todos: dict[str, int] = field(default_factory=dict)
    estimated_minutes: int = 0


@dataclass
class Assessment:
    workflows: list[WorkflowAssessment] = field(default_factory=list)
    skipped: list[tuple[str, str]] = field(default_factory=list)
    project_warnings: list[str] = field(default_factory=list)

    @property
    def counts(self) -> dict[str, int]:
        out = {"automatic": 0, "review": 0, "manual": 0}
        for wf in self.workflows:
            out[wf.grade] += 1
        return out

    @property
    def total_minutes(self) -> int:
        return sum(wf.estimated_minutes for wf in self.workflows)


def assess_project(project: Project, options: GeneratorOptions | None = None) -> Assessment:
    """Grade every manifest in the project by converting it in memory."""
    options = options or GeneratorOptions()
    assessment = Assessment(
        skipped=list(project.skipped),
        project_warnings=list(project.warnings),
    )

    libraries = project.libraries
    shared_info: SharedModuleInfo | None = None
    if libraries:
        _code, shared_info = generate_shared_module(libraries, options)

    for file in project.files:
        for wf in file.workflows:
            assessment.workflows.append(_assess_workflow(wf, file.name, shared_info, options))
    return assessment


def _assess_workflow(
    wf: Workflow,
    source: str,
    shared_info: SharedModuleInfo | None,
    options: GeneratorOptions,
) -> WorkflowAssessment:
    try:
        code, _plans = generate_module([wf], options, shared=shared_info)
    except Exception as exc:  # conversion crash: worst grade, precise reason
        return WorkflowAssessment(
            name=wf.display_name,
            kind=wf.kind,
            source=source,
            grade="manual",
            warnings=[f"Conversion failed: {exc}"],
            estimated_minutes=_BASE_MINUTES["manual"],
        )

    todos: dict[str, int] = {}
    for match in _TODO_RE.finditer(code):
        todos[match.group(1)] = todos.get(match.group(1), 0) + 1

    if "NotImplementedError" in code:
        grade = "manual"
    elif todos or wf.warnings:
        grade = "review"
    else:
        grade = "automatic"

    return WorkflowAssessment(
        name=wf.display_name,
        kind=wf.kind,
        source=source,
        grade=grade,
        features=_features(wf),
        warnings=list(wf.warnings),
        todos=dict(sorted(todos.items())),
        estimated_minutes=_BASE_MINUTES[grade] + _MINUTES_PER_TODO * sum(todos.values()),
    )


def _features(wf: Workflow) -> dict[str, int]:
    """Count the Argo features a workflow uses (drives the fleet histogram)."""
    counts: dict[str, int] = {}

    def bump(key: str, by: int = 1) -> None:
        if by:
            counts[key] = counts.get(key, 0) + by

    for template in wf.templates:
        bump(f"template:{template.kind.value}")
        bump("retries", 1 if template.retry else 0)
        bump("timeout", 1 if template.timeout_seconds else 0)
        bump("synchronization", 1 if template.synchronization else 0)
        bump("memoize", 1 if template.memoize else 0)
        bump("artifacts", len(template.input_artifacts) + len(template.output_artifacts))
        calls = template.dag_tasks + [c for g in template.step_groups for c in g]
        for call in calls:
            bump("loops", 1 if (call.with_items or call.with_param or call.with_sequence) else 0)
            bump("conditions", 1 if call.when else 0)
            bump("templateRef", 1 if call.template_ref else 0)
            bump("depends", 1 if call.depends else 0)
    bump("schedules", len(wf.schedules))
    bump("exit-handler", 1 if wf.on_exit else 0)
    bump("workflowTemplateRef", 1 if wf.workflow_template_ref else 0)
    return dict(sorted(counts.items()))


# --------------------------------------------------------------------------- #
# Renderers
# --------------------------------------------------------------------------- #
def render_json(assessment: Assessment) -> str:
    payload = {
        "summary": {
            "workflows": len(assessment.workflows),
            "grades": assessment.counts,
            "estimated_review_minutes": assessment.total_minutes,
        },
        "workflows": [asdict(wf) for wf in assessment.workflows],
        "skipped_files": [{"file": name, "reason": why} for name, why in assessment.skipped],
        "project_warnings": assessment.project_warnings,
        "todo_legend": TODO_CODES,
    }
    return json.dumps(payload, indent=2) + "\n"


def render_markdown(assessment: Assessment) -> str:
    counts = assessment.counts
    total = len(assessment.workflows)
    hours = assessment.total_minutes / 60
    out: list[str] = [
        "# Argo → Prefect migration assessment",
        "",
        f"**{total} workflow manifest(s)** assessed by running the full conversion",
        "pipeline in memory. Grades reflect actual converter output:",
        "",
        "| Grade | Count | Meaning |",
        "|---|---:|---|",
        f"| automatic | {counts['automatic']} | converts clean — no follow-up |",
        f"| review | {counts['review']} | converts fully; carries TODO(s) to read |",
        f"| manual | {counts['manual']} | has stubs that raise until implemented |",
        "",
        f"Estimated human review effort: **~{hours:.1f} hours** "
        f"({assessment.total_minutes} min — rule of thumb, not a quote).",
        "",
        "## Per-workflow grades",
        "",
        "| Workflow | Kind | Source | Grade | TODOs | Est. min |",
        "|---|---|---|---|---|---:|",
    ]
    grade_order = {"manual": 0, "review": 1, "automatic": 2}
    for wf in sorted(assessment.workflows, key=lambda w: (grade_order[w.grade], w.name)):
        todo_list = ", ".join(f"{code}×{n}" for code, n in wf.todos.items()) or "—"
        out.append(
            f"| {wf.name} | {wf.kind} | {wf.source} | **{wf.grade}** "
            f"| {todo_list} | {wf.estimated_minutes} |"
        )

    used_codes = sorted({code for wf in assessment.workflows for code in wf.todos})
    if used_codes:
        out += ["", "## TODO legend", ""]
        for code in used_codes:
            out.append(f"- **{code}** — {TODO_CODES[code]}")

    flagged = [wf for wf in assessment.workflows if wf.warnings]
    if flagged:
        out += ["", "## Warnings by workflow", ""]
        for wf in flagged:
            out.append(f"### {wf.name} ({wf.source})")
            out.extend(f"- {w}" for w in wf.warnings)
            out.append("")

    if assessment.skipped:
        out += ["", "## Skipped files", ""]
        out.extend(f"- `{name}`: {why}" for name, why in assessment.skipped)
    if assessment.project_warnings:
        out += ["", "## Project warnings", ""]
        out.extend(f"- {w}" for w in assessment.project_warnings)
    return "\n".join(out).rstrip() + "\n"


_HTML_STYLE = """
body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
       max-width: 60rem; margin: 2rem auto; padding: 0 1rem; color: #1a1a2e; }
table { border-collapse: collapse; width: 100%; margin: 1rem 0; }
th, td { text-align: left; padding: 0.4rem 0.7rem; border-bottom: 1px solid #ddd; }
th { background: #f4f4f8; }
.grade-automatic { color: #0a7d33; font-weight: 600; }
.grade-review { color: #b26a00; font-weight: 600; }
.grade-manual { color: #b3261e; font-weight: 600; }
code { background: #f4f4f8; padding: 0.1rem 0.3rem; border-radius: 3px; }
"""


def render_html(assessment: Assessment) -> str:
    counts = assessment.counts
    rows = []
    for wf in sorted(
        assessment.workflows, key=lambda w: (w.grade != "manual", w.grade != "review", w.name)
    ):
        todo_list = ", ".join(f"{code}×{n}" for code, n in wf.todos.items()) or "—"
        rows.append(
            "<tr>"
            f"<td>{html.escape(wf.name)}</td><td>{html.escape(wf.kind)}</td>"
            f"<td>{html.escape(wf.source)}</td>"
            f'<td class="grade-{wf.grade}">{wf.grade}</td>'
            f"<td>{html.escape(todo_list)}</td><td>{wf.estimated_minutes}</td>"
            "</tr>"
        )
    used_codes = sorted({code for wf in assessment.workflows for code in wf.todos})
    legend = "".join(
        f"<li><code>{code}</code> — {html.escape(TODO_CODES[code])}</li>" for code in used_codes
    )
    return f"""<!doctype html>
<html><head><meta charset="utf-8"><title>Argo → Prefect assessment</title>
<style>{_HTML_STYLE}</style></head><body>
<h1>Argo → Prefect migration assessment</h1>
<p><strong>{len(assessment.workflows)} workflow manifest(s)</strong> —
{counts["automatic"]} automatic, {counts["review"]} review, {counts["manual"]} manual.
Estimated review effort ~{assessment.total_minutes / 60:.1f} hours (rule of thumb).</p>
<table>
<tr><th>Workflow</th><th>Kind</th><th>Source</th><th>Grade</th><th>TODOs</th><th>Est. min</th></tr>
{"".join(rows)}
</table>
{f"<h2>TODO legend</h2><ul>{legend}</ul>" if legend else ""}
</body></html>
"""


def render_migration_report(files: dict[str, str], warnings: list[str]) -> str:
    """Consolidate every TODO in a set of generated modules into one checklist."""
    out: list[str] = [
        "# Migration report",
        "",
        "Every follow-up item the converter left in the generated code, in one",
        "place. Work through the checklist, then run `argo2prefect verify` on",
        "this directory.",
        "",
    ]
    total = 0
    for filename in sorted(files):
        hits: list[tuple[int, str]] = []
        for lineno, line in enumerate(files[filename].splitlines(), start=1):
            if _TODO_RE.search(line):
                hits.append((lineno, line.strip().lstrip("# ")))
        if not hits:
            continue
        out.append(f"## {filename}")
        out.append("")
        for lineno, text in hits:
            out.append(f"- [ ] `{filename}:{lineno}` — {text}")
            total += 1
        out.append("")
    if total == 0:
        out.append("No TODOs — every construct converted cleanly. 🎉")
    used_codes = sorted({m.group(1) for code in files.values() for m in _TODO_RE.finditer(code)})
    if used_codes:
        out += ["", "## TODO legend", ""]
        out.extend(f"- **{code}** — {TODO_CODES[code]}" for code in used_codes)
    if warnings:
        out += ["", "## Conversion warnings", ""]
        out.extend(f"- {w}" for w in warnings)
    return "\n".join(out).rstrip() + "\n"
