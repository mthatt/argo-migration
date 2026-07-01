"""Translate Argo template expressions (``{{...}}``) into Python expressions.

Argo interpolates values into strings with a Go-template-ish ``{{ scope.path }}``
syntax. This module converts those into Python that is valid inside generated
Prefect flows/tasks.

Resolution rules (by scope prefix):

* ``inputs.parameters.NAME``   -> the task function's ``NAME`` argument.
* ``workflow.parameters.NAME`` -> the flow function's ``NAME`` argument.
* ``tasks.NODE.outputs.*`` /
  ``steps.NODE.outputs.*``     -> the upstream node's Prefect future
                                  (``NODE_fut``), resolved with ``.result()``
                                  when embedded inside a larger string.
* ``item`` / ``item.KEY``      -> the loop variable for ``withItems``/``withParam``.
* ``workflow.name`` / ``.uid`` -> ``prefect.runtime.flow_run`` attributes.

Anything unrecognised is preserved verbatim (and a warning is recorded) so a
human reviewer can finish the job instead of getting silently wrong output.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from typing import Optional

from .naming import sanitize_identifier

PLACEHOLDER_RE = re.compile(r"\{\{\s*(.*?)\s*\}\}")


@dataclass
class Scope:
    """Resolution context for a particular position in the generated code."""

    # Argo input-parameter name -> local Python identifier (leaf task bodies).
    inputs: dict[str, str] = field(default_factory=dict)
    # Workflow-level parameter name -> local Python identifier (flow scope).
    workflow_params: dict[str, str] = field(default_factory=dict)
    # Loop variable name when inside a withItems/withParam expansion.
    item_var: Optional[str] = None
    # Collected as a side effect: runtime imports needed (e.g. "flow_run").
    used_runtime: set[str] = field(default_factory=set)
    # Collected as a side effect: human-readable notes about unresolved bits.
    warnings: list[str] = field(default_factory=list)

    def output_var(self, node_name: str) -> str:
        """Future variable name the generator assigns to an upstream node."""
        return f"{sanitize_identifier(node_name)}_fut"


def has_placeholders(text: str) -> bool:
    return bool(PLACEHOLDER_RE.search(text or ""))


def translate_value(expr: str, scope: Scope) -> str:
    """Translate an Argo string into a Python expression that yields its value.

    * No placeholders -> a string literal.
    * Exactly one placeholder spanning the whole string -> the bare expression
      (so futures pass through lazily and typed values are preserved).
    * Otherwise -> an f-string.
    """
    if expr is None:
        return "None"
    segments = _segments(expr)
    if not any(kind == "ph" for kind, _ in segments):
        return _py_str_literal(expr)

    if len(segments) == 1 and segments[0][0] == "ph":
        resolved = _resolve(segments[0][1], scope, embedded=False)
        if resolved is not None:
            return resolved
        scope.warnings.append(f"Unresolved expression '{{{{{segments[0][1]}}}}}'; left as literal.")
        return _py_str_literal(expr)

    parts: list[str] = []
    for kind, value in segments:
        if kind == "lit":
            parts.append(_escape_fstring_literal(value))
        else:
            resolved = _resolve(value, scope, embedded=True)
            if resolved is None:
                scope.warnings.append(f"Unresolved expression '{{{{{value}}}}}'; left as literal.")
                parts.append(_escape_fstring_literal("{{" + value + "}}"))
            else:
                parts.append("{" + resolved + "}")
    return 'f"' + "".join(parts) + '"'


def translate_condition(when: str, scope: Scope) -> str:
    """Best-effort translation of an Argo ``when`` expression to Python.

    Argo and Python share ``==``/``!=``/``<``/``>``; we additionally map the
    boolean operators. Regex (``=~``) and anything exotic is preserved and
    flagged for manual review.
    """
    result = when

    def repl(match: re.Match) -> str:
        resolved = _resolve(match.group(1).strip(), scope, embedded=True)
        if resolved is None:
            scope.warnings.append(f"Unresolved condition expression '{match.group(0)}'.")
            return match.group(0)
        return resolved

    result = PLACEHOLDER_RE.sub(repl, result)
    result = result.replace("&&", " and ").replace("||", " or ")
    if "=~" in result:
        scope.warnings.append(
            f"Condition uses regex match (=~): '{when}'. Rewrite with re.search() manually."
        )
    return result.strip()


def _resolve(token: str, scope: Scope, *, embedded: bool) -> Optional[str]:
    """Map a single ``{{ token }}`` body to a Python expression, or ``None``."""
    if token.startswith("inputs.parameters."):
        name = token[len("inputs.parameters.") :]
        return scope.inputs.get(name, sanitize_identifier(name))
    if token.startswith("workflow.parameters."):
        name = token[len("workflow.parameters.") :]
        # Default to the shared dict the generator emits; single quotes keep this
        # valid when embedded inside a double-quoted f-string on Python < 3.12.
        escaped = name.replace("\\", "\\\\").replace("'", "\\'")
        return scope.workflow_params.get(name, f"WORKFLOW_PARAMETERS['{escaped}']")
    if token.startswith("tasks.") or token.startswith("steps."):
        parts = token.split(".")
        if len(parts) >= 2:
            var = scope.output_var(parts[1])
            return f"{var}.result()" if embedded else var
        return None
    if token == "item":
        return scope.item_var or "item"
    if token.startswith("item."):
        return f"{scope.item_var or 'item'}['{token[len('item.'):]}']"
    if token == "workflow.name":
        scope.used_runtime.add("flow_run")
        return "flow_run.name"
    if token in ("workflow.uid", "workflow.uuid"):
        scope.used_runtime.add("flow_run")
        return "flow_run.id"
    return None


def _segments(text: str) -> list[tuple[str, str]]:
    """Split text into ``("lit", str)`` and ``("ph", token)`` segments."""
    out: list[tuple[str, str]] = []
    last = 0
    for match in PLACEHOLDER_RE.finditer(text):
        if match.start() > last:
            out.append(("lit", text[last : match.start()]))
        out.append(("ph", match.group(1).strip()))
        last = match.end()
    if last < len(text):
        out.append(("lit", text[last:]))
    return out


def _py_str_literal(value: str) -> str:
    """A safe double-quoted Python string literal (via JSON escaping)."""
    return json.dumps(value, ensure_ascii=False)


def _escape_fstring_literal(value: str) -> str:
    value = value.replace("\\", "\\\\").replace('"', '\\"')
    value = value.replace("{", "{{").replace("}", "}}")
    value = value.replace("\n", "\\n").replace("\r", "\\r").replace("\t", "\\t")
    return value
