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
    item_var: str | None = None
    # Collected as a side effect: runtime imports needed (e.g. "flow_run").
    used_runtime: set[str] = field(default_factory=set)
    # Collected as a side effect: generated-module helpers needed.
    helpers: set[str] = field(default_factory=set)
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
    """Translate an Argo ``when`` expression to Python.

    Argo compares interpolated strings, so bare words are string operands:
    ``{{p}} == heads`` means ``p == "heads"`` — the bare word is quoted, not
    left as a (dangling) Python name. Boolean operators and common expr-lang
    (``{{= ...}}``) constructs are mapped. The result is **guaranteed to be a
    valid Python expression over known names**: anything else becomes
    ``False`` with a warning, so the generated module always parses and a
    skipped gate is explicit rather than silently wrong.
    """
    substitutions: list[str | None] = []

    def repl(match: re.Match) -> str:
        token = match.group(1).strip()
        resolved = _resolve(token, scope, embedded=True)
        if resolved is None:
            scope.warnings.append(f"Unresolved condition expression '{match.group(0)}'.")
        substitutions.append(resolved)
        return f"__A2P{len(substitutions) - 1}X__"

    work = PLACEHOLDER_RE.sub(repl, when.strip())
    work = _map_operators(work)
    work = _quote_bare_words(work)
    for index, expr in enumerate(substitutions):
        work = work.replace(f"__A2P{index}X__", expr if expr is not None else "__A2P_UNRESOLVED__")

    if "=~" in when:
        scope.warnings.append(
            f"Condition uses regex match (=~): '{when}'. Rewrite with re.search() manually."
        )
    if not _compiles(work) or not _names_known(work, scope):
        scope.warnings.append(
            f"Condition '{when}' could not be translated to Python; the branch is "
            "emitted as `if False` — port the gate manually."
        )
        return "False"
    return work.strip()


def _resolve(token: str, scope: Scope, *, embedded: bool) -> str | None:
    """Map a single ``{{ token }}`` body to a Python expression, or ``None``."""
    if token.startswith("="):
        # ``{{= ...}}`` is an expr-lang expression, not a plain reference.
        return _translate_expr_lang(token[1:].strip(), scope)
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
        if len(parts) >= 5 and parts[2] == "outputs" and parts[3] == "parameters":
            # Named output parameters came from files (valueFrom); only stdout
            # is captured, so route through a loud helper instead of silently
            # substituting the wrong value.
            scope.helpers.add("output_param")
            param = ".".join(parts[4:])
            scope.warnings.append(
                f"'{token}': named output parameters are not migrated automatically "
                "(only stdout); the generated _argo_output_param() raises until mapped."
            )
            var = scope.output_var(parts[1])
            return f"_argo_output_param({var}.result(), {param!r})"
        if len(parts) >= 4 and parts[2] == "outputs" and parts[3] not in ("result",):
            return None  # exitCode / artifacts / ... — no faithful mapping.
        if len(parts) >= 2:
            var = scope.output_var(parts[1])
            return f"{var}.result()" if embedded else var
        return None
    if token == "item":
        return scope.item_var or "item"
    if token.startswith("item."):
        return f"{scope.item_var or 'item'}['{token[len('item.') :]}']"
    if token == "workflow.name":
        scope.used_runtime.add("flow_run")
        return "flow_run.name"
    if token in ("workflow.uid", "workflow.uuid"):
        scope.used_runtime.add("flow_run")
        return "flow_run.id"
    return None


# --------------------------------------------------------------------------- #
# expr-lang (``{{= ...}}``) translation
# --------------------------------------------------------------------------- #
# Subscript references: inputs.parameters['x'] / workflow.parameters["y"].
_EXPR_SUBSCRIPT = re.compile(r"(inputs|workflow)\.parameters\[(['\"])(.+?)\2\]")
# Dotted references: inputs.parameters.x (name may contain dashes).
_EXPR_DOTTED = re.compile(r"(inputs|workflow)\.parameters\.([A-Za-z_][A-Za-z0-9_\-]*)")

#: expr-lang / sprig functions with direct Python builtins.
_EXPR_FUNCS = {
    "asInt": "int",
    "asFloat": "float",
    "string": "str",
    "sprig.trim": "str.strip",
    "sprig.lower": "str.lower",
    "sprig.upper": "str.upper",
}


def _translate_expr_lang(expr: str, scope: Scope) -> str | None:
    """Best-effort translation of an expr-lang expression to Python.

    Handles parameter references (dotted and subscripted), the shared
    comparison operators, boolean operators, and a few common conversion
    functions. Returns ``None`` when the result would not be valid Python,
    so callers fall back to their safe path (literal string / ``False``).
    """

    def sub_ref(scope_name: str, param: str) -> str:
        if scope_name == "inputs":
            return scope.inputs.get(param, sanitize_identifier(param))
        escaped = param.replace("\\", "\\\\").replace("'", "\\'")
        return scope.workflow_params.get(param, f"WORKFLOW_PARAMETERS['{escaped}']")

    result = _EXPR_SUBSCRIPT.sub(lambda m: sub_ref(m.group(1), m.group(3)), expr)
    result = _EXPR_DOTTED.sub(lambda m: sub_ref(m.group(1), m.group(2)), result)
    if scope.item_var:
        result = re.sub(r"\bitem\b", scope.item_var, result)
    for name, py in _EXPR_FUNCS.items():
        result = result.replace(f"{name}(", f"{py}(")
    result = _map_literals(result)
    result = _map_operators(result).strip()
    if not _compiles(result) or not _names_known(result, scope):
        return None
    return result


def _map_operators(text: str) -> str:
    """Map expr-lang boolean operators onto Python's."""

    def map_segment(segment: str) -> str:
        segment = segment.replace("&&", " and ").replace("||", " or ")
        # `!x` -> `not x`, but leave `!=` alone.
        segment = re.sub(r"!(?!=)", " not ", segment)
        return re.sub(r"  +", " ", segment)

    parts = _QUOTED_SEGMENT.split(text)
    return "".join(map_segment(part) if i % 2 == 0 else part for i, part in enumerate(parts))


_LITERALS = {"true": "True", "false": "False", "nil": "None"}


def _map_literals(text: str) -> str:
    """Map expr-lang literals to Python's, outside quoted strings only."""

    def map_words(segment: str) -> str:
        return re.sub(r"\b(true|false|nil)\b", lambda m: _LITERALS[m.group(1)], segment)

    parts = _QUOTED_SEGMENT.split(text)
    return "".join(map_words(part) if i % 2 == 0 else part for i, part in enumerate(parts))


def _compiles(expr: str) -> bool:
    try:
        compile(expr, "<argo-expression>", "eval")
    except (SyntaxError, ValueError):
        return False
    return True


_QUOTED_SEGMENT = re.compile(r"('[^']*'|\"[^\"]*\")")
_BARE_WORD = re.compile(r"\b[A-Za-z_][\w\-]*\b")
_SENTINEL = re.compile(r"__A2P\d+X__")
_PY_KEYWORDS = {"and", "or", "not", "in", "is", "True", "False", "None"}

#: Names allowed to appear in a translated expression, beyond scope-derived
#: identifiers: the shared parameter dict, mapped conversion functions,
#: Prefect runtime handles, and generated-module helpers.
_KNOWN_NAMES = {
    "WORKFLOW_PARAMETERS",
    "int",
    "float",
    "str",
    "len",
    "flow_run",
    "task_run",
    "_argo_output_param",
    "_argo_sequence",
}

_FUT_NAME = re.compile(r"\w+_fut$")


def _quote_bare_words(text: str) -> str:
    """Quote bare-word operands (Argo compares strings): ``== heads`` -> ``== "heads"``.

    Quoted segments, Python keywords, and substitution sentinels are left
    untouched.
    """

    def quote_words(segment: str) -> str:
        def quote(match: re.Match) -> str:
            word = match.group(0)
            if word in _PY_KEYWORDS or _SENTINEL.fullmatch(word):
                return word
            return json.dumps(word)

        return _BARE_WORD.sub(quote, segment)

    parts = _QUOTED_SEGMENT.split(text)
    # Even indices are outside quotes; odd indices are quoted segments.
    return "".join(quote_words(part) if i % 2 == 0 else part for i, part in enumerate(parts))


def _names_known(expr: str, scope: Scope) -> bool:
    """True when every free name in ``expr`` is one we deliberately emitted.

    This is the guard against expressions that *compile* but would raise
    ``NameError`` at run time (e.g. untranslated ``jsonpath(...)``).
    """
    import ast

    allowed = _KNOWN_NAMES | set(scope.inputs.values()) | set(scope.workflow_params.values())
    if scope.item_var:
        allowed.add(scope.item_var)
    try:
        tree = ast.parse(expr, mode="eval")
    except SyntaxError:
        return False
    for node in ast.walk(tree):
        if isinstance(node, ast.Name):
            if node.id not in allowed and not _FUT_NAME.fullmatch(node.id):
                return False
    return True


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
