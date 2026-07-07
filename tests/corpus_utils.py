"""Shared helpers for the corpus coverage harness.

Every manifest in ``tests/corpus/`` is classified into one of:

* ``ok``             - parses, generates, and the generated code is valid Python.
* ``not-a-workflow`` - contains no workflow-bearing document (e.g. a ConfigMap);
                       skipped, not a failure.
* ``parse-error``    - the parser rejected a manifest it should handle.
* ``parse-crash``    - the parser raised something other than ``ParseError``.
* ``generate-error`` - code generation raised.
* ``invalid-code``   - generated code does not ``ast.parse``.

``baseline.json`` records the status of every file that is not ``ok``. The
corpus test fails on any drift in either direction so the baseline always
matches reality; run ``python tests/update_corpus_baseline.py`` to refresh it
after an intentional change.
"""

from __future__ import annotations

import ast
import json
from pathlib import Path

from argo2prefect.generator import generate_code
from argo2prefect.parser import ParseError, parse_workflows

CORPUS_DIR = Path(__file__).parent / "corpus"
BASELINE_PATH = Path(__file__).parent / "corpus_baseline.json"

OK = "ok"
NOT_A_WORKFLOW = "not-a-workflow"


def iter_corpus() -> list[Path]:
    return sorted(p for p in CORPUS_DIR.rglob("*.yaml") if p.is_file())


def rel_name(path: Path) -> str:
    return path.relative_to(CORPUS_DIR).as_posix()


def classify(path: Path) -> str:
    """Run a manifest through the full pipeline and report how far it got."""
    text = path.read_text(encoding="utf-8")
    try:
        workflows = parse_workflows(text)
    except ParseError as exc:
        if "No Argo Workflow manifest found" in str(exc):
            return NOT_A_WORKFLOW
        return "parse-error"
    except Exception:
        return "parse-crash"

    try:
        code = generate_code(workflows)
    except Exception:
        return "generate-error"

    try:
        ast.parse(code)
    except SyntaxError:
        return "invalid-code"
    return OK


def load_baseline() -> dict[str, str]:
    if not BASELINE_PATH.exists():
        return {}
    return json.loads(BASELINE_PATH.read_text(encoding="utf-8"))


def build_baseline() -> dict[str, str]:
    """Classify the whole corpus; return the non-``ok`` entries."""
    return {rel_name(path): status for path in iter_corpus() if (status := classify(path)) != OK}
