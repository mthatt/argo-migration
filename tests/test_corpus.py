"""Corpus coverage harness.

Runs every manifest in ``tests/corpus/`` through parse -> generate ->
``ast.parse`` and compares the outcome to ``corpus_baseline.json``. Any drift
fails: a new failure is a regression, and a newly passing file must be
promoted out of the baseline (run ``python tests/update_corpus_baseline.py``).

This measures "produces syntactically valid code without crashing", not
semantic fidelity — deeper checks land in later phases (see REBUILD_PLAN.md).
"""

from __future__ import annotations

import pytest
from corpus_utils import OK, classify, iter_corpus, load_baseline, rel_name

BASELINE = load_baseline()
CORPUS = iter_corpus()


def test_corpus_is_present() -> None:
    assert len(CORPUS) > 100, "corpus looks truncated; see tests/corpus/README.md"


@pytest.mark.parametrize("path", CORPUS, ids=rel_name)
def test_corpus_manifest(path) -> None:
    status = classify(path)
    expected = BASELINE.get(rel_name(path), OK)
    if status == expected:
        return
    if expected == OK:
        pytest.fail(f"regression: {rel_name(path)} was passing, now {status!r}")
    if status == OK:
        pytest.fail(
            f"{rel_name(path)} now passes but is still in the baseline; "
            "run `python tests/update_corpus_baseline.py` and commit the result"
        )
    pytest.fail(
        f"{rel_name(path)} changed failure mode {expected!r} -> {status!r}; "
        "review and run `python tests/update_corpus_baseline.py`"
    )


def test_baseline_has_no_stale_entries() -> None:
    known = {rel_name(p) for p in CORPUS}
    stale = sorted(set(BASELINE) - known)
    assert not stale, f"baseline entries for missing corpus files: {stale}"


def test_corpus_as_linked_project() -> None:
    """The whole corpus converted as ONE project: the linker must resolve
    every named templateRef (upstream examples reference each other's
    WorkflowTemplates), and every module must be valid Python except the
    known ``{{=expr}}`` failures tracked in the baseline."""
    import ast
    import re

    from corpus_utils import CORPUS_DIR

    from argo2prefect.generator import generate_project
    from argo2prefect.project import load_project

    out = generate_project(load_project([CORPUS_DIR]))
    assert len(out.files) > 100

    expected_invalid = {
        f"{name.rsplit('/', 1)[-1].removesuffix('.yaml')}_flow.py"
        for name, status in BASELINE.items()
        if status == "invalid-code"
    }
    invalid = set()
    for name, code in out.files.items():
        try:
            ast.parse(code)
        except SyntaxError:
            invalid.add(name)
    assert invalid == expected_invalid

    # Nameless stubs are Argo `inline:` templates (not yet supported); any
    # stub with a *name* means the linker failed to resolve a templateRef.
    unresolved = {
        name
        for name, code in out.files.items()
        if re.search(r"Unresolved templateRef: [^\"']", code)
    }
    assert unresolved == set()
