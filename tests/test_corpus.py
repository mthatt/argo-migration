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
