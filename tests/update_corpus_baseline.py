"""Regenerate ``tests/corpus_baseline.json`` from the current corpus results.

Usage::

    python tests/update_corpus_baseline.py

Prints a summary of what changed so an intentional coverage change is easy to
describe in a commit message. The corpus pass-rate may only go up: review any
newly failing entries before committing.
"""

from __future__ import annotations

import json
import sys
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from corpus_utils import BASELINE_PATH, NOT_A_WORKFLOW, build_baseline, iter_corpus, load_baseline


def main() -> int:
    old = load_baseline()
    new = build_baseline()

    total = len(iter_corpus())
    skipped = sum(1 for s in new.values() if s == NOT_A_WORKFLOW)
    failing = len(new) - skipped
    passing = total - len(new)

    for name in sorted(set(old) - set(new)):
        print(f"now passing: {name} (was {old[name]})")
    for name in sorted(set(new) - set(old)):
        print(f"NEW FAILURE: {name} ({new[name]})")
    for name in sorted(set(new) & set(old)):
        if new[name] != old[name]:
            print(f"changed: {name} ({old[name]} -> {new[name]})")

    BASELINE_PATH.write_text(json.dumps(new, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    counts = Counter(new.values())
    print(f"\nWrote {BASELINE_PATH}")
    print(
        f"corpus: {passing}/{total - skipped} passing "
        f"({skipped} skipped as not-a-workflow, {failing} known failures)"
    )
    for status, count in sorted(counts.items()):
        print(f"  {status}: {count}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
