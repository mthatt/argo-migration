"""Golden-snapshot tests for the curated examples.

Generator output for ``examples/argo/*.yaml`` (default options) is pinned
under ``tests/golden/``. Any change to generated code shows up as an explicit
diff here — intentional changes are accepted by regenerating::

    python tests/test_golden.py   # rewrites the snapshots
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

from argo2prefect.generator import generate_code
from argo2prefect.parser import parse_workflows

EXAMPLES = sorted((Path(__file__).parents[1] / "examples" / "argo").glob("*.yaml"))
GOLDEN_DIR = Path(__file__).parent / "golden"


def _generate(manifest: Path) -> str:
    return generate_code(parse_workflows(manifest.read_text(encoding="utf-8")))


@pytest.mark.parametrize("manifest", EXAMPLES, ids=lambda p: p.stem)
def test_golden(manifest: Path) -> None:
    golden = GOLDEN_DIR / f"{manifest.stem}_flow.py"
    assert golden.exists(), f"missing snapshot {golden.name}; run `python {__file__}`"
    assert _generate(manifest) == golden.read_text(encoding="utf-8"), (
        f"generated code for {manifest.name} changed; if intentional, "
        f"run `python {__file__}` and review the diff"
    )


def main() -> int:
    GOLDEN_DIR.mkdir(exist_ok=True)
    for manifest in EXAMPLES:
        golden = GOLDEN_DIR / f"{manifest.stem}_flow.py"
        golden.write_text(_generate(manifest), encoding="utf-8")
        print(f"wrote {golden}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
