"""Stable TODO codes emitted into generated code.

Every follow-up item the generator leaves behind carries a stable id
(``# TODO(A2P-101): ...``) so reports can group, count, and link them, and
clients can grep for exactly one code across a whole conversion.

Codes are append-only: never renumber or reuse.
"""

from __future__ import annotations

TODO_CODES: dict[str, str] = {
    "A2P-101": "Translated when-condition needs review",
    "A2P-102": "depends status gating (Failed/Errored/||) needs a manual port",
    "A2P-103": "Input artifact must be fetched manually",
    "A2P-104": "Output artifact must be published manually",
    "A2P-105": "Named output parameter came from a file; only stdout is migrated",
    "A2P-106": "Exit handler used Argo workflow context; supply equivalents",
    "A2P-107": "Indefinite suspend; consider pause_flow_run()",
    "A2P-108": "Unresolved templateRef; include the referenced manifest",
    "A2P-109": "No entrypoint template found",
    "A2P-110": "Condition could not be translated; branch emitted as `if False`",
    "A2P-111": "Kubernetes namespace defaults to 'default'; override if needed",
    "A2P-112": "Template type has no automatic migration; stub emitted",
}


def todo(code: str, message: str) -> str:
    """Render a stable-id TODO comment body (without the leading ``#``)."""
    assert code in TODO_CODES, f"unknown TODO code {code}"
    return f"TODO({code}): {message}"
