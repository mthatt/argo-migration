"""Helpers for turning Argo names into valid, readable Python identifiers.

Argo names are DNS-label-ish (``my-task-1``) and sometimes camelCase. Python
functions/variables want ``snake_case`` identifiers that are not keywords. These
helpers centralise that conversion so names are stable across the generator.
"""

from __future__ import annotations

import keyword
import re

_CAMEL_BOUNDARY = re.compile(r"(?<=[a-z0-9])(?=[A-Z])")
_NON_IDENT = re.compile(r"[^0-9a-zA-Z]+")


def to_snake(name: str) -> str:
    """Convert an arbitrary Argo name to ``snake_case``.

    ``"generate-report"`` -> ``"generate_report"``
    ``"generateReport"``   -> ``"generate_report"``
    ``"A-B.C"``            -> ``"a_b_c"``
    """
    if not name:
        return "_"
    # Split camelCase boundaries first so they survive lower-casing.
    spaced = _CAMEL_BOUNDARY.sub("_", name)
    snake = _NON_IDENT.sub("_", spaced).strip("_").lower()
    return snake or "_"


def sanitize_identifier(name: str, *, prefix: str = "x") -> str:
    """Return a valid, non-keyword Python identifier derived from ``name``."""
    ident = to_snake(name)
    if not ident or ident == "_":
        ident = prefix
    if ident[0].isdigit():
        ident = f"{prefix}_{ident}"
    if keyword.iskeyword(ident) or keyword.issoftkeyword(ident):
        ident = f"{ident}_"
    return ident


def unique(name: str, used: set[str]) -> str:
    """Return ``name`` (or ``name_2``, ``name_3`` ...) not already in ``used``.

    Mutates ``used`` by adding the returned value.
    """
    candidate = name
    counter = 2
    while candidate in used:
        candidate = f"{name}_{counter}"
        counter += 1
    used.add(candidate)
    return candidate
