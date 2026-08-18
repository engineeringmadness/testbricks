"""Rewrite Databricks-style ``schema.table`` refs in SQL to SparkProxy temp views."""

from __future__ import annotations

import re

from .identifier import TableIdentifier

_IDENT = r"(?:`[^`]+`|[A-Za-z_][\w]*)"
_TABLE_REF = rf"{_IDENT}(?:\s*\.\s*{_IDENT}){{1,2}}"
_FROM_JOIN = re.compile(
    rf"\b((?:FROM|JOIN)\s+)({_TABLE_REF})",
    re.IGNORECASE,
)
_NOOP_SQL = re.compile(
    r"^\s*(?:OPTIMIZE|VACUUM|ANALYZE\s+TABLE|REFRESH(?:\s+TABLE)?|FSCK\s+REPAIR\s+TABLE)\b",
    re.IGNORECASE,
)


def is_maintenance_noop(query: str) -> bool:
    return bool(_NOOP_SQL.match(query or ""))


def _ref_to_view_name(ref: str) -> str:
    return TableIdentifier.parse(ref).view_name


def rewrite_from_join_identifiers(query: str) -> str:
    """Replace dotted table names after FROM/JOIN; leave literals like ``1.5`` alone."""

    def _replace(match: re.Match[str]) -> str:
        return match.group(1) + _ref_to_view_name(match.group(2))

    return _FROM_JOIN.sub(_replace, query)
