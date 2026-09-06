"""Canonical CSV option keys, aliases, and case-insensitive lookups.

This module is the single source of truth for which ``option(...)`` keys the
CSV catalog honors, how Spark-style aliases map to canonical names, and how
options are matched case-insensitively.
"""

from __future__ import annotations

from typing import Mapping, Optional

DEFAULT_CSV_READ_OPTIONS: dict[str, str] = {"header": "true", "inferSchema": "true"}

CSV_OPTION_KEYS = frozenset(
    {
        "delimiter",
        "sep",
        "quote",
        "escape",
        "nullvalue",
        "dateformat",
        "timestampformat",
        "header",
    }
)

_OPTION_ALIASES = {
    "sep": "delimiter",
    "delimiter": "delimiter",
    "quote": "quote",
    "escape": "escape",
    "nullvalue": "nullValue",
    "dateformat": "dateFormat",
    "timestampformat": "timestampFormat",
    "header": "header",
}


def normalize_csv_options(options: Optional[Mapping[str, str]]) -> dict[str, str]:
    """Keep only known CSV options, mapping aliases to canonical names."""
    if not options:
        return {}
    normalized: dict[str, str] = {}
    for key, value in options.items():
        canonical = _OPTION_ALIASES.get(key.lower())
        if canonical is None:
            continue
        normalized[canonical] = str(value)
    return normalized


def option_lookup(options: Optional[Mapping[str, str]], *names: str) -> Optional[str]:
    """Case-insensitive lookup returning the first match as ``str``."""
    if not options:
        return None
    lowered = {key.lower(): value for key, value in options.items()}
    for name in names:
        if name.lower() in lowered:
            return str(lowered[name.lower()])
    return None


def option_flag(options: Mapping[str, str], *names: str) -> bool:
    """Case-insensitive truthy check for Spark-style boolean option values."""
    wanted = {name.lower() for name in names}
    for key, value in options.items():
        if key.lower() in wanted:
            return str(value).lower() in {"true", "1", "yes"}
    return False
