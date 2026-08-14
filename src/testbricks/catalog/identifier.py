"""Shared table identity helpers for CSV-backed SparkMock tables."""

from __future__ import annotations

from dataclasses import dataclass

from .errors import InvalidTableNameError


def _strip_identifier_part(part: str) -> str:
    part = part.strip()
    if len(part) >= 2 and part[0] == "`" and part[-1] == "`":
        return part[1:-1]
    return part


def _split_qualified_name(table_name: str) -> list[str]:
    parts: list[str] = []
    current: list[str] = []
    in_ticks = False
    for char in table_name.strip():
        if char == "`":
            in_ticks = not in_ticks
            current.append(char)
        elif char == "." and not in_ticks:
            parts.append(_strip_identifier_part("".join(current)))
            current = []
        else:
            current.append(char)
    parts.append(_strip_identifier_part("".join(current)))
    return parts


@dataclass(frozen=True)
class TableIdentifier:
    """Table name ``schema.table`` (optional ``catalog.`` prefix) → CSV + temp view."""

    schema: str
    table: str
    catalog: str | None = None

    @classmethod
    def parse(cls, table_name: str) -> TableIdentifier:
        parts = _split_qualified_name(table_name)
        if len(parts) == 3:
            catalog, schema, table = parts
            if catalog and schema and table:
                return cls(schema=schema, table=table, catalog=catalog)
        elif len(parts) == 2:
            schema, table = parts
            if schema and table:
                return cls(schema=schema, table=table)

        raise InvalidTableNameError(
            "Invalid table name format. Expected 'schema_name.table_name' "
            f"or 'catalog.schema_name.table_name', got '{table_name}'"
        )

    @property
    def view_name(self) -> str:
        return f"{self.schema}_{self.table}"

    @property
    def relative_csv_path(self) -> str:
        return f"{self.schema}/{self.table}.csv"

    def __str__(self) -> str:
        if self.catalog:
            return f"{self.catalog}.{self.schema}.{self.table}"
        return f"{self.schema}.{self.table}"
