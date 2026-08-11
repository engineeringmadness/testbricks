"""Shared table identity helpers for CSV-backed SparkMock tables."""

from __future__ import annotations

from dataclasses import dataclass

from .errors import InvalidTableNameError


@dataclass(frozen=True)
class TableIdentifier:
    """Two-part table name: ``schema.table`` → CSV path + temp view name."""

    schema: str
    table: str

    @classmethod
    def parse(cls, table_name: str) -> TableIdentifier:
        parts = table_name.split(".")
        if len(parts) != 2 or not parts[0] or not parts[1]:
            raise InvalidTableNameError(
                f"Invalid table name format. Expected 'schema_name.table_name', got '{table_name}'"
            )
        return cls(schema=parts[0], table=parts[1])

    @property
    def view_name(self) -> str:
        return f"{self.schema}_{self.table}"

    @property
    def relative_csv_path(self) -> str:
        return f"{self.schema}/{self.table}.csv"

    def __str__(self) -> str:
        return f"{self.schema}.{self.table}"
