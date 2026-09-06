"""Spark-shaped catalog API over the CSV ``TableCatalog``."""

from __future__ import annotations

import fnmatch
from typing import List, Optional

from pyspark.sql.catalog import Database, Table

from .errors import InvalidTableNameError
from .identifier import TableIdentifier
from .table_catalog import TableCatalog


def _matches(name: str, pattern: Optional[str]) -> bool:
    return pattern is None or fnmatch.fnmatch(name, pattern)


class CatalogFacade:
    """Subset of ``pyspark.sql.catalog.Catalog`` used by Databricks notebooks."""

    def __init__(self, tables: TableCatalog):
        self._tables = tables

    def exists(self, ident: TableIdentifier) -> bool:
        return self._tables.exists(ident)

    def tableExists(self, tableName: str, dbName: Optional[str] = None) -> bool:
        try:
            ident = (
                TableIdentifier(schema=dbName, table=tableName)
                if dbName is not None
                else TableIdentifier.parse(tableName)
            )
        except InvalidTableNameError:
            return False
        return self._tables.exists(ident)

    def listTables(
        self,
        dbName: Optional[str] = None,
        pattern: Optional[str] = None,
    ) -> List[Table]:
        tables = []
        for ident in self._tables.iter_identifiers():
            if dbName is not None and ident.schema != dbName:
                continue
            if not _matches(ident.table, pattern):
                continue
            tables.append(
                Table(
                    name=ident.table,
                    catalog=ident.catalog,
                    namespace=[ident.schema],
                    description=None,
                    tableType="MANAGED",
                    isTemporary=False,
                )
            )
        return tables

    def listDatabases(self, pattern: Optional[str] = None) -> List[Database]:
        return [
            Database(
                name=name,
                catalog=None,
                description=None,
                locationUri=self._tables.full_path(name),
            )
            for name in self._tables.iter_schema_names()
            if _matches(name, pattern)
        ]
