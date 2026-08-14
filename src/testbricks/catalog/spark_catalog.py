"""Spark-shaped catalog API over the CSV ``TableCatalog``."""

from __future__ import annotations

import fnmatch
from typing import List, Optional

from pyspark.sql.catalog import Database, Table

from .errors import InvalidTableNameError
from .identifier import TableIdentifier
from .table_catalog import TableCatalog


class CatalogFacade:
    """Subset of ``pyspark.sql.catalog.Catalog`` used by Databricks notebooks.

    Unknown attributes fall through to ``TableCatalog`` so existing
    ``spark.catalog.read_csv`` / ``save_dataframe`` call sites keep working.
    """

    def __init__(self, tables: TableCatalog):
        self._tables = tables

    def tableExists(self, tableName: str, dbName: Optional[str] = None) -> bool:
        if dbName is not None:
            ident = TableIdentifier(schema=dbName, table=tableName)
            return self._tables.exists(ident)
        try:
            ident = TableIdentifier.parse(tableName)
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
            if pattern is not None and not fnmatch.fnmatch(ident.table, pattern):
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
        databases = []
        for name in self._tables.iter_schema_names():
            if pattern is not None and not fnmatch.fnmatch(name, pattern):
                continue
            location = self._tables.full_path(name)
            databases.append(
                Database(
                    name=name,
                    catalog=None,
                    description=None,
                    locationUri=location,
                )
            )
        return databases

    def __getattr__(self, name):
        return getattr(self._tables, name)
