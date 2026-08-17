"""CSV + temp-view registry for SparkMock tables."""

from __future__ import annotations

import os
import tempfile
from pathlib import Path
from typing import Mapping, Optional

import pandas as pd
from pyspark.sql import DataFrame, SparkSession

from .errors import SchemaMismatchError
from .identifier import TableIdentifier

_DEFAULT_READ_OPTIONS = {"header": "true", "inferSchema": "true"}


class TableCatalog:
    """Maps ``schema.table`` identifiers to local CSV files and Spark temp views."""

    def __init__(self, spark_session: SparkSession, base_path: str):
        self._spark = spark_session
        self._base_path = base_path
        self._root = Path(base_path)

    @property
    def base_path(self) -> str:
        return self._base_path

    def path_for(self, ident: TableIdentifier) -> str:
        return str(self._root / ident.relative_csv_path)

    def full_path(self, relative_path: str) -> str:
        return str(self._root / relative_path)

    def ensure_schema_dir(self, ident: TableIdentifier) -> str:
        schema_path = self._root / ident.schema
        schema_path.mkdir(parents=True, exist_ok=True)
        return str(schema_path)

    def exists(self, ident: TableIdentifier) -> bool:
        return os.path.exists(self.path_for(ident))

    def iter_schema_names(self) -> list[str]:
        if not self._root.exists():
            return []
        return sorted(path.name for path in self._root.iterdir() if path.is_dir())

    def iter_identifiers(self) -> list[TableIdentifier]:
        idents: list[TableIdentifier] = []
        for schema in self.iter_schema_names():
            for csv_path in sorted((self._root / schema).glob("*.csv")):
                idents.append(TableIdentifier(schema=schema, table=csv_path.stem))
        return idents

    def load_all(self) -> None:
        for ident in self.iter_identifiers():
            self.read_csv(ident, _DEFAULT_READ_OPTIONS).createOrReplaceTempView(
                ident.view_name
            )

    def read_csv(
        self,
        ident: TableIdentifier,
        options: Optional[Mapping[str, str]] = None,
    ) -> DataFrame:
        reader = self._spark.read
        for key, value in (options or {}).items():
            reader = reader.option(key, value)
        return reader.csv(self.path_for(ident))

    def save_dataframe(
        self,
        ident: TableIdentifier,
        dataframe: DataFrame,
        mode: Optional[str] = None,
        header: bool = True,
    ) -> None:
        self.ensure_schema_dir(ident)
        csv_path = self.path_for(ident)
        new_pdf = dataframe.toPandas()

        if mode == "append" and os.path.exists(csv_path):
            existing_pdf = pd.read_csv(csv_path)
            if set(existing_pdf.columns) != set(new_pdf.columns):
                raise SchemaMismatchError(
                    f"Cannot append to '{ident}': schema mismatch. "
                    f"Existing columns={list(existing_pdf.columns)}, "
                    f"new columns={list(new_pdf.columns)}"
                )
            new_pdf = pd.concat(
                [existing_pdf, new_pdf[existing_pdf.columns]],
                ignore_index=True,
            )

        self._write_csv_atomic(new_pdf, csv_path, header=header)
        self._spark.createDataFrame(new_pdf).createOrReplaceTempView(ident.view_name)

    @staticmethod
    def _write_csv_atomic(pandas_df: pd.DataFrame, csv_path: str, header: bool = True) -> None:
        directory = os.path.dirname(csv_path)
        fd, temp_path = tempfile.mkstemp(suffix=".csv", dir=directory)
        os.close(fd)
        try:
            pandas_df.to_csv(temp_path, index=False, header=header)
            os.replace(temp_path, csv_path)
        except Exception:
            if os.path.exists(temp_path):
                os.remove(temp_path)
            raise
