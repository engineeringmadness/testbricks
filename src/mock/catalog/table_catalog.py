"""CSV + temp-view registry for SparkMock tables."""

from __future__ import annotations

import os
import tempfile
from typing import Mapping, Optional

import pandas as pd
from pyspark.sql import DataFrame, SparkSession

from .errors import SchemaMismatchError
from .identifier import TableIdentifier


class TableCatalog:
    """Maps ``schema.table`` identifiers to local CSV files and Spark temp views.

    All table reads/writes that go through SparkMock should use this catalog so
    CSV files and temp views stay in sync.
    """

    def __init__(self, spark_session: SparkSession, base_path: str):
        self._spark = spark_session
        self._base_path = base_path

    @property
    def base_path(self) -> str:
        return self._base_path

    def path_for(self, ident: TableIdentifier) -> str:
        return os.path.join(self._base_path, ident.relative_csv_path)

    def full_path(self, relative_path: str) -> str:
        return os.path.join(self._base_path, relative_path)

    def ensure_schema_dir(self, ident: TableIdentifier) -> str:
        schema_path = os.path.join(self._base_path, ident.schema)
        os.makedirs(schema_path, exist_ok=True)
        return schema_path

    def exists(self, ident: TableIdentifier) -> bool:
        return os.path.exists(self.path_for(ident))

    def load_all(self) -> None:
        """Discover ``{base}/{schema}/{table}.csv`` and register temp views."""
        if not os.path.exists(self._base_path):
            return

        for folder_name in os.listdir(self._base_path):
            folder_path = os.path.join(self._base_path, folder_name)
            if not os.path.isdir(folder_path):
                continue

            for filename in os.listdir(folder_path):
                if not filename.endswith(".csv"):
                    continue
                table = os.path.splitext(filename)[0]
                ident = TableIdentifier(schema=folder_name, table=table)
                df = self._spark.read.csv(
                    self.path_for(ident), header=True, inferSchema=True
                )
                df.createOrReplaceTempView(ident.view_name)

    def read_csv(
        self,
        ident: TableIdentifier,
        options: Optional[Mapping[str, str]] = None,
    ) -> DataFrame:
        reader = self._spark.read
        if options:
            for key, value in options.items():
                reader = reader.option(key, value)
        return reader.csv(self.path_for(ident))

    def save_dataframe(
        self,
        ident: TableIdentifier,
        dataframe: DataFrame,
        mode: Optional[str] = None,
        header: bool = True,
    ) -> None:
        """Persist a DataFrame as CSV and refresh the matching temp view.

        ``mode="append"`` concatenates onto an existing CSV (schema must match).
        Any other mode (including ``None`` / ``overwrite``) replaces the table.
        """
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
            combined_pdf = pd.concat(
                [existing_pdf, new_pdf[existing_pdf.columns]],
                ignore_index=True,
            )
        else:
            combined_pdf = new_pdf

        self._write_csv_atomic(combined_pdf, csv_path, header=header)
        combined_df = self._spark.createDataFrame(combined_pdf)
        combined_df.createOrReplaceTempView(ident.view_name)

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
