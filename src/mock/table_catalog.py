"""CSV-backed table catalog for SparkMock.

Owns schema.table ↔ on-disk CSV ↔ Spark temp view synchronization.
"""

from __future__ import annotations

import logging
import os
import tempfile
from typing import Any

logger = logging.getLogger(__name__)


class TableCatalog:
    def __init__(self, base_path: str, spark_session):
        self._base_path = base_path
        self._spark = spark_session

    @property
    def base_path(self) -> str:
        return self._base_path

    def parse_table_name(self, name: str) -> tuple[str, str]:
        parts = name.split(".")
        if len(parts) != 2 or not parts[0] or not parts[1]:
            raise ValueError(
                f"Invalid table name format. Expected 'schema_name.table_name', got '{name}'"
            )
        return parts[0], parts[1]

    def view_name(self, schema: str, table: str) -> str:
        return f"{schema}_{table}"

    def csv_path(self, schema: str, table: str) -> str:
        return os.path.join(self._base_path, schema, f"{table}.csv")

    def get_full_path(self, relative_path: str) -> str:
        return os.path.join(self._base_path, relative_path)

    def table_exists(self, name: str) -> bool:
        schema, table = self.parse_table_name(name)
        return os.path.isfile(self.csv_path(schema, table))

    def register_view(self, name: str, dataframe) -> None:
        schema, table = self.parse_table_name(name)
        dataframe.createOrReplaceTempView(self.view_name(schema, table))

    def drop_table(self, name: str) -> None:
        schema, table = self.parse_table_name(name)
        path = self.csv_path(schema, table)
        if os.path.isfile(path):
            os.remove(path)
        view = self.view_name(schema, table)
        try:
            self._spark.catalog.dropTempView(view)
        except Exception:
            logger.debug("Temp view %s was not registered", view, exc_info=True)

    def load_all(self) -> None:
        if not os.path.isdir(self._base_path):
            return

        for folder_name in os.listdir(self._base_path):
            folder_path = os.path.join(self._base_path, folder_name)
            if not os.path.isdir(folder_path):
                continue
            for filename in os.listdir(folder_path):
                if not filename.endswith(".csv"):
                    continue
                table = os.path.splitext(filename)[0]
                name = f"{folder_name}.{table}"
                logger.debug("Loading table %s into temp view", name)
                self.read_table(name)

    def read_table(self, name: str, options: dict[str, Any] | None = None):
        schema, table = self.parse_table_name(name)
        path = self.csv_path(schema, table)
        if not os.path.isfile(path):
            raise FileNotFoundError(f"Table not found: {name} (expected CSV at {path})")

        merged = {"header": "true", "inferSchema": "true"}
        if options:
            merged.update({str(k): str(v) if not isinstance(v, str) else v for k, v in options.items()})

        reader = self._spark.read
        for key, value in merged.items():
            reader = reader.option(key, value)
        df = reader.csv(path)
        self.register_view(name, df)
        return df

    def write_table(
        self,
        name: str,
        dataframe,
        mode: str | None = None,
        options: dict[str, Any] | None = None,
    ) -> None:
        schema, table = self.parse_table_name(name)
        path = self.csv_path(schema, table)
        exists = os.path.isfile(path)
        save_mode = (mode or "error").lower()

        if save_mode in ("error", "errorifexists") and exists:
            raise FileExistsError(f"Table already exists: {name}")
        if save_mode == "ignore" and exists:
            return
        if save_mode not in ("overwrite", "append", "ignore", "error", "errorifexists"):
            raise ValueError(f"Unsupported write mode: {mode}")

        schema_dir = os.path.join(self._base_path, schema)
        os.makedirs(schema_dir, exist_ok=True)

        pandas_df = dataframe.toPandas()
        header = True
        if options and "header" in options:
            header = str(options["header"]).lower() == "true"

        if save_mode == "append" and exists:
            import pandas as pd

            existing = pd.read_csv(path)
            if set(existing.columns) != set(pandas_df.columns):
                raise ValueError(
                    f"Append column mismatch for {name}: "
                    f"existing={list(existing.columns)} new={list(pandas_df.columns)}"
                )
            pandas_df = pd.concat(
                [existing, pandas_df[existing.columns]], ignore_index=True
            )

        self._atomic_to_csv(pandas_df, path, header=header)

        # Re-read so the registered view matches on-disk types/options.
        self.read_table(name, options=options)

    def _atomic_to_csv(self, pandas_df, path: str, header: bool) -> None:
        directory = os.path.dirname(path)
        fd, tmp_path = tempfile.mkstemp(prefix=".tmp_", suffix=".csv", dir=directory)
        os.close(fd)
        try:
            pandas_df.to_csv(tmp_path, index=False, header=header)
            os.replace(tmp_path, path)
        except Exception:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
            raise
