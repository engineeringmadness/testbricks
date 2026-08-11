import os
import tempfile

import pandas as pd


class DataFrameWriter:
    def __init__(self, spark_mock, dataframe):
        self._spark = spark_mock
        self._dataframe = dataframe
        self._options = {}
        self._mode = None

    def option(self, key, value):
        self._options[key] = value
        return self

    def options(self, **kwargs):
        self._options.update(kwargs)
        return self

    def mode(self, save_mode):
        self._mode = save_mode
        return self

    def csv(self, path):
        full_path = self._spark._get_full_path(path)

        writer = self._dataframe.write

        if self._mode:
            writer = writer.mode(self._mode)

        for key, value in self._options.items():
            writer = writer.option(key, value)

        writer.csv(full_path)

    def saveAsTable(self, table_name):
        parts = table_name.split('.')
        if len(parts) != 2:
            raise ValueError(
                f"Invalid table name format. Expected 'schema_name.table_name', got '{table_name}'"
            )

        schema_name, table = parts
        view_name = f"{schema_name}_{table}"

        schema_path = os.path.join(self._spark._base_path, schema_name)
        os.makedirs(schema_path, exist_ok=True)

        csv_path = os.path.join(schema_path, f"{table}.csv")
        new_pdf = self._dataframe.toPandas()

        write_header = True
        if 'header' in self._options:
            write_header = self._options['header'].lower() == 'true'

        if self._mode == "append" and os.path.exists(csv_path):
            existing_pdf = pd.read_csv(csv_path)
            if set(existing_pdf.columns) != set(new_pdf.columns):
                raise ValueError(
                    f"Cannot append to '{table_name}': schema mismatch. "
                    f"Existing columns={list(existing_pdf.columns)}, "
                    f"new columns={list(new_pdf.columns)}"
                )
            combined_pdf = pd.concat(
                [existing_pdf, new_pdf[existing_pdf.columns]],
                ignore_index=True,
            )
        else:
            combined_pdf = new_pdf

        self._write_csv_atomic(combined_pdf, csv_path, header=write_header)

        combined_df = self._spark._spark_session.createDataFrame(combined_pdf)
        combined_df.createOrReplaceTempView(view_name)

    @staticmethod
    def _write_csv_atomic(pandas_df, csv_path, header=True):
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
