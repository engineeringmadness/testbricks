from .catalog import TableIdentifier
from .data_frame_wrapper import DataFrameWrapper


class DataFrameReader:
    def __init__(self, spark_mock):
        self._spark = spark_mock
        self._options = {}
        self._format = None

    def format(self, source):
        self._format = source
        return self

    def option(self, key, value):
        self._options[key] = value
        return self

    def options(self, **kwargs):
        self._options.update(kwargs)
        return self

    def table(self, table_name):
        ident = TableIdentifier.parse(table_name)
        merged = {"header": "true", "inferSchema": "true", **self._options}
        df = self._spark._catalog.read_csv(ident, merged)
        return DataFrameWrapper(self._spark, df)
