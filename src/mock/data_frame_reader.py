from .data_frame_wrapper import DataFrameWrapper


class DataFrameReader:
    def __init__(self, spark_mock):
        self._spark = spark_mock
        self._options = {}

    def option(self, key, value):
        self._options[key] = value
        return self

    def options(self, **kwargs):
        self._options.update(kwargs)
        return self

    def table(self, table_name):
        df = self._spark._catalog.read_table(table_name, self._options)
        return DataFrameWrapper(self._spark, df)
