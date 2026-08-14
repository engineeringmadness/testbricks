from pyspark.sql import DataFrame
from pyspark.sql.group import GroupedData

from .data_frame_writer import DataFrameWriter


def _wrap_spark_result(spark_mock, result):
    if isinstance(result, DataFrame):
        return DataFrameWrapper(spark_mock, result)
    if isinstance(result, GroupedData):
        return GroupedDataWrapper(spark_mock, result)
    return result


class GroupedDataWrapper:
    def __init__(self, spark_mock, grouped):
        self._spark = spark_mock
        self._grouped = grouped

    def __getattr__(self, name):
        attr = getattr(self._grouped, name)
        if callable(attr):
            def wrapper(*args, **kwargs):
                return _wrap_spark_result(self._spark, attr(*args, **kwargs))

            return wrapper
        return attr


class DataFrameWrapper:
    def __init__(self, spark_mock, dataframe):
        self._spark = spark_mock
        self._dataframe = dataframe
        self._write = None

    def __getattr__(self, name):
        attr = getattr(self._dataframe, name)
        if callable(attr):
            def wrapper(*args, **kwargs):
                return _wrap_spark_result(self._spark, attr(*args, **kwargs))

            return wrapper
        return attr

    @property
    def write(self):
        if self._write is None:
            self._write = DataFrameWriter(self._spark, self._dataframe)
        return self._write
