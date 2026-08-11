"""Wrapper so GroupedData aggregations return DataFrameWrapper."""

from pyspark.sql import DataFrame


class GroupedDataWrapper:
    def __init__(self, spark_mock, grouped):
        self._spark = spark_mock
        self._grouped = grouped

    def __getattr__(self, name):
        attr = getattr(self._grouped, name)
        if callable(attr):
            def wrapper(*args, **kwargs):
                result = attr(*args, **kwargs)
                if isinstance(result, DataFrame):
                    from .data_frame_wrapper import DataFrameWrapper

                    return DataFrameWrapper(self._spark, result)
                return result

            return wrapper
        return attr
