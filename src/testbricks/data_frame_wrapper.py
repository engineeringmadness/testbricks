from pyspark.sql import DataFrame
from .data_frame_writer import DataFrameWriter


class DataFrameWrapper:
    def __init__(self, spark_mock, dataframe):
        self._spark = spark_mock
        self._dataframe = dataframe
        self._write = None
    
    def __getattr__(self, name):
        attr = getattr(self._dataframe, name)
        if callable(attr):
            def wrapper(*args, **kwargs):
                result = attr(*args, **kwargs)
                if isinstance(result, DataFrame):
                    return DataFrameWrapper(self._spark, result)
                return result
            return wrapper
        return attr
    
    @property
    def write(self):
        if self._write is None:
            self._write = DataFrameWriter(self._spark, self._dataframe)
        return self._write
