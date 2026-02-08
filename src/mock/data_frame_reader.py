from pyspark.sql import SparkSession
from .data_frame_writer import DataFrameWriter


class MockDataFrame:
    def __init__(self, spark_mock, dataframe):
        self._spark = spark_mock
        self._dataframe = dataframe
        self._write = None
    
    def __getattr__(self, name):
        return getattr(self._dataframe, name)
    
    @property
    def write(self):
        if self._write is None:
            self._write = DataFrameWriter(self._spark, self._dataframe)
        return self._write


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
        parts = table_name.split('.')
        if len(parts) != 2:
            raise ValueError(f"Invalid table name format. Expected 'schema_name.table_name', got '{table_name}'")
        
        schema_name, table = parts
        relative_path = f"{schema_name}/{table}.csv"
        csv_path = self._spark._get_full_path(relative_path)
        
        reader = self._spark._spark_session.read
        
        for key, value in self._options.items():
            reader = reader.option(key, value)
        
        df = reader.csv(csv_path)
        return MockDataFrame(self._spark, df)
