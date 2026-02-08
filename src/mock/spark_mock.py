from pyspark.sql import SparkSession
from .data_frame_reader import DataFrameReader, MockDataFrame
import os


class SparkMock:
    def __init__(self, base_path):
        self._base_path = base_path
        self._spark_session = SparkSession.builder.appName("SparkMock").getOrCreate()
        self._read = None
        self._load_tables_into_temp_views()
    
    @property
    def read(self):
        if self._read is None:
            self._read = DataFrameReader(self)
        return self._read
    
    def sql(self, query):
        modified_query = query.replace('.', '_')
        df = self._spark_session.sql(modified_query)
        return MockDataFrame(self, df)
    
    def _get_full_path(self, relative_path):
        return os.path.join(self._base_path, relative_path)
    
    def _load_tables_into_temp_views(self):
        if not os.path.exists(self._base_path):
            return
        
        for folder_name in os.listdir(self._base_path):
            print("traversing Folder Name:", folder_name)
            folder_path = os.path.join(self._base_path, folder_name)
            
            if not os.path.isdir(folder_path):
                continue
            
            for filename in os.listdir(folder_path):
                if filename.endswith('.csv'):
                    file_name_without_extension = os.path.splitext(filename)[0]
                    view_name = f"{folder_name}_{file_name_without_extension}"
                    print("Creating Temp View:", view_name)
                    csv_path = os.path.join(folder_path, filename)
                    
                    df = self._spark_session.read.csv(csv_path, header=True, inferSchema=True)
                    
                    df.createOrReplaceTempView(view_name)
