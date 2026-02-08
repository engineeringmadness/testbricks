import os


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
            raise ValueError(f"Invalid table name format. Expected 'schema_name.table_name', got '{table_name}'")
        
        schema_name, table = parts
        self._dataframe.createOrReplaceTempView(f"{schema_name}_{table}")
        
        schema_path = os.path.join(self._spark._base_path, schema_name)
        if not os.path.exists(schema_path):
            os.makedirs(schema_path)
        
        csv_path = os.path.join(schema_path, f"{table}.csv")
        
        pandas_df = self._dataframe.toPandas()
        
        write_options = {'index': False}
        if 'header' in self._options:
            write_options['header'] = self._options['header'].lower() == 'true'
        else:
            write_options['header'] = True
        
        pandas_df.to_csv(csv_path, **write_options)
