from pyspark.sql import SparkSession
from .data_frame_reader import DataFrameReader
from .data_frame_wrapper import DataFrameWrapper
from .sql_gateway import SqlGateway
from .table_catalog import TableCatalog


class SparkMock:
    def __init__(self, base_path):
        self._base_path = base_path
        self._spark_session = SparkSession.builder.appName("SparkMock").getOrCreate()
        self._catalog = TableCatalog(base_path, self._spark_session)
        self._sql_gateway = SqlGateway(self)
        self._read = None
        self._catalog.load_all()

    @property
    def read(self):
        if self._read is None:
            self._read = DataFrameReader(self)
        return self._read

    def sql(self, query):
        return self._sql_gateway.execute(query)

    def parallelize(self, c, numSlices=None):
        return DataFrameWrapper(
            self, self._spark_session.sparkContext.parallelize(c, numSlices)
        )

    def _get_full_path(self, relative_path):
        return self._catalog.get_full_path(relative_path)
