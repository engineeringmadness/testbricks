from pyspark.sql import SparkSession

from .catalog import CatalogFacade, TableCatalog, is_maintenance_noop, rewrite_from_join_identifiers
from .data_frame_reader import DataFrameReader
from .data_frame_wrapper import DataFrameWrapper


class SparkMock:
    def __init__(self, base_path):
        self._base_path = base_path
        self._spark_session = SparkSession.builder.appName("SparkMock").getOrCreate()
        self._catalog = TableCatalog(self._spark_session, base_path)
        self._spark_catalog = CatalogFacade(self._catalog)
        self._read = None
        self._catalog.load_all()

    @property
    def catalog(self):
        return self._spark_catalog

    @property
    def read(self):
        if self._read is None:
            self._read = DataFrameReader(self)
        return self._read

    @property
    def sparkContext(self):
        return self._spark_session.sparkContext

    def table(self, tableName):
        return self.read.table(tableName)

    def createDataFrame(self, *args, **kwargs):
        return DataFrameWrapper(
            self, self._spark_session.createDataFrame(*args, **kwargs)
        )

    def range(self, *args, **kwargs):
        return DataFrameWrapper(self, self._spark_session.range(*args, **kwargs))

    def sql(self, query):
        if is_maintenance_noop(query):
            empty = self._spark_session.createDataFrame([], schema="status string")
            return DataFrameWrapper(self, empty)

        modified_query = rewrite_from_join_identifiers(query)
        df = self._spark_session.sql(modified_query)
        return DataFrameWrapper(self, df)

    def parallelize(self, c, numSlices=None):
        return DataFrameWrapper(
            self, self._spark_session.sparkContext.parallelize(c, numSlices)
        )

    def _get_full_path(self, relative_path):
        return self._catalog.full_path(relative_path)
