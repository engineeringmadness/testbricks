from pyspark.sql import SparkSession

from .catalog import CatalogFacade, TableCatalog, is_maintenance_noop, rewrite_from_join_identifiers
from .data_frame_wrapper import DataFrameReader, DataFrameWrapper


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

    def _wrap(self, result):
        return DataFrameWrapper(self, result)

    def createDataFrame(self, *args, **kwargs):
        return self._wrap(self._spark_session.createDataFrame(*args, **kwargs))

    def range(self, *args, **kwargs):
        return self._wrap(self._spark_session.range(*args, **kwargs))

    def sql(self, query):
        if is_maintenance_noop(query):
            empty = self._spark_session.createDataFrame([], schema="status string")
            return self._wrap(empty)
        return self._wrap(self._spark_session.sql(rewrite_from_join_identifiers(query)))

    def parallelize(self, c, numSlices=None):
        return self._wrap(self._spark_session.sparkContext.parallelize(c, numSlices))

    def _get_full_path(self, relative_path):
        return self._catalog.full_path(relative_path)
