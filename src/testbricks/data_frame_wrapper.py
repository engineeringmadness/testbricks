from pyspark.sql import DataFrame
from pyspark.sql.group import GroupedData
from pyspark.sql.utils import AnalysisException

from .catalog import TableIdentifier


class _IoBuilder:
    """Shared format/option chaining used by both reader and writer."""

    def __init__(self):
        self._options = {}
        self._format = None

    def format(self, source):
        self._format = source
        return self

    def option(self, key, value):
        self._options[key] = value
        return self

    def options(self, **kwargs):
        self._options.update(kwargs)
        return self


class DataFrameReader(_IoBuilder):
    def __init__(self, spark_proxy):
        super().__init__()
        self._spark = spark_proxy

    def table(self, table_name):
        ident = TableIdentifier.parse(table_name)
        merged = {"header": "true", "inferSchema": "true", **self._options}
        df = self._spark._catalog.read_csv(ident, merged)
        return DataFrameWrapper(self._spark, df)


class DataFrameWriter(_IoBuilder):
    def __init__(self, spark_proxy, dataframe):
        super().__init__()
        self._spark = spark_proxy
        self._dataframe = dataframe
        self._mode = None
        self._partition_by = ()

    def partitionBy(self, *cols):
        self._partition_by = cols
        return self

    def mode(self, save_mode):
        self._mode = save_mode
        return self

    def csv(self, path):
        writer = self._dataframe.write
        if self._mode:
            writer = writer.mode(self._mode)
        for key, value in self._options.items():
            writer = writer.option(key, value)
        writer.csv(self._spark._get_full_path(path))

    def saveAsTable(self, table_name):
        ident = TableIdentifier.parse(table_name)
        header = self._options.get("header", "true").lower() == "true"
        self._spark._catalog.save_dataframe(
            ident,
            self._dataframe,
            mode=self._mode,
            header=header,
        )

    def insertInto(self, table_name, overwrite=False):
        """Append or overwrite rows in an existing table (Spark DataFrameWriter.insertInto)."""
        ident = TableIdentifier.parse(table_name)
        if not self._spark._catalog.exists(ident):
            raise AnalysisException(
                f"[TABLE_OR_VIEW_NOT_FOUND] The table or view {ident} cannot be found. "
                "Verify the table exists before calling insertInto."
            )
        writer_mode = str(self._mode).strip().lower() if self._mode else None
        if overwrite or writer_mode == "overwrite":
            mode = "overwrite"
        else:
            mode = "append"
        header = self._options.get("header", "true").lower() == "true"
        self._spark._catalog.save_dataframe(
            ident,
            self._dataframe,
            mode=mode,
            header=header,
        )


def _wrap_spark_result(spark_proxy, result):
    if isinstance(result, DataFrame):
        return DataFrameWrapper(spark_proxy, result)
    if isinstance(result, GroupedData):
        return GroupedDataWrapper(spark_proxy, result)
    return result


def _proxy_callable(spark_proxy, target, name):
    attr = getattr(target, name)
    if not callable(attr):
        return attr

    def wrapper(*args, **kwargs):
        return _wrap_spark_result(spark_proxy, attr(*args, **kwargs))

    return wrapper


class GroupedDataWrapper:
    def __init__(self, spark_proxy, grouped):
        self._spark = spark_proxy
        self._grouped = grouped

    def __getattr__(self, name):
        return _proxy_callable(self._spark, self._grouped, name)


class DataFrameWrapper:
    def __init__(self, spark_proxy, dataframe):
        self._spark = spark_proxy
        self._dataframe = dataframe
        self._write = None

    def __getattr__(self, name):
        return _proxy_callable(self._spark, self._dataframe, name)

    @property
    def write(self):
        if self._write is None:
            self._write = DataFrameWriter(self._spark, self._dataframe)
        return self._write
