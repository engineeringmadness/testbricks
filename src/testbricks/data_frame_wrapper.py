import logging

from pyspark.sql import DataFrame
from pyspark.sql.group import GroupedData
from pyspark.sql.utils import AnalysisException

from .catalog import TableIdentifier

logger = logging.getLogger(__name__)


_FILE_FORMATS = {
    "parquet": "parquet",
    "delta": "parquet",
    "json": "json",
    "csv": "csv",
}


def _resolve_file_format(fmt) -> str:
    if fmt is None:
        return "parquet"
    resolved = _FILE_FORMATS.get(str(fmt).strip().lower())
    if resolved is None:
        raise ValueError(
            f"Unknown format '{fmt}' for file save(). "
            "Supported formats: parquet, json, csv, delta (delta is stored as parquet)."
        )
    return resolved


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
        self._bucket_by = None
        self._sort_by = ()

    def partitionBy(self, *cols):
        flattened = []
        for col in cols:
            if isinstance(col, (list, tuple)):
                flattened.extend(col)
            else:
                flattened.append(col)
        self._partition_by = tuple(flattened)
        return self

    def bucketBy(self, numBuckets, *cols):
        """Accepted no-op: Hive-style bucketing is not simulated locally."""
        flattened = []
        for col in cols:
            if isinstance(col, (list, tuple)):
                flattened.extend(col)
            else:
                flattened.append(col)
        self._bucket_by = (numBuckets, tuple(flattened))
        logger.info(
            "bucketBy(%s, %s) is accepted but not simulated",
            numBuckets,
            flattened,
        )
        return self

    def sortBy(self, col, *cols):
        """Accepted no-op: sortBy is not simulated locally."""
        flattened = [col, *cols]
        self._sort_by = tuple(flattened)
        logger.info("sortBy(%s) is accepted but not simulated", flattened)
        return self

    def mode(self, save_mode):
        self._mode = save_mode
        return self

    def csv(self, path):
        self._native_writer().csv(self._spark._get_full_path(path))

    def parquet(self, path):
        self._native_writer().parquet(self._spark._get_full_path(path))

    def json(self, path):
        self._native_writer().json(self._spark._get_full_path(path))

    def save(self, path, format=None, **options):
        if options:
            self._options.update(options)
        fmt = format or self._format or "parquet"
        resolved = _resolve_file_format(fmt)
        full_path = self._spark._get_full_path(path)
        writer = self._native_writer()
        if resolved == "csv":
            writer.csv(full_path)
        elif resolved == "json":
            writer.json(full_path)
        else:
            writer.parquet(full_path)

    def _native_writer(self):
        writer = self._dataframe.write
        if self._mode:
            writer = writer.mode(self._mode)
        if self._partition_by:
            writer = writer.partitionBy(*self._partition_by)
        for key, value in self._options.items():
            writer = writer.option(key, value)
        return writer

    def _validate_partition_columns(self):
        if not self._partition_by:
            return
        available = list(self._dataframe.columns)
        missing = [col for col in self._partition_by if col not in available]
        if missing:
            raise AnalysisException(
                f"partitionBy columns {missing} do not exist in the DataFrame. "
                f"Available columns: {available}"
            )

    def _header_flag(self):
        return str(self._options.get("header", "true")).lower() == "true"

    def _replace_where(self):
        return self._options.get("replaceWhere") or self._options.get("replacewhere")

    def _csv_options(self):
        return {
            key: value
            for key, value in self._options.items()
            if key.lower()
            in {
                "delimiter",
                "sep",
                "quote",
                "escape",
                "nullvalue",
                "dateformat",
                "timestampformat",
                "header",
            }
        }

    def _option_flag(self, *names):
        wanted = {name.lower() for name in names}
        for key, value in self._options.items():
            if key.lower() in wanted:
                return str(value).lower() in {"true", "1", "yes"}
        return False

    def saveAsTable(self, table_name):
        ident = TableIdentifier.parse(table_name)
        self._validate_partition_columns()
        self._spark._catalog.save_dataframe(
            ident,
            self._dataframe,
            mode=self._mode,
            header=self._header_flag(),
            replace_where=self._replace_where(),
            csv_options=self._csv_options(),
            overwrite_schema=self._option_flag("overwriteSchema"),
            merge_schema=self._option_flag("mergeSchema"),
        )

    def insertInto(self, table_name, overwrite=False):
        """Append or overwrite rows in an existing table (Spark DataFrameWriter.insertInto)."""
        ident = TableIdentifier.parse(table_name)
        self._validate_partition_columns()
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
        self._spark._catalog.save_dataframe(
            ident,
            self._dataframe,
            mode=mode,
            header=self._header_flag(),
            replace_where=self._replace_where(),
            csv_options=self._csv_options(),
            overwrite_schema=self._option_flag("overwriteSchema"),
            merge_schema=self._option_flag("mergeSchema"),
        )


class DataFrameWriterV2:
    """Minimal Spark DataFrameWriterV2 façade over ``saveAsTable``.

    Implements ``create`` / ``replace`` / ``append``. Full V2 verbs such as
    ``createOrReplace`` and ``overwritePartitions`` raise ``NotImplementedError``
    with a migration hint to ``saveAsTable``.
    """

    def __init__(self, spark_proxy, dataframe, table_name):
        self._spark = spark_proxy
        self._dataframe = dataframe
        self._table_name = table_name
        self._options = {}
        self._partitioned_by = ()
        self._using = None

    def using(self, provider):
        self._using = provider
        return self

    def option(self, key, value):
        self._options[key] = value
        return self

    def options(self, **kwargs):
        self._options.update(kwargs)
        return self

    def tableProperty(self, property, value):
        return self

    def partitionedBy(self, *cols):
        flattened = []
        for col in cols:
            if isinstance(col, (list, tuple)):
                flattened.extend(col)
            else:
                flattened.append(col)
        self._partitioned_by = tuple(flattened)
        return self

    def _writer(self, mode):
        writer = DataFrameWriter(self._spark, self._dataframe)
        writer._mode = mode
        writer._format = self._using
        writer._partition_by = self._partitioned_by
        writer._options.update(self._options)
        return writer

    def create(self):
        self._writer("error").saveAsTable(self._table_name)

    def replace(self):
        writer = self._writer("overwrite")
        writer._options.setdefault("overwriteSchema", "true")
        writer.saveAsTable(self._table_name)

    def append(self):
        self._writer("append").saveAsTable(self._table_name)

    def createOrReplace(self):
        raise NotImplementedError(
            "writeTo(...).createOrReplace() is not implemented. "
            "Use df.write.mode('overwrite').option('overwriteSchema', 'true')"
            ".saveAsTable(...) instead."
        )

    def overwritePartitions(self):
        raise NotImplementedError(
            "writeTo(...).overwritePartitions() is not implemented. "
            "Use df.write.mode('overwrite').option('replaceWhere', predicate)"
            ".saveAsTable(...) instead."
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

    def writeTo(self, table):
        return DataFrameWriterV2(self._spark, self._dataframe, table)
