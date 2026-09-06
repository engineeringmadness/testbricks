import logging

from pyspark.sql import DataFrame
from pyspark.sql.group import GroupedData
from pyspark.sql.utils import AnalysisException

from .catalog import TableIdentifier, normalize_csv_options, option_flag

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


def _flatten_columns(cols):
    flattened = []
    for col in cols:
        if isinstance(col, (list, tuple)):
            flattened.extend(col)
        else:
            flattened.append(col)
    return flattened


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
        df = self._spark.read_table(ident, merged)
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

    @classmethod
    def _for_table_write(
        cls,
        spark_proxy,
        dataframe,
        *,
        mode,
        format=None,
        options=None,
        partition_by=(),
    ):
        writer = cls(spark_proxy, dataframe)
        writer._mode = mode
        writer._format = format
        writer._partition_by = tuple(partition_by)
        if options:
            writer._options.update(options)
        return writer

    def partitionBy(self, *cols):
        self._partition_by = tuple(_flatten_columns(cols))
        return self

    def bucketBy(self, numBuckets, *cols):
        """Accepted no-op: Hive-style bucketing is not simulated locally."""
        flattened = _flatten_columns(cols)
        self._bucket_by = (numBuckets, tuple(flattened))
        logger.info(
            "bucketBy(%s, %s) is accepted but not simulated",
            numBuckets,
            flattened,
        )
        return self

    def sortBy(self, col, *cols):
        """Accepted no-op: sortBy is not simulated locally."""
        self._sort_by = tuple([col, *cols])
        logger.info("sortBy(%s) is accepted but not simulated", [col, *cols])
        return self

    def mode(self, save_mode):
        self._mode = save_mode
        return self

    def csv(self, path):
        self._native_writer().csv(self._spark.full_path(path))

    def parquet(self, path):
        self._native_writer().parquet(self._spark.full_path(path))

    def json(self, path):
        self._native_writer().json(self._spark.full_path(path))

    def save(self, path, format=None, **options):
        if options:
            self._options.update(options)
        fmt = format or self._format or "parquet"
        resolved = _resolve_file_format(fmt)
        full_path = self._spark.full_path(path)
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

    def _save_to_table(self, ident, mode):
        self._spark.save_table(
            ident,
            self._dataframe,
            mode=mode,
            header=self._header_flag(),
            replace_where=self._replace_where(),
            csv_options=normalize_csv_options(self._options),
            overwrite_schema=option_flag(self._options, "overwriteSchema"),
            merge_schema=option_flag(self._options, "mergeSchema"),
        )

    def saveAsTable(self, table_name):
        ident = TableIdentifier.parse(table_name)
        self._validate_partition_columns()
        self._save_to_table(ident, self._mode)

    def insertInto(self, table_name, overwrite=False):
        """Append or overwrite rows in an existing table (Spark DataFrameWriter.insertInto)."""
        ident = TableIdentifier.parse(table_name)
        self._validate_partition_columns()
        if not self._spark.catalog.exists(ident):
            raise AnalysisException(
                f"[TABLE_OR_VIEW_NOT_FOUND] The table or view {ident} cannot be found. "
                "Verify the table exists before calling insertInto."
            )
        writer_mode = str(self._mode).strip().lower() if self._mode else None
        if overwrite or writer_mode == "overwrite":
            mode = "overwrite"
        else:
            mode = "append"
        self._save_to_table(ident, mode)


class DataFrameWriterV2(_IoBuilder):
    """Minimal Spark DataFrameWriterV2 façade over ``saveAsTable``.

    Implements ``create`` / ``replace`` / ``append``. Full V2 verbs such as
    ``createOrReplace`` and ``overwritePartitions`` raise ``NotImplementedError``
    with a migration hint to ``saveAsTable``.
    """

    def __init__(self, spark_proxy, dataframe, table_name):
        super().__init__()
        self._spark = spark_proxy
        self._dataframe = dataframe
        self._table_name = table_name
        self._partitioned_by = ()

    def using(self, provider):
        self._format = provider
        return self

    def tableProperty(self, property, value):
        return self

    def partitionedBy(self, *cols):
        self._partitioned_by = tuple(_flatten_columns(cols))
        return self

    def _writer(self, mode):
        return DataFrameWriter._for_table_write(
            self._spark,
            self._dataframe,
            mode=mode,
            format=self._format,
            options=self._options,
            partition_by=self._partitioned_by,
        )

    def create(self):
        self._writer("error").saveAsTable(self._table_name)

    def replace(self):
        options = {"overwriteSchema": "true", **self._options}
        DataFrameWriter._for_table_write(
            self._spark,
            self._dataframe,
            mode="overwrite",
            format=self._format,
            options=options,
            partition_by=self._partitioned_by,
        ).saveAsTable(self._table_name)

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
