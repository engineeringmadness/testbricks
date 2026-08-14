from .catalog import TableIdentifier


class DataFrameWriter:
    def __init__(self, spark_mock, dataframe):
        self._spark = spark_mock
        self._dataframe = dataframe
        self._options = {}
        self._mode = None
        self._format = None
        self._partition_by = ()

    def format(self, source):
        self._format = source
        return self

    def partitionBy(self, *cols):
        self._partition_by = cols
        return self

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
        ident = TableIdentifier.parse(table_name)

        write_header = True
        if "header" in self._options:
            write_header = self._options["header"].lower() == "true"

        self._spark._catalog.save_dataframe(
            ident,
            self._dataframe,
            mode=self._mode,
            header=write_header,
        )
