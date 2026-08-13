import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import pytest

from testbricks.catalog import (
    InvalidTableNameError,
    SchemaMismatchError,
    TableCatalog,
    TableIdentifier,
)
from testbricks.spark_mock import SparkMock


class TestTableIdentifier:
    def test_parse_valid_name(self):
        ident = TableIdentifier.parse("bronze.customers")
        assert ident.schema == "bronze"
        assert ident.table == "customers"
        assert ident.view_name == "bronze_customers"
        assert ident.relative_csv_path == "bronze/customers.csv"
        assert str(ident) == "bronze.customers"

    def test_parse_invalid_names(self):
        for bad in ("customers", "a.b.c", "", ".table", "schema."):
            with pytest.raises(InvalidTableNameError, match="Invalid table name format"):
                TableIdentifier.parse(bad)


class TestTableCatalog:
    @pytest.fixture
    def catalog(self, tmp_path):
        from pyspark.sql import SparkSession

        spark = SparkSession.builder.appName("TableCatalogTest").getOrCreate()
        base = tmp_path / "data"
        base.mkdir()
        return TableCatalog(spark, str(base)), str(base)

    def test_load_all_registers_temp_views(self, tmp_path):
        base = tmp_path / "data"
        schema = base / "f1_data"
        schema.mkdir(parents=True)
        (schema / "drivers.csv").write_text("Name,Age\nAlice,30\nBob,25\n")

        spark_mock = SparkMock(str(base))
        result = spark_mock.sql("SELECT * FROM f1_data_drivers")
        assert result.count() == 2

    def test_path_for_and_exists(self, catalog):
        cat, base = catalog
        ident = TableIdentifier.parse("schema1.table1")
        assert cat.path_for(ident) == os.path.join(base, "schema1", "table1.csv")
        assert not cat.exists(ident)

        cat.ensure_schema_dir(ident)
        with open(cat.path_for(ident), "w", encoding="utf-8") as handle:
            handle.write("id\n1\n")
        assert cat.exists(ident)

    def test_save_dataframe_overwrite_and_append(self, catalog):
        cat, _base = catalog
        spark = cat._spark
        ident = TableIdentifier.parse("default.people")

        first = spark.createDataFrame([("Alice", 30), ("Bob", 25)], ["Name", "Age"])
        cat.save_dataframe(ident, first, mode="overwrite")
        assert cat.exists(ident)
        assert spark.sql("SELECT * FROM default_people").count() == 2

        second = spark.createDataFrame([("Charlie", 35)], ["Name", "Age"])
        cat.save_dataframe(ident, second, mode="append")
        assert spark.sql("SELECT * FROM default_people").count() == 3

    def test_save_dataframe_append_schema_mismatch(self, catalog):
        cat, _base = catalog
        spark = cat._spark
        ident = TableIdentifier.parse("default.people")

        first = spark.createDataFrame([("Alice", 30)], ["Name", "Age"])
        cat.save_dataframe(ident, first, mode="overwrite")

        second = spark.createDataFrame([("Charlie",)], ["Name"])
        with pytest.raises(SchemaMismatchError, match="schema mismatch"):
            cat.save_dataframe(ident, second, mode="append")
