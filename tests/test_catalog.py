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
from testbricks.spark_proxy import SparkProxy


class TestTableIdentifier:
    def test_parse_valid_name(self):
        ident = TableIdentifier.parse("bronze.customers")
        assert ident.schema == "bronze"
        assert ident.table == "customers"
        assert ident.view_name == "bronze_customers"
        assert ident.relative_csv_path == "bronze/customers.csv"
        assert str(ident) == "bronze.customers"

    def test_parse_three_part_name(self):
        ident = TableIdentifier.parse("main.bronze.customers")
        assert ident.catalog == "main"
        assert ident.schema == "bronze"
        assert ident.table == "customers"
        assert ident.view_name == "bronze_customers"
        assert ident.relative_csv_path == "bronze/customers.csv"
        assert str(ident) == "main.bronze.customers"

    def test_parse_backticks(self):
        ident = TableIdentifier.parse("`bronze`.`customers`")
        assert ident.schema == "bronze"
        assert ident.table == "customers"
        assert ident.catalog is None

    def test_parse_invalid_names(self):
        for bad in ("customers", "a.b.c.d", "", ".table", "schema.", "a..b"):
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

        spark_proxy = SparkProxy(str(base))
        result = spark_proxy.sql("SELECT * FROM f1_data_drivers")
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


class TestSqlRewrite:
    def test_rewrites_from_and_join(self):
        from testbricks.catalog import rewrite_from_join_identifiers

        query = (
            "SELECT * FROM bronze.customers c "
            "JOIN silver.orders o ON c.id = o.customer_id"
        )
        rewritten = rewrite_from_join_identifiers(query)
        assert "FROM bronze_customers c" in rewritten
        assert "JOIN silver_orders o" in rewritten

    def test_rewrites_backticks_and_three_part_names(self):
        from testbricks.catalog import rewrite_from_join_identifiers

        query = "SELECT * FROM `bronze`.`customers` JOIN main.silver.orders"
        rewritten = rewrite_from_join_identifiers(query)
        assert "FROM bronze_customers" in rewritten
        assert "JOIN silver_orders" in rewritten

    def test_leaves_decimal_literals_unchanged(self):
        from testbricks.catalog import rewrite_from_join_identifiers

        query = "SELECT 1.5 AS v FROM bronze.customers"
        assert rewrite_from_join_identifiers(query) == "SELECT 1.5 AS v FROM bronze_customers"

    def test_leaves_view_names_unchanged(self):
        from testbricks.catalog import rewrite_from_join_identifiers

        query = "SELECT * FROM bronze_customers"
        assert rewrite_from_join_identifiers(query) == query

    def test_maintenance_noop_detection(self):
        from testbricks.catalog import is_maintenance_noop

        assert is_maintenance_noop("OPTIMIZE bronze.customers")
        assert is_maintenance_noop("  vacuum bronze.customers")
        assert is_maintenance_noop("REFRESH TABLE bronze.customers")
        assert is_maintenance_noop("ANALYZE TABLE bronze.customers COMPUTE STATISTICS")
        assert not is_maintenance_noop("SELECT * FROM bronze.customers")


class TestCatalogFacade:
    def test_table_exists_list_tables_and_databases(self, tmp_path):
        base = tmp_path / "data"
        spark_proxy = SparkProxy(str(base))
        df = spark_proxy.createDataFrame([("Alice", 30)], ["Name", "Age"])
        df.write.mode("overwrite").saveAsTable("default.people")

        assert spark_proxy.catalog.tableExists("default.people")
        assert spark_proxy.catalog.tableExists("main.default.people")
        assert spark_proxy.catalog.tableExists("people", "default")
        assert not spark_proxy.catalog.tableExists("default.missing")
        assert not spark_proxy.catalog.tableExists("people")

        tables = spark_proxy.catalog.listTables("default")
        assert [table.name for table in tables] == ["people"]
        assert tables[0].namespace == ["default"]

        databases = spark_proxy.catalog.listDatabases()
        assert "default" in [database.name for database in databases]

        filtered = spark_proxy.catalog.listTables(pattern="peo*")
        assert [table.name for table in filtered] == ["people"]
        assert [db.name for db in spark_proxy.catalog.listDatabases(pattern="def*")] == [
            "default"
        ]
        ident = TableIdentifier.parse("default.people")
        assert spark_proxy.catalog.exists(ident)

