import sys
import os

# Ensure src is on the path so `testbricks` can be imported during pytest collection.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import pytest
import shutil

from testbricks.spark_proxy import SparkProxy


TEST_DIR = "tests/data"


@pytest.fixture(scope="session")
def spark_session():
    """Single SparkProxy instance shared across the test session."""
    return SparkProxy(TEST_DIR)


@pytest.fixture
def spark(spark_session):
    """Lightweight alias so tests can request a fresh reference."""
    return spark_session


@pytest.fixture
def temp_spark(tmp_path):
    """Provide a SparkProxy pointing at an isolated temp directory."""
    base = tmp_path / "spark_data"
    base.mkdir()
    yield SparkProxy(str(base))


def _make_df(spark_proxy, rows, columns):
    """Create a DataFrameWrapper from local data without using parallelize."""
    from testbricks.data_frame_wrapper import DataFrameWrapper
    spark_df = spark_proxy._spark_session.createDataFrame(rows, schema=columns)
    return DataFrameWrapper(spark_proxy, spark_df)


class TestReadTable:
    def test_read_table_returns_expected_row_count(self, spark):
        df = spark.read.option("header", "true").table("f1_data.drivers")
        assert df.count() == 21, "DataFrame should have 21 rows"

    def test_read_table_defaults_header_and_infers_schema(self, spark):
        df = spark.read.table("f1_data.drivers")
        assert df.count() == 21
        assert "Abbreviation" in df.columns

    def test_read_table_invalid_table_name_raises(self, spark):
        with pytest.raises(ValueError, match="Invalid table name format"):
            spark.read.table("drivers")

    def test_read_table_missing_file_raises(self, spark):
        with pytest.raises(Exception):
            spark.read.option("header", "true").table("f1_data.missing_table")


class TestReadSql:
    def test_read_sql_returns_distinct_values(self, spark):
        df = spark.sql("SELECT DISTINCT Abbreviation FROM f1_data_drivers")
        assert df.count() == 21, "All 21 driver abbreviations should be distinct"

    def test_read_sql_select_all_returns_all_rows(self, spark):
        df = spark.sql("SELECT * FROM f1_data_drivers")
        assert df.count() == 21

    def test_read_sql_dotted_table_name(self, spark):
        df = spark.sql("SELECT * FROM f1_data.drivers")
        assert df.count() == 21

    def test_read_sql_backticks_and_three_part_name(self, spark):
        df = spark.sql("SELECT * FROM `f1_data`.`drivers`")
        assert df.count() == 21
        three_part = spark.sql("SELECT * FROM hive_metastore.f1_data.drivers")
        assert three_part.count() == 21

    def test_read_sql_preserves_decimal_literal(self, spark):
        df = spark.sql("SELECT 1.5 AS v")
        assert df.collect()[0].v == 1.5

    def test_sql_maintenance_commands_are_noops(self, spark):
        df = spark.sql("OPTIMIZE f1_data.drivers")
        assert df.count() == 0
        assert spark.sql("REFRESH TABLE f1_data.drivers").count() == 0


class TestSparkTableAndCreate:
    def test_table_alias_and_three_part_read(self, spark):
        df = spark.table("f1_data.drivers")
        assert df.count() == 21
        assert spark.table("main.f1_data.drivers").count() == 21

    def test_range_and_spark_context(self, temp_spark):
        df = temp_spark.range(3)
        assert df.count() == 3
        assert temp_spark.sparkContext is not None

    def test_create_dataframe_save_as_table(self, temp_spark):
        df = temp_spark.createDataFrame([("Alice", 30)], ["Name", "Age"])
        df.write.mode("overwrite").saveAsTable("default.people")
        result = temp_spark.sql("SELECT * FROM default.people")
        assert result.count() == 1
        assert result.collect()[0].Name == "Alice"

    def test_groupby_agg_save_as_table(self, temp_spark):
        df = temp_spark.createDataFrame(
            [("a", 1), ("a", 2), ("b", 3)],
            ["n", "v"],
        )
        aggregated = df.groupBy("n").sum("v")
        aggregated.write.mode("overwrite").saveAsTable("default.totals")

        csv_path = os.path.join(temp_spark._base_path, "default", "totals.csv")
        assert os.path.exists(csv_path)
        result = temp_spark.sql("SELECT * FROM default.totals")
        assert result.count() == 2


class TestDeltaWriteShims:
    def test_format_partition_by_overwrite_schema_save_as_table(self, temp_spark):
        df = temp_spark.createDataFrame(
            [("Alice", 30, "2024-01-01")],
            ["Name", "Age", "dt"],
        )
        df.write.format("delta").mode("overwrite").option(
            "overwriteSchema", "true"
        ).partitionBy("dt").saveAsTable("silver.people")

        csv_path = os.path.join(temp_spark._base_path, "silver", "people.csv")
        assert os.path.exists(csv_path)
        result = temp_spark.sql("SELECT * FROM silver.people")
        assert result.count() == 1
        assert set(result.columns) == {"Name", "Age", "dt"}

    def test_read_format_delta_table(self, spark):
        df = spark.read.format("delta").table("f1_data.drivers")
        assert df.count() == 21

    def test_save_as_table_three_part_name(self, temp_spark):
        df = temp_spark.createDataFrame([("Bob", 25)], ["Name", "Age"])
        df.write.mode("overwrite").saveAsTable("main.default.people")
        assert os.path.exists(
            os.path.join(temp_spark._base_path, "default", "people.csv")
        )
        assert temp_spark.sql("SELECT * FROM default.people").count() == 1


class TestWriteTable:
    def test_save_as_table_creates_temp_view_and_csv(self, temp_spark):
        df = _make_df(
            temp_spark,
            [("Alice", 30), ("Bob", 25), ("Charlie", 35)],
            ["Name", "Age"],
        )

        df.write.mode("overwrite").saveAsTable("default.sample_data")
        result = temp_spark.sql("SELECT * FROM default_sample_data")

        assert result.count() == 3, "Saved table should contain 3 rows"
        assert set(result.columns) == {"Name", "Age"}

    def test_save_as_table_invalid_table_name_raises(self, temp_spark):
        df = _make_df(temp_spark, [("x", 1)], ["Name", "Age"])
        with pytest.raises(ValueError, match="Invalid table name format"):
            df.write.saveAsTable("sample_data")

    def test_save_as_table_creates_csv_file(self, temp_spark):
        df = _make_df(temp_spark, [("A", 1)], ["c1", "c2"])
        df.write.saveAsTable("schema1.table1")

        csv_path = os.path.join(temp_spark._base_path, "schema1", "table1.csv")
        assert os.path.exists(csv_path)

    def test_save_as_table_append_to_missing_table_creates_rows(self, temp_spark):
        df = _make_df(temp_spark, [("Alice", 30)], ["Name", "Age"])
        df.write.mode("append").saveAsTable("default.people")

        result = temp_spark.sql("SELECT * FROM default_people")
        assert result.count() == 1
        assert {row.Name for row in result.collect()} == {"Alice"}

    def test_save_as_table_append_adds_rows_to_existing_table(self, temp_spark):
        first = _make_df(temp_spark, [("Alice", 30), ("Bob", 25)], ["Name", "Age"])
        first.write.mode("overwrite").saveAsTable("default.people")

        second = _make_df(temp_spark, [("Charlie", 35)], ["Name", "Age"])
        second.write.mode("append").saveAsTable("default.people")

        result = temp_spark.sql("SELECT * FROM default_people")
        assert result.count() == 3
        assert {row.Name for row in result.collect()} == {"Alice", "Bob", "Charlie"}

        read_back = temp_spark.read.option("header", "true").table("default.people")
        assert read_back.count() == 3

    def test_save_as_table_overwrite_still_replaces_rows(self, temp_spark):
        first = _make_df(temp_spark, [("Alice", 30), ("Bob", 25)], ["Name", "Age"])
        first.write.mode("overwrite").saveAsTable("default.people")

        second = _make_df(temp_spark, [("Charlie", 35)], ["Name", "Age"])
        second.write.mode("overwrite").saveAsTable("default.people")

        result = temp_spark.sql("SELECT * FROM default_people")
        assert result.count() == 1
        assert {row.Name for row in result.collect()} == {"Charlie"}

    def test_save_as_table_default_mode_still_replaces_rows(self, temp_spark):
        first = _make_df(temp_spark, [("Alice", 30)], ["Name", "Age"])
        first.write.saveAsTable("default.people")

        second = _make_df(temp_spark, [("Bob", 25)], ["Name", "Age"])
        second.write.saveAsTable("default.people")

        result = temp_spark.sql("SELECT * FROM default_people")
        assert result.count() == 1
        assert {row.Name for row in result.collect()} == {"Bob"}

    def test_save_as_table_append_schema_mismatch_raises(self, temp_spark):
        first = _make_df(temp_spark, [("Alice", 30)], ["Name", "Age"])
        first.write.mode("overwrite").saveAsTable("default.people")

        second = _make_df(temp_spark, [("Charlie",)], ["Name"])
        with pytest.raises(ValueError, match="schema mismatch"):
            second.write.mode("append").saveAsTable("default.people")

    def test_save_as_table_error_mode_raises_when_table_exists(self, temp_spark):
        from pyspark.sql.utils import AnalysisException

        first = _make_df(temp_spark, [("Alice", 30)], ["Name", "Age"])
        first.write.mode("overwrite").saveAsTable("default.people")

        second = _make_df(temp_spark, [("Bob", 25)], ["Name", "Age"])
        with pytest.raises(AnalysisException, match="default.people"):
            second.write.mode("error").saveAsTable("default.people")
        with pytest.raises(AnalysisException, match="already exists"):
            second.write.mode("errorIfExists").saveAsTable("default.people")

        result = temp_spark.sql("SELECT * FROM default.people")
        assert result.count() == 1
        assert result.collect()[0].Name == "Alice"

    def test_save_as_table_ignore_mode_skips_existing_table(self, temp_spark):
        first = _make_df(temp_spark, [("Alice", 30)], ["Name", "Age"])
        first.write.mode("overwrite").saveAsTable("default.people")
        csv_path = os.path.join(temp_spark._base_path, "default", "people.csv")
        mtime_before = os.path.getmtime(csv_path)

        second = _make_df(temp_spark, [("Bob", 25)], ["Name", "Age"])
        second.write.mode("ignore").saveAsTable("default.people")

        assert os.path.getmtime(csv_path) == mtime_before
        result = temp_spark.sql("SELECT * FROM default.people")
        assert result.count() == 1
        assert result.collect()[0].Name == "Alice"

    def test_save_as_table_error_and_ignore_create_missing_table(self, temp_spark):
        error_df = _make_df(temp_spark, [("Alice", 30)], ["Name", "Age"])
        error_df.write.mode("error").saveAsTable("default.from_error")
        assert temp_spark.sql("SELECT * FROM default.from_error").count() == 1

        ignore_df = _make_df(temp_spark, [("Bob", 25)], ["Name", "Age"])
        ignore_df.write.mode("IGNORE").saveAsTable("default.from_ignore")
        assert temp_spark.sql("SELECT * FROM default.from_ignore").count() == 1

    def test_save_as_table_unknown_mode_raises(self, temp_spark):
        df = _make_df(temp_spark, [("Alice", 30)], ["Name", "Age"])
        with pytest.raises(ValueError, match="Unknown save mode"):
            df.write.mode("upsert").saveAsTable("default.people")


class TestInsertInto:
    def test_insert_into_appends_to_existing_table(self, temp_spark):
        first = _make_df(temp_spark, [("Alice", 30)], ["Name", "Age"])
        first.write.mode("overwrite").saveAsTable("default.people")

        second = _make_df(temp_spark, [("Bob", 25)], ["Name", "Age"])
        second.write.insertInto("default.people")

        result = temp_spark.sql("SELECT * FROM default.people")
        assert result.count() == 2
        assert {row.Name for row in result.collect()} == {"Alice", "Bob"}

    def test_insert_into_overwrite_kwarg_replaces_rows(self, temp_spark):
        first = _make_df(temp_spark, [("Alice", 30), ("Bob", 25)], ["Name", "Age"])
        first.write.mode("overwrite").saveAsTable("default.people")

        second = _make_df(temp_spark, [("Charlie", 35)], ["Name", "Age"])
        second.write.insertInto("default.people", overwrite=True)

        result = temp_spark.sql("SELECT * FROM default.people")
        assert result.count() == 1
        assert result.collect()[0].Name == "Charlie"

    def test_insert_into_honors_writer_overwrite_mode(self, temp_spark):
        first = _make_df(temp_spark, [("Alice", 30)], ["Name", "Age"])
        first.write.mode("overwrite").saveAsTable("default.people")

        second = _make_df(temp_spark, [("Dana", 40)], ["Name", "Age"])
        second.write.mode("overwrite").insertInto("default.people")

        result = temp_spark.sql("SELECT * FROM default.people")
        assert result.count() == 1
        assert result.collect()[0].Name == "Dana"

    def test_insert_into_missing_table_raises(self, temp_spark):
        from pyspark.sql.utils import AnalysisException

        df = _make_df(temp_spark, [("Alice", 30)], ["Name", "Age"])
        with pytest.raises(AnalysisException, match="TABLE_OR_VIEW_NOT_FOUND"):
            df.write.insertInto("default.missing")


class TestWriteTransformedTable:
    def test_write_transformed_table_creates_expected_csv(self, spark):
        # Uses the shared spark fixture because the source table lives in tests/data.
        df = spark.read.option("header", "true").table("f1_data.drivers")
        uk = df.filter("Country = 'United Kingdom'") \
               .select("Abbreviation") \
               .distinct()
        uk.write.mode("overwrite").saveAsTable("f1_data.uk_drivers")

        csv_path = os.path.join(spark._base_path, "f1_data", "uk_drivers.csv")
        assert os.path.exists(csv_path)

        result = spark.read.option("header", "true").table("f1_data.uk_drivers")
        assert result.count() == 4
        assert set(result.columns) == {"Abbreviation"}
        assert set(row.Abbreviation for row in result.collect()) == {"NOR", "RUS", "HAM", "BEA"}

    @pytest.mark.skipif(sys.platform == "win32", reason="Native Spark CSV writer requires Hadoop winutils on Windows")
    def test_write_csv_with_mode_and_options(self, temp_spark):
        df = _make_df(temp_spark, [(1,), (2,)], ["id"])
        df.write.mode("overwrite").option("header", "false").csv("output/no_header")

        output_dir = os.path.join(temp_spark._base_path, "output", "no_header")
        assert os.path.isdir(output_dir)
        csv_files = [f for f in os.listdir(output_dir) if f.endswith(".csv")]
        assert len(csv_files) >= 1
        assert all(f.startswith("part-") for f in csv_files)


class TestDataFrameReader:
    def test_option_and_options_chain(self, temp_spark):
        reader = temp_spark.read.option("header", "true").options(delimiter=",")
        assert reader._options == {"header": "true", "delimiter": ","}


class TestDataFrameWriter:
    def test_writer_mode_option_chain(self, temp_spark):
        df = _make_df(temp_spark, [(1,)], ["id"])
        writer = df.write.format("delta").mode("overwrite").partitionBy("id").option(
            "header", "true"
        )
        assert writer._mode == "overwrite"
        assert writer._format == "delta"
        assert writer._partition_by == ("id",)
        assert writer._options == {"header": "true"}


class TestSparkProxyLifecycle:
    def test_base_path_unchanged(self, spark):
        assert spark._base_path == TEST_DIR

    def test_tables_loaded_as_temp_views(self, spark):
        # catalog.listTables() triggers Hadoop winutils on Windows; rely on sql instead.
        df = spark.sql("SELECT * FROM f1_data_drivers")
        assert df.count() == 21
