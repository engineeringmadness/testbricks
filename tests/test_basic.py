import sys
import os

# Ensure src is on the path so `mock` can be imported during pytest collection.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import pytest
import shutil

from mock.spark_mock import SparkMock


TEST_DIR = "tests/data"


@pytest.fixture(scope="session")
def spark_session():
    """Single SparkMock instance shared across the test session."""
    return SparkMock(TEST_DIR)


@pytest.fixture
def spark(spark_session):
    """Lightweight alias so tests can request a fresh reference."""
    return spark_session


@pytest.fixture
def temp_spark(tmp_path):
    """Provide a SparkMock pointing at an isolated temp directory."""
    base = tmp_path / "spark_data"
    base.mkdir()
    yield SparkMock(str(base))


def _make_df(spark_mock, rows, columns):
    """Create a DataFrameWrapper from local data without using parallelize."""
    from mock.data_frame_wrapper import DataFrameWrapper
    spark_df = spark_mock._spark_session.createDataFrame(rows, schema=columns)
    return DataFrameWrapper(spark_mock, spark_df)


class TestReadTable:
    def test_read_table_returns_expected_row_count(self, spark):
        df = spark.read.option("header", "true").table("f1_data.drivers")
        assert df.count() == 21, "DataFrame should have 21 rows"

    def test_read_table_defaults_to_header_true(self, spark):
        df = spark.read.table("f1_data.drivers")
        # Catalog defaults header=true / inferSchema=true when options omitted.
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

    def test_write_table_alias(self, temp_spark):
        df = _make_df(temp_spark, [("A", 1)], ["c1", "c2"])
        df.write.mode("overwrite").table("schema1.table1")
        assert temp_spark.read.table("schema1.table1").count() == 1

    def test_insert_into_appends(self, temp_spark):
        df = _make_df(temp_spark, [("A", 1)], ["c1", "c2"])
        df.write.mode("overwrite").saveAsTable("schema1.table1")
        df.write.insertInto("schema1.table1")
        assert temp_spark.read.table("schema1.table1").count() == 2


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
        writer = df.write.mode("overwrite").option("header", "true")
        assert writer._mode == "overwrite"
        assert writer._options == {"header": "true"}


class TestSparkMockLifecycle:
    def test_base_path_unchanged(self, spark):
        assert spark._base_path == TEST_DIR

    def test_tables_loaded_as_temp_views(self, spark):
        # catalog.listTables() triggers Hadoop winutils on Windows; rely on sql instead.
        df = spark.sql("SELECT * FROM f1_data_drivers")
        assert df.count() == 21
