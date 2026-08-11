import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import pytest
from pyspark.sql import SparkSession

from mock.table_catalog import TableCatalog


@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.master("local[1]").appName("catalog-tests").getOrCreate()


@pytest.fixture
def catalog(tmp_path, spark):
    return TableCatalog(str(tmp_path), spark)


def test_parse_table_name_valid(catalog):
    assert catalog.parse_table_name("schema.table") == ("schema", "table")


def test_parse_table_name_rejects_one_and_three_part(catalog):
    with pytest.raises(ValueError, match="schema_name.table_name"):
        catalog.parse_table_name("table")
    with pytest.raises(ValueError, match="schema_name.table_name"):
        catalog.parse_table_name("a.b.c")


def test_view_and_csv_path(catalog, tmp_path):
    assert catalog.view_name("s", "t") == "s_t"
    assert catalog.csv_path("s", "t") == os.path.join(str(tmp_path), "s", "t.csv")


def test_write_overwrite_and_read_roundtrip(catalog, spark):
    df = spark.createDataFrame([(1, "a")], ["id", "name"])
    catalog.write_table("default.sample", df, mode="overwrite")
    assert catalog.table_exists("default.sample")
    out = catalog.read_table("default.sample")
    assert out.count() == 1
    assert set(out.columns) == {"id", "name"}
    assert spark.sql("SELECT * FROM default_sample").count() == 1


def test_write_mode_error_if_exists(catalog, spark):
    df = spark.createDataFrame([(1,)], ["id"])
    catalog.write_table("s.t", df, mode="overwrite")
    with pytest.raises(Exception):
        catalog.write_table("s.t", df, mode="error")


def test_write_mode_ignore(catalog, spark):
    df1 = spark.createDataFrame([(1,)], ["id"])
    df2 = spark.createDataFrame([(1,), (2,)], ["id"])
    catalog.write_table("s.t", df1, mode="overwrite")
    catalog.write_table("s.t", df2, mode="ignore")
    assert catalog.read_table("s.t").count() == 1


def test_write_mode_append(catalog, spark):
    df1 = spark.createDataFrame([(1,)], ["id"])
    df2 = spark.createDataFrame([(2,)], ["id"])
    catalog.write_table("s.t", df1, mode="overwrite")
    catalog.write_table("s.t", df2, mode="append")
    assert catalog.read_table("s.t").count() == 2
