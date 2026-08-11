import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import pytest

from mock.spark_mock import SparkMock
from mock.data_frame_wrapper import DataFrameWrapper
from mock.sql_gateway import rewrite_two_part_identifiers


@pytest.fixture
def temp_spark(tmp_path):
    base = tmp_path / "spark_data"
    base.mkdir()
    return SparkMock(str(base))


def _make_df(spark_mock, rows, columns):
    spark_df = spark_mock._spark_session.createDataFrame(rows, schema=columns)
    return DataFrameWrapper(spark_mock, spark_df)


def test_rewrites_schema_table_only():
    q = "SELECT t.col, 1.5, 'a.b' FROM schema.table t"
    out = rewrite_two_part_identifiers(q)
    assert "schema_table" in out
    assert "t.col" in out
    assert "1.5" in out
    assert "'a.b'" in out
    assert "schema.table" not in out


def test_select_via_spark_sql(temp_spark):
    df = _make_df(temp_spark, [(1,)], ["id"])
    df.write.mode("overwrite").saveAsTable("s.t")
    out = temp_spark.sql("SELECT t.id FROM s.t t WHERE t.id = 1")
    assert out.count() == 1


def test_delete_where(temp_spark):
    df = _make_df(temp_spark, [(1,), (2,), (3,)], ["id"])
    df.write.mode("overwrite").saveAsTable("s.t")
    temp_spark.sql("DELETE FROM s.t WHERE id = 2")
    assert [r.id for r in temp_spark.read.table("s.t").collect()] == [1, 3]


def test_update_where(temp_spark):
    df = _make_df(temp_spark, [(1, "a"), (2, "b")], ["id", "name"])
    df.write.mode("overwrite").saveAsTable("s.t")
    temp_spark.sql("UPDATE s.t SET name = 'z' WHERE id = 1")
    rows = {r.id: r.name for r in temp_spark.read.table("s.t").collect()}
    assert rows == {1: "z", 2: "b"}


def test_insert_into_select(temp_spark):
    df = _make_df(temp_spark, [(1,)], ["id"])
    df.write.mode("overwrite").saveAsTable("s.t")
    temp_spark.sql("INSERT INTO s.t SELECT id FROM s.t")
    assert temp_spark.read.table("s.t").count() == 2


def test_ctas_and_create_or_replace(temp_spark):
    df = _make_df(temp_spark, [(1,)], ["id"])
    df.write.mode("overwrite").saveAsTable("s.src")
    temp_spark.sql("CREATE TABLE s.dst AS SELECT * FROM s.src")
    assert temp_spark.read.table("s.dst").count() == 1
    with pytest.raises(Exception):
        temp_spark.sql("CREATE TABLE s.dst AS SELECT * FROM s.src")
    temp_spark.sql("CREATE OR REPLACE TABLE s.dst AS SELECT * FROM s.src WHERE id = 1")
    assert temp_spark.read.table("s.dst").count() == 1


def test_merge_matched_update_not_matched_insert(temp_spark):
    target = _make_df(temp_spark, [(1, "old"), (2, "keep")], ["id", "name"])
    target.write.mode("overwrite").saveAsTable("s.target")
    source = _make_df(temp_spark, [(1, "new"), (3, "ins")], ["id", "name"])
    source.write.mode("overwrite").saveAsTable("s.source")
    temp_spark.sql(
        """
        MERGE INTO s.target t
        USING s.source s
        ON t.id = s.id
        WHEN MATCHED THEN UPDATE SET t.name = s.name
        WHEN NOT MATCHED THEN INSERT (id, name) VALUES (s.id, s.name)
        """
    )
    rows = {r.id: r.name for r in temp_spark.read.table("s.target").collect()}
    assert rows == {1: "new", 2: "keep", 3: "ins"}


def test_merge_matched_delete_unsupported(temp_spark):
    target = _make_df(temp_spark, [(1, "old")], ["id", "name"])
    target.write.mode("overwrite").saveAsTable("s.target")
    source = _make_df(temp_spark, [(1, "new")], ["id", "name"])
    source.write.mode("overwrite").saveAsTable("s.source")
    with pytest.raises(NotImplementedError):
        temp_spark.sql(
            """
            MERGE INTO s.target t USING s.source s ON t.id = s.id
            WHEN MATCHED THEN DELETE
            WHEN NOT MATCHED THEN INSERT (id, name) VALUES (s.id, s.name)
            """
        )
