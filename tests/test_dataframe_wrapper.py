import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import pytest

from mock.spark_mock import SparkMock
from mock.data_frame_wrapper import DataFrameWrapper


@pytest.fixture
def temp_spark(tmp_path):
    base = tmp_path / "spark_data"
    base.mkdir()
    return SparkMock(str(base))


def _make_df(spark_mock, rows, columns):
    spark_df = spark_mock._spark_session.createDataFrame(rows, schema=columns)
    return DataFrameWrapper(spark_mock, spark_df)


def test_groupby_agg_write_save_as_table(temp_spark):
    df = _make_df(temp_spark, [("a", 1), ("a", 2), ("b", 3)], ["k", "v"])
    result = df.groupBy("k").agg({"v": "sum"})
    result.write.mode("overwrite").saveAsTable("default.agg_out")
    out = temp_spark.read.table("default.agg_out")
    assert out.count() == 2
