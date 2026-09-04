import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import pandas as pd

from testbricks.dbutils import dbutils
from testbricks.spark_proxy import SparkProxy


class TestRestartPython:
    def test_restart_python_returns_true_without_restarting(self):
        marker = object()
        dbutils._sentinel = marker
        assert dbutils.library.restartPython() is True
        assert dbutils._sentinel is marker


class TestSummarize:
    def test_summarize_prints_pandas_describe(self, capsys):
        frame = pd.DataFrame({"n": [1, 2, 3]})
        assert dbutils.data.summarize(frame) is None
        captured = capsys.readouterr()
        assert "count" in captured.out
        assert "1" in captured.out or "mean" in captured.out

    def test_summarize_prints_spark_describe(self, tmp_path, capsys):
        spark = SparkProxy(str(tmp_path))
        df = spark.createDataFrame([(1,), (2,), (3,)], ["n"])
        assert dbutils.data.summarize(df, precise=False) is None
        captured = capsys.readouterr()
        assert "count" in captured.out or "summary" in captured.out
