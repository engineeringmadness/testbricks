import os
import sys

import pytest

# Single place ensuring `testbricks` is importable (src layout) before any
# conftest/test imports it. `pythonpath = src` in pytest.ini covers it on CI;
# this keeps local invocations from any CWD working too.
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "src"))

from testbricks.dbutils import configure, dbutils

# PySpark workers on Windows need an explicit Python executable path.
# On this CI/dev box `python3` is not available, but `python` is.
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)


@pytest.fixture
def notebook_executor(tmp_path):
    """Configured dbutils executor pointed at an isolated temp directory."""
    configure(str(tmp_path), source_dir=str(tmp_path))
    yield dbutils.executor
    dbutils.widgets.removeAll()
