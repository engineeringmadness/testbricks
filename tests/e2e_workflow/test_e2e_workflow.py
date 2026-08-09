import os
import shutil
import sys

# Ensure src is on the path so `mock` can be imported during pytest collection.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "src"))

import pytest

from mock.local_workflow_runner import LocalWorkflowRunner
from mock.spark_mock import SparkMock


TEST_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(TEST_DIR, "data")
NOTEBOOKS_DIR = os.path.join(TEST_DIR, "notebooks")
WORKFLOW_PATH = os.path.join(TEST_DIR, "workflow.json")

WIDGET_PARAMS = {"filter_country": "USA", "min_amount": "100"}


@pytest.fixture
def e2e_env(tmp_path):
    # Copy data to tmp_path so output writes don't pollute the repo.
    data_dir = tmp_path / "data"
    shutil.copytree(DATA_DIR, str(data_dir))
    # Set widget parameters as env vars BEFORE the workflow starts.
    for key, value in WIDGET_PARAMS.items():
        os.environ[key] = value
    spark = SparkMock(str(data_dir))
    yield spark, str(data_dir)
    # Tear down env vars to prevent cross-test leakage.
    for key in WIDGET_PARAMS:
        os.environ.pop(key, None)


def test_e2e_workflow_runs_full_pipeline(e2e_env):
    spark, data_dir = e2e_env
    runner = LocalWorkflowRunner(NOTEBOOKS_DIR, WORKFLOW_PATH, data_dir)
    runner.run_workflow(extra_globals={"spark": spark})

    # NB1: USA filter → 3 customers (Alice, Charlie, Eve)
    customers = spark.read.option("header", "true").option("inferSchema", "true") \
        .table("silver.customers_enriched")
    assert customers.count() == 3
    assert {r.name for r in customers.collect()} == {"Alice", "Charlie", "Eve"}

    # NB2: amount >= 100 → 4 orders (101, 103, 105, 107)
    orders = spark.read.option("header", "true").option("inferSchema", "true") \
        .table("silver.orders_enriched")
    assert orders.count() == 4
    assert {r.order_id for r in orders.collect()} == {101, 103, 105, 107}

    # NB3: inner join → Alice(370.0), Eve(300.0); Charlie dropped (no qualifying orders)
    summary = spark.read.option("header", "true").option("inferSchema", "true") \
        .table("gold.customer_order_summary")
    rows = summary.collect()
    assert len(rows) == 2
    assert rows[0].name == "Alice" and float(rows[0].total_amount) == 370.0
    assert rows[1].name == "Eve" and float(rows[1].total_amount) == 300.0
