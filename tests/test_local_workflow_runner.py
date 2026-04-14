import os

from src.mock.local_workflow_runner import LocalWorkflowRunner


ROOT_DIR = os.path.dirname(os.path.dirname(__file__))
WORKFLOW_SAMPLE_PATH = os.path.join(ROOT_DIR, "specs", "workflow_sample.json")


def test_builds_dag_using_notebook_names():
    runner = LocalWorkflowRunner("src", WORKFLOW_SAMPLE_PATH)

    assert runner.dag["auxillary_dims"] == {"data_quality"}
    assert runner.dag["reviews_fact"] == {"data_quality"}
    assert runner.dag["data_quality"] == {"semantic_layer"}
    assert runner.dag["semantic_layer"] == set()


def test_execution_order_respects_dependencies():
    runner = LocalWorkflowRunner("src", WORKFLOW_SAMPLE_PATH)
    order = runner.execution_order

    assert order.index("data_quality") > order.index("auxillary_dims")
    assert order.index("data_quality") > order.index("reviews_fact")
    assert order.index("semantic_layer") > order.index("data_quality")
