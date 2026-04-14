import json
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


def test_run_workflow_executes_in_dependency_order(tmp_path):
    source_dir = tmp_path / "local_src"
    source_dir.mkdir()
    log_file = tmp_path / "execution.log"

    workflow = {
        "tasks": [
            {
                "task_key": "first_task",
                "notebook_task": {"notebook_path": "/Workspace/any/first"},
            },
            {
                "task_key": "second_task",
                "depends_on": [{"task_key": "first_task"}],
                "notebook_task": {"notebook_path": "/Workspace/any/second"},
            },
            {
                "task_key": "third_task",
                "depends_on": [{"task_key": "first_task"}],
                "notebook_task": {"notebook_path": "/Workspace/any/third"},
            },
        ]
    }
    workflow_path = tmp_path / "workflow.json"
    workflow_path.write_text(json.dumps(workflow), encoding="utf-8")
    for notebook_name in ["first", "second", "third"]:
        script_path = source_dir / f"{notebook_name}.py"
        script_path.write_text(f'print("Hello from notebook {notebook_name}")')

    runner = LocalWorkflowRunner(str(source_dir), str(workflow_path))
    runner.run_workflow()