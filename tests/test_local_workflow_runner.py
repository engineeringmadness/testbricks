import sys
import os

# Ensure src is on the path so `mock` can be imported during pytest collection.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import json
import pytest

from mock.local_workflow_runner import LocalWorkflowRunner


ROOT_DIR = os.path.dirname(os.path.dirname(__file__))
WORKFLOW_SAMPLE_PATH = os.path.join(ROOT_DIR, "specs", "workflow_sample.json")


class TestSampleWorkflowDAG:
    def test_builds_dag_using_notebook_names(self):
        runner = LocalWorkflowRunner("src", WORKFLOW_SAMPLE_PATH)

        assert runner.dag["auxillary_dims"] == {"data_quality"}
        assert runner.dag["reviews_fact"] == {"data_quality"}
        assert runner.dag["data_quality"] == {"semantic_layer"}
        assert runner.dag["semantic_layer"] == set()

    def test_execution_order_respects_dependencies(self):
        runner = LocalWorkflowRunner("src", WORKFLOW_SAMPLE_PATH)
        order = runner.execution_order

        assert order.index("data_quality") > order.index("auxillary_dims")
        assert order.index("data_quality") > order.index("reviews_fact")
        assert order.index("semantic_layer") > order.index("data_quality")

    def test_format_dag_includes_nodes_and_execution_order(self):
        runner = LocalWorkflowRunner("src", WORKFLOW_SAMPLE_PATH)
        formatted = runner.format_dag()

        assert "- auxillary_dims -> [data_quality]" in formatted
        assert "- reviews_fact -> [data_quality]" in formatted
        assert "- data_quality -> [semantic_layer]" in formatted
        assert "- semantic_layer -> []" in formatted
        assert "Execution order:" in formatted
        assert " -> ".join(runner.execution_order) in formatted


class TestNotebookNameExtraction:
    @pytest.mark.parametrize(
        "notebook_path,expected",
        [
            ("/Workspace/any/first", "first"),
            ("/Workspace/any/first/", "first"),
            ("just_a_notebook", "just_a_notebook"),
            ("/deeply/nested/path/notebook_name", "notebook_name"),
        ],
    )
    def test_extract_notebook_name(self, tmp_path, notebook_path, expected):
        workflow = {
            "tasks": [
                {
                    "task_key": "task_1",
                    "notebook_task": {"notebook_path": notebook_path},
                }
            ]
        }
        workflow_path = tmp_path / "workflow.json"
        workflow_path.write_text(json.dumps(workflow), encoding="utf-8")

        runner = LocalWorkflowRunner(str(tmp_path), str(workflow_path))
        assert runner._task_to_notebook["task_1"] == expected
        assert runner.execution_order == [expected]


class TestWorkflowExecution:
    def test_run_workflow_executes_in_dependency_order(self, tmp_path, capsys):
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
            script_path.write_text(
                f'with open(r"{log_file}", "a") as f: f.write("{notebook_name}\\n")'
            )

        runner = LocalWorkflowRunner(str(source_dir), str(workflow_path))
        runner.run_workflow()

        assert log_file.exists()
        lines = log_file.read_text(encoding="utf-8").splitlines()
        assert lines[0] == "first"
        assert set(lines[1:]) == {"second", "third"}

        captured = capsys.readouterr()
        assert "Executing workflow:" in captured.out


class TestWorkflowValidation:
    def test_missing_tasks_raises(self, tmp_path):
        workflow_path = tmp_path / "workflow.json"
        workflow_path.write_text(json.dumps({}), encoding="utf-8")

        with pytest.raises(ValueError, match="must contain a 'tasks' list"):
            LocalWorkflowRunner(str(tmp_path), str(workflow_path))

    def test_non_dict_task_raises(self, tmp_path):
        workflow = {"tasks": ["not_a_dict"]}
        workflow_path = tmp_path / "workflow.json"
        workflow_path.write_text(json.dumps(workflow), encoding="utf-8")

        with pytest.raises(ValueError, match="Each task must be a JSON object"):
            LocalWorkflowRunner(str(tmp_path), str(workflow_path))

    def test_missing_task_key_raises(self, tmp_path):
        workflow = {"tasks": [{"notebook_task": {"notebook_path": "/a/b"}}]}
        workflow_path = tmp_path / "workflow.json"
        workflow_path.write_text(json.dumps(workflow), encoding="utf-8")

        with pytest.raises(ValueError, match="must include a non-empty 'task_key'"):
            LocalWorkflowRunner(str(tmp_path), str(workflow_path))

    def test_duplicate_task_key_raises(self, tmp_path):
        workflow = {
            "tasks": [
                {"task_key": "dup", "notebook_task": {"notebook_path": "/a/b"}},
                {"task_key": "dup", "notebook_task": {"notebook_path": "/a/c"}},
            ]
        }
        workflow_path = tmp_path / "workflow.json"
        workflow_path.write_text(json.dumps(workflow), encoding="utf-8")

        with pytest.raises(ValueError, match="Duplicate task_key found"):
            LocalWorkflowRunner(str(tmp_path), str(workflow_path))

    def test_missing_notebook_task_raises(self, tmp_path):
        workflow = {"tasks": [{"task_key": "t1"}]}
        workflow_path = tmp_path / "workflow.json"
        workflow_path.write_text(json.dumps(workflow), encoding="utf-8")

        with pytest.raises(ValueError, match="is missing 'notebook_task'"):
            LocalWorkflowRunner(str(tmp_path), str(workflow_path))

    @pytest.mark.parametrize(
        "notebook_path",
        [None, "", "   ", 123],
    )
    def test_invalid_notebook_path_raises(self, tmp_path, notebook_path):
        workflow = {
            "tasks": [
                {"task_key": "t1", "notebook_task": {"notebook_path": notebook_path}}
            ]
        }
        workflow_path = tmp_path / "workflow.json"
        workflow_path.write_text(json.dumps(workflow), encoding="utf-8")

        with pytest.raises(ValueError, match="invalid notebook_path"):
            LocalWorkflowRunner(str(tmp_path), str(workflow_path))

    def test_invalid_depends_on_format_raises(self, tmp_path):
        workflow = {
            "tasks": [
                {
                    "task_key": "t1",
                    "depends_on": "t0",
                    "notebook_task": {"notebook_path": "/a/b"},
                }
            ]
        }
        workflow_path = tmp_path / "workflow.json"
        workflow_path.write_text(json.dumps(workflow), encoding="utf-8")

        with pytest.raises(ValueError, match="invalid 'depends_on' format"):
            LocalWorkflowRunner(str(tmp_path), str(workflow_path))

    def test_malformed_dependency_entry_raises(self, tmp_path):
        workflow = {
            "tasks": [
                {
                    "task_key": "t1",
                    "depends_on": [{"not_task_key": "t0"}],
                    "notebook_task": {"notebook_path": "/a/b"},
                }
            ]
        }
        workflow_path = tmp_path / "workflow.json"
        workflow_path.write_text(json.dumps(workflow), encoding="utf-8")

        with pytest.raises(ValueError, match="malformed dependency entry"):
            LocalWorkflowRunner(str(tmp_path), str(workflow_path))

    def test_unknown_dependency_raises(self, tmp_path):
        workflow = {
            "tasks": [
                {
                    "task_key": "t1",
                    "depends_on": [{"task_key": "missing"}],
                    "notebook_task": {"notebook_path": "/a/b"},
                }
            ]
        }
        workflow_path = tmp_path / "workflow.json"
        workflow_path.write_text(json.dumps(workflow), encoding="utf-8")

        with pytest.raises(ValueError, match="depends on unknown task"):
            LocalWorkflowRunner(str(tmp_path), str(workflow_path))

    def test_cyclic_workflow_raises(self, tmp_path):
        workflow = {
            "tasks": [
                {
                    "task_key": "a",
                    "depends_on": [{"task_key": "c"}],
                    "notebook_task": {"notebook_path": "/a/a"},
                },
                {
                    "task_key": "b",
                    "depends_on": [{"task_key": "a"}],
                    "notebook_task": {"notebook_path": "/a/b"},
                },
                {
                    "task_key": "c",
                    "depends_on": [{"task_key": "b"}],
                    "notebook_task": {"notebook_path": "/a/c"},
                },
            ]
        }
        workflow_path = tmp_path / "workflow.json"
        workflow_path.write_text(json.dumps(workflow), encoding="utf-8")

        with pytest.raises(ValueError, match="contains a cycle"):
            LocalWorkflowRunner(str(tmp_path), str(workflow_path))

    def test_missing_notebook_file_raises(self, tmp_path):
        workflow = {
            "tasks": [
                {"task_key": "t1", "notebook_task": {"notebook_path": "/a/missing"}}
            ]
        }
        workflow_path = tmp_path / "workflow.json"
        workflow_path.write_text(json.dumps(workflow), encoding="utf-8")

        runner = LocalWorkflowRunner(str(tmp_path), str(workflow_path))
        with pytest.raises(FileNotFoundError, match="Notebook file not found"):
            runner.run_workflow()

    def test_duplicate_notebook_name_raises(self, tmp_path):
        workflow = {
            "tasks": [
                {"task_key": "t1", "notebook_task": {"notebook_path": "/a/name"}},
                {"task_key": "t2", "notebook_task": {"notebook_path": "/b/name"}},
            ]
        }
        workflow_path = tmp_path / "workflow.json"
        workflow_path.write_text(json.dumps(workflow), encoding="utf-8")

        with pytest.raises(ValueError, match="Duplicate notebook name found"):
            LocalWorkflowRunner(str(tmp_path), str(workflow_path))
