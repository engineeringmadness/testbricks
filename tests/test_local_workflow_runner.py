import sys
import os

# Ensure src is on the path so `testbricks` can be imported during pytest collection.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import json
import pytest

from testbricks.local_workflow_runner import LocalWorkflowRunner, transform_run_commands


ROOT_DIR = os.path.dirname(os.path.dirname(__file__))
WORKFLOW_SAMPLE_PATH = os.path.join(ROOT_DIR, "tests", "data", "workflow_sample.json")
DEFAULT_BASE_PATH = os.path.join(ROOT_DIR, "tests", "data")


class TestSampleWorkflowDAG:
    def test_builds_dag_using_notebook_names(self):
        runner = LocalWorkflowRunner("src", WORKFLOW_SAMPLE_PATH, DEFAULT_BASE_PATH)

        assert runner.dag["dimensions"] == {"quality_checks"}
        assert runner.dag["reviews_fact"] == {"quality_checks"}
        assert runner.dag["quality_checks"] == {"semantic_layer"}
        assert runner.dag["semantic_layer"] == set()

    def test_execution_order_respects_dependencies(self):
        runner = LocalWorkflowRunner("src", WORKFLOW_SAMPLE_PATH, DEFAULT_BASE_PATH)
        order = runner.execution_order

        assert order.index("quality_checks") > order.index("dimensions")
        assert order.index("quality_checks") > order.index("reviews_fact")
        assert order.index("semantic_layer") > order.index("quality_checks")

    def test_format_dag_includes_nodes_and_execution_order(self):
        runner = LocalWorkflowRunner("src", WORKFLOW_SAMPLE_PATH, DEFAULT_BASE_PATH)
        formatted = runner.format_dag()

        assert "- dimensions -> [quality_checks]" in formatted
        assert "- reviews_fact -> [quality_checks]" in formatted
        assert "- quality_checks -> [semantic_layer]" in formatted
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

        runner = LocalWorkflowRunner(str(tmp_path), str(workflow_path), str(tmp_path))
        assert runner._task_to_notebook["task_1"] == expected
        assert runner.execution_order == ["task_1"]


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

        runner = LocalWorkflowRunner(str(source_dir), str(workflow_path), str(tmp_path))
        runner.run_workflow()

        assert log_file.exists()
        lines = log_file.read_text(encoding="utf-8").splitlines()
        assert lines[0] == "first"
        assert set(lines[1:]) == {"second", "third"}

        captured = capsys.readouterr()
        assert "Executing workflow:" in captured.out

    def test_extra_globals_inject_keys_into_notebook_namespace(self, tmp_path):
        source_dir = tmp_path / "local_src"
        source_dir.mkdir()
        marker = tmp_path / "injected.txt"
        (source_dir / "main.py").write_text(
            f'with open(r"{marker}", "w") as f: f.write(str(injected_value))\n',
            encoding="utf-8",
        )

        runner = LocalWorkflowRunner(
            str(source_dir), _write_single_task_workflow(tmp_path), str(tmp_path)
        )
        runner.run_workflow(extra_globals={"injected_value": 42})

        assert marker.read_text(encoding="utf-8") == "42"

    def test_base_parameters_seed_env_when_unset(self, tmp_path):
        source_dir = tmp_path / "local_src"
        source_dir.mkdir()
        marker = tmp_path / "seeded.txt"
        (source_dir / "main.py").write_text(
            'import os\n'
            f'with open(r"{marker}", "w") as f: f.write(os.environ["mode"])\n',
            encoding="utf-8",
        )

        workflow = {
            "tasks": [
                {
                    "task_key": "task_1",
                    "notebook_task": {
                        "notebook_path": "/Workspace/any/main",
                        "base_parameters": {"mode": "default"},
                    },
                }
            ]
        }
        workflow_path = tmp_path / "workflow.json"
        workflow_path.write_text(json.dumps(workflow), encoding="utf-8")

        runner = LocalWorkflowRunner(str(source_dir), str(workflow_path), str(tmp_path))
        runner.run_workflow()

        assert marker.read_text(encoding="utf-8") == "default"

    def test_base_parameters_do_not_clobber_existing_env(self, tmp_path):
        source_dir = tmp_path / "local_src"
        source_dir.mkdir()
        marker = tmp_path / "preserved.txt"
        (source_dir / "main.py").write_text(
            'import os\n'
            f'with open(r"{marker}", "w") as f: f.write(os.environ["mode"])\n',
            encoding="utf-8",
        )

        workflow = {
            "tasks": [
                {
                    "task_key": "task_1",
                    "notebook_task": {
                        "notebook_path": "/Workspace/any/main",
                        "base_parameters": {"mode": "default"},
                    },
                }
            ]
        }
        workflow_path = tmp_path / "workflow.json"
        workflow_path.write_text(json.dumps(workflow), encoding="utf-8")

        os.environ["mode"] = "from_test"
        try:
            runner = LocalWorkflowRunner(
                str(source_dir), str(workflow_path), str(tmp_path)
            )
            runner.run_workflow()
        finally:
            os.environ.pop("mode", None)

        assert marker.read_text(encoding="utf-8") == "from_test"


class TestTaskValuesPropagation:
    def test_downstream_task_reads_upstream_task_value(self, tmp_path):
        source_dir = tmp_path / "local_src"
        source_dir.mkdir()
        marker = tmp_path / "value.txt"
        (source_dir / "producer.py").write_text(
            'dbutils.jobs.taskValues.set(key="region", value="eu")\n',
            encoding="utf-8",
        )
        (source_dir / "consumer.py").write_text(
            'value = dbutils.jobs.taskValues.get(taskKey="producer", key="region")\n'
            f'with open(r"{marker}", "w") as f: f.write(value)\n',
            encoding="utf-8",
        )
        workflow = {
            "tasks": [
                {
                    "task_key": "producer",
                    "notebook_task": {"notebook_path": "/Workspace/any/producer"},
                },
                {
                    "task_key": "consumer",
                    "depends_on": [{"task_key": "producer"}],
                    "notebook_task": {"notebook_path": "/Workspace/any/consumer"},
                },
            ]
        }
        workflow_path = tmp_path / "workflow.json"
        workflow_path.write_text(json.dumps(workflow), encoding="utf-8")

        runner = LocalWorkflowRunner(str(source_dir), str(workflow_path), str(tmp_path))
        runner.run_workflow()

        assert marker.read_text(encoding="utf-8") == "eu"

    def test_base_parameters_seed_task_values(self, tmp_path):
        source_dir = tmp_path / "local_src"
        source_dir.mkdir()
        marker = tmp_path / "seeded_tv.txt"
        (source_dir / "main.py").write_text(
            'value = dbutils.jobs.taskValues.get(taskKey="task_1", key="mode")\n'
            f'with open(r"{marker}", "w") as f: f.write(value)\n',
            encoding="utf-8",
        )
        workflow = {
            "tasks": [
                {
                    "task_key": "task_1",
                    "notebook_task": {
                        "notebook_path": "/Workspace/any/main",
                        "base_parameters": {"mode": "default"},
                    },
                }
            ]
        }
        workflow_path = tmp_path / "workflow.json"
        workflow_path.write_text(json.dumps(workflow), encoding="utf-8")

        runner = LocalWorkflowRunner(str(source_dir), str(workflow_path), str(tmp_path))
        runner.run_workflow()

        assert marker.read_text(encoding="utf-8") == "default"

    def test_shared_namespace_sees_set_immediately_in_later_task(self, tmp_path):
        source_dir = tmp_path / "local_src"
        source_dir.mkdir()
        marker = tmp_path / "shared.txt"
        (source_dir / "first.py").write_text(
            'dbutils.jobs.taskValues.set(key="flag", value="on")\n',
            encoding="utf-8",
        )
        (source_dir / "second.py").write_text(
            'value = dbutils.jobs.taskValues.get(taskKey="first_task", key="flag")\n'
            f'with open(r"{marker}", "w") as f: f.write(value)\n',
            encoding="utf-8",
        )
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
            ]
        }
        workflow_path = tmp_path / "workflow.json"
        workflow_path.write_text(json.dumps(workflow), encoding="utf-8")

        runner = LocalWorkflowRunner(str(source_dir), str(workflow_path), str(tmp_path))
        runner.run_workflow()

        assert marker.read_text(encoding="utf-8") == "on"


def _write_notebooks(source_dir, mapping):
    for name, body in mapping.items():
        (source_dir / f"{name}.py").write_text(body, encoding="utf-8")


class TestRunIfConditions:
    def _runner(self, tmp_path, tasks, notebooks):
        source_dir = tmp_path / "local_src"
        source_dir.mkdir()
        _write_notebooks(source_dir, notebooks)
        workflow_path = tmp_path / "workflow.json"
        workflow_path.write_text(json.dumps({"tasks": tasks}), encoding="utf-8")
        return LocalWorkflowRunner(str(source_dir), str(workflow_path), str(tmp_path))

    def test_all_success_skips_when_dependency_fails(self, tmp_path):
        log_file = tmp_path / "execution.log"
        runner = self._runner(
            tmp_path,
            [
                {
                    "task_key": "fail_task",
                    "notebook_task": {"notebook_path": "/Workspace/any/fail"},
                },
                {
                    "task_key": "downstream",
                    "run_if": "ALL_SUCCESS",
                    "depends_on": [{"task_key": "fail_task"}],
                    "notebook_task": {"notebook_path": "/Workspace/any/down"},
                },
            ],
            {
                "fail": "raise RuntimeError('boom')\n",
                "down": f'with open(r"{log_file}", "a") as f: f.write("down\\n")\n',
            },
        )
        with pytest.raises(RuntimeError, match="boom"):
            runner.run_workflow()
        assert runner.task_statuses["fail_task"] == "FAILED"
        assert runner.task_statuses["downstream"] == "SKIPPED"
        assert not log_file.exists()

    def test_all_failed_runs_when_dependency_fails(self, tmp_path):
        log_file = tmp_path / "execution.log"
        runner = self._runner(
            tmp_path,
            [
                {
                    "task_key": "fail_task",
                    "notebook_task": {"notebook_path": "/Workspace/any/fail"},
                },
                {
                    "task_key": "cleanup",
                    "run_if": "ALL_FAILED",
                    "depends_on": [{"task_key": "fail_task"}],
                    "notebook_task": {"notebook_path": "/Workspace/any/cleanup"},
                },
            ],
            {
                "fail": "raise RuntimeError('boom')\n",
                "cleanup": f'with open(r"{log_file}", "a") as f: f.write("cleanup\\n")\n',
            },
        )
        with pytest.raises(RuntimeError, match="boom"):
            runner.run_workflow()
        assert runner.task_statuses["fail_task"] == "FAILED"
        assert runner.task_statuses["cleanup"] == "SUCCESS"
        assert log_file.read_text(encoding="utf-8").splitlines() == ["cleanup"]

    def test_all_failed_skips_when_dependency_succeeds(self, tmp_path):
        log_file = tmp_path / "execution.log"
        runner = self._runner(
            tmp_path,
            [
                {
                    "task_key": "ok_task",
                    "notebook_task": {"notebook_path": "/Workspace/any/ok"},
                },
                {
                    "task_key": "cleanup",
                    "run_if": "ALL_FAILED",
                    "depends_on": [{"task_key": "ok_task"}],
                    "notebook_task": {"notebook_path": "/Workspace/any/cleanup"},
                },
            ],
            {
                "ok": "pass\n",
                "cleanup": f'with open(r"{log_file}", "a") as f: f.write("cleanup\\n")\n',
            },
        )
        runner.run_workflow()
        assert runner.task_statuses["ok_task"] == "SUCCESS"
        assert runner.task_statuses["cleanup"] == "SKIPPED"
        assert not log_file.exists()

    def test_all_done_runs_after_failed_dependency(self, tmp_path):
        log_file = tmp_path / "execution.log"
        runner = self._runner(
            tmp_path,
            [
                {
                    "task_key": "fail_task",
                    "notebook_task": {"notebook_path": "/Workspace/any/fail"},
                },
                {
                    "task_key": "always",
                    "run_if": "ALL_DONE",
                    "depends_on": [{"task_key": "fail_task"}],
                    "notebook_task": {"notebook_path": "/Workspace/any/always"},
                },
            ],
            {
                "fail": "raise RuntimeError('boom')\n",
                "always": f'with open(r"{log_file}", "a") as f: f.write("always\\n")\n',
            },
        )
        with pytest.raises(RuntimeError, match="boom"):
            runner.run_workflow()
        assert runner.task_statuses["always"] == "SUCCESS"
        assert log_file.read_text(encoding="utf-8").splitlines() == ["always"]

    def test_none_failed_skips_when_dependency_fails(self, tmp_path):
        runner = self._runner(
            tmp_path,
            [
                {
                    "task_key": "fail_task",
                    "notebook_task": {"notebook_path": "/Workspace/any/fail"},
                },
                {
                    "task_key": "next",
                    "run_if": "NONE_FAILED",
                    "depends_on": [{"task_key": "fail_task"}],
                    "notebook_task": {"notebook_path": "/Workspace/any/next"},
                },
            ],
            {"fail": "raise RuntimeError('boom')\n", "next": "pass\n"},
        )
        with pytest.raises(RuntimeError, match="boom"):
            runner.run_workflow()
        assert runner.task_statuses["next"] == "SKIPPED"

    def test_none_failed_runs_when_dependency_skipped(self, tmp_path):
        runner = self._runner(
            tmp_path,
            [
                {
                    "task_key": "fail_task",
                    "notebook_task": {"notebook_path": "/Workspace/any/fail"},
                },
                {
                    "task_key": "mid",
                    "run_if": "ALL_SUCCESS",
                    "depends_on": [{"task_key": "fail_task"}],
                    "notebook_task": {"notebook_path": "/Workspace/any/mid"},
                },
                {
                    "task_key": "tail",
                    "run_if": "NONE_FAILED",
                    "depends_on": [{"task_key": "mid"}],
                    "notebook_task": {"notebook_path": "/Workspace/any/tail"},
                },
            ],
            {
                "fail": "raise RuntimeError('boom')\n",
                "mid": "pass\n",
                "tail": "pass\n",
            },
        )
        with pytest.raises(RuntimeError, match="boom"):
            runner.run_workflow()
        assert runner.task_statuses["mid"] == "SKIPPED"
        assert runner.task_statuses["tail"] == "SUCCESS"

    def test_at_least_one_success_runs_if_any_dep_succeeded(self, tmp_path):
        log_file = tmp_path / "execution.log"
        runner = self._runner(
            tmp_path,
            [
                {
                    "task_key": "ok_task",
                    "notebook_task": {"notebook_path": "/Workspace/any/ok"},
                },
                {
                    "task_key": "fail_task",
                    "notebook_task": {"notebook_path": "/Workspace/any/fail"},
                },
                {
                    "task_key": "join",
                    "run_if": "AT_LEAST_ONE_SUCCESS",
                    "depends_on": [
                        {"task_key": "ok_task"},
                        {"task_key": "fail_task"},
                    ],
                    "notebook_task": {"notebook_path": "/Workspace/any/join"},
                },
            ],
            {
                "ok": "pass\n",
                "fail": "raise RuntimeError('boom')\n",
                "join": f'with open(r"{log_file}", "a") as f: f.write("join\\n")\n',
            },
        )
        with pytest.raises(RuntimeError, match="boom"):
            runner.run_workflow()
        assert runner.task_statuses["join"] == "SUCCESS"
        assert log_file.read_text(encoding="utf-8").splitlines() == ["join"]

    def test_depends_on_outcome_skips_when_status_does_not_match(self, tmp_path):
        runner = self._runner(
            tmp_path,
            [
                {
                    "task_key": "ok_task",
                    "notebook_task": {"notebook_path": "/Workspace/any/ok"},
                },
                {
                    "task_key": "only_on_fail",
                    "depends_on": [{"task_key": "ok_task", "outcome": "FAILED"}],
                    "notebook_task": {"notebook_path": "/Workspace/any/only_on_fail"},
                },
            ],
            {"ok": "pass\n", "only_on_fail": "pass\n"},
        )
        runner.run_workflow()
        assert runner.task_statuses["only_on_fail"] == "SKIPPED"

    def test_unknown_run_if_raises(self, tmp_path):
        workflow = {
            "tasks": [
                {
                    "task_key": "t1",
                    "run_if": "SOMETIMES",
                    "notebook_task": {"notebook_path": "/a/b"},
                }
            ]
        }
        workflow_path = tmp_path / "workflow.json"
        workflow_path.write_text(json.dumps(workflow), encoding="utf-8")
        with pytest.raises(ValueError, match="Unsupported run_if"):
            LocalWorkflowRunner(str(tmp_path), str(workflow_path), str(tmp_path))


class TestConditionTasks:
    def _runner(self, tmp_path, tasks, notebooks):
        source_dir = tmp_path / "local_src"
        source_dir.mkdir()
        _write_notebooks(source_dir, notebooks)
        workflow_path = tmp_path / "workflow.json"
        workflow_path.write_text(json.dumps({"tasks": tasks}), encoding="utf-8")
        return LocalWorkflowRunner(str(source_dir), str(workflow_path), str(tmp_path))

    def test_true_branch_runs_and_false_branch_skipped(self, tmp_path):
        log_file = tmp_path / "execution.log"
        runner = self._runner(
            tmp_path,
            [
                {
                    "task_key": "producer",
                    "notebook_task": {"notebook_path": "/Workspace/any/producer"},
                },
                {
                    "task_key": "check",
                    "depends_on": [{"task_key": "producer"}],
                    "condition_task": {
                        "op": "EQUAL_TO",
                        "left": "{{tasks.producer.values.region}}",
                        "right": "eu",
                    },
                },
                {
                    "task_key": "on_true",
                    "depends_on": [{"task_key": "check", "outcome": "true"}],
                    "notebook_task": {"notebook_path": "/Workspace/any/on_true"},
                },
                {
                    "task_key": "on_false",
                    "depends_on": [{"task_key": "check", "outcome": "false"}],
                    "notebook_task": {"notebook_path": "/Workspace/any/on_false"},
                },
            ],
            {
                "producer": 'dbutils.jobs.taskValues.set(key="region", value="eu")\n',
                "on_true": f'with open(r"{log_file}", "a") as f: f.write("true\\n")\n',
                "on_false": f'with open(r"{log_file}", "a") as f: f.write("false\\n")\n',
            },
        )
        runner.run_workflow()
        assert runner.task_statuses["check"] == "SUCCESS"
        assert runner.task_results["check"] == "true"
        assert runner.task_statuses["on_true"] == "SUCCESS"
        assert runner.task_statuses["on_false"] == "SKIPPED"
        assert log_file.read_text(encoding="utf-8").splitlines() == ["true"]

    def test_false_branch_runs_when_condition_fails(self, tmp_path):
        log_file = tmp_path / "execution.log"
        runner = self._runner(
            tmp_path,
            [
                {
                    "task_key": "producer",
                    "notebook_task": {"notebook_path": "/Workspace/any/producer"},
                },
                {
                    "task_key": "check",
                    "depends_on": [{"task_key": "producer"}],
                    "condition_task": {
                        "op": "EQUAL",
                        "left": "{{tasks.producer.values.region}}",
                        "right": "eu",
                    },
                },
                {
                    "task_key": "on_true",
                    "depends_on": [{"task_key": "check", "outcome": "true"}],
                    "notebook_task": {"notebook_path": "/Workspace/any/on_true"},
                },
                {
                    "task_key": "on_false",
                    "depends_on": [{"task_key": "check", "outcome": "false"}],
                    "notebook_task": {"notebook_path": "/Workspace/any/on_false"},
                },
            ],
            {
                "producer": 'dbutils.jobs.taskValues.set(key="region", value="us")\n',
                "on_true": f'with open(r"{log_file}", "a") as f: f.write("true\\n")\n',
                "on_false": f'with open(r"{log_file}", "a") as f: f.write("false\\n")\n',
            },
        )
        runner.run_workflow()
        assert runner.task_results["check"] == "false"
        assert runner.task_statuses["on_true"] == "SKIPPED"
        assert runner.task_statuses["on_false"] == "SUCCESS"
        assert log_file.read_text(encoding="utf-8").splitlines() == ["false"]

    def test_greater_than_compares_numerically(self, tmp_path):
        log_file = tmp_path / "execution.log"
        runner = self._runner(
            tmp_path,
            [
                {
                    "task_key": "producer",
                    "notebook_task": {"notebook_path": "/Workspace/any/producer"},
                },
                {
                    "task_key": "check",
                    "depends_on": [{"task_key": "producer"}],
                    "condition_task": {
                        "op": "GREATER_THAN",
                        "left": "{{tasks.producer.values.count}}",
                        "right": "10",
                    },
                },
                {
                    "task_key": "on_true",
                    "depends_on": [{"task_key": "check", "outcome": "true"}],
                    "notebook_task": {"notebook_path": "/Workspace/any/on_true"},
                },
            ],
            {
                "producer": 'dbutils.jobs.taskValues.set(key="count", value="12")\n',
                "on_true": f'with open(r"{log_file}", "a") as f: f.write("true\\n")\n',
            },
        )
        runner.run_workflow()
        assert runner.task_results["check"] == "true"
        assert log_file.read_text(encoding="utf-8").splitlines() == ["true"]

    def test_condition_task_without_notebook_file(self, tmp_path):
        runner = self._runner(
            tmp_path,
            [
                {
                    "task_key": "check",
                    "condition_task": {
                        "op": "EQUAL_TO",
                        "left": "a",
                        "right": "a",
                    },
                }
            ],
            {},
        )
        runner.run_workflow()
        assert runner.task_statuses["check"] == "SUCCESS"
        assert runner.task_results["check"] == "true"

    def test_missing_task_type_still_raises(self, tmp_path):
        workflow = {"tasks": [{"task_key": "t1"}]}
        workflow_path = tmp_path / "workflow.json"
        workflow_path.write_text(json.dumps(workflow), encoding="utf-8")
        with pytest.raises(ValueError, match="is missing 'notebook_task'"):
            LocalWorkflowRunner(str(tmp_path), str(workflow_path), str(tmp_path))


class TestWorkflowValidation:
    def test_missing_tasks_raises(self, tmp_path):
        workflow_path = tmp_path / "workflow.json"
        workflow_path.write_text(json.dumps({}), encoding="utf-8")

        with pytest.raises(ValueError, match="must contain a 'tasks' list"):
            LocalWorkflowRunner(str(tmp_path), str(workflow_path), str(tmp_path))

    def test_non_dict_task_raises(self, tmp_path):
        workflow = {"tasks": ["not_a_dict"]}
        workflow_path = tmp_path / "workflow.json"
        workflow_path.write_text(json.dumps(workflow), encoding="utf-8")

        with pytest.raises(ValueError, match="Each task must be a JSON object"):
            LocalWorkflowRunner(str(tmp_path), str(workflow_path), str(tmp_path))

    def test_missing_task_key_raises(self, tmp_path):
        workflow = {"tasks": [{"notebook_task": {"notebook_path": "/a/b"}}]}
        workflow_path = tmp_path / "workflow.json"
        workflow_path.write_text(json.dumps(workflow), encoding="utf-8")

        with pytest.raises(ValueError, match="must include a non-empty 'task_key'"):
            LocalWorkflowRunner(str(tmp_path), str(workflow_path), str(tmp_path))

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
            LocalWorkflowRunner(str(tmp_path), str(workflow_path), str(tmp_path))

    def test_missing_notebook_task_raises(self, tmp_path):
        workflow = {"tasks": [{"task_key": "t1"}]}
        workflow_path = tmp_path / "workflow.json"
        workflow_path.write_text(json.dumps(workflow), encoding="utf-8")

        with pytest.raises(ValueError, match="is missing 'notebook_task'"):
            LocalWorkflowRunner(str(tmp_path), str(workflow_path), str(tmp_path))

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
            LocalWorkflowRunner(str(tmp_path), str(workflow_path), str(tmp_path))

    def test_non_dict_base_parameters_raises(self, tmp_path):
        workflow = {
            "tasks": [
                {
                    "task_key": "t1",
                    "notebook_task": {
                        "notebook_path": "/a/b",
                        "base_parameters": "not_a_dict",
                    },
                }
            ]
        }
        workflow_path = tmp_path / "workflow.json"
        workflow_path.write_text(json.dumps(workflow), encoding="utf-8")

        with pytest.raises(ValueError, match="invalid 'base_parameters' format"):
            LocalWorkflowRunner(str(tmp_path), str(workflow_path), str(tmp_path))

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
            LocalWorkflowRunner(str(tmp_path), str(workflow_path), str(tmp_path))

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
            LocalWorkflowRunner(str(tmp_path), str(workflow_path), str(tmp_path))

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
            LocalWorkflowRunner(str(tmp_path), str(workflow_path), str(tmp_path))

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
            LocalWorkflowRunner(str(tmp_path), str(workflow_path), str(tmp_path))

    def test_missing_notebook_file_raises(self, tmp_path):
        workflow = {
            "tasks": [
                {"task_key": "t1", "notebook_task": {"notebook_path": "/a/missing"}}
            ]
        }
        workflow_path = tmp_path / "workflow.json"
        workflow_path.write_text(json.dumps(workflow), encoding="utf-8")

        runner = LocalWorkflowRunner(str(tmp_path), str(workflow_path), str(tmp_path))
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
            LocalWorkflowRunner(str(tmp_path), str(workflow_path), str(tmp_path))


class TestPercentRunCommands:
    def test_transform_run_command_comment(self, tmp_path):
        notebook_path = str(tmp_path / "main.py")
        source = "# %run ./helpers/setup\nprint('done')\n"
        transformed = transform_run_commands(source, notebook_path)

        assert "# %run" not in transformed
        assert "__run_notebook__('./helpers/setup')" in transformed
        assert "print('done')" in transformed

    def test_transform_magic_run_command_comment(self, tmp_path):
        notebook_path = str(tmp_path / "main.py")
        source = "# MAGIC %run ../common/utils\n"
        transformed = transform_run_commands(source, notebook_path)

        assert "# MAGIC %run" not in transformed
        assert "__run_notebook__('../common/utils')" in transformed

    def test_run_notebook_executes_percent_run_target(self, tmp_path):
        helpers_dir = tmp_path / "helpers"
        helpers_dir.mkdir()
        (helpers_dir / "setup.py").write_text(
            "SHARED_VALUE = 'from_setup'\n", encoding="utf-8"
        )
        main_path = tmp_path / "main.py"
        main_path.write_text(
            "# %run ./helpers/setup\nRESULT = SHARED_VALUE\n", encoding="utf-8"
        )

        runner = LocalWorkflowRunner(str(tmp_path), _write_single_task_workflow(tmp_path), str(tmp_path))
        namespace = _notebook_namespace(str(main_path), runner)

        runner._execfile(str(main_path), namespace, namespace)

        assert namespace["RESULT"] == "from_setup"

    def test_nested_percent_run_commands(self, tmp_path):
        common_dir = tmp_path / "common"
        common_dir.mkdir()
        (common_dir / "base.py").write_text("BASE_VALUE = 10\n", encoding="utf-8")
        (tmp_path / "middle.py").write_text(
            "# %run ./common/base\nMIDDLE_VALUE = BASE_VALUE + 1\n", encoding="utf-8"
        )
        main_path = tmp_path / "main.py"
        main_path.write_text(
            "# %run ./middle\nRESULT = MIDDLE_VALUE + 1\n", encoding="utf-8"
        )

        runner = LocalWorkflowRunner(str(tmp_path), _write_single_task_workflow(tmp_path), str(tmp_path))
        namespace = _notebook_namespace(str(main_path), runner)

        runner._execfile(str(main_path), namespace, namespace)

        assert namespace["RESULT"] == 12

    def test_empty_percent_run_path_raises(self, tmp_path):
        notebook_path = str(tmp_path / "main.py")

        with pytest.raises(ValueError, match="Empty %run path"):
            transform_run_commands("# %run   \n", notebook_path)


class TestTopLevelNotebookExit:
    def test_top_level_exit_stops_file(self, tmp_path):
        notebook_path = tmp_path / "main.py"
        marker = tmp_path / "marker.txt"
        notebook_path.write_text(
            'dbutils.notebook.exit("early")\n'
            f'open(r"{marker}", "w").write("ran")\n',
            encoding="utf-8",
        )

        runner = LocalWorkflowRunner(
            str(tmp_path), _write_single_task_workflow(tmp_path), str(tmp_path)
        )
        runner.run_workflow()

        assert not marker.exists()

    def test_top_level_exit_continues_workflow(self, tmp_path):
        source_dir = tmp_path / "local_src"
        source_dir.mkdir()
        log_file = tmp_path / "execution.log"

        (source_dir / "first.py").write_text(
            'dbutils.notebook.exit("early")\n'
            f'with open(r"{log_file}", "a") as f: f.write("first_after\\n")',
            encoding="utf-8",
        )
        (source_dir / "second.py").write_text(
            f'with open(r"{log_file}", "a") as f: f.write("second\\n")',
            encoding="utf-8",
        )

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
            ]
        }
        workflow_path = tmp_path / "workflow.json"
        workflow_path.write_text(json.dumps(workflow), encoding="utf-8")

        runner = LocalWorkflowRunner(str(source_dir), str(workflow_path), str(tmp_path))
        runner.run_workflow()

        lines = log_file.read_text(encoding="utf-8").splitlines()
        assert lines == ["second"]


def _notebook_namespace(main_path, runner):
    namespace = {"__name__": "__main__", "__file__": main_path}
    namespace["__run_notebook__"] = lambda path: runner._run_notebook(path, namespace)
    return namespace


def _write_single_task_workflow(tmp_path):
    workflow = {
        "tasks": [
            {
                "task_key": "task_1",
                "notebook_task": {"notebook_path": "/Workspace/any/main"},
            }
        ]
    }
    workflow_path = tmp_path / "workflow.json"
    workflow_path.write_text(json.dumps(workflow), encoding="utf-8")
    return str(workflow_path)
