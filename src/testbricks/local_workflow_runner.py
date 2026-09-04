import json
import os
from contextlib import contextmanager
from graphlib import CycleError, TopologicalSorter

from testbricks.notebook_executor import transform_run_commands

__all__ = ["LocalWorkflowRunner", "transform_run_commands"]

RUN_IF_VALUES = {
    "ALL_SUCCESS",
    "ALL_FAILED",
    "AT_LEAST_ONE_SUCCESS",
    "ALL_DONE",
    "NONE_FAILED",
    "AT_LEAST_ONE_FAILED",
}


def _require(condition, message):
    if not condition:
        raise ValueError(message)


def _matches_run_if(run_if, statuses):
    if not statuses:
        return True
    if run_if == "ALL_SUCCESS":
        return all(status == "SUCCESS" for status in statuses)
    if run_if == "ALL_FAILED":
        return all(status == "FAILED" for status in statuses)
    if run_if == "AT_LEAST_ONE_SUCCESS":
        return any(status == "SUCCESS" for status in statuses)
    if run_if == "ALL_DONE":
        return True
    if run_if == "NONE_FAILED":
        return all(status != "FAILED" for status in statuses)
    if run_if == "AT_LEAST_ONE_FAILED":
        return any(status == "FAILED" for status in statuses)
    return False


class LocalWorkflowRunner:
    def __init__(self, source_dir, workflow_json_path, base_path):
        self.source_dir = source_dir
        self.workflow_json_path = workflow_json_path
        self.base_path = base_path

        with open(workflow_json_path, encoding="utf-8") as workflow_file:
            workflow = json.load(workflow_file)
        tasks = workflow.get("tasks")
        _require(isinstance(tasks, list), "Workflow JSON must contain a 'tasks' list")

        self._task_to_notebook = {}
        self._notebook_to_task = {}
        self._task_dependencies = {}
        self._task_dep_specs = {}
        self._task_run_if = {}
        self._notebook_insertion_order = []
        self._notebook_base_params = {}
        self.task_statuses = {}
        self.task_results = {}
        self._parse_tasks(tasks)

        self.dag, predecessors = self._build_graphs()
        try:
            self.execution_order = list(TopologicalSorter(predecessors).static_order())
        except CycleError as exc:
            raise ValueError("Workflow graph contains a cycle") from exc

    def _parse_tasks(self, tasks):
        for task in tasks:
            _require(isinstance(task, dict), "Each task must be a JSON object")
            task_key = task.get("task_key")
            _require(task_key, "Each task must include a non-empty 'task_key'")
            _require(
                task_key not in self._task_to_notebook,
                f"Duplicate task_key found: {task_key}",
            )

            notebook_task = task.get("notebook_task")
            _require(
                isinstance(notebook_task, dict),
                f"Task '{task_key}' is missing 'notebook_task'",
            )
            notebook_name = self._extract_notebook_name(
                notebook_task.get("notebook_path"), task_key
            )
            _require(
                notebook_name not in self._notebook_insertion_order,
                f"Duplicate notebook name found: {notebook_name}",
            )

            base_parameters = notebook_task.get("base_parameters", {})
            _require(
                isinstance(base_parameters, dict),
                f"Task '{task_key}' has invalid 'base_parameters' format",
            )

            depends_on = task.get("depends_on", [])
            _require(
                isinstance(depends_on, list),
                f"Task '{task_key}' has invalid 'depends_on' format",
            )
            dependency_keys = []
            dep_specs = []
            for dependency in depends_on:
                _require(
                    isinstance(dependency, dict) and dependency.get("task_key"),
                    f"Task '{task_key}' has malformed dependency entry",
                )
                dependency_keys.append(dependency["task_key"])
                dep_specs.append((dependency["task_key"], dependency.get("outcome")))

            run_if = task.get("run_if") or "ALL_SUCCESS"
            _require(
                isinstance(run_if, str) and run_if.upper() in RUN_IF_VALUES,
                f"Unsupported run_if '{run_if}' on task '{task_key}'",
            )

            self._task_to_notebook[task_key] = notebook_name
            self._notebook_to_task[notebook_name] = task_key
            self._task_dependencies[task_key] = dependency_keys
            self._task_dep_specs[task_key] = dep_specs
            self._task_run_if[task_key] = run_if.upper()
            self._notebook_insertion_order.append(notebook_name)
            self._notebook_base_params[notebook_name] = base_parameters

    def _extract_notebook_name(self, notebook_path, task_key):
        _require(
            isinstance(notebook_path, str) and notebook_path.strip(),
            f"Task '{task_key}' has an invalid notebook_path",
        )
        notebook_name = notebook_path.rstrip("/").split("/")[-1]
        _require(notebook_name, f"Task '{task_key}' has an invalid notebook_path")
        return notebook_name

    def _build_graphs(self):
        successors = {name: set() for name in self._notebook_insertion_order}
        predecessors = {name: set() for name in self._notebook_insertion_order}
        for task_key, dependency_keys in self._task_dependencies.items():
            current = self._task_to_notebook[task_key]
            for dependency_key in dependency_keys:
                _require(
                    dependency_key in self._task_to_notebook,
                    f"Task '{task_key}' depends on unknown task '{dependency_key}'",
                )
                dependency = self._task_to_notebook[dependency_key]
                successors[dependency].add(current)
                predecessors[current].add(dependency)
        return successors, predecessors

    def _outcome_matches(self, dep_key, status, outcome):
        wanted = str(outcome).upper()
        if wanted in {"SUCCESS", "FAILED", "SKIPPED"}:
            return status == wanted
        result = self.task_results.get(dep_key)
        if result is not None:
            return str(result).lower() == str(outcome).lower()
        return status == wanted

    def _is_eligible(self, task_key):
        specs = self._task_dep_specs.get(task_key, [])
        if not specs:
            return True
        for dep_key, outcome in specs:
            status = self.task_statuses.get(dep_key)
            if status is None:
                return False
            if outcome is not None and not self._outcome_matches(
                dep_key, status, outcome
            ):
                return False
        statuses = [self.task_statuses[dep_key] for dep_key, _ in specs]
        return _matches_run_if(self._task_run_if[task_key], statuses)

    def format_dag(self):
        lines = []
        for notebook_name in self._notebook_insertion_order:
            outgoing = sorted(self.dag.get(notebook_name, set()))
            lines.append(f"- {notebook_name} -> [{', '.join(outgoing)}]")
        lines.append("Execution order:")
        lines.append(" -> ".join(self.execution_order))
        return "\n".join(lines)

    def _executor(self):
        from testbricks.dbutils import dbutils

        return dbutils.executor

    def _run_notebook(self, relative_path, namespace):
        self._executor().run_shared(relative_path, namespace)

    def _execfile(self, file_path, global_namespace, local_namespace):
        self._executor().exec_file(file_path, global_namespace, top_level=False)

    @contextmanager
    def _seeded_env(self, values):
        saved = {key: os.environ.get(key) for key in values}
        for key, value in values.items():
            os.environ.setdefault(key, str(value))
        try:
            yield
        finally:
            for key, original in saved.items():
                if original is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = original

    def run_workflow(self, extra_globals=None):
        from testbricks.dbutils import configure, dbutils
        from testbricks.dbutils.widgets import argument_override_context

        configure(self.base_path, source_dir=self.source_dir)
        executor = dbutils.executor
        store = dbutils.jobs.taskValues
        store.clear()
        execution_globals = executor.namespace("", extra=extra_globals)
        print(f"\nExecuting workflow: {self.workflow_json_path}\n")
        print("==========================================")
        print(self.format_dag())
        self.task_statuses = {}
        self.task_results = {}
        first_error = None
        for notebook_name in self.execution_order:
            task_key = self._notebook_to_task[notebook_name]
            if not self._is_eligible(task_key):
                self.task_statuses[task_key] = "SKIPPED"
                print(f"Skipping task '{task_key}' ({notebook_name}): run_if not met")
                continue
            notebook_path = os.path.join(self.source_dir, f"{notebook_name}.py")
            if not os.path.exists(notebook_path):
                raise FileNotFoundError(f"Notebook file not found: {notebook_path}")
            base_params = self._notebook_base_params.get(notebook_name, {})
            with (
                self._seeded_env(base_params),
                argument_override_context(base_params.keys()),
                store.current_task(task_key),
            ):
                for param_key, param_value in base_params.items():
                    store.set(key=param_key, value=param_value, update_env=False)
                try:
                    executor.exec_file(
                        notebook_path, execution_globals, top_level=True
                    )
                except Exception as exc:
                    self.task_statuses[task_key] = "FAILED"
                    if first_error is None:
                        first_error = exc
                    print(f"Task '{task_key}' failed: {exc}")
                    continue
            self.task_statuses[task_key] = "SUCCESS"
        if first_error is not None:
            raise first_error
