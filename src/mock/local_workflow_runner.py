import json
import os
import re
from graphlib import CycleError, TopologicalSorter

RUN_COMMAND_PATTERN = re.compile(
    r"^\s*#\s*(?:MAGIC\s+)?%run\s+(.+?)\s*$",
    re.MULTILINE,
)


def parse_run_path(raw_path, file_path):
    path = raw_path.strip()
    if len(path) >= 2 and path[0] == path[-1] and path[0] in ("'", '"'):
        path = path[1:-1]
    path = path.strip()
    if not path:
        raise ValueError(f"Empty %run path in notebook '{file_path}'")
    return path


def transform_run_commands(source, file_path):
    def replace(match):
        relative_path = parse_run_path(match.group(1), file_path)
        return f"__run_notebook__({relative_path!r})"

    return RUN_COMMAND_PATTERN.sub(replace, source)


class LocalWorkflowRunner:
    def __init__(self, source_dir, workflow_json_path, base_path):
        self.source_dir = source_dir
        self.workflow_json_path = workflow_json_path
        self.base_path = base_path

        workflow = self._load_workflow()
        tasks = workflow.get("tasks")
        if not isinstance(tasks, list):
            raise ValueError("Workflow JSON must contain a 'tasks' list")

        self._task_to_notebook = {}
        self._task_dependencies = {}
        self._notebook_insertion_order = []
        self._parse_tasks(tasks)

        self.dag = self._build_dag()
        self.execution_order = self._build_execution_order()

    def _load_workflow(self):
        with open(self.workflow_json_path, "r", encoding="utf-8") as workflow_file:
            return json.load(workflow_file)

    def _parse_tasks(self, tasks):
        for task in tasks:
            if not isinstance(task, dict):
                raise ValueError("Each task must be a JSON object")

            task_key = task.get("task_key")
            if not task_key:
                raise ValueError("Each task must include a non-empty 'task_key'")
            if task_key in self._task_to_notebook:
                raise ValueError(f"Duplicate task_key found: {task_key}")

            notebook_task = task.get("notebook_task")
            if not isinstance(notebook_task, dict):
                raise ValueError(f"Task '{task_key}' is missing 'notebook_task'")

            notebook_path = notebook_task.get("notebook_path")
            notebook_name = self._extract_notebook_name(notebook_path, task_key)
            if notebook_name in self._notebook_insertion_order:
                raise ValueError(f"Duplicate notebook name found: {notebook_name}")

            depends_on = task.get("depends_on", [])
            dependency_keys = []
            if not isinstance(depends_on, list):
                raise ValueError(f"Task '{task_key}' has invalid 'depends_on' format")
            for dependency in depends_on:
                if not isinstance(dependency, dict) or not dependency.get("task_key"):
                    raise ValueError(
                        f"Task '{task_key}' has malformed dependency entry"
                    )
                dependency_keys.append(dependency["task_key"])

            self._task_to_notebook[task_key] = notebook_name
            self._task_dependencies[task_key] = dependency_keys
            self._notebook_insertion_order.append(notebook_name)

    def _extract_notebook_name(self, notebook_path, task_key):
        if not isinstance(notebook_path, str) or not notebook_path.strip():
            raise ValueError(f"Task '{task_key}' has an invalid notebook_path")

        notebook_name = notebook_path.rstrip("/").split("/")[-1]
        if not notebook_name:
            raise ValueError(f"Task '{task_key}' has an invalid notebook_path")
        return notebook_name

    def _build_dag(self):
        dag = {notebook_name: set() for notebook_name in self._notebook_insertion_order}

        for task_key, dependency_keys in self._task_dependencies.items():
            current_notebook = self._task_to_notebook[task_key]
            for dependency_key in dependency_keys:
                if dependency_key not in self._task_to_notebook:
                    raise ValueError(
                        f"Task '{task_key}' depends on unknown task '{dependency_key}'"
                    )

                dependency_notebook = self._task_to_notebook[dependency_key]
                dag[dependency_notebook].add(current_notebook)

        return dag

    def _build_execution_order(self):
        predecessors = {
            notebook_name: set() for notebook_name in self._notebook_insertion_order
        }

        for source_notebook, target_notebooks in self.dag.items():
            for target_notebook in target_notebooks:
                predecessors[target_notebook].add(source_notebook)

        try:
            return list(TopologicalSorter(predecessors).static_order())
        except CycleError as exc:
            raise ValueError("Workflow graph contains a cycle") from exc

    def format_dag(self):
        lines = []
        for notebook_name in self._notebook_insertion_order:
            outgoing = sorted(self.dag.get(notebook_name, set()))
            if outgoing:
                lines.append(f"- {notebook_name} -> [{', '.join(outgoing)}]")
            else:
                lines.append(f"- {notebook_name} -> []")

        lines.append("Execution order:")
        lines.append(" -> ".join(self.execution_order))
        return "\n".join(lines)

    def _resolve_notebook_path(self, relative_path, base_file):
        notebook_path = os.path.normpath(
            os.path.join(os.path.dirname(base_file), relative_path)
        )
        if not notebook_path.endswith(".py"):
            notebook_path += ".py"
        return notebook_path

    def _run_notebook(self, relative_path, namespace):
        notebook_path = self._resolve_notebook_path(
            relative_path, namespace["__file__"]
        )
        self._execfile(notebook_path, namespace, namespace)

    def _execute_source(self, source, file_path, global_namespace, local_namespace):
        transformed = transform_run_commands(source, file_path)
        exec(compile(transformed, file_path, "exec"), global_namespace, local_namespace)

    def _execfile(self, file_path, global_namespace, local_namespace):
        with open(file_path, encoding="utf-8") as notebook_file:
            source = notebook_file.read()
        global_namespace["__file__"] = file_path
        self._execute_source(source, file_path, global_namespace, local_namespace)

    def run_workflow(self):
        from mock.dbutils import configure, dbutils

        configure(self.base_path)
        execution_globals = {
            "__name__": "__main__",
            "dbutils": dbutils,
        }
        execution_globals["__run_notebook__"] = lambda path: self._run_notebook(
            path, execution_globals
        )
        print(f"\nExecuting workflow: {self.workflow_json_path}\n")
        print("==========================================")
        print(self.format_dag())
        for notebook_name in self.execution_order:
            notebook_path = os.path.join(self.source_dir, f"{notebook_name}.py")
            if not os.path.exists(notebook_path):
                raise FileNotFoundError(f"Notebook file not found: {notebook_path}")
            execution_globals["__file__"] = notebook_path
            self._execfile(notebook_path, execution_globals, execution_globals)
