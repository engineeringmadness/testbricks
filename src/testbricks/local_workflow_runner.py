import json
import os
import re
import time
from dataclasses import dataclass, field
from graphlib import CycleError, TopologicalSorter

from testbricks.dbutils import configure, dbutils
from testbricks.dbutils.widgets import argument_override_context, seeded_environ
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

CONDITION_OPS = {
    "EQUAL",
    "EQUAL_TO",
    "NOT_EQUAL",
    "NOT_EQUAL_TO",
    "GREATER_THAN",
    "GREATER_THAN_OR_EQUAL",
    "LESS_THAN",
    "LESS_THAN_OR_EQUAL",
}

TASK_VALUE_RE = re.compile(r"^\{\{\s*tasks\.([^.]+)\.values\.([^}]+?)\s*\}\}$")
JOB_PARAM_RE = re.compile(r"^\{\{\s*job\.parameters\.([^}]+?)\s*\}\}$")


def _require(condition, message):
    if not condition:
        raise ValueError(message)


@dataclass(frozen=True)
class TaskSpec:
    """Validated, kind-agnostic view of one workflow task."""

    task_key: str
    kind: str  # "notebook" | "condition" | "for_each"
    notebook_name: str | None = None
    base_parameters: dict = field(default_factory=dict)
    depends_on: list = field(default_factory=list)
    dep_specs: list = field(default_factory=list)
    run_if: str = "ALL_SUCCESS"
    condition_task: dict | None = None
    for_each_task: dict | None = None
    max_retries: int = 0
    retry_interval_ms: int = 0
    timeout_seconds: int = 0


def _extract_notebook_name(notebook_path, task_key):
    _require(
        isinstance(notebook_path, str) and notebook_path.strip(),
        f"Task '{task_key}' has an invalid notebook_path",
    )
    notebook_name = notebook_path.rstrip("/").split("/")[-1]
    _require(notebook_name, f"Task '{task_key}' has an invalid notebook_path")
    return notebook_name


def _parse_non_negative_int(raw, default, field_name, task_key):
    if raw is None:
        return default
    try:
        value = int(raw)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Task '{task_key}' has invalid '{field_name}': {raw!r}") from exc
    _require(value >= 0, f"Task '{task_key}' has invalid '{field_name}': {raw!r}")
    return value


def _parse_condition_task(condition_task, task_key):
    _require(
        condition_task.get("op"),
        f"Task '{task_key}' has invalid 'condition_task'",
    )
    op = str(condition_task.get("op")).upper()
    _require(
        op in CONDITION_OPS,
        f"Unsupported condition op '{condition_task.get('op')}' on task '{task_key}'",
    )
    _require(
        "left" in condition_task and "right" in condition_task,
        f"Task '{task_key}' condition_task requires 'left' and 'right'",
    )
    return {**condition_task, "op": op}


def _parse_for_each_task(for_each_task, task_key):
    nested = for_each_task.get("task")
    _require(
        isinstance(nested, dict),
        f"Task '{task_key}' for_each_task requires a nested 'task'",
    )
    nested_notebook = nested.get("notebook_task")
    _require(
        isinstance(nested_notebook, dict),
        f"Task '{task_key}' for_each nested task is missing 'notebook_task'",
    )
    _extract_notebook_name(nested_notebook.get("notebook_path"), task_key)
    nested_params = nested_notebook.get("base_parameters", {})
    _require(
        isinstance(nested_params, dict),
        f"Task '{task_key}' has invalid nested 'base_parameters' format",
    )
    _require(
        "inputs" in for_each_task,
        f"Task '{task_key}' for_each_task requires 'inputs'",
    )
    return for_each_task


def _parse_dependencies(depends_on, task_key):
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
    return dependency_keys, dep_specs


def _parse_run_if(run_if, task_key):
    run_if = run_if or "ALL_SUCCESS"
    _require(
        isinstance(run_if, str) and run_if.upper() in RUN_IF_VALUES,
        f"Unsupported run_if '{run_if}' on task '{task_key}'",
    )
    return run_if.upper()


def parse_workflow(tasks) -> list[TaskSpec]:
    """Validate a workflow ``tasks`` list and return one TaskSpec per task."""
    specs = []
    seen_task_keys = set()
    seen_notebook_names = set()
    for task in tasks:
        _require(isinstance(task, dict), "Each task must be a JSON object")
        task_key = task.get("task_key")
        _require(task_key, "Each task must include a non-empty 'task_key'")
        _require(task_key not in seen_task_keys, f"Duplicate task_key found: {task_key}")
        seen_task_keys.add(task_key)

        notebook_task = task.get("notebook_task")
        condition_task = task.get("condition_task")
        for_each_task = task.get("for_each_task")
        has_notebook = isinstance(notebook_task, dict)
        has_condition = isinstance(condition_task, dict)
        has_for_each = isinstance(for_each_task, dict)
        _require(
            has_notebook or has_condition or has_for_each,
            f"Task '{task_key}' is missing 'notebook_task'",
        )
        _require(
            [has_notebook, has_condition, has_for_each].count(True) == 1,
            f"Task '{task_key}' cannot mix notebook, condition, and for_each tasks",
        )

        notebook_name = None
        base_parameters = {}
        if has_notebook:
            notebook_name = _extract_notebook_name(notebook_task.get("notebook_path"), task_key)
            _require(
                notebook_name not in seen_notebook_names,
                f"Duplicate notebook name found: {notebook_name}",
            )
            seen_notebook_names.add(notebook_name)
            base_parameters = notebook_task.get("base_parameters", {})
            _require(
                isinstance(base_parameters, dict),
                f"Task '{task_key}' has invalid 'base_parameters' format",
            )
        elif has_condition:
            condition_task = _parse_condition_task(condition_task, task_key)
        else:
            for_each_task = _parse_for_each_task(for_each_task, task_key)

        depends_on, dep_specs = _parse_dependencies(task.get("depends_on", []), task_key)
        run_if = _parse_run_if(task.get("run_if"), task_key)

        if has_condition:
            kind = "condition"
        elif has_for_each:
            kind = "for_each"
        else:
            kind = "notebook"

        specs.append(
            TaskSpec(
                task_key=task_key,
                kind=kind,
                notebook_name=notebook_name,
                base_parameters=base_parameters,
                depends_on=depends_on,
                dep_specs=dep_specs,
                run_if=run_if,
                condition_task=condition_task,
                for_each_task=for_each_task,
                max_retries=_parse_non_negative_int(
                    task.get("max_retries"), 0, "max_retries", task_key
                ),
                retry_interval_ms=_parse_non_negative_int(
                    task.get("min_retry_interval_millis"),
                    0,
                    "min_retry_interval_millis",
                    task_key,
                ),
                timeout_seconds=_parse_non_negative_int(
                    task.get("timeout_seconds"), 0, "timeout_seconds", task_key
                ),
            )
        )
    return specs


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


def _as_numbers(left, right):
    try:
        return float(left), float(right)
    except (TypeError, ValueError):
        return None


def _compare_condition(op, left, right):
    normalized = op.upper()
    nums = _as_numbers(left, right)
    if normalized in {"EQUAL", "EQUAL_TO"}:
        if nums is not None:
            return nums[0] == nums[1]
        return str(left) == str(right)
    if normalized in {"NOT_EQUAL", "NOT_EQUAL_TO"}:
        if nums is not None:
            return nums[0] != nums[1]
        return str(left) != str(right)
    if nums is None:
        raise ValueError(
            f"Condition op '{op}' requires numeric operands, got {left!r} and {right!r}"
        )
    left_n, right_n = nums
    if normalized == "GREATER_THAN":
        return left_n > right_n
    if normalized == "GREATER_THAN_OR_EQUAL":
        return left_n >= right_n
    if normalized == "LESS_THAN":
        return left_n < right_n
    if normalized == "LESS_THAN_OR_EQUAL":
        return left_n <= right_n
    raise ValueError(f"Unsupported condition op '{op}'")


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
        self._task_kind = {}
        self._condition_task = {}
        self._for_each_task = {}
        self._task_max_retries = {}
        self._task_retry_interval_ms = {}
        self._task_timeout_seconds = {}
        self._task_insertion_order = []
        self._notebook_insertion_order = []
        self._task_base_params = {}
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
        for spec in parse_workflow(tasks):
            task_key = spec.task_key
            self._task_kind[task_key] = spec.kind
            self._condition_task[task_key] = spec.condition_task
            self._for_each_task[task_key] = spec.for_each_task
            self._task_to_notebook[task_key] = spec.notebook_name
            if spec.notebook_name is not None:
                self._notebook_to_task[spec.notebook_name] = task_key
                self._notebook_insertion_order.append(spec.notebook_name)
                self._notebook_base_params[spec.notebook_name] = spec.base_parameters
            self._task_dependencies[task_key] = spec.depends_on
            self._task_dep_specs[task_key] = spec.dep_specs
            self._task_run_if[task_key] = spec.run_if
            self._task_max_retries[task_key] = spec.max_retries
            self._task_retry_interval_ms[task_key] = spec.retry_interval_ms
            self._task_timeout_seconds[task_key] = spec.timeout_seconds
            self._task_insertion_order.append(task_key)
            self._task_base_params[task_key] = spec.base_parameters

    def _build_graphs(self):
        successors = {key: set() for key in self._task_insertion_order}
        predecessors = {key: set() for key in self._task_insertion_order}
        for task_key, dependency_keys in self._task_dependencies.items():
            for dependency_key in dependency_keys:
                _require(
                    dependency_key in self._task_kind,
                    f"Task '{task_key}' depends on unknown task '{dependency_key}'",
                )
                successors[dependency_key].add(task_key)
                predecessors[task_key].add(dependency_key)
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
            if outcome is not None and not self._outcome_matches(dep_key, status, outcome):
                return False
        statuses = [self.task_statuses[dep_key] for dep_key, _ in specs]
        return _matches_run_if(self._task_run_if[task_key], statuses)

    def _resolve_operand(self, raw, store):
        if raw is None:
            return ""
        if not isinstance(raw, str):
            return str(raw)
        text = raw.strip()
        match = TASK_VALUE_RE.match(text)
        if match:
            return store.get(taskKey=match.group(1), key=match.group(2).strip())
        match = JOB_PARAM_RE.match(text)
        if match:
            key = match.group(1).strip()
            if key not in os.environ:
                raise ValueError(f"Job parameter '{key}' not found")
            return os.environ[key]
        return raw

    def _evaluate_condition(self, task_key, store):
        spec = self._condition_task[task_key]
        left = self._resolve_operand(spec.get("left"), store)
        right = self._resolve_operand(spec.get("right"), store)
        matched = _compare_condition(spec["op"], left, right)
        return "true" if matched else "false"

    def format_dag(self):
        lines = []
        for task_key in self._task_insertion_order:
            outgoing = sorted(self.dag.get(task_key, set()))
            lines.append(f"- {task_key} -> [{', '.join(outgoing)}]")
        lines.append("Execution order:")
        lines.append(" -> ".join(self.execution_order))
        return "\n".join(lines)

    def _selected_tasks(self, only, from_task):
        if only is not None and from_task is not None:
            raise ValueError("Pass only one of 'only' or 'from_task'")
        if only is None and from_task is None:
            return None
        if only is not None:
            selected = set(only)
            unknown = selected - set(self._task_kind)
            _require(not unknown, f"unknown task in only: {sorted(unknown)}")
            return selected
        _require(
            from_task in self._task_kind,
            f"unknown task in from_task: '{from_task}'",
        )
        selected = {from_task}
        stack = [from_task]
        while stack:
            current = stack.pop()
            for successor in self.dag.get(current, ()):
                if successor not in selected:
                    selected.add(successor)
                    stack.append(successor)
        return selected

    def _run_task_with_retries(self, task_key, executor, store, execution_globals, extra_globals):
        timeout = self._task_timeout_seconds.get(task_key) or 0
        if timeout:
            print(f"Task '{task_key}' timeout_seconds={timeout} accepted but not enforced")

        def action():
            kind = self._task_kind[task_key]
            if kind == "condition":
                self.task_results[task_key] = self._evaluate_condition(task_key, store)
            elif kind == "for_each":
                self._run_for_each_task(task_key, executor, store, extra_globals)
            else:
                self._run_notebook_task(task_key, executor, store, execution_globals)

        retries = self._task_max_retries.get(task_key, 0)
        interval_ms = self._task_retry_interval_ms.get(task_key, 0)
        attempt = 0
        while True:
            try:
                action()
                return
            except Exception as exc:
                attempt += 1
                if attempt > retries:
                    raise
                print(
                    f"Task '{task_key}' failed (attempt {attempt}/{retries + 1}), retrying: {exc}"
                )
                if interval_ms:
                    time.sleep(interval_ms / 1000.0)

    def _executor(self):
        return dbutils.executor

    def _run_notebook(self, relative_path, namespace):
        self._executor().run_shared(relative_path, namespace)

    def _execfile(self, file_path, global_namespace, local_namespace):
        self._executor().exec_file(file_path, global_namespace, top_level=False)

    def _run_notebook_task(self, task_key, executor, store, execution_globals):
        notebook_name = self._task_to_notebook[task_key]
        notebook_path = os.path.join(self.source_dir, f"{notebook_name}.py")
        if not os.path.exists(notebook_path):
            raise FileNotFoundError(f"Notebook file not found: {notebook_path}")

        base_params = self._task_base_params.get(task_key, {})
        with (
            seeded_environ(base_params, overwrite=False),
            argument_override_context(base_params.keys()),
            store.current_task(task_key),
        ):
            for param_key, param_value in base_params.items():
                store.set(key=param_key, value=param_value, update_env=False)
            executor.exec_file(notebook_path, execution_globals, top_level=True)

    def _input_as_str(self, item):
        if isinstance(item, (dict, list)):
            return json.dumps(item)
        return str(item)

    def _render_input_template(self, value, item, index):
        if not isinstance(value, str):
            return str(value)
        text = value.strip()
        if re.match(r"^\{\{\s*input\s*\}\}$", text):
            return self._input_as_str(item)
        field = re.match(r"^\{\{\s*input\.([^}]+?)\s*\}\}$", text)
        if field:
            key = field.group(1).strip()
            if isinstance(item, dict):
                return self._input_as_str(item.get(key))
            raise ValueError(f"for_each input is not an object; cannot read '{key}'")
        if "{{input}}" in value:
            return value.replace("{{input}}", self._input_as_str(item))
        return value

    def _resolve_for_each_inputs(self, raw, store):
        if isinstance(raw, list):
            return raw
        if not isinstance(raw, str):
            raise ValueError(f"for_each inputs must be a JSON list, got {raw!r}")
        text = raw.strip()
        match = TASK_VALUE_RE.match(text)
        if match:
            text = store.get(taskKey=match.group(1), key=match.group(2).strip())
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError as exc:
            raise ValueError(f"for_each inputs must be a JSON list: {raw!r}") from exc
        if not isinstance(parsed, list):
            raise ValueError(f"for_each inputs must be a JSON list: {raw!r}")
        return parsed

    def _run_for_each_task(self, task_key, executor, store, extra_globals):
        spec = self._for_each_task[task_key]
        inputs = self._resolve_for_each_inputs(spec.get("inputs"), store)
        nested = spec["task"]
        notebook_task = nested["notebook_task"]
        notebook_name = _extract_notebook_name(notebook_task.get("notebook_path"), task_key)
        nested_key = nested.get("task_key") or task_key
        base_parameters = notebook_task.get("base_parameters", {}) or {}
        caller = os.path.join(self.source_dir, "_workflow.py")
        for index, item in enumerate(inputs):
            params = {
                key: self._render_input_template(value, item, index)
                for key, value in base_parameters.items()
            }
            with (
                store.current_task(nested_key),
                executor.caller_context(caller),
            ):
                executor.run_isolated(
                    f"/Workspace/{notebook_name}",
                    arguments=params,
                    extra=extra_globals,
                )

    def run_workflow(self, extra_globals=None, only=None, from_task=None):
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
        selected = self._selected_tasks(only, from_task)
        first_error = None
        for task_key in self.execution_order:
            if selected is not None and task_key not in selected:
                self.task_statuses[task_key] = "SUCCESS"
                continue
            if not self._is_eligible(task_key):
                self.task_statuses[task_key] = "SKIPPED"
                print(f"Skipping task '{task_key}': run_if not met")
                continue
            try:
                self._run_task_with_retries(
                    task_key, executor, store, execution_globals, extra_globals
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
