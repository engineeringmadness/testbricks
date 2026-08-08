# dbutils.notebook Mock Design

**Date:** 2026-08-08  
**Status:** Approved for implementation  
**Scope:** `dbutils.notebook` module — `run`, `exit`; shared `NotebookExecutor` for `%run` and `dbutils.notebook.run()`

## Goal

Implement `dbutils.notebook.run()` and `dbutils.notebook.exit()` so Databricks notebook orchestration patterns work locally. Extract a shared `NotebookExecutor` that powers both `%run` (shared namespace) and `dbutils.notebook.run()` (isolated namespace), replacing the execution logic currently embedded in `LocalWorkflowRunner`.

This complements the existing `dbutils.fs`, `dbutils.widgets`, and `SparkMock` proxies.

## Requirements Summary

| Decision | Choice |
|---|---|
| `run()` execution model | Databricks-isolated — fresh namespace; child variables do not leak to parent |
| `exit()` at top level | Stop current notebook file only; `LocalWorkflowRunner` continues to next task |
| Path resolution | Relative paths from caller's `__file__`; workspace-style paths (`/Workspace/...`, `/Repos/...`) from configured `source_dir` |
| `timeout_seconds` | Accepted for API compatibility; not enforced locally |
| `arguments` | Merged into global `os.environ`; argument keys protected from widget default overwrite |
| `run()` return value | `str` exit value from child's `exit()`; `""` if child completes without calling `exit()` |
| Architecture | Shared `NotebookExecutor` used by `%run`, `dbutils.notebook.run()`, and workflow top-level execution |

## Architecture

```
src/mock/
  notebook_executor.py     # NotebookExecutor, NotebookExit, path resolution, %run transform
  local_workflow_runner.py # DAG orchestration; delegates exec to NotebookExecutor

src/mock/dbutils/
  notebook.py              # NotebookMock: run(), exit()
  dbutils_mock.py          # self.notebook = NotebookMock(executor)
  widgets.py               # Minor change: respect argument-override context
  __init__.py              # configure(base_path, source_dir=None)
```

### Component Responsibilities

| Component | Responsibility |
|---|---|
| `NotebookExit` | Control-flow exception carrying exit value; not a `DbutilsError` |
| `NotebookExecutor` | Resolve paths, transform `%run`, execute notebooks in shared/isolated/top-level modes |
| `NotebookMock` | Public `dbutils.notebook` API; delegates to `NotebookExecutor` |
| `WidgetsMock` | Honor argument-override context so `run()` arguments are not clobbered by widget defaults |
| `LocalWorkflowRunner` | Workflow DAG + injection of `dbutils`, `__run_notebook__`; uses `NotebookExecutor` for all file execution |
| `configure()` | Store `base_path` (existing) and optional `source_dir` for workspace path resolution |

### Execution Modes

| Mode | Trigger | Namespace | `NotebookExit` handling |
|---|---|---|---|
| **SHARED** | `%run` / `__run_notebook__` | Parent namespace (variables propagate) | Propagates — stops caller file execution |
| **ISOLATED** | `dbutils.notebook.run()` | Fresh dict with injected `dbutils`, `__file__`, `__name__` | Caught — value returned to `run()` caller |
| **TOP-LEVEL** | `LocalWorkflowRunner` task execution | Workflow `execution_globals` | Caught — stops current file; workflow continues |

### Data Flow — `dbutils.notebook.run()`

```
Parent notebook
  → dbutils.notebook.run("./child", 60, {"env": "prod"})
  → NotebookMock.run()
  → set os.environ["env"] = "prod" + track override keys
  → NotebookExecutor.run_isolated("./child", arguments)
  → resolve path relative to caller __file__
  → exec child in fresh namespace (with dbutils injected)
  → child calls dbutils.notebook.exit("done")
  → NotebookExit caught
  → return "done"
```

### Data Flow — `%run` (unchanged behavior)

```
Parent notebook with "# %run ./helpers/setup"
  → transform_run_commands → __run_notebook__("./helpers/setup")
  → NotebookExecutor.run_shared("./helpers/setup", parent_namespace)
  → exec child into same namespace
  → variables defined in child visible in parent
```

### Data Flow — top-level `exit()`

```
Workflow task notebook
  → dbutils.notebook.exit("early")
  → NotebookExit raised
  → NotebookExecutor catches in TOP-LEVEL mode
  → remaining lines in file skipped
  → LocalWorkflowRunner proceeds to next task in execution_order
```

## Configuration

Extend `configure()`:

```python
def configure(base_path, source_dir=None):
    dbutils.configure(base_path, source_dir)
```

- `base_path` — existing; used by `dbutils.fs` (unchanged).
- `source_dir` — root directory for workspace-style notebook paths. `LocalWorkflowRunner.run_workflow()` passes `self.source_dir`.

`DbutilsMock.configure(base_path, source_dir=None)` stores both values. `NotebookExecutor` reads `source_dir` from the dbutils singleton for absolute path resolution.

### Caller context

`NotebookExecutor` tracks the current caller `__file__` via a `contextvars.ContextVar`, set before every `exec_file` / `run_shared` / `run_isolated` call. `NotebookMock.run()` reads this to resolve relative paths when no explicit caller is passed.

## Path Resolution

`NotebookExecutor.resolve_path(path, caller_file, source_dir)` returns an absolute local `.py` path.

### Rules

1. Strip surrounding quotes from `path` if present.
2. If `path` starts with `/` (workspace-style):
   - Strip known prefixes in order: `/Workspace/`, `/Repos/`
   - Join the remainder to `source_dir`
   - Example: `/Workspace/project/notebooks/setup` + `source_dir=./src` → `./src/project/notebooks/setup.py`
3. Otherwise (relative path):
   - Resolve relative to `os.path.dirname(caller_file)`
   - Example: `./helpers/setup` from `/tmp/main.py` → `/tmp/helpers/setup.py`
4. Append `.py` if the resolved path does not end with `.py`.
5. Normalize with `os.path.normpath`.
6. If resolved file does not exist → raise `DbutilsError` with a clear message including the resolved path.

`source_dir` must be configured (non-`None`) for workspace-style paths; otherwise raise `DbutilsError("source_dir not configured — required for workspace paths")`.

Relative paths only require a valid `caller_file` in context.

## API

### `dbutils.notebook.exit(value)`

- Raises `NotebookExit(str(value))`.
- Does not return.
- When called inside a `run()` child: caught by `run_isolated`, value returned to `run()` caller.
- When called at top level: caught by `exec_file` in TOP-LEVEL mode; remaining code in the file is skipped.

Non-string values are converted with `str(value)` before being stored on `NotebookExit`.

### `dbutils.notebook.run(path, timeout_seconds, arguments=None)`

**Signature:** matches Databricks — `run(path, timeout_seconds, arguments={})`

| Parameter | Behavior |
|---|---|
| `path` | Notebook path (relative or workspace-style); resolved per rules above |
| `timeout_seconds` | Accepted; not enforced locally |
| `arguments` | `dict` of `str → str` (non-string values coerced with `str()`); each key set in `os.environ` before child exec |

**Returns:** `str` — exit value from child's `dbutils.notebook.exit()`, or `""` if child completes without calling `exit()`.

**Raises:**
- `DbutilsError` — notebook file not found, `source_dir` missing for workspace path, or other resolution errors
- Propagates unhandled exceptions from child notebook execution (syntax errors, runtime errors, etc.)

### Widget / argument interaction

When `run()` is called with `arguments`:

1. For each `(key, value)`: set `os.environ[key] = str(value)`.
2. Enter an argument-override context (e.g. `contextvars` set of keys) for the duration of child execution.
3. `WidgetsMock.text()` / `dropdown()`: if `name` is in the override set, register the widget but **do not overwrite** `os.environ[name]`.
4. Exit override context after child execution completes (whether or not `exit()` was called).

This matches Databricks behavior where `run()` arguments override widget defaults defined in the child notebook.

## `NotebookExecutor` API (internal)

```python
class NotebookExit(BaseException):
    def __init__(self, value: str): ...

class NotebookExecutor:
    def exec_file(self, file_path, namespace, *, top_level=False) -> str | None: ...
    def run_shared(self, path, namespace) -> None: ...
    def run_isolated(self, path, arguments=None) -> str: ...
    def resolve_path(self, path, caller_file=None) -> str: ...
```

| Method | Description |
|---|---|
| `exec_file` | Load file, set `__file__` in namespace, transform `%run`, exec. If `top_level=True`, catch `NotebookExit` and stop file. Returns exit value if `NotebookExit` caught, else `None`. |
| `run_shared` | Resolve path from `namespace["__file__"]`, exec into same namespace (SHARED mode). `NotebookExit` propagates. |
| `run_isolated` | Resolve path from caller context, apply arguments, exec into fresh namespace (ISOLATED mode). Catch `NotebookExit`, return value. |
| `resolve_path` | Path resolution rules above. |

### `%run` transform (moved from `local_workflow_runner.py`)

`transform_run_commands(source, file_path)` and `parse_run_path(raw_path, file_path)` move to `notebook_executor.py`. Existing tests import from the new location (re-export from `local_workflow_runner` for backward compatibility if needed, or update test imports).

Transformed output remains `__run_notebook__(<path>)` — the lambda injected by `LocalWorkflowRunner` calls `executor.run_shared()`.

## `LocalWorkflowRunner` Changes

1. Create a `NotebookExecutor` instance (or use a module-level singleton).
2. `run_workflow()` calls `configure(self.base_path, source_dir=self.source_dir)`.
3. Replace `_execfile`, `_execute_source`, `_run_notebook`, `_resolve_notebook_path` with `NotebookExecutor` calls.
4. Inject `__run_notebook__` as `lambda path: executor.run_shared(path, execution_globals)`.
5. Top-level task execution: `executor.exec_file(notebook_path, execution_globals, top_level=True)`.

DAG building, task parsing, and `format_dag()` remain unchanged.

## Error Handling

| Condition | Behavior |
|---|---|
| Notebook file not found | `DbutilsError` with resolved path |
| Workspace path without `source_dir` | `DbutilsError("source_dir not configured — required for workspace paths")` |
| Empty `%run` path | `ValueError` (unchanged) |
| Child runtime error | Propagates normally (not caught by executor) |
| `NotebookExit` in SHARED mode | Propagates to parent file (stops parent execution too) |
| `NotebookExit` in TOP-LEVEL mode | Caught; file stops; workflow continues |
| `NotebookExit` in ISOLATED mode | Caught; value returned from `run()` |

`NotebookExit` is a `BaseException` subclass (like `SystemExit`) so it is not accidentally caught by `except Exception` in user notebook code.

## Testing Plan

### New: `tests/test_dbutils_notebook.py`

| Test | Verifies |
|---|---|
| `test_exit_raises_notebook_exit` | `exit("val")` raises `NotebookExit` with value |
| `test_run_returns_exit_value` | Child calls `exit("result")` → `run()` returns `"result"` |
| `test_run_returns_empty_when_no_exit` | Child completes without `exit()` → `run()` returns `""` |
| `test_run_isolated_namespace` | Variable set in child not visible in parent after `run()` |
| `test_run_passes_arguments_via_env` | `arguments={"env": "prod"}` → child sees `os.environ["env"] == "prod"` |
| `test_run_arguments_override_widget_default` | Child `text("env", "dev")` + argument `{"env": "prod"}` → `get("env") == "prod"` |
| `test_run_relative_path` | `./child` resolves from caller `__file__` |
| `test_run_workspace_path` | `/Workspace/project/child` resolves under `source_dir` |
| `test_run_missing_notebook_raises` | Nonexistent path → `DbutilsError` |
| `test_timeout_accepted_not_enforced` | `run(path, 0, {})` does not raise due to timeout |

### Updated: `tests/test_local_workflow_runner.py`

| Test | Verifies |
|---|---|
| Existing `%run` tests | Still pass — shared namespace, nested `%run` |
| `test_top_level_exit_stops_file` | `exit()` in workflow notebook skips remaining lines |
| `test_top_level_exit_continues_workflow` | `exit()` in task 1 does not prevent task 2 execution |

### Test setup

- Autouse fixture: `configure(tmp_path, source_dir=tmp_path)` before each test.
- `dbutils.widgets.removeAll()` in teardown to isolate widget state.
- Tests that call `dbutils.notebook.run()` directly must set caller context (e.g. `executor.set_caller_file(path)` or a test helper) so relative path resolution works outside a workflow.

## Migration / Compatibility

- `transform_run_commands` and `parse_run_path` move to `notebook_executor.py`. Update `tests/test_local_workflow_runner.py` imports (or add re-exports in `local_workflow_runner.py` for backward compatibility).
- `dbutils.notebook` changes from `NoOpModule` (returns `True`) to real implementation (returns `str` from `run()`, raises from `exit()`).
- No changes to `SparkMock` or `dbutils.fs`.

## Out of Scope

- `timeout_seconds` enforcement
- `dbutils.notebook.run()` running on a remote cluster / subprocess isolation
- Notebook path prefixes beyond `/Workspace/` and `/Repos/` (e.g. `/Users/`)
- `dbutils.notebook.run()` parallel execution or job scheduling
- Scala notebook support
- Return value size limits (Databricks 2MB cap)
