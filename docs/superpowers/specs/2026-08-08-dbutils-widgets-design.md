# dbutils.widgets Mock Design

**Date:** 2026-08-08  
**Status:** Approved for implementation  
**Scope:** `dbutils.widgets` module — `text`, `dropdown`, `get`, `remove`, `removeAll`

## Goal

Enhance the local `dbutils` proxy so notebook code using Databricks widgets can run in CI and local workflows. Widget values are stored in process environment variables (keyed by widget name) so they are accessible throughout program execution via both `dbutils.widgets.get()` and `os.environ` / `os.getenv()`.

This complements the existing `dbutils.fs` mock and the `SparkProxy` proxy.

## Requirements Summary

| Decision | Choice |
|---|---|
| Storage | `os.environ` using widget name as the key (e.g. `text("env", "dev")` → `os.environ["env"] = "dev"`) |
| Overwrite on create | `text` / `dropdown` always set the env var to the provided default, overwriting any pre-existing value |
| `get` on unknown widget | Raise `DbutilsError` |
| `remove` on unknown widget | Return `None` (no error) |
| `dropdown` validation | Raise `DbutilsError` if default is not in `choices` |
| `label` parameter | Accepted but ignored (Databricks-compatible signature) |
| Return values | `None` for all widget methods (matches Databricks; unlike no-op stubs that return `True`) |
| Registry | Internal dict tracks registered widgets so `removeAll` only clears widget env vars |

## Architecture

```
src/mock/dbutils/
  widgets.py          # WidgetsMock with text, dropdown, get, remove, removeAll
  dbutils_mock.py     # self.widgets = WidgetsMock()  (replaces NoOpModule stub)
```

### Component Responsibilities

| Component | Responsibility |
|---|---|
| `WidgetsMock` | Create/read/remove widgets; sync values to `os.environ`; maintain registry |
| `_WidgetRegistry` | Track registered widget names (and optional metadata) for `get`, `remove`, `removeAll` |
| `DbutilsMock` | Expose `.widgets` as a real `WidgetsMock` instance |

Widgets are independent of `configure(base_path)` — no path configuration required.

### Data Flow

```
dbutils.widgets.text("env", "dev")
  → os.environ["env"] = "dev"
  → registry.register("env")

dbutils.widgets.get("env")
  → registry.exists("env")?  →  return os.environ["env"]

os.getenv("env")               # accessible anywhere in the process
```

## API

### `text(name, default, label=None)`

- Sets `os.environ[name] = str(default)` (overwrites existing value).
- Registers the widget in the internal registry.
- `label` is ignored.
- Returns `None`.

### `dropdown(name, default, choices, label=None)`

- Validates `default in choices`; raises `DbutilsError` if not.
- Sets `os.environ[name] = str(default)` (overwrites existing value).
- Registers the widget in the internal registry.
- `label` is ignored.
- Returns `None`.

### `get(name)`

- If widget is not registered → raise `DbutilsError("Widget '{name}' does not exist")`.
- Returns `os.environ[name]`.

### `remove(name)`

- If widget is not registered → return `None`.
- Deletes `os.environ[name]` (using `os.environ.pop(name, None)`).
- Removes entry from registry.
- Returns `None`.

### `removeAll()`

- Iterates all registered widget names.
- Deletes each from `os.environ`.
- Clears the registry.
- Returns `None`.
- Does **not** delete env vars that were never registered via `text` / `dropdown`.

## Error Handling

All errors use existing `DbutilsError`:

| Condition | Error |
|---|---|
| `get("unknown")` | `Widget 'unknown' does not exist` |
| `dropdown(..., default="x", choices=["a","b"])` | `Default value 'x' is not in choices ['a', 'b']` |

`remove("unknown")` does not raise — returns `None`.

## Testing Plan

New test file: `tests/test_dbutils_widgets.py`

| Test | Verifies |
|---|---|
| `test_text_sets_env_var` | `text()` sets `os.environ`; `get()` returns value |
| `test_text_overwrites_existing_env` | Pre-set env var is overwritten by `text()` default |
| `test_dropdown_valid_default` | Valid dropdown sets env var |
| `test_dropdown_invalid_default_raises` | Default not in choices → `DbutilsError` |
| `test_get_unregistered_raises` | `get()` on unknown widget → `DbutilsError` |
| `test_remove_deletes_env_and_registry` | `remove()` clears env var; subsequent `get()` raises |
| `test_remove_unregistered_returns_none` | `remove()` on unknown widget → `None`, no error |
| `test_remove_all_clears_only_widgets` | `removeAll()` removes widget env vars; unrelated env vars remain |
| `test_env_var_accessible_outside_dbutils` | `os.getenv(name)` works after `text()` |

### Test Isolation

Extend the existing autouse `reset_dbutils` fixture in widget tests (or globally) to call `dbutils.widgets.removeAll()` in setup/teardown so widget env vars do not leak between tests.

## Out of Scope

- `combobox`, `multiselect`
- Widget `label` display/rendering
- `dbutils.widgets.getArgument` (notebook-level API)
- Cross-process or persistent widget state beyond the current process `os.environ`
